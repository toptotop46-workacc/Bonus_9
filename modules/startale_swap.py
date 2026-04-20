"""Swap ETH → USDSC via Uniswap V4 UniversalRouter on Soneium."""

import random
import time
from eth_abi import encode
from web3 import Web3
from modules import logger, web3_utils, db
from modules.portal_api import require_account_status

# ── Contract addresses ───────────────────────────────────────────────────────
UNIVERSAL_ROUTER  = Web3.to_checksum_address("0x4cded7Edf52c8AA5259A54Ec6a3CE7C6D2a455Df")
USDSC_TOKEN       = Web3.to_checksum_address("0x3f99231dD03a9F0E7e3421c92B7b90fbe012985a")
FEE_COLLECTOR     = Web3.to_checksum_address("0x858607dcEf869C1a25a62c52c3F410e56A86a764")
V4_POOL_MANAGER   = Web3.to_checksum_address("0x360E68fACcCA8cA495c1B759Fd9EEe466db9FB32")
NATIVE_CURRENCY   = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

# ── Pool parameters (ETH/USDSC, без hooks на пуле — иначе poolId не совпадает с ликвидностью) ─
POOL_FEE          = 3000
TICK_SPACING      = 60
POOL_HOOKS        = NATIVE_CURRENCY   # IHooks = address(0)

# PoolManager: mapping(PoolId => Pool.State) находится в storage slot 6 (Uniswap v4 StateLibrary)
POOLS_SLOT = (6).to_bytes(32, "big")

# USD-эквивалент свапа: в микродолларах (1e-6 USD), т.е. 1_100_000..2_000_000 ≈ $1.10..$2.00
SWAP_USD_MICRO_MIN = 1_100_000
SWAP_USD_MICRO_MAX = 2_000_000

# Повторы чтения sqrtPrice из PoolManager (только блокчейн, без fallback-курсов)
SQRT_PRICE_RETRIES = 8
SQRT_PRICE_RETRY_DELAY_SEC = 1.5

# ── Router commands ──────────────────────────────────────────────────────────
CMD_V4_SWAP  = 0x10
CMD_TRANSFER = 0x05

# ── Special amounts / recipients (v4-periphery ActionConstants) ───────────────
# Должно совпадать с ActionConstants.CONTRACT_BALANCE (только старший бит; не |2)
CONTRACT_BALANCE = 1 << 255  # 0x8000…0000 — иначе pay() не узнаёт флаг и шлёт «сумму» = целый uint256
# recipient = address(2) → UniversalRouter (address(this)) для TAKE_PORTION (доля комиссии на роутер)
ADDRESS_THIS_FLAG = "0x0000000000000000000000000000000000000002"
# Доля выхода USDSC, которая идёт на роутер и затем TRANSFER → FeeCollector (~0,85 %, как в эталонных tx)
FEE_PORTION_BIPS = 85

# ── Function selector ────────────────────────────────────────────────────────
# execute(bytes commands, bytes[] inputs, uint256 deadline)
EXECUTE_SELECTOR = bytes.fromhex("3593564c")

# ── V4 Swap action codes (Uniswap v4-periphery libraries/Actions.sol) ────────
ACTION_SWAP_EXACT_IN_SINGLE = 0x06
ACTION_SETTLE_ALL           = 0x0c
ACTION_TAKE_PORTION         = 0x10   # доля credit → recipient (см. BipsLibrary)
ACTION_TAKE_ALL             = 0x0f   # остаток credit → msgSender (пользователь)


def _encode_uint256(v: int) -> bytes:
    return v.to_bytes(32, "big")


def _encode_bytes32(b: bytes) -> bytes:
    return b.ljust(32, b"\x00")[:32]


def _encode_address(addr: str) -> bytes:
    return bytes.fromhex(addr.removeprefix("0x").lower().zfill(64))


def _encode_bool(v: bool) -> bytes:
    return _encode_uint256(1 if v else 0)


def _encode_bytes_field(data: bytes) -> bytes:
    """ABI-encode a 'bytes' field: length + padded data."""
    length_enc = _encode_uint256(len(data))
    pad = (32 - len(data) % 32) % 32
    return length_enc + data + b"\x00" * pad


def _abi_encode_bytes_array(items: list[bytes]) -> bytes:
    """ABI-encode bytes[] (dynamic array of bytes)."""
    n = len(items)
    # head: count + n offsets
    # offset starts after count + n*32 bytes
    offsets = []
    cur = 32 * n  # relative to start of array data (after count)
    for item in items:
        offsets.append(cur)
        cur += 32 + len(item) + (32 - len(item) % 32) % 32

    result = _encode_uint256(n)
    for o in offsets:
        result += _encode_uint256(o)
    for item in items:
        result += _encode_bytes_field(item)
    return result


def _build_v4_swap_input(
    amount_in: int,
    amount_out_min: int,
    recipient: str,
) -> bytes:
    """
    Build the V4_SWAP (0x10) command input using V4Router actions.
    Encodes: SWAP_EXACT_IN_SINGLE + SETTLE_ALL + TAKE_PORTION(→router) + TAKE_ALL(→user).
    Так же, как в реальных tx Startale: небольшая доля на роутер → TRANSFER в FeeCollector,
    основной вывод — на кошелёк пользователя.
    """
    # ── Action 1: SWAP_EXACT_IN_SINGLE ──────────────────────────────────────
    # PoolKey struct (5 fields × 32 bytes each):
    # currency0 (address), currency1 (address), fee (uint24 as uint256),
    # tickSpacing (int24 as int256), hooks (address)
    pool_key = (
        _encode_address("0x0000000000000000000000000000000000000000")  # currency0 = ETH
        + _encode_address(USDSC_TOKEN)                                   # currency1 = USDSC
        + _encode_uint256(POOL_FEE)                                      # fee
        + _encode_uint256(TICK_SPACING)                                  # tickSpacing
        + _encode_address(POOL_HOOKS)                                    # hooks
    )

    # ExactInputSingleParams:
    # poolKey, zeroForOne, amountIn, amountOutMinimum, sqrtPriceLimitX96, hookData
    swap_params = (
        pool_key
        + _encode_bool(True)            # zeroForOne (ETH→USDSC)
        + _encode_uint256(amount_in)
        + _encode_uint256(amount_out_min)
        + _encode_uint256(0)            # sqrtPriceLimitX96
    )
    # hookData offset + length=0
    swap_params += _encode_uint256(5 * 32 + len(pool_key))  # offset to hookData... simplified
    # Actually encode inline with offset:
    # Rebuild with proper ABI for the struct
    # Use a simpler flat encoding matching the contract:
    swap_params = (
        pool_key
        + _encode_bool(True)
        + _encode_uint256(amount_in)
        + _encode_uint256(amount_out_min)
        + _encode_uint256(0)            # sqrtPriceLimitX96
        + _encode_uint256(32 * 7)       # hookData offset (7 fields before it, rel. to struct start)
    )
    # hookData = empty bytes
    swap_params += _encode_uint256(0)   # hookData length = 0

    action1 = bytes([ACTION_SWAP_EXACT_IN_SINGLE]) + swap_params

    # ── Action 2: SETTLE_ALL (pay currency0 = ETH from router's msg.value) ──
    # params: currency (address), maxAmount (uint256)
    action2 = bytes([ACTION_SETTLE_ALL]) + (
        _encode_address("0x0000000000000000000000000000000000000000")
        + _encode_uint256(amount_in)
    )

    # ── Action 3: TAKE_PORTION — часть USDSC credit на роутер (потом TRANSFER → FeeCollector)
    # params: currency, recipient, bips (десятитысячные доли)
    action3 = bytes([ACTION_TAKE_PORTION]) + (
        _encode_address(USDSC_TOKEN)
        + _encode_address(ADDRESS_THIS_FLAG)
        + _encode_uint256(FEE_PORTION_BIPS)
    )

    # ── Action 4: TAKE_ALL — остаток USDSC пользователю (msgSender = locker = EOA)
    # minAmount: минимум для *оставшегося* credit после TAKE_PORTION
    min_user_out = amount_out_min * (10_000 - FEE_PORTION_BIPS) // 10_000
    if min_user_out == 0 and amount_out_min > 0:
        min_user_out = 1
    action4 = bytes([ACTION_TAKE_ALL]) + (
        _encode_address(USDSC_TOKEN)
        + _encode_uint256(min_user_out)
    )

    # Pack actions: bytes actions + bytes[] params
    actions_bytes = bytes(
        [ACTION_SWAP_EXACT_IN_SINGLE, ACTION_SETTLE_ALL, ACTION_TAKE_PORTION, ACTION_TAKE_ALL]
    )
    params_list = [swap_params, action2[1:], action3[1:], action4[1:]]

    # V4_SWAP input = abi.encode(actions, params)
    # actions = bytes (dynamic), params = bytes[] (dynamic)
    # Two dynamic fields → two offsets at head
    actions_enc = _encode_bytes_field(actions_bytes)
    params_enc  = _abi_encode_bytes_array(params_list)

    offset_actions = 64        # after 2 offsets
    offset_params  = 64 + 32 + len(actions_bytes) + (32 - len(actions_bytes) % 32) % 32

    v4_swap_input = (
        _encode_uint256(offset_actions)
        + _encode_uint256(offset_params)
        + _encode_bytes_field(actions_bytes)
        + params_enc
    )
    return v4_swap_input


def _build_transfer_input(token: str, recipient: str, amount: int) -> bytes:
    """TRANSFER command input: abi.encode(address token, address recipient, uint256 amount)"""
    return (
        _encode_address(token)
        + _encode_address(recipient)
        + _encode_uint256(amount)
    )


def _pool_key_encoded() -> bytes:
    """ABI-encode PoolKey как в Uniswap v4 (для keccak256 → PoolId)."""
    return encode(
        ["address", "address", "uint24", "int24", "address"],
        [NATIVE_CURRENCY, USDSC_TOKEN, POOL_FEE, TICK_SPACING, POOL_HOOKS],
    )


def pool_id() -> bytes:
    return Web3.keccak(_pool_key_encoded())


def _pool_state_slot(pool_id_bytes: bytes) -> bytes:
    """StateLibrary._getPoolStateSlot: keccak256(abi.encodePacked(poolId, POOLS_SLOT))."""
    return Web3.keccak(pool_id_bytes + POOLS_SLOT)


def _extsload_slot0_raw(w3: Web3) -> bytes:
    """Читает первый слот Pool.State (slot0) через PoolManager.extsload."""
    slot = _pool_state_slot(pool_id())
    sel = Web3.keccak(text="extsload(bytes32)")[:4]
    data = sel + slot
    return web3_utils.eth_call(w3, V4_POOL_MANAGER, data)


def _decode_slot0_sqrt_price_x96(word: bytes) -> int:
    """Распаковка slot0 как в StateLibrary.getSlot0 (нужны младшие 160 бит)."""
    val = int.from_bytes(word, "big")
    return val & ((1 << 160) - 1)


def get_sqrt_price_x96(w3: Web3) -> int:
    """
    Текущий sqrtPriceX96 из ликвидного пула ETH/USDSC (только on-chain, без API).
    Повторяет запрос при сбоях RPC/нулевой цене.
    """
    last_err: Exception | None = None
    for attempt in range(SQRT_PRICE_RETRIES):
        try:
            raw = _extsload_slot0_raw(w3)
            if len(raw) < 32:
                raise RuntimeError(f"extsload вернул {len(raw)} байт")
            sqrt_p = _decode_slot0_sqrt_price_x96(raw[-32:])
            if sqrt_p == 0:
                raise RuntimeError("sqrtPriceX96 = 0 (пул не инициализирован)")
            return sqrt_p
        except Exception as e:
            last_err = e
            if attempt + 1 < SQRT_PRICE_RETRIES:
                time.sleep(SQRT_PRICE_RETRY_DELAY_SEC * (1 + attempt * 0.5))
    raise RuntimeError(
        f"Не удалось получить sqrtPriceX96 из PoolManager после {SQRT_PRICE_RETRIES} попыток: {last_err}"
    ) from last_err


def eth_wei_for_usd_micro(sqrt_price_x96: int, usd_micro: int) -> int:
    """
    Сколько wei ETH нужно для свапа на usd_micro «микродолларов» (1e-6 USD),
    по спот-цене пула (currency0 = ETH, currency1 = USDSC).
    eth_wei = usd_micro * 2^192 / sqrtPriceX96^2
    """
    d = sqrt_price_x96 * sqrt_price_x96
    if d == 0:
        raise ValueError("sqrtPriceX96 is 0")
    return (usd_micro * (2**192)) // d


def expected_usdsc_out_raw(sqrt_price_x96: int, eth_wei: int) -> int:
    """Ожидаемое количество USDSC (минимальные единицы, 6 decimals) при спот-цене."""
    return (eth_wei * sqrt_price_x96 * sqrt_price_x96) // (2**192)


def _build_execute_calldata(amount_in: int, amount_out_min: int,
                             sender: str, deadline: int) -> bytes:
    """Build full calldata for execute(bytes, bytes[], uint256)."""
    commands = bytes([CMD_V4_SWAP, CMD_TRANSFER])

    v4_swap_input  = _build_v4_swap_input(amount_in, amount_out_min, sender)
    transfer_input = _build_transfer_input(USDSC_TOKEN, FEE_COLLECTOR, CONTRACT_BALANCE)

    # abi.encode(bytes commands, bytes[] inputs, uint256 deadline)
    # 3 params: bytes(dynamic), bytes[](dynamic), uint256(static)
    # offsets are relative to start of param block (after selector)
    offset_commands = 3 * 32        # after 3 head slots
    commands_enc    = _encode_bytes_field(commands)
    offset_inputs   = offset_commands + 32 + len(commands_enc) - 32  # wait, simpler:

    # Proper ABI encoding:
    cmd_enc   = _encode_bytes_field(commands)
    inputs_enc = _abi_encode_bytes_array([v4_swap_input, transfer_input])

    # Head:
    # slot0: offset of commands  = 3*32 = 96
    # slot1: offset of inputs    = 96 + 32 + len(cmd_enc_padded)
    # slot2: deadline

    cmd_padded_len = 32 + len(commands) + (32 - len(commands) % 32) % 32
    offset_cmd    = 96
    offset_inp    = 96 + cmd_padded_len

    head = (
        _encode_uint256(offset_cmd)
        + _encode_uint256(offset_inp)
        + _encode_uint256(deadline)
    )
    body = cmd_enc + inputs_enc

    return EXECUTE_SELECTOR + head + body


def swap_eth_to_usdsc(
    private_key: str,
    w3: Web3,
    slippage_bps: int = 200,   # 2% slippage
    gas_limit_multiplier: float = 1.2,
    proxy: str | None = None,
    proxy_pool: list[str | None] | None = None,
) -> str | None:
    """
    ETH → USDSC swap. Перед отправкой tx — обязательная проверка портала (с повторами).
    Если swap уже засчитан — возвращает None (БД синхронизирована).
    Если ETH не хватает на свап + газ или оценка газа не удалась — возвращает None.
    Если портал недоступен — RuntimeError, tx не шлётся.
    """
    from web3 import Web3 as _W3
    account = _W3.to_checksum_address(
        __import__("eth_account").Account.from_key(private_key).address
    )

    st = require_account_status(account, proxy, proxy_pool=proxy_pool)
    if st.get("swap_done"):
        info = db.get_account_info(account) or {}
        db.upsert_account(
            account,
            swap_done=True,
            swap_tx=info.get("swap_tx") or "portal",
        )
        logger.info(f"[Startale] {account} swap уже на портале — пропуск")
        return None

    sqrt_p = get_sqrt_price_x96(w3)
    usd_micro = random.randint(SWAP_USD_MICRO_MIN, SWAP_USD_MICRO_MAX)
    eth_amount_wei = eth_wei_for_usd_micro(sqrt_p, usd_micro)
    expected_out = expected_usdsc_out_raw(sqrt_p, eth_amount_wei)
    amount_out_min = expected_out * (10000 - slippage_bps) // 10000

    # deadline = now + 20 min
    deadline = int(time.time()) + 1200

    calldata = _build_execute_calldata(
        amount_in=eth_amount_wei,
        amount_out_min=amount_out_min,
        sender=account,
        deadline=deadline,
    )

    usd_f = usd_micro / 1_000_000
    logger.info(
        f"[Startale] swap ~${usd_f:.2f} ETH {eth_amount_wei/1e18:.4f} → USDSC min={amount_out_min}"
    )

    try:
        tx = web3_utils.prepare_eip1559_tx(
            w3,
            private_key,
            UNIVERSAL_ROUTER,
            data=calldata,
            value=eth_amount_wei,
            gas_limit_multiplier=gas_limit_multiplier,
        )
    except Exception as e:
        logger.warning(f"[Startale] оценка газа: {e} — пропуск свапа")
        return None

    needed_wei = web3_utils.tx_max_cost_wei(tx)
    balance_wei = w3.eth.get_balance(account)
    if balance_wei < needed_wei:
        logger.warning(
            f"[Startale] недостаточно ETH: баланс {balance_wei / 1e18:.6f}, "
            f"нужно ≥{needed_wei / 1e18:.6f} (свап + газ) — пропуск"
        )
        return None

    tx_hash = web3_utils.send_prepared_tx(w3, private_key, tx)
    db.mark_swap_done(account, tx_hash)
    th = str(tx_hash)
    logger.success(f"[Startale] OK swap tx {th[:10]}…{th[-6:]}")
    return str(tx_hash)
