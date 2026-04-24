"""
ERC-4337 UserOperation v0.7 infrastructure for Startale smart accounts.
Used by: scripts/tests (ELHEXA UserOp). ELHEXA daily check-in в браузере — modules/elhexa.py + startale_gm.py.

Bundler/paymaster по умолчанию — те же, что у app.startale.com, не прямой scs URL.

Адреса factory/bootstrap совпадают с @startale-scs/aa-sdk (ACCOUNT_FACTORY_ADDRESS,
BOOTSTRAP_ADDRESS). Ранее ошибочно использовался только Bootstrap — из-за этого
eth_call к «createAccount(address,uint256)» откатывался и срабатывал пустой fallback.
"""

import os
import re
import subprocess
import time
import json
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from eth_abi import encode
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3
from modules import logger, db

# ── Addresses ────────────────────────────────────────────────────────────────
ENTRY_POINT       = "0x0000000071727De22E5E9d8BAf0edAc6f37da032"
# StartaleAccountFactory — createAccount(bytes,bytes32) / computeAccountAddress(bytes,bytes32)
ACCOUNT_FACTORY   = Web3.to_checksum_address("0x0000003B3E7b530b4f981aE80d9350392Defef90")
# Bootstrap — initWithDefaultValidatorAndOtherModules внутри initData (как в aa-sdk)
BOOTSTRAP_ADDRESS = Web3.to_checksum_address("0x000000552A5fAe3Db7a8F3917C435448F49BA6a9")
SA_IMPL           = "0x000000b8f5f723A680d3D7EE624Fe0bC84a6E05A"
PAYMASTER         = "0x00000095901E8AB695Dc24FA52B0Cce15E9896Ad"

# Как в браузере (app.startale.com.har): JSON-RPC через /api/.../soneium, не прямой scs —
# иначе часто -32603 «internal error: rpc provider error» на eth_getUserOperationReceipt.
_DEFAULT_BUNDLER_URL = "https://app.startale.com/api/bundler/soneium"
_DEFAULT_PAYMASTER_URL = "https://app.startale.com/api/paymaster/soneium"

_STARTALE_APP_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "*/*",
    "Origin": "https://app.startale.com",
    "Referer": "https://app.startale.com/miniapps",
}

CHAIN_ID         = 1868
CHAIN_ID_HEX     = hex(CHAIN_ID)


def bundler_url() -> str:
    return os.environ.get("BONUS9_BUNDLER_URL", _DEFAULT_BUNDLER_URL).strip()


def paymaster_url() -> str:
    return os.environ.get("BONUS9_PAYMASTER_URL", _DEFAULT_PAYMASTER_URL).strip()


def _bundler_http_proxies(wallet_proxy: str | None) -> dict | None:
    """Прокси кошелька к bundler/paymaster не подмешиваем (см. BONUS9_USE_PROXY_FOR_BUNDLER)."""
    if not wallet_proxy:
        return None
    v = os.environ.get("BONUS9_USE_PROXY_FOR_BUNDLER", "").strip().lower()
    if v in ("1", "true", "yes", "on"):
        return {"http": wallet_proxy, "https": wallet_proxy}
    return None


def bundler_receipt_fallback_urls() -> list[str]:
    """
    Дополнительные URL для eth_getUserOperationReceipt (тот же пул, иной шлюз).
    BONUS9_BUNDLER_RECEIPT_FALLBACK — через запятую; пусто = прямой scs bundler.
    """
    primary = bundler_url()
    raw = os.environ.get("BONUS9_BUNDLER_RECEIPT_FALLBACK", "").strip()
    out: list[str] = [primary]
    if raw:
        for u in raw.split(","):
            u = u.strip()
            if u and u not in out:
                out.append(u)
    else:
        fb = (
            "https://soneium.bundler.scs.startale.com/"
            "?apikey=n5gc7Dspt1FDwgZqqWTtASzUcLJKzxwW"
        )
        if fb not in out:
            out.append(fb)
    return out


def bundler_receipt_poll_urls() -> list[str]:
    """
    Порядок опроса receipt / ByHash.
    По умолчанию сначала прямой scs bundler (быстрее и реже Read timeout),
    затем app API — иначе каждый цикл начинается с зависания на app.startale.com.
    BONUS9_RECEIPT_POLL_PRIMARY_FIRST=1 — сначала как в bundler_url().
    """
    urls = bundler_receipt_fallback_urls()
    primary_first = os.environ.get("BONUS9_RECEIPT_POLL_PRIMARY_FIRST", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if not primary_first and len(urls) > 1:
        return list(reversed(urls))
    return urls


def _receipt_wait_http_timeout() -> tuple[int, int]:
    """(connect, read) для опроса receipt/ByHash — отдельно от send/estimate."""
    read = int(os.environ.get("BONUS9_RECEIPT_READ_TIMEOUT", "75"))
    conn = int(os.environ.get("BONUS9_RECEIPT_CONNECT_TIMEOUT", "15"))
    return (max(5, conn), max(20, read))


def _rpc_post_parallel(
    urls: list[str],
    payload: dict,
    proxy: str | None,
    timeout: int | tuple[int, int],
) -> list[tuple[str, dict | None, str | None]]:
    """
    Параллельный POST одного JSON-RPC ко всем URL (scs + app).
    Иначе при последовательных вызовах app даёт Read timeout 75s и съедает весь лимит ожидания.
    Возвращает список (url, json | None, err_msg | None).
    """

    def one(u: str) -> tuple[str, dict | None, str | None]:
        try:
            r = _post_json_rpc(u, payload, proxy, timeout=timeout)
            return (u, r.json(), None)
        except Exception as e:
            return (u, None, str(e))

    if not urls:
        return []
    if len(urls) == 1:
        u, j, e = one(urls[0])
        return [(u, j, e)]
    with ThreadPoolExecutor(max_workers=len(urls)) as pool:
        return list(pool.map(one, urls))


def _post_json_rpc(
    url: str,
    payload: dict,
    proxy: str | None,
    timeout: int | tuple[int, int],
) -> requests.Response:
    """Прямой scs без Origin; app.startale.com — с заголовками как в браузере."""
    if "app.startale.com" in url:
        headers = _STARTALE_APP_HEADERS
    else:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
    return requests.post(
        url,
        json=payload,
        headers=headers,
        proxies=_bundler_http_proxies(proxy),
        timeout=timeout,
    )


def _post_bundler(payload: dict, proxy: str | None, timeout: int) -> requests.Response:
    return _post_json_rpc(bundler_url(), payload, proxy, timeout)


def _post_paymaster(payload: dict, proxy: str | None, timeout: int) -> requests.Response:
    return _post_json_rpc(paymaster_url(), payload, proxy, timeout)


class BundlerRpcError(RuntimeError):
    """Ответ bundler с полем error (JSON-RPC)."""

    def __init__(self, err: dict):
        self.err = err
        super().__init__(str(err))


def _bump_fee_fields_after_replacement_error(user_op: dict, data: dict | None) -> bool:
    """
    Поднять maxFeePerGas / maxPriorityFeePerGas выше текущей pending UserOp
    (ответ replacement underpriced). Данные из error.data; иначе +50% к текущим полям.
    """
    if data:
        cur_prio = data.get("currentMaxPriorityFee")
        cur_max = data.get("currentMaxFee")
        if cur_prio and cur_max:
            try:
                p = int(cur_prio, 16) if isinstance(cur_prio, str) else int(cur_prio)
                m = int(cur_max, 16) if isinstance(cur_max, str) else int(cur_max)
            except (TypeError, ValueError):
                pass
            else:
                # replacement underpriced: нужен заметный шаг, иначе bundler снова отклоняет
                user_op["maxPriorityFeePerGas"] = hex(int(p * 3 // 2) + 3_000_000)
                user_op["maxFeePerGas"] = hex(int(m * 3 // 2) + 3_000_000)
                return True
    try:
        p = int(user_op["maxPriorityFeePerGas"], 16)
        m = int(user_op["maxFeePerGas"], 16)
    except (TypeError, ValueError, KeyError):
        return False
    user_op["maxPriorityFeePerGas"] = hex(int(p * 2) + 5_000_000)
    user_op["maxFeePerGas"] = hex(int(m * 2) + 5_000_000)
    return True


def _apply_fee_hints_from_paymaster(user_op: dict, blob: dict | None) -> None:
    """pm_getPaymasterStubData / pm_getPaymasterData часто возвращают maxFee* как в HAR."""
    if not blob:
        return
    for key in ("maxFeePerGas", "maxPriorityFeePerGas"):
        v = blob.get(key)
        if not v:
            continue
        if isinstance(v, str) and v.startswith("0x"):
            user_op[key] = v
        else:
            try:
                user_op[key] = hex(int(v))
            except (TypeError, ValueError):
                pass


def _eip1559_user_op_fees(rpc_url: str) -> tuple[int, int]:
    """
    Потолки maxFeePerGas / maxPriorityFeePerGas для UserOp (как у bundled EIP-1559 tx).
    Только eth_gasPrice занижает значения на L2 — UserOp тогда долго «висит» у bundler.
    """
    try:
        r = requests.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": ["latest", False],
                "id": 1,
            },
            timeout=15,
        )
        block = r.json().get("result") or {}
        bf = block.get("baseFeePerGas")
        if bf is None:
            raise ValueError("no baseFeePerGas")
        base_fee = int(bf, 16) if isinstance(bf, str) else int(bf)
        r2 = requests.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "method": "eth_maxPriorityFeePerGas",
                "params": [],
                "id": 2,
            },
            timeout=15,
        )
        j2 = r2.json()
        if j2.get("error") or not j2.get("result"):
            prio = max(base_fee // 16, 1_000_000)
        else:
            res = j2["result"]
            prio = int(res, 16) if isinstance(res, str) else int(res)
        prio = max(prio, 1_000_000)
        max_fee = base_fee * 2 + prio
        prio = int(prio * 12 // 10) + 500_000
        max_fee = int(max_fee * 12 // 10) + 1_000_000
        return max_fee, prio
    except Exception as e:
        logger.warning(f"[ERC4337] EIP-1559 fees → fallback eth_gasPrice ({e})")
        payload = {"jsonrpc": "2.0", "method": "eth_gasPrice", "params": [], "id": 1}
        gas_price = int(
            requests.post(rpc_url, json=payload, timeout=10).json().get("result", "0x1"),
            16,
        )
        prio = max(gas_price, 2_000_000)
        max_fee = max(gas_price * 2, prio * 2)
        return max_fee, prio


def _ensure_fee_floor(user_op: dict, floor_max: int, floor_prio: int) -> None:
    """Не даём paymaster-ответу занизить maxFee* ниже расчёта по сети."""
    try:
        mf = int(user_op["maxFeePerGas"], 16)
        mp = int(user_op["maxPriorityFeePerGas"], 16)
    except (TypeError, ValueError, KeyError):
        return
    user_op["maxFeePerGas"] = hex(max(mf, floor_max))
    user_op["maxPriorityFeePerGas"] = hex(max(mp, floor_prio))


def _byhash_shows_mined(result: dict | None) -> tuple[bool, str | None]:
    """По ответу eth_getUserOperationByHash — попала ли UserOp в блок и hash bundled tx."""
    if not result:
        return False, None
    bn = result.get("blockNumber")
    th = result.get("transactionHash")
    if bn is None or th is None:
        return False, None
    try:
        if isinstance(bn, str):
            if int(bn, 16) <= 0:
                return False, None
        elif isinstance(bn, int) and bn <= 0:
            return False, None
    except (TypeError, ValueError):
        return False, None
    if isinstance(th, str) and (not th or th in ("0x", "0x0")):
        return False, None
    return True, th


def _receipt_dict_from_byhash(tx_hash: str, user_op_hash: str) -> dict:
    """Формат как у eth_getUserOperationReceipt — для execute_user_op."""
    return {
        "success": True,
        "userOpHash": user_op_hash,
        "receipt": {"transactionHash": tx_hash},
    }


def _try_get_included_user_op(
    user_op_hash: str,
    proxy: str | None,
    urls: list[str],
    rpc_timeout: int | tuple[int, int],
) -> dict | None:
    """eth_getUserOperationByHash — иногда receipt=null, пока ByHash уже показывает блок."""
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getUserOperationByHash",
        "params": [user_op_hash, ENTRY_POINT],
        "id": 1,
    }
    for url, data, err in _rpc_post_parallel(urls, payload, proxy, rpc_timeout):
        if err:
            msg = err[:120]
            if "timed out" in msg.lower() or "timeout" in msg.lower():
                logger.debug(f"[ERC4337] getUserOperationByHash timeout ({url[:40]}…)")
            else:
                logger.debug(f"[ERC4337] getUserOperationByHash ({url[:40]}…): {msg}")
            continue
        if not data or data.get("error"):
            continue
        res = data.get("result")
        mined, txh = _byhash_shows_mined(res if isinstance(res, dict) else None)
        if mined and txh:
            if len(urls) > 1:
                logger.debug(f"[ERC4337] включение в блок по ByHash ({url[:52]}…)")
            return _receipt_dict_from_byhash(txh, user_op_hash)
    return None


# ── ABI encoding helpers ─────────────────────────────────────────────────────

def _u256(v: int) -> bytes:
    return v.to_bytes(32, "big")

def _addr(a: str) -> bytes:
    return bytes.fromhex(a.removeprefix("0x").lower().zfill(64))


def _addr20(a: str) -> bytes:
    """Адрес как 20 байт (для abi.encodePacked в ExecutionLib)."""
    return bytes.fromhex(Web3.to_checksum_address(a).removeprefix("0x").lower())

def _bytes_enc(b: bytes) -> bytes:
    pad = (32 - len(b) % 32) % 32
    return _u256(len(b)) + b + b"\x00" * pad

# ── Startale initData (как getInitData + getFactoryData в @startale-scs/aa-sdk) ─

_INIT_INNER_TYPES = (
    "bytes",
    "(address,bytes)[]",
    "(address,bytes)[]",
    "(address,bytes)",
    "(address,bytes)[]",
    "(uint256,address,bytes)[]",
)
_INIT_INNER_SIG = (
    "initWithDefaultValidatorAndOtherModules("
    "bytes,(address,bytes)[],(address,bytes)[],(address,bytes),(address,bytes)[],"
    "(uint256,address,bytes)[])"
)


def build_startale_init_bytes(eoa_address: str) -> bytes:
    """
    initData для computeAccountAddress / createAccount: abi.encode(bootstrap, calldata),
    где calldata — initWithDefaultValidatorAndOtherModules(...), default validator
    data = 20 байт адреса EOA (toDefaultModule в aa-sdk).
    """
    eoa = Web3.to_checksum_address(eoa_address)
    dv_init = bytes.fromhex(eoa[2:])
    hook = ("0x0000000000000000000000000000000000000000", bytes(32))
    inner_body = encode(
        _INIT_INNER_TYPES,
        (dv_init, [], [], hook, [], []),
    )
    inner_sel = Web3.keccak(text=_INIT_INNER_SIG)[:4]
    inner_call = inner_sel + inner_body
    return encode(["address", "bytes"], [BOOTSTRAP_ADDRESS, inner_call])


def get_smart_account_address(
    eoa_address: str,
    account_index: int = 0,
    rpc_url: str = "https://soneium-rpc.publicnode.com",
) -> str:
    """
    Counterfactual адрес смарт-аккаунта: factory.computeAccountAddress(initData, salt).
    salt = uint256(account_index), как pad(toHex(index), 32) в aa-sdk.
    """
    init_bytes = build_startale_init_bytes(eoa_address)
    salt = _u256(account_index)
    sel = Web3.keccak(text="computeAccountAddress(bytes,bytes32)")[:4]
    call_data = sel + encode(["bytes", "bytes32"], [init_bytes, salt])
    payload = {
        "jsonrpc": "2.0",
        "method":  "eth_call",
        "params":  [
            {"to": ACCOUNT_FACTORY, "data": "0x" + call_data.hex()},
            "latest",
        ],
        "id": 1,
    }
    try:
        resp = requests.post(rpc_url, json=payload, timeout=15)
        data = resp.json()
        if data.get("error"):
            raise RuntimeError(data["error"])
        result = data.get("result", "0x")
        if isinstance(result, str) and len(result) >= 66:
            return Web3.to_checksum_address("0x" + result[-40:])
    except Exception as e:
        logger.warning(f"[ERC4337] computeAccountAddress: {e}")
    return "0x0000000000000000000000000000000000000000"


def find_startale_account_index_for_address(
    eoa_address: str,
    target_sa: str,
    rpc_url: str = "https://soneium-rpc.publicnode.com",
    max_index: int | None = None,
) -> int | None:
    """
    Подбирает salt/index, при котором factory.computeAccountAddress(init, salt) == target_sa.
    Нужен, когда Startale API отдаёт SA, а BONUS9_STARTALE_ACCOUNT_INDEX неверен: иначе EOA из keys.txt
    не совпадает с владельцем контракта и bundler даёт -32507.
    """
    if max_index is None:
        max_index = max(0, int(os.environ.get("BONUS9_STARTALE_ACCOUNT_INDEX_SEARCH_MAX", "128")))
    want = Web3.to_checksum_address(target_sa).lower()
    for i in range(max_index + 1):
        got = get_smart_account_address(eoa_address, i, rpc_url)
        if got and got.lower() == want:
            return i
    return None


def get_factory_init_code(eoa_address: str, account_index: int = 0) -> str:
    """initCode = ACCOUNT_FACTORY ++ createAccount(initData, salt)."""
    init_bytes = build_startale_init_bytes(eoa_address)
    salt = _u256(account_index)
    sel = Web3.keccak(text="createAccount(bytes,bytes32)")[:4]
    body = encode(["bytes", "bytes32"], [init_bytes, salt])
    return ACCOUNT_FACTORY + (sel + body).hex()


def is_smart_account_deployed(sa_address: str, rpc_url: str = "https://soneium-rpc.publicnode.com") -> bool:
    """Check if smart account is already deployed (has code)."""
    payload = {
        "jsonrpc": "2.0",
        "method":  "eth_getCode",
        "params":  [sa_address, "latest"],
        "id":      1,
    }
    try:
        resp = requests.post(rpc_url, json=payload, timeout=15)
        code = resp.json().get("result", "0x")
        return len(code) > 4  # "0x" = not deployed
    except Exception:
        return False


# ── Nonce (как viem toSmartAccount + toStartaleSmartAccount.getNonce) ─────────

# aa-sdk: adjustedKey = keyMs % TIMESTAMP_ADJUSTMENT; key = concat(3b, 0x00, moduleAddress)
STARTALE_NONCE_TIMESTAMP_MOD = 16777215  # 2**24 - 1, как TIMESTAMP_ADJUSTMENT в toStartaleSmartAccount.js


def _resolve_startale_nonce_key_ms() -> int:
    raw = os.environ.get("BONUS9_STARTALE_NONCE_KEY_MS", "").strip()
    if raw:
        return int(raw, 0)
    return int(time.time() * 1000)


def startale_entrypoint_nonce_key_uint192(
    key_ms: int | None = None,
    module_address: str | None = None,
) -> int:
    """
    uint192 второй аргумент EntryPoint.getNonce(sender, key), как в Startale aa-sdk:
    toHex((keyMs % MOD), 3) ++ 0x00 ++ module (20 байт; default validator = zero address).
    key_ms — как Date.now() из viem nonceKeyManager (мс); override: BONUS9_STARTALE_NONCE_KEY_MS.
    """
    if key_ms is None:
        key_ms = _resolve_startale_nonce_key_ms()
    mod = STARTALE_NONCE_TIMESTAMP_MOD
    adjusted = key_ms % mod
    mod_addr = module_address or "0x0000000000000000000000000000000000000000"
    mod20 = bytes.fromhex(Web3.to_checksum_address(mod_addr).removeprefix("0x").lower())
    b = adjusted.to_bytes(3, "big") + bytes([0]) + mod20
    return int.from_bytes(b, "big")


def get_nonce(
    sa_address: str,
    rpc_url: str = "https://soneium-rpc.publicnode.com",
    *,
    key_ms: int | None = None,
    module_address: str | None = None,
) -> int:
    """
    Счётчик UserOp для пары (sender, uint192 key). Нельзя использовать key=0:
    viem prepareUserOperation вызывает account.getNonce() с key из nonceKeyManager (мс),
    Startale упаковывает его в uint192 — иначе userOpHash и подпись не совпадают с контрактом (-32507).
    """
    km = key_ms if key_ms is not None else _resolve_startale_nonce_key_ms()
    key192 = startale_entrypoint_nonce_key_uint192(km, module_address)
    selector = Web3.keccak(text="getNonce(address,uint192)")[:4]
    call_data = selector + _addr(sa_address) + _u256(key192)
    logger.debug(f"[ERC4337] getNonce(sender, key192) key_ms={km} → 0x{key192:x}")
    payload = {
        "jsonrpc": "2.0",
        "method":  "eth_call",
        "params":  [
            {"to": ENTRY_POINT, "data": "0x" + call_data.hex()},
            "latest",
        ],
        "id": 1,
    }
    try:
        resp   = requests.post(rpc_url, json=payload, timeout=15)
        result = resp.json().get("result", "0x0")
        return int(result, 16)
    except Exception:
        return 0


# ── execute calldata (ERC-7579 single call mode) ──────────────────────────────

EXEC_SELECTOR = bytes.fromhex("e9ae5c53")  # execute(bytes32 mode, bytes executionCalldata)
SINGLE_MODE   = b"\x00" * 32               # mode = 0 = single call

def build_execute_calldata(target: str, value: int, inner_calldata: bytes) -> bytes:
    """
    Calldata для SmartAccount.execute(ExecutionMode, bytes executionCalldata).

    Для CALLTYPE_SINGLE Startale кодирует внутренний batch как
    abi.encodePacked(target, value, inner) — см. ExecutionLib.decodeSingle,
    а не стандартный abi.encode(address,uint256,bytes).
    """
    tgt = Web3.to_checksum_address(target)
    packed_exec = _addr20(tgt) + _u256(value) + inner_calldata
    return EXEC_SELECTOR + encode(["bytes32", "bytes"], [SINGLE_MODE, packed_exec])


def simulate_sa_execute_call(sa_address: str, call_data: bytes, rpc_url: str) -> None:
    """
    eth_call: SA.execute(...) с msg.sender = EntryPoint (как при реальном UserOp).

    Вызов с from=SA→SA даёт ложный откат (например 0xac52ccbe): execute разрешён не самому SA,
    а EntryPoint’у после validateUserOp. Контракт чекина при этом может быть полностью валиден.
    """
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [
            {
                "from": ENTRY_POINT,
                "to": sa_address,
                "data": "0x" + call_data.hex(),
            },
            "latest",
        ],
        "id": 1,
    }
    r = requests.post(rpc_url, json=payload, timeout=25).json()
    if r.get("error"):
        e = r["error"]
        msg = e.get("message", str(e))
        data = e.get("data", "")
        raise RuntimeError(
            f"Симуляция execute откатилась: {msg} {data}. "
            "Проверь: контракт ELHEXA на паузе, неверный checkin id, или внутренний вызов; "
            "при долгом мемпуле без receipt — bundler/paymaster."
        )


# ── Gas estimation via bundler ────────────────────────────────────────────────

def estimate_user_op_gas(user_op: dict, proxy: str | None = None) -> dict:
    """Call eth_estimateUserOperationGas on bundler."""
    payload = {
        "jsonrpc": "2.0",
        "method":  "eth_estimateUserOperationGas",
        "params":  [user_op, ENTRY_POINT],
        "id":      1,
    }
    resp = _post_bundler(payload, proxy, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"Gas estimation error: {data['error']}")
    return data["result"]


# ── Paymaster ─────────────────────────────────────────────────────────────────

def get_paymaster_stub_data(user_op: dict, proxy: str | None = None) -> dict:
    """Get stub paymaster data for gas estimation."""
    payload = {
        "jsonrpc": "2.0",
        "method":  "pm_getPaymasterStubData",
        "params":  [user_op, ENTRY_POINT, CHAIN_ID_HEX, {"paymasterId": "pm_startaleapp"}],
        "id":      1,
    }
    resp = _post_paymaster(payload, proxy, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"Paymaster stub error: {data['error']}")
    return data.get("result", {})


def get_paymaster_data(user_op: dict, proxy: str | None = None) -> dict:
    """Get final paymaster data to include in UserOp."""
    payload = {
        "jsonrpc": "2.0",
        "method":  "pm_getPaymasterData",
        "params":  [user_op, ENTRY_POINT, CHAIN_ID_HEX, {"paymasterId": "pm_startaleapp"}],
        "id":      1,
    }
    resp = _post_paymaster(payload, proxy, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"Paymaster data error: {data['error']}")
    return data.get("result", {})


# ── UserOp hash & signing ─────────────────────────────────────────────────────
# Как UserOperationLib.hash + EntryPoint.getUserOpHash (account-abstraction v0.7.0):
# inner = keccak256(abi.encode(...)); userOpHash = keccak256(abi.encode(inner, entryPoint, chainId)).
# Web3.solidity_keccak(...) НЕ эквивалентен keccak256(abi.encode(...)) для тех же полей.


def _init_code_bytes(op: dict) -> bytes:
    """initCode = factory ++ factoryData (как в PackedUserOperation)."""
    ic = (op.get("initCode") or "").strip()
    if ic and ic not in ("0x", "0x0"):
        return bytes.fromhex(ic.removeprefix("0x"))
    fac = (op.get("factory") or "").strip()
    fd = (op.get("factoryData") or "0x").strip()
    zero = "0x0000000000000000000000000000000000000000"
    if not fac or fac == "0x" or fac.lower() == zero:
        return b""
    return bytes.fromhex(fac.removeprefix("0x")) + bytes.fromhex(fd.removeprefix("0x"))


def _paymaster_and_data_bytes(op: dict) -> bytes:
    """Сырые bytes paymasterAndData: address ++ uint128 ++ uint128 ++ data (как в контракте)."""
    pm = (op.get("paymaster") or "").strip()
    if not pm or pm.lower() == "0x0000000000000000000000000000000000000000":
        return b""
    pm = Web3.to_checksum_address(pm)
    vg = int(op.get("paymasterVerificationGasLimit", "0x0"), 16)
    pg = int(op.get("paymasterPostOpGasLimit", "0x0"), 16)
    tail = (op.get("paymasterData") or "0x").removeprefix("0x")
    return (
        bytes.fromhex(pm[2:].zfill(40))
        + vg.to_bytes(16, "big")
        + pg.to_bytes(16, "big")
        + (bytes.fromhex(tail) if tail else b"")
    )


def _inner_user_op_hash(op: dict) -> bytes:
    """userOp.hash() — keccak256(abi.encode(...)) из UserOperationLib (v0.7)."""
    sender = Web3.to_checksum_address(op["sender"])
    nonce = int(op["nonce"], 16)
    init_code = _init_code_bytes(op)
    call_data = bytes.fromhex(op["callData"].removeprefix("0x"))
    verif_gas = int(op.get("verificationGasLimit", "0x0"), 16)
    call_gas = int(op.get("callGasLimit", "0x0"), 16)
    pre_verif = int(op.get("preVerificationGas", "0x0"), 16)
    max_fee = int(op.get("maxFeePerGas", "0x0"), 16)
    max_priority = int(op.get("maxPriorityFeePerGas", "0x0"), 16)
    account_gas_limits = verif_gas.to_bytes(16, "big") + call_gas.to_bytes(16, "big")
    gas_fees = max_priority.to_bytes(16, "big") + max_fee.to_bytes(16, "big")
    paymaster_raw = _paymaster_and_data_bytes(op)

    inner_enc = encode(
        [
            "address",
            "uint256",
            "bytes32",
            "bytes32",
            "bytes32",
            "uint256",
            "bytes32",
            "bytes32",
        ],
        [
            sender,
            nonce,
            Web3.keccak(init_code),
            Web3.keccak(call_data),
            account_gas_limits,
            pre_verif,
            gas_fees,
            Web3.keccak(paymaster_raw),
        ],
    )
    return Web3.keccak(inner_enc)


def get_user_op_hash(op: dict) -> bytes:
    """Тот же digest, что EntryPoint.getUserOpHash (для подписи аккаунтом)."""
    inner = _inner_user_op_hash(op)
    outer_enc = encode(
        ["bytes32", "address", "uint256"],
        [bytes(inner), Web3.to_checksum_address(ENTRY_POINT), CHAIN_ID],
    )
    return Web3.keccak(outer_enc)


def _viem_sign_script_path() -> Path | None:
    """
    Путь к sign_userop_viem.mjs (рядом с node_modules/viem) — только через окружение.
    Пример: BONUS9_VIEM_SIGN_SCRIPT=D:\\tools\\sign_userop_viem.mjs
    В репозитории скрипт не поставляется; ELHEXA в браузере подпись не использует.
    """
    raw = os.environ.get("BONUS9_VIEM_SIGN_SCRIPT", "").strip()
    if not raw:
        return None
    p = Path(os.path.expandvars(os.path.expanduser(raw)))
    if not p.is_absolute():
        p = (Path(__file__).resolve().parent.parent / p).resolve()
    return p


def _env_flag_true(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def _env_flag_false(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("0", "false", "no", "off")


def _use_viem_signing_by_default() -> bool:
    """Viem, если задан BONUS9_VIEM_SIGN_SCRIPT и файл существует. Отключение: BONUS9_SIGN_USEROP_WITH_VIEM=0."""
    if _env_flag_false("BONUS9_SIGN_USEROP_WITH_VIEM"):
        return False
    script = _viem_sign_script_path()
    if script is None or not script.is_file():
        if _env_flag_true("BONUS9_SIGN_USEROP_WITH_VIEM"):
            logger.warning(
                "[ERC4337] BONUS9_SIGN_USEROP_WITH_VIEM=1, но скрипт не найден — "
                "задайте BONUS9_VIEM_SIGN_SCRIPT (путь к sign_userop_viem.mjs)"
            )
        return False
    return True


def _sign_user_op_viem(op: dict, private_key: str) -> str | None:
    """Та же подпись, что в браузере (viem getUserOperationHash + signMessage raw)."""
    script = _viem_sign_script_path()
    if script is None or not script.is_file():
        return None
    payload = {
        "privateKey": private_key if private_key.startswith("0x") else "0x" + private_key,
        "userOperation": op,
        "chainId": CHAIN_ID,
    }
    cwd = str(script.parent)
    try:
        proc = subprocess.run(
            ["node", str(script.name)],
            input=json.dumps(payload),
            capture_output=True,
            text=True,
            cwd=cwd,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        logger.warning(f"[ERC4337] viem sign: {e}")
        return None
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()[:400]
        logger.warning(f"[ERC4337] viem sign exit {proc.returncode}: {err}")
        return None
    line = (proc.stdout or "").strip().splitlines()[-1] if proc.stdout else ""
    try:
        out = json.loads(line)
        sig = out.get("signature")
        if isinstance(sig, str) and sig.startswith("0x") and len(sig) >= 132:
            return sig
    except json.JSONDecodeError:
        pass
    logger.warning(f"[ERC4337] viem sign: неверный JSON: {(line or '')[:200]}")
    return None


def sign_user_op(op: dict, private_key: str) -> str:
    """
    Как @startale-scs/aa-sdk toValidator.signUserOpHash → signMessage({ raw: userOpHash }):
    EIP-191 «Ethereum Signed Message» над 32-байтным userOpHash, не raw ECDSA (unsafe_sign_hash).

    Опционально node+viem: путь к скрипту в BONUS9_VIEM_SIGN_SCRIPT (как в браузере).
    BONUS9_SIGN_USEROP_WITH_VIEM=0 — только eth_account (без Node).

    Raw-подпись даёт bundler: Invalid account signature (-32507).
    """
    if _use_viem_signing_by_default():
        sig = _sign_user_op_viem(op, private_key)
        if sig:
            return sig
        sp = _viem_sign_script_path()
        if sp is not None and sp.is_file():
            logger.warning("[ERC4337] viem sign не удалась — fallback на eth_account")
    digest = get_user_op_hash(op)
    msg = encode_defunct(primitive=digest)
    signed = Account.sign_message(msg, private_key=private_key)
    return "0x" + signed.signature.hex()


def _is_bundler_signature_validation_error(exc: BaseException) -> bool:
    """Ошибки валидации подписи на estimate/send — не подменять дефолтным газом."""
    s = str(exc).lower()
    if "-32507" in s or "invalid account signature" in s:
        return True
    if "aa23" in s or "aa24" in s or "aa25" in s:
        return True
    return bool(re.search(r"\b-3250[0-9]\b", s))


# ── Send UserOperation ────────────────────────────────────────────────────────

def send_user_op(user_op: dict, proxy: str | None = None) -> str:
    """Submit UserOperation to bundler. Returns userOpHash."""
    payload = {
        "jsonrpc": "2.0",
        "method":  "eth_sendUserOperation",
        "params":  [user_op, ENTRY_POINT],
        "id":      1,
    }
    resp = _post_bundler(payload, proxy, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise BundlerRpcError(data["error"])
    return data["result"]


def wait_for_user_op(user_op_hash: str, proxy: str | None = None,
                     timeout: int | None = None) -> dict:
    """
    Опрашивает bundler: eth_getUserOperationReceipt.
    За один цикл опроса пробуем все URL из bundler_receipt_poll_urls() —
    по умолчанию сначала scs, затем app (у app чаще Read timeout при опросе).
    Дополнительно eth_getUserOperationByHash, если receipt долго null.
    Долго без логов выглядит как «зависание» — пишем прогресс каждые ~12 с.
    """
    if timeout is None:
        timeout = int(os.environ.get("BONUS9_USEROP_RECEIPT_TIMEOUT", "420"))
    poll = max(1, int(os.environ.get("BONUS9_USEROP_RECEIPT_POLL_SEC", "3")))
    urls = bundler_receipt_poll_urls()
    deadline = time.time() + timeout
    next_log_at = time.time() + 12.0
    logger.info(
        f"[ERC4337] ожидание receipt UserOp (до {timeout}s, каждые {poll}s, "
        f"{len(urls)} endpoint(s), connect/read={_receipt_wait_http_timeout()})…"
    )
    rpc_to = _receipt_wait_http_timeout()
    payload = {
        "jsonrpc": "2.0",
        "method":  "eth_getUserOperationReceipt",
        "params":  [user_op_hash],
        "id":      1,
    }
    while time.time() < deadline:
        for url, data, err in _rpc_post_parallel(urls, payload, proxy, rpc_to):
            if err:
                msg = str(err)[:160]
                low = msg.lower()
                if "timed out" in low or "timeout" in low:
                    logger.debug(
                        f"[ERC4337] bundler receipt timeout ({url[:44]}…): {msg[:100]}"
                    )
                else:
                    logger.warning(f"[ERC4337] bundler receipt ({url[:40]}…): {msg}")
                continue
            if not data:
                continue
            result = data.get("result")
            if result:
                rev = (result.get("reason") or "").strip()
                if rev and rev not in ("0x", "0x0"):
                    raise RuntimeError(
                        f"UserOp исполнилась с revert: {rev} (opHash {user_op_hash})"
                    )
                if len(urls) > 1:
                    logger.debug(f"[ERC4337] receipt получен с {url[:60]}…")
                return result
            jerr = data.get("error")
            if jerr:
                logger.debug(
                    f"[ERC4337] getUserOperationReceipt ({url[:48]}…): {jerr}"
                )
        alt = _try_get_included_user_op(user_op_hash, proxy, urls, rpc_to)
        if alt:
            return alt
        now = time.time()
        if now >= next_log_at:
            left = max(0, int(deadline - now))
            logger.info(
                f"[ERC4337] UserOp ещё в очереди/мемпуле… осталось ≤{left}s "
                f"(opHash {user_op_hash[:14]}…)"
            )
            next_log_at = now + 12.0
        time.sleep(poll)
    raise RuntimeError(
        f"UserOp не получила receipt за {timeout}s (bundler). "
        f"Проверь opHash в эксплорере: {user_op_hash}"
    )


# ── Full flow: build → estimate → paymaster → sign → send ────────────────────

def execute_user_op(
    private_key: str,
    target: str,
    inner_calldata: bytes,
    value: int = 0,
    proxy: str | None = None,
    rpc_url: str = "https://soneium-rpc.publicnode.com",
    account_index: int = 0,
    smart_account_address: str | None = None,
    allow_api_sa_mismatch: bool = False,
) -> str:
    """
    High-level: build and send a single-call UserOperation.
    Returns tx hash from receipt.
    account_index — salt смарт-аккаунта (как index в toStartaleSmartAccount), по умолчанию 0.
    smart_account_address — если задан (например из GET /user linked_accounts Startale),
    используется вместо factory.computeAccountAddress; иначе квесты могут не засчитать tx.
    allow_api_sa_mismatch — разрешить SA из API, даже если он не воспроизводится текущей
    локальной моделью factory.computeAccountAddress(initData, salt). Нужен для real-world
    кейсов Startale, где API-адрес валиден, а локальная реконструкция initData не совпадает.
    """
    eoa_address = Account.from_key(private_key).address
    computed = get_smart_account_address(eoa_address, account_index, rpc_url)
    if not computed or computed == "0x0000000000000000000000000000000000000000":
        raise RuntimeError(f"Не удалось получить адрес смарт-аккаунта для {eoa_address}")

    if smart_account_address:
        sa_address = Web3.to_checksum_address(smart_account_address.strip())
        if computed.lower() != sa_address.lower():
            if allow_api_sa_mismatch:
                logger.warning(
                    "[ERC4337] SA из API не совпал с локальным factory.computeAccountAddress, "
                    "но allow_api_sa_mismatch=True — использую API-адрес без подмены"
                )
                computed = sa_address
            else:
                found = find_startale_account_index_for_address(
                    eoa_address, sa_address, rpc_url
                )
                if found is not None:
                    if found != account_index:
                        logger.warning(
                            f"[ERC4337] SA из API = factory при index={found} "
                            f"(BONUS9_STARTALE_ACCOUNT_INDEX={account_index} неверен — исправлено)"
                        )
                    account_index = found
                    computed = get_smart_account_address(eoa_address, account_index, rpc_url)
                else:
                    mx = int(os.environ.get("BONUS9_STARTALE_ACCOUNT_INDEX_SEARCH_MAX", "128"))
                    raise RuntimeError(
                        f"Адрес SA из Startale API ({sa_address}) не совпадает ни с одним "
                        f"factory.computeAccountAddress для этого ключа (index 0…{mx}). "
                        "Подпись UserOp будет отклонена (-32507): SA в API не тот смарт-аккаунт, "
                        "который получается из этого private key через factory. "
                        "Запусти с BONUS9_USE_FACTORY_SMART_ACCOUNT_ONLY=1 (UserOp с factory-адресом) "
                        "или проверь, что keys.txt — тот же кошелёк, что в Startale."
                    )
    else:
        sa_address = computed
        cached = db.get_smart_account(eoa_address)
        if cached and cached.lower() != sa_address.lower():
            logger.warning("[ERC4337] адрес SA в БД не совпадает с factory — перезаписываю")
    db.set_smart_account(eoa_address, sa_address)

    logger.debug(f"[ERC4337] SA {sa_address}")

    deployed   = is_smart_account_deployed(sa_address, rpc_url)
    if not deployed and smart_account_address:
        raise RuntimeError(
            f"Смарт-аккаунт {sa_address} из API не развёрнут в сети. "
            "Откройте приложение Startale с этим кошельком."
        )
    _nonce_key_ms = _resolve_startale_nonce_key_ms()
    nonce = get_nonce(sa_address, rpc_url, key_ms=_nonce_key_ms)
    # EntryPoint v0.7: если callData не начинается с селектора executeUserOp, innerHandleOp
    # делает sender.call(callData) без среза (см. _executeUserOp). encodeExecute из aa-sdk
    # даёт только execute(bytes32,bytes) — тот же путь. Префикс из 4 нулевых байт ломал вызов:
    # на прокси приходил msg.sig=0 → MissingFallbackHandler(0).
    call_data  = build_execute_calldata(target, value, inner_calldata)

    if os.environ.get("BONUS9_SKIP_EXECUTE_SIMULATION", "").strip().lower() not in (
        "1",
        "true",
        "yes",
    ):
        simulate_sa_execute_call(sa_address, call_data, rpc_url)

    # maxFee* в UserOp не списывают ETH с EOA: исполнение оплачивает paymaster.
    # Поля всё равно обязательны — это потолки для bundled tx (EIP-1559) и условие
    # «замены» UserOp в мемпуле с тем же nonce (иначе bundler: replacement underpriced).
    max_fee, prio = _eip1559_user_op_fees(rpc_url)
    floor_max, floor_prio = max_fee, prio

    # ── Build base UserOp ────────────────────────────────────────────────────
    user_op: dict = {
        "sender":               sa_address,
        "nonce":                hex(nonce),
        "callData":             "0x" + call_data.hex(),
        "callGasLimit":         hex(200_000),
        "verificationGasLimit": hex(500_000),
        "preVerificationGas":   hex(50_000),
        "maxFeePerGas":         hex(max_fee),
        "maxPriorityFeePerGas": hex(prio),
        "signature":            "0x" + "00" * 65,
    }

    if not deployed:
        init_code = get_factory_init_code(eoa_address, account_index)
        user_op["factory"]     = ACCOUNT_FACTORY
        user_op["factoryData"] = "0x" + init_code[len(ACCOUNT_FACTORY):]
        user_op.pop("initCode", None)
    else:
        # Уже задеплоен: не передаём factory/factoryData (иначе bundler: initCode nonempty).
        user_op.pop("factory", None)
        user_op.pop("factoryData", None)

    # ── Get paymaster stub data ──────────────────────────────────────────────
    try:
        stub = get_paymaster_stub_data(user_op, proxy)
        user_op["paymaster"]                    = stub.get("paymaster", PAYMASTER)
        user_op["paymasterData"]                = stub.get("paymasterData", "0x")
        user_op["paymasterVerificationGasLimit"] = stub.get("paymasterVerificationGasLimit", hex(100_000))
        user_op["paymasterPostOpGasLimit"]       = stub.get("paymasterPostOpGasLimit", hex(50_000))
        _apply_fee_hints_from_paymaster(user_op, stub)
        _ensure_fee_floor(user_op, floor_max, floor_prio)
    except Exception as e:
        logger.warning(f"[ERC4337] paymaster stub → defaults ({e})")
        user_op["paymaster"]                    = PAYMASTER
        user_op["paymasterData"]                = "0x"
        user_op["paymasterVerificationGasLimit"] = hex(100_000)
        user_op["paymasterPostOpGasLimit"]       = hex(50_000)

    # Bundler симулирует validateUserOp — нужна подпись по реальному userOpHash,
    # а не 0x00… (иначе AA23 на eth_estimateUserOperationGas).
    user_op["signature"] = sign_user_op(user_op, private_key)

    # ── Estimate gas ─────────────────────────────────────────────────────────
    try:
        gas_est = estimate_user_op_gas(user_op, proxy)
        user_op["callGasLimit"]         = gas_est.get("callGasLimit", user_op["callGasLimit"])
        user_op["verificationGasLimit"] = gas_est.get("verificationGasLimit", user_op["verificationGasLimit"])
        user_op["preVerificationGas"]   = gas_est.get("preVerificationGas", user_op["preVerificationGas"])
    except Exception as e:
        if _is_bundler_signature_validation_error(e):
            raise RuntimeError(
                f"eth_estimateUserOperationGas: ошибка подписи/валидации (см. подпись UserOp): {e}"
            ) from e
        logger.warning(f"[ERC4337] gas est → defaults ({e})")
        user_op["callGasLimit"] = hex(800_000)
        user_op["verificationGasLimit"] = hex(1_000_000)
        user_op["preVerificationGas"] = hex(100_000)

    user_op["signature"] = sign_user_op(user_op, private_key)

    # ── Get final paymaster data ─────────────────────────────────────────────
    try:
        pm_data = get_paymaster_data(user_op, proxy)
        user_op["paymaster"]                    = pm_data.get("paymaster", user_op["paymaster"])
        user_op["paymasterData"]                = pm_data.get("paymasterData", user_op["paymasterData"])
        user_op["paymasterVerificationGasLimit"] = pm_data.get("paymasterVerificationGasLimit", user_op["paymasterVerificationGasLimit"])
        user_op["paymasterPostOpGasLimit"]       = pm_data.get("paymasterPostOpGasLimit", user_op["paymasterPostOpGasLimit"])
        _apply_fee_hints_from_paymaster(user_op, pm_data)
        _ensure_fee_floor(user_op, floor_max, floor_prio)
    except Exception as e:
        logger.warning(f"[ERC4337] paymaster data: {e}")

    user_op["signature"] = sign_user_op(user_op, private_key)

    # ── Send (при «replacement underpriced» — bump maxFee*, новый paymaster, подпись) ─
    logger.info(f"[ERC4337] UserOp → {sa_address}")
    op_hash = None
    for attempt in range(6):
        user_op["signature"] = sign_user_op(user_op, private_key)
        try:
            op_hash = send_user_op(user_op, proxy)
            break
        except BundlerRpcError as e:
            inner = e.err if isinstance(e.err, dict) else {}
            if inner.get("code") != -32602 or inner.get("message") != "replacement underpriced":
                raise RuntimeError(f"sendUserOperation error: {e.err}") from e
            if not _bump_fee_fields_after_replacement_error(user_op, inner.get("data")):
                raise RuntimeError(f"sendUserOperation error: {e.err}") from e
            logger.warning(
                f"[ERC4337] replacement underpriced → bump maxFee* + refresh paymaster "
                f"(попытка {attempt + 1})"
            )
            user_op["signature"] = sign_user_op(user_op, private_key)
            try:
                pm_data = get_paymaster_data(user_op, proxy)
                user_op["paymaster"] = pm_data.get("paymaster", user_op["paymaster"])
                user_op["paymasterData"] = pm_data.get("paymasterData", user_op["paymasterData"])
                user_op["paymasterVerificationGasLimit"] = pm_data.get(
                    "paymasterVerificationGasLimit", user_op["paymasterVerificationGasLimit"]
                )
                user_op["paymasterPostOpGasLimit"] = pm_data.get(
                    "paymasterPostOpGasLimit", user_op["paymasterPostOpGasLimit"]
                )
            except Exception as ex:
                logger.warning(f"[ERC4337] paymaster после bump: {ex}")
            continue
    if op_hash is None:
        raise RuntimeError("sendUserOperation: не удалось после повторов")
    logger.debug(f"[ERC4337] opHash {op_hash}")

    # ── Wait for receipt (с rebroadcast, если UserOp «висит» в мемпуле) ───────
    rebroadcast_max = max(1, int(os.environ.get("BONUS9_USEROP_REBROADCAST_ATTEMPTS", "3")))
    rebroadcast_wait = max(60, int(os.environ.get("BONUS9_USEROP_REBROADCAST_WAIT_SEC", "150")))
    receipt = None
    for include_try in range(rebroadcast_max):
        try:
            timeout = None if include_try == 0 else rebroadcast_wait
            receipt = wait_for_user_op(op_hash, proxy, timeout=timeout)
            break
        except RuntimeError as e:
            msg = str(e).lower()
            if "не получила receipt" not in msg and "receipt" not in msg:
                raise
            if include_try >= rebroadcast_max - 1:
                raise
            logger.warning(
                f"[ERC4337] UserOp долго в мемпуле — rebroadcast "
                f"({include_try + 1}/{rebroadcast_max - 1})"
            )
            _bump_fee_fields_after_replacement_error(user_op, None)
            try:
                pm_data = get_paymaster_data(user_op, proxy)
                user_op["paymaster"] = pm_data.get("paymaster", user_op["paymaster"])
                user_op["paymasterData"] = pm_data.get("paymasterData", user_op["paymasterData"])
                user_op["paymasterVerificationGasLimit"] = pm_data.get(
                    "paymasterVerificationGasLimit", user_op["paymasterVerificationGasLimit"]
                )
                user_op["paymasterPostOpGasLimit"] = pm_data.get(
                    "paymasterPostOpGasLimit", user_op["paymasterPostOpGasLimit"]
                )
            except Exception as ex:
                logger.warning(f"[ERC4337] paymaster после timeout: {ex}")
            user_op["signature"] = sign_user_op(user_op, private_key)
            try:
                op_hash = send_user_op(user_op, proxy)
            except BundlerRpcError as se:
                inner = se.err if isinstance(se.err, dict) else {}
                if inner.get("code") == -32602 and inner.get("message") == "replacement underpriced":
                    if _bump_fee_fields_after_replacement_error(user_op, inner.get("data")):
                        user_op["signature"] = sign_user_op(user_op, private_key)
                        op_hash = send_user_op(user_op, proxy)
                    else:
                        raise RuntimeError(f"sendUserOperation error: {se.err}") from se
                else:
                    raise RuntimeError(f"sendUserOperation error: {se.err}") from se
    if receipt is None:
        raise RuntimeError("UserOp не получила receipt после rebroadcast")
    tx_hash = receipt.get("receipt", {}).get("transactionHash", op_hash)
    if isinstance(tx_hash, bytes):
        tx_hash = Web3.to_hex(tx_hash)
    elif isinstance(tx_hash, str) and tx_hash and not tx_hash.startswith("0x"):
        tx_hash = "0x" + tx_hash
    th = str(tx_hash)
    tx_short = f"{th[:10]}…{th[-6:]}" if len(th) >= 16 else th
    logger.success(f"[ERC4337] OK tx {tx_short}")
    return tx_hash
