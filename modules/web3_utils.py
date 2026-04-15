"""Low-level web3 helpers: EIP-1559 tx building and sending."""

import time
import random
import requests
from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from eth_account import Account
from modules import logger


CHAIN_ID     = 1868
EXPLORER_URL = "https://soneium.blockscout.com/tx/"


def get_w3(rpc_url: str, proxy: str | None = None, disable_ssl: bool = False) -> Web3:
    session = requests.Session()
    session.trust_env = False
    if proxy:
        session.proxies = {"http": proxy, "https": proxy}
    if disable_ssl:
        session.verify = False
    from web3 import HTTPProvider
    provider = HTTPProvider(rpc_url, session=session)
    w3 = Web3(provider)
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    # Сохраняем параметры подключения, чтобы можно было переподнять provider после сетевых сбоев.
    setattr(w3, "_bonus9_rpc_url", rpc_url)
    setattr(w3, "_bonus9_proxy", proxy)
    setattr(w3, "_bonus9_disable_ssl", disable_ssl)
    return w3


def reconnect_w3(w3: Web3) -> Web3:
    rpc_url = getattr(w3, "_bonus9_rpc_url", None)
    proxy = getattr(w3, "_bonus9_proxy", None)
    disable_ssl = bool(getattr(w3, "_bonus9_disable_ssl", False))

    if not rpc_url:
        provider = getattr(w3, "provider", None)
        rpc_url = getattr(provider, "endpoint_uri", None)
        request_kwargs = getattr(provider, "_request_kwargs", {}) or {}
        if proxy is None:
            proxies = request_kwargs.get("proxies") or {}
            proxy = proxies.get("https") or proxies.get("http")
        if not disable_ssl:
            disable_ssl = request_kwargs.get("verify") is False

    if not rpc_url:
        raise RuntimeError("Cannot reconnect Web3 provider: rpc_url is unknown")
    return get_w3(str(rpc_url), proxy, disable_ssl)


def _is_transient_rpc_error(exc: BaseException) -> bool:
    s = str(exc).lower()
    markers = (
        "connection aborted",
        "connection reset",
        "reset by peer",
        "remotedisconnected",
        "read timed out",
        "timed out",
        "timeout",
        "temporary failure",
        "temporarily unavailable",
        "connection refused",
        "too many requests",
        "429",
        "502",
        "503",
        "504",
    )
    return any(m in s for m in markers)


def get_eip1559_fees(w3: Web3) -> tuple[int, int]:
    """Returns (maxFeePerGas, maxPriorityFeePerGas) in wei."""
    latest = w3.eth.get_block("latest")
    base_fee = latest["baseFeePerGas"]
    try:
        priority = w3.eth.max_priority_fee
    except Exception:
        priority = Web3.to_wei(0.001, "gwei")
    # random jitter 1.0–1.2x on priority fee (anti-sybil)
    priority = int(priority * random.uniform(1.0, 1.2))
    max_fee  = base_fee * 2 + priority
    return max_fee, priority


def prepare_eip1559_tx(
    w3: Web3,
    private_key: str,
    to: str,
    data: bytes | str = b"",
    value: int = 0,
    gas_limit_multiplier: float = 1.2,
    extra_gas: int = 0,
) -> dict:
    """
    Собирает unsigned EIP-1559-транзакцию с полем gas (один вызов estimate_gas).
    При ошибке оценки — RuntimeError, как в build_and_send_tx.
    """
    account = Account.from_key(private_key)
    sender = account.address
    if isinstance(data, str):
        data = bytes.fromhex(data.removeprefix("0x"))
    max_fee, priority = get_eip1559_fees(w3)
    nonce = w3.eth.get_transaction_count(sender, "pending")
    tx: dict = {
        "chainId": CHAIN_ID,
        "from": sender,
        "to": Web3.to_checksum_address(to),
        "value": value,
        "data": data,
        "nonce": nonce,
        "maxFeePerGas": max_fee,
        "maxPriorityFeePerGas": priority,
        "type": 2,
    }
    try:
        estimated = w3.eth.estimate_gas(tx)
    except Exception as e:
        raise RuntimeError(f"Gas estimation failed: {e}") from e
    tx["gas"] = int(estimated * gas_limit_multiplier) + extra_gas
    return tx


def tx_max_cost_wei(tx: dict) -> int:
    """Верхняя оценка списания: value + gas * maxFeePerGas (EIP-1559)."""
    return int(tx["value"]) + int(tx["gas"]) * int(tx["maxFeePerGas"])


def send_prepared_tx(w3: Web3, private_key: str, tx: dict) -> str:
    """Подпись готовой tx (с gas), отправка, ожидание receipt. Возвращает tx hash."""
    signed = Account.sign_transaction(tx, private_key)
    tx_hash = Web3.to_hex(w3.eth.send_raw_transaction(signed.raw_transaction))
    th = str(tx_hash)
    logger.info(f"[Tx] sent {th[:10]}…{th[-6:]}")

    for _ in range(90):
        try:
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            if receipt is not None:
                if receipt["status"] == 1:
                    th = str(tx_hash)
                    logger.success(f"[Tx] OK {th[:10]}…{th[-6:]}")
                else:
                    raise RuntimeError(f"Tx reverted: {EXPLORER_URL}{tx_hash}")
                return tx_hash
        except Exception as e:
            if "not found" not in str(e).lower():
                raise
        time.sleep(2)
    raise RuntimeError(f"Tx не подтверждена за 180 сек: {tx_hash}")


def build_and_send_tx(
    w3: Web3,
    private_key: str,
    to: str,
    data: bytes | str = b"",
    value: int = 0,
    gas_limit_multiplier: float = 1.2,
    extra_gas: int = 0,
) -> str:
    """Build EIP-1559 tx, simulate, send, wait for receipt. Returns tx hash."""
    tx = prepare_eip1559_tx(
        w3,
        private_key,
        to,
        data=data,
        value=value,
        gas_limit_multiplier=gas_limit_multiplier,
        extra_gas=extra_gas,
    )
    return send_prepared_tx(w3, private_key, tx)


def eth_call(w3: Web3, to: str, data: bytes | str, sender: str | None = None) -> bytes:
    """Read-only eth_call."""
    if isinstance(data, str):
        data = bytes.fromhex(data.removeprefix("0x"))
    call = {"to": Web3.to_checksum_address(to), "data": data}
    if sender:
        call["from"] = Web3.to_checksum_address(sender)
    retries = 4
    delay = 1.0
    cur_w3 = w3
    last_err: Exception | None = None

    for attempt in range(retries):
        try:
            result = cur_w3.eth.call(call)
            return bytes(result)
        except Exception as exc:
            last_err = exc
            if attempt + 1 >= retries or not _is_transient_rpc_error(exc):
                raise
            logger.debug(
                f"[web3] eth_call retry {attempt + 1}/{retries} after transport error: {exc}"
            )
            time.sleep(delay)
            delay *= 1.5
            cur_w3 = reconnect_w3(cur_w3)

    raise RuntimeError(f"eth_call failed after {retries} attempts: {last_err}") from last_err


def get_eoa_address(private_key: str) -> str:
    return Account.from_key(private_key).address
