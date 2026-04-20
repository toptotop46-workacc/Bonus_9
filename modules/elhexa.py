"""
ELHEXA daily check-in — прямой on-chain вызов checkin(id, amount) от EOA.

Контракт: 0xb97DDf414748d1DBEF846fc2Fe74391f7Bc8A715

Логика:
  1. DB  → elhexa_total >= 3 → пропуск (квест закрыт локально)
  2. Портал → elhexa >= required → пропуск (квест закрыт на портале)
  3. DB  → elhexa_last_date == сегодня (UTC) → пропуск (уже чекинились)
  4. Контракт paused → пропуск
  5. Отправка checkin(1, 0) → запись в DB
"""

from __future__ import annotations

from eth_account import Account
from web3 import Web3

from modules import logger
from modules.db import is_elhexa_done_today, mark_elhexa_done, get_account_info
from modules.portal_api import require_account_status
from modules.web3_utils import build_and_send_tx, eth_call, get_w3

ELHEXA_CONTRACT = "0xb97DDf414748d1DBEF846fc2Fe74391f7Bc8A715"

CHECKIN_SELECTOR = bytes.fromhex("7c21bd5a")  # checkin(uint256,uint256)
PAUSED_SELECTOR = Web3.keccak(text="paused()")[:4]

REQUIRED_CHECKINS = 3


def _build_checkin_calldata(check_id: int, amount: int = 0) -> bytes:
    return (
        CHECKIN_SELECTOR
        + check_id.to_bytes(32, "big")
        + amount.to_bytes(32, "big")
    )


def _is_paused(w3: Web3) -> bool:
    try:
        raw = eth_call(w3, ELHEXA_CONTRACT, PAUSED_SELECTOR)
        return int.from_bytes(raw, "big") != 0
    except Exception:
        return False


def do_elhexa_checkin(
    private_key: str,
    proxy: str | None = None,
    rpc_url: str = "https://soneium-rpc.publicnode.com",
    check_id: int = 1,
    headless: bool | None = None,
    proxy_pool: list[str | None] | None = None,
) -> bool:
    """On-chain ELHEXA daily check-in от EOA."""
    acct = Account.from_key(private_key)
    eoa = acct.address

    # 1) DB — квест закрыт локально?
    db_total = int(get_account_info(eoa).get("elhexa_total", 0))
    if db_total >= REQUIRED_CHECKINS:
        logger.info(f"[ELHEXA] квест закрыт (DB {db_total}/{REQUIRED_CHECKINS}), пропуск")
        return False

    # 2) Портал — квест закрыт?
    st = require_account_status(eoa, proxy, proxy_pool=proxy_pool)
    req = int(st.get("elhexa_required", REQUIRED_CHECKINS))
    cnt = int(st.get("elhexa", 0))
    if st.get("elhexa_done") or cnt >= req:
        logger.info(f"[ELHEXA] квест закрыт (портал {cnt}/{req}), пропуск")
        return False

    # 3) DB — уже сегодня чекинились (UTC)?
    if is_elhexa_done_today(eoa):
        logger.info(f"[ELHEXA] {eoa} уже чекинился сегодня (DB), пропуск")
        return False

    w3 = get_w3(rpc_url, proxy)

    # 4) Контракт на паузе?
    if _is_paused(w3):
        logger.warning("[ELHEXA] контракт на паузе, пропуск")
        return False

    # 5) Отправка checkin(1, 0)
    logger.info(f"[ELHEXA] {eoa} checkin(id=1, amount=0) [{cnt}/{req}]")
    calldata = _build_checkin_calldata(1, 0)
    try:
        tx_hash = build_and_send_tx(w3, private_key, ELHEXA_CONTRACT, data=calldata)
        logger.success(f"[ELHEXA] OK tx={tx_hash}")
        mark_elhexa_done(eoa, tx_hash)
        return True
    except Exception as exc:
        logger.error(f"[ELHEXA] tx failed: {exc}")
        return False
