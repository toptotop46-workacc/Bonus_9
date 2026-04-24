"""Daily GM check-in прямым on-chain вызовом checkIn() от EOA."""

from __future__ import annotations

import os
import random
import time

import requests
from eth_account import Account
from web3 import Web3

from modules import db, logger
from modules.portal_api import get_bonus_dapp_data, parse_account_status, require_account_status
from modules.web3_utils import get_w3, build_and_send_tx

CHECKIN_CONTRACT = "0x0B9f730bF4C1Bf1c0D5B548556a239d5eC0A1D3e"
CHECKIN_SELECTOR = bytes.fromhex("183ff085")  # checkIn()


def is_checked_in_today(sa_address: str, rpc_url: str = "https://soneium-rpc.publicnode.com") -> bool:
    selector = Web3.keccak(text="hasCheckedInToday(address)")[:4]
    call_data = selector + bytes.fromhex(sa_address.removeprefix("0x").zfill(64))
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": CHECKIN_CONTRACT, "data": "0x" + call_data.hex()}, "latest"],
        "id": 1,
    }
    try:
        resp = requests.post(rpc_url, json=payload, timeout=15)
        result = resp.json().get("result", "0x")
        return int(result, 16) != 0
    except Exception:
        return False


def get_checkin_status(sa_address: str, rpc_url: str = "https://soneium-rpc.publicnode.com") -> dict:
    selector = Web3.keccak(text="getCheckInStatus(address)")[:4]
    call_data = selector + bytes.fromhex(sa_address.removeprefix("0x").zfill(64))
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [{"to": CHECKIN_CONTRACT, "data": "0x" + call_data.hex()}, "latest"],
        "id": 1,
    }
    try:
        resp = requests.post(rpc_url, json=payload, timeout=15)
        result = resp.json().get("result", "0x")
        raw = bytes.fromhex(result.removeprefix("0x"))
        if len(raw) >= 128:
            return {
                "checked_today": int.from_bytes(raw[0:32], "big") != 0,
                "total": int.from_bytes(raw[32:64], "big"),
                "last_day": int.from_bytes(raw[64:96], "big"),
                "current_day": int.from_bytes(raw[96:128], "big"),
            }
    except Exception as e:
        logger.debug(f"[GM] getCheckInStatus error: {e}")
    return {"checked_today": False, "total": 0, "last_day": 0, "current_day": 0}


def _wait_portal_gm_credit(
    eoa_address: str,
    proxy: str | None,
    *,
    tries: int = 4,
    delay_sec: int = 30,
) -> None:
    """Портал может засчитать GM с лагом; мягко ждём и логируем прогресс."""
    for i in range(1, tries + 1):
        try:
            raw = get_bonus_dapp_data(eoa_address, proxy)
            if raw:
                st = parse_account_status(raw)
                gm = int(st.get("gm", 0))
                req = int(st.get("gm_required", 5))
                logger.debug(f"[GM] portal GM {gm}/{req} (check {i}/{tries})")
                if gm >= req:
                    return
        except Exception as e:
            logger.debug(f"[GM] portal GM check {i}/{tries} failed: {e}")
        if i < tries:
            time.sleep(delay_sec)


def do_gm(
    private_key: str,
    proxy: str | None = None,
    rpc_url: str = "https://soneium-rpc.publicnode.com",
    force: bool = False,
    headless: bool | None = None,
    proxy_pool: list[str | None] | None = None,
) -> bool:
    """Ежедневный GM: прямой checkIn() от EOA."""

    acct = Account.from_key(private_key)
    eoa_address = acct.address

    if not force:
        try:
            st = require_account_status(eoa_address, proxy, proxy_pool=proxy_pool)
            gm_req = int(st.get("gm_required", 5))
            if int(st.get("gm", 0)) >= gm_req:
                logger.info(f"[GM] {eoa_address} портал OK ({st['gm']}/{gm_req}), пропуск")
                db.mark_gm_done(eoa_address)
                return False
        except Exception as e:
            logger.warning(f"[GM] {eoa_address} портал недоступен ({e}), продолжаем")
        if db.is_gm_done_today(eoa_address):
            logger.info(f"[GM] {eoa_address} уже сегодня (БД)")
            return False

    use_proxy = proxy
    if not use_proxy and proxy_pool:
        candidates = [p for p in proxy_pool if p]
        if candidates:
            use_proxy = random.choice(candidates)

    if is_checked_in_today(eoa_address, rpc_url):
        logger.info(f"[GM] {eoa_address} уже GM сегодня (on-chain)")
        db.mark_gm_done(eoa_address)
        return True

    logger.info(f"[GM] {eoa_address} → checkIn (EOA)")
    try:
        w3 = get_w3(rpc_url, proxy=use_proxy)
        tx_hash = build_and_send_tx(
            w3=w3,
            private_key=private_key,
            to=CHECKIN_CONTRACT,
            data=CHECKIN_SELECTOR,
            value=0,
        )
        logger.success(f"[GM] {eoa_address} checkIn OK tx={tx_hash}")
        db.mark_gm_done(eoa_address)
        portal_tries = max(1, int(os.environ.get("BONUS9_GM_PORTAL_CONFIRM_TRIES", "4")))
        portal_delay = max(5, int(os.environ.get("BONUS9_GM_PORTAL_CONFIRM_DELAY_SEC", "30")))
        _wait_portal_gm_credit(
            eoa_address,
            use_proxy,
            tries=portal_tries,
            delay_sec=portal_delay,
        )
        return True
    except Exception as e:
        logger.error(f"[GM] {eoa_address} checkIn failed: {e}")
        return False
