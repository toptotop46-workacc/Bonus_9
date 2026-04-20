"""
ELHEXA daily check-in через Playwright (app.startale.com/miniapps#elhexa).

Флоу как в браузере: Mock Wallet → при необходимости возврат на miniapps → «Explore Mini Apps» →
Sign (elhexa.io) → iframe game.elhexa.io: Gift Airdrop / REWARDS на canvas → CHECK-IN BONUS $0 →
оверлей Startale «Approve transaction» → Confirm → ждём tx, DOM «Claimed» (если есть) или рост elhexa на портале (canvas без текста в DOM).
"""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import random
import re
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

import requests
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3

from modules import db, logger, proxy_utils

from modules.portal_api import get_bonus_dapp_data, parse_account_status, require_account_status
from modules.startale_gm import (
    APP_URL,
    CHAIN_HEX,
    MOCK_WALLET_SCRIPT,
    RPC_ENDPOINT,
    USER_AGENT,
    _playwright_proxy,
)

# ELHEXA Checkin contract (USDSC в конфиге игры; для тестов UserOp / совместимости)
ELHEXA_CONTRACT = "0xb97DDf414748d1DBEF846fc2Fe74391f7Bc8A715"
ELHEXA_API_BASE = "https://api.elhexa.io/api/v1"
PORTAL_MAPPING_URL = "https://portal.soneium.org/api/profile/mapping"
DAILY_CHECKIN_CONTRACT = "0x0B9f730bF4C1Bf1c0D5B548556a239d5eC0A1D3e"

# checkin(uint256 id, uint256 amount) selector
CHECKIN_SELECTOR = bytes.fromhex("7c21bd5a")
GET_CHECKIN_STATUS_SELECTOR = Web3.keccak(text="getCheckInStatus(address)")[:4]

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BROWSER_DATA_PATH = PROJECT_ROOT / "browser_data" / "elhexa_camoufox"

DEFAULT_APP_URL = f"{APP_URL}/miniapps#elhexa"


def build_checkin_calldata(check_id: int = 1, amount: int = 0) -> bytes:
    """Build calldata for checkin(id, amount). Default: id=1, amount=0 (free)."""
    return (
        CHECKIN_SELECTOR
        + check_id.to_bytes(32, "big")
        + amount.to_bytes(32, "big")
    )


def post_elhexa_checkin_verify(
    tx_hash: str,
    wallet_sa: str,
    checkin_id: int,
    proxy: str | None,
) -> None:
    """
    POST /checkin/verify как в game.elhexa.io (HAR). Нужен x-user-id из игры.
    Без BONUS9_ELHEXA_USER_ID вызов пропускается — Soneium Score всё равно видит on-chain tx с правильным SA.
    """
    uid = os.environ.get("BONUS9_ELHEXA_USER_ID", "").strip()
    if not uid:
        return
    token = (os.environ.get("BONUS9_ELHEXA_CHECKIN_TOKEN", "USDSC") or "USDSC").strip()
    ver = os.environ.get("BONUS9_ELHEXA_CLIENT_VERSION", "0.9.3-release1").strip()
    payload = {
        "txHash": tx_hash,
        "walletAddress": Web3.to_checksum_address(wallet_sa),
        "checkinId": checkin_id,
        "token": token,
    }
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": "https://game.elhexa.io",
        "Referer": "https://game.elhexa.io/",
        "x-user-id": uid,
        "x-client-version": ver,
    }
    try:
        sess = requests.Session()
        sess.trust_env = False
        kwargs: dict = {"headers": headers, "json": payload, "timeout": 35}
        if proxy:
            kwargs["proxies"] = {"http": proxy, "https": proxy}
        r = sess.post(f"{ELHEXA_API_BASE}/checkin/verify", **kwargs)
        if r.ok:
            logger.debug(f"[ELHEXA] checkin/verify OK ({r.status_code})")
        else:
            logger.warning(f"[ELHEXA] checkin/verify {r.status_code}: {r.text[:200]}")
    except Exception as e:
        logger.warning(f"[ELHEXA] checkin/verify: {e}")


def is_paused(rpc_url: str = "https://soneium-rpc.publicnode.com") -> bool:
    """Check if ELHEXA contract is paused."""
    selector = Web3.keccak(text="paused()")[:4]
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [
            {"to": ELHEXA_CONTRACT, "data": "0x" + selector.hex()},
            "latest",
        ],
        "id": 1,
    }
    try:
        resp = requests.post(rpc_url, json=payload, timeout=15)
        result = resp.json().get("result", "0x")
        return int(result, 16) != 0
    except Exception:
        return False


def _portal_mapping_headers() -> dict:
    return {
        "accept": "application/json, text/plain, */*",
        "referer": "https://portal.soneium.org/en/profile/",
        "user-agent": USER_AGENT,
    }


def _fetch_smart_accounts_from_mapping(
    eoa_address: str,
    proxy: str | None,
    proxy_pool: list[str | None] | None = None,
) -> list[str]:
    attempts: list[str | None] = []
    if proxy and str(proxy).strip():
        attempts.append(str(proxy).strip())
    for item in proxy_utils.nonempty_proxies(proxy_pool or []):
        if item not in attempts:
            attempts.append(item)
    if not attempts:
        attempts = [None]

    for use_proxy in attempts:
        try:
            sess = requests.Session()
            sess.trust_env = False
            kwargs: dict = {
                "params": {"eoaAddress": eoa_address},
                "headers": _portal_mapping_headers(),
                "timeout": (5, 15),
            }
            if use_proxy:
                kwargs["proxies"] = {"http": use_proxy, "https": use_proxy}
            resp = sess.get(PORTAL_MAPPING_URL, **kwargs)
            if resp.status_code == 404:
                logger.debug(f"[ELHEXA] mapping 404 for {eoa_address}")
                return []
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.debug(
                f"[ELHEXA] mapping fetch fail {_proxy_host_port(use_proxy)}: {exc}"
            )
            continue

        if not isinstance(data, dict):
            logger.debug(f"[ELHEXA] mapping unexpected payload type: {type(data).__name__}")
            return []

        found: list[str] = []
        for raw in data.get("smartAccounts") or []:
            if isinstance(raw, str) and Web3.is_address(raw):
                found.append(Web3.to_checksum_address(raw))
        logger.debug(f"[ELHEXA] mapping smartAccounts={found or '[]'}")
        return found

    return []


def _get_onchain_checkin_status(
    sa_address: str,
    rpc_url: str = "https://soneium-rpc.publicnode.com",
) -> dict | None:
    try:
        sa = Web3.to_checksum_address(sa_address)
        call_data = GET_CHECKIN_STATUS_SELECTOR + bytes.fromhex(sa.removeprefix("0x").zfill(64))
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": DAILY_CHECKIN_CONTRACT, "data": "0x" + call_data.hex()}, "latest"],
            "id": 1,
        }
        sess = requests.Session()
        sess.trust_env = False
        resp = sess.post(rpc_url, json=payload, timeout=15)
        resp.raise_for_status()
        result = str((resp.json() or {}).get("result") or "0x")
        raw = bytes.fromhex(result.removeprefix("0x"))
        if len(raw) < 128:
            logger.debug(f"[ELHEXA] getCheckInStatus short response for {sa}: {result[:42]}")
            return None
        return {
            "checked_today": int.from_bytes(raw[0:32], "big") != 0,
            "total": int.from_bytes(raw[32:64], "big"),
            "last_day": int.from_bytes(raw[64:96], "big"),
            "current_day": int.from_bytes(raw[96:128], "big"),
        }
    except Exception as exc:
        logger.debug(f"[ELHEXA] getCheckInStatus error for {sa_address}: {exc}")
        return None


def _is_checked_today_onchain(sa_address: str, rpc_url: str) -> bool:
    """True если контракт сообщает, что SA уже делал чекин сегодня."""
    status = _get_onchain_checkin_status(sa_address, rpc_url)
    if not status:
        return False
    logger.debug(
        f"[ELHEXA] on-chain SA={sa_address} "
        f"checked={status['checked_today']} total={status['total']} "
        f"lastDay={status['last_day']} currentDay={status['current_day']}"
    )
    return bool(status["checked_today"])


def _last_tx_hash(bucket: list[str]) -> str | None:
    return bucket[-1] if bucket else None


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _proxy_host_port(proxy: str | None) -> str:
    if not proxy:
        return "(нет)"
    s = str(proxy).strip()
    if s.startswith(("http://", "https://", "socks5://")):
        parsed = urlparse(s)
        if parsed.hostname:
            return f"{parsed.hostname}:{parsed.port}" if parsed.port else str(parsed.hostname)
    return s[:36] + "..." if len(s) > 36 else s


def _browser_proxy_preflight_ok(proxy: str | None, app_url: str) -> bool:
    """
    Быстрый health-check прокси до запуска Camoufox.
    Цель: отсечь мёртвые/битые прокси, не открывая браузер заведомо с ошибкой.
    """
    if not proxy or not str(proxy).strip():
        return True

    from curl_cffi import requests as curl_requests

    connect_timeout = _env_float("BONUS9_ELHEXA_PROXY_CHECK_CONNECT_TIMEOUT", 3.0)
    read_timeout = _env_float("BONUS9_ELHEXA_PROXY_CHECK_READ_TIMEOUT", 8.0)
    test_url = (os.environ.get("BONUS9_ELHEXA_PROXY_CHECK_URL") or "").strip() or app_url or APP_URL

    sess = curl_requests.Session(impersonate="chrome124")
    sess.trust_env = False
    sess.proxies = {"http": proxy, "https": proxy}
    try:
        resp = sess.get(
            test_url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            allow_redirects=True,
            timeout=(connect_timeout, read_timeout),
        )
        status = int(getattr(resp, "status_code", 0) or 0)
        if 200 <= status < 500 and status != 407:
            logger.debug(f"[ELHEXA] proxy precheck OK {status} {_proxy_host_port(proxy)}")
            return True
        logger.debug(f"[ELHEXA] proxy precheck bad HTTP {status} {_proxy_host_port(proxy)}")
        return False
    except Exception as exc:
        logger.debug(f"[ELHEXA] proxy precheck fail {_proxy_host_port(proxy)}: {exc}")
        return False


def _select_alive_browser_proxy(
    proxy: str | None,
    proxy_pool: list[str | None] | None,
    app_url: str,
) -> str | None:
    """
    Выбрать рабочий прокси для браузера: сначала текущий, затем альтернативы из proxy_pool.
    """
    candidates: list[str] = []
    if proxy and str(proxy).strip():
        candidates.append(str(proxy).strip())
    for item in proxy_utils.nonempty_proxies(proxy_pool or []):
        if item not in candidates:
            candidates.append(item)

    if not candidates:
        return proxy

    max_attempts = min(
        len(candidates),
        max(1, int(os.environ.get("BONUS9_ELHEXA_BROWSER_PROXY_ATTEMPTS", str(len(candidates))))),
    )

    for idx, candidate in enumerate(candidates[:max_attempts], 1):
        if _browser_proxy_preflight_ok(candidate, app_url):
            if candidate != proxy:
                logger.warning(
                    f"[ELHEXA] browser proxy rotate -> {_proxy_host_port(candidate)}"
                )
            return candidate
        if idx < max_attempts:
            logger.warning(
                f"[ELHEXA] browser proxy dead {_proxy_host_port(candidate)} -> next ({idx}/{max_attempts})"
            )

    logger.error("[ELHEXA] нет рабочего прокси для браузера, playwright не запускаем")
    return None


def _response_might_contain_userop_receipt(url: str) -> bool:
    """Релеи / bundler / RPC часто без слова «bundler» в URL — всё равно ищем transactionHash в теле."""
    u = (url or "").lower()
    keys = (
        "bundler",
        "paymaster",
        "startale",
        "useroperation",
        "userop",
        "pimlico",
        "biconomy",
        "stackup",
        "alchemy",
        "elhexa.io",
        "soneium",
    )
    return any(k in u for k in keys)


async def _capture_bundler_tx_hash(response, bucket: list[str]) -> None:
    if not _response_might_contain_userop_receipt(response.url or ""):
        return
    try:
        text = await response.text()
    except Exception:
        return
    if "transactionHash" not in text:
        return
    for m in re.finditer(r'"transactionHash"\s*:\s*"(0x[a-fA-F]{64})"', text):
        h = m.group(1)
        if h not in bucket:
            bucket.append(h)


def _parse_elhexa_checkin_state(data: dict | None) -> dict | None:
    if not isinstance(data, dict):
        return None
    raw = data.get("Results")
    if not isinstance(raw, dict):
        raw = data.get("results")
    if not isinstance(raw, dict):
        raw = data

    def _normalize_int_list(value) -> list[int]:
        if value is None or value == "":
            return []
        if isinstance(value, (list, tuple, set)):
            seq = list(value)
        else:
            seq = [value]
        out: list[int] = []
        for item in seq:
            try:
                out.append(int(item))
            except Exception:
                continue
        return out

    claimed_ids = _normalize_int_list(raw.get("claimedIds"))
    today_ids = _normalize_int_list(raw.get("todayIds"))
    try:
        current_day = int(raw.get("currentDay")) if raw.get("currentDay") is not None else None
    except Exception:
        current_day = None

    return {
        "phase": raw.get("phase"),
        "current_day": current_day,
        "claimed_ids": claimed_ids,
        "today_ids": today_ids,
    }


def _derive_today_free_checkin_id(state: dict | None) -> int | None:
    if not state:
        return None
    today_ids = state.get("today_ids") or []
    if len(today_ids) == 1:
        return int(today_ids[0])
    current_day = state.get("current_day")
    if isinstance(current_day, int) and 1 <= current_day <= 7:
        return 1 + (current_day - 1) * 3
    return None


def _checkin_api_says_claimed(state: dict | None) -> bool:
    if not state:
        return False
    claimed_ids = set(state.get("claimed_ids") or [])
    today_ids = set(state.get("today_ids") or [])
    if claimed_ids and today_ids and claimed_ids.intersection(today_ids):
        return True
    today_free_id = _derive_today_free_checkin_id(state)
    return bool(today_free_id and today_free_id in claimed_ids)


async def _capture_elhexa_checkin_request_meta(response, bucket: dict) -> None:
    if "/api/v1/checkin" not in (response.url or "").lower():
        return
    try:
        headers = await response.request.all_headers()
    except Exception:
        try:
            headers = response.request.headers
        except Exception:
            return

    out: dict[str, str] = {}
    for key in (
        "authorization",
        "cookie",
        "x-user-id",
        "x-client-version",
        "user-agent",
        "accept",
        "origin",
        "referer",
    ):
        val = headers.get(key)
        if val:
            out[key] = val
    if "accept" not in out:
        out["accept"] = "application/json"
    if "origin" not in out:
        out["origin"] = "https://game.elhexa.io"
    if "referer" not in out:
        out["referer"] = "https://game.elhexa.io/"

    if out:
        bucket.clear()
        bucket.update(out)


async def _capture_elhexa_checkin_state(response, bucket: dict) -> None:
    if "/api/v1/checkin" not in (response.url or "").lower():
        return
    try:
        text = await response.text()
        data = json.loads(text)
    except Exception:
        return

    state = _parse_elhexa_checkin_state(data)
    if not state:
        return

    changed = state != bucket
    bucket.clear()
    bucket.update(state)
    if changed:
        logger.debug(
            "[ELHEXA] checkin api "
            f"day={state.get('current_day')} today={state.get('today_ids')} claimed={state.get('claimed_ids')}"
        )


def _fetch_elhexa_checkin_state(meta: dict | None, proxy: str | None) -> dict | None:
    if not meta:
        return None
    headers = {
        "accept": meta.get("accept", "application/json"),
        "origin": meta.get("origin", "https://game.elhexa.io"),
        "referer": meta.get("referer", "https://game.elhexa.io/"),
        "user-agent": meta.get("user-agent", USER_AGENT),
    }
    for key in ("authorization", "cookie", "x-user-id", "x-client-version"):
        val = meta.get(key)
        if val:
            headers[key] = val

    try:
        sess = requests.Session()
        sess.trust_env = False
        kwargs: dict = {"headers": headers, "timeout": 30}
        if proxy:
            kwargs["proxies"] = {"http": proxy, "https": proxy}
        resp = sess.get(f"{ELHEXA_API_BASE}/checkin", **kwargs)
        if not resp.ok:
            logger.debug(f"[ELHEXA] active checkin api http={resp.status_code}")
            return None
        return _parse_elhexa_checkin_state(resp.json())
    except Exception as exc:
        logger.debug(f"[ELHEXA] active checkin api error: {exc}")
        return None


async def _refresh_elhexa_checkin_state(
    proxy: str | None,
    request_meta: dict | None,
    bucket: dict,
    *,
    reason: str | None = None,
) -> dict | None:
    state = await asyncio.to_thread(_fetch_elhexa_checkin_state, request_meta, proxy)
    if not state:
        return bucket or None

    changed = state != bucket
    bucket.clear()
    bucket.update(state)
    if changed or reason:
        logger.debug(
            "[ELHEXA] active checkin api "
            f"day={state.get('current_day')} today={state.get('today_ids')} "
            f"claimed={state.get('claimed_ids')}"
            + (f" | {reason}" if reason else "")
        )
    return state


async def _goto_miniapps_if_needed(page, app_url: str) -> None:
    """После логина со страницы sign-up / log-in вернуться на мини-приложение ELHEXA."""
    await page.wait_for_timeout(1500)
    u = page.url or ""
    if "miniapps" in u and "elhexa" in u.lower():
        return
    if "/sign-up" in u or "/log-in" in u or u.rstrip("/").endswith("app.startale.com"):
        logger.debug("[ELHEXA] miniapps после auth")
        await page.goto(app_url, wait_until="domcontentloaded", timeout=120_000)


async def _try_explore_mini_apps_intro(page) -> None:
    """Закрыть приветственный диалог «Mini Apps are here» → Explore Mini Apps."""
    try:
        btn = page.get_by_role("button", name=re.compile(r"Explore Mini Apps", re.I))
        if await btn.count() > 0:
            await btn.first.click(timeout=8000)
            logger.debug("[ELHEXA] Explore Mini Apps")
            await page.wait_for_timeout(1500)
    except Exception:
        pass


def _all_page_targets(page) -> list:
    out: list = [page]
    try:
        for fr in page.frames:
            if fr != page.main_frame:
                out.append(fr)
    except Exception:
        pass
    return out


async def _frame_has_sign_message_modal(fr) -> bool:
    try:
        markers = (
            r"Sign message",
            r"Copy message details",
            r"wants you to sign in with your Ethereum account",
            r"Sign in to El Hexa",
        )
        for marker in markers:
            if await fr.locator(f"text=/{marker}/i").count() > 0:
                return True
    except Exception:
        pass
    return False


async def _first_visible_locator(locator, limit: int = 6):
    try:
        count = min(await locator.count(), limit)
    except Exception:
        return None

    for idx in range(count):
        item = locator.nth(idx)
        try:
            if await item.is_visible():
                return item
        except Exception:
            continue
    return None


async def _try_confirm_startale_sign_message_once(page) -> bool:
    for target in _all_page_targets(page):
        try:
            if not await _frame_has_sign_message_modal(target):
                continue
            btn = await _first_visible_locator(
                target.get_by_role("button", name=re.compile(r"^\s*Sign\s*$", re.I))
            )
            if btn is None:
                btn = await _first_visible_locator(
                    target.get_by_text(re.compile(r"^\s*Sign\s*$", re.I))
                )
            if btn is None:
                continue
            await btn.click(timeout=20_000)
            logger.debug("[ELHEXA] Sign message confirmed")
            await page.wait_for_timeout(1500)
            return True
        except Exception:
            continue
    return False


async def _confirm_startale_sign_message(page) -> bool:
    """Startale modal «Sign message» → Sign."""
    max_wait_s = int(os.environ.get("BONUS9_ELHEXA_SIGN_WAIT_S", "45"))
    poll_ms = int(os.environ.get("BONUS9_ELHEXA_SIGN_POLL_MS", "500"))
    deadline = time.monotonic() + max_wait_s

    while time.monotonic() < deadline:
        if await _try_confirm_startale_sign_message_once(page):
            return True
        await page.wait_for_timeout(poll_ms)
    return False


async def _find_sign_trigger(target):
    exact_sign = re.compile(r"^\s*Sign\s*$", re.I)
    candidates = (
        ("role=button", target.get_by_role("button", name=exact_sign)),
        ("button text", target.locator("button").filter(has_text=exact_sign)),
        ("[role=button]", target.locator("[role='button']").filter(has_text=exact_sign)),
        ("text", target.get_by_text(exact_sign)),
    )
    for source, locator in candidates:
        item = await _first_visible_locator(locator)
        if item is not None:
            return item, source
    return None, None


async def _try_sign_elhexa_login(page) -> bool:
    """После входа в miniapp сначала жмём Sign, затем подтверждаем Startale Sign message."""
    max_wait_s = int(os.environ.get("BONUS9_ELHEXA_SIGN_TRIGGER_WAIT_S", "25"))
    poll_ms = int(os.environ.get("BONUS9_ELHEXA_SIGN_POLL_MS", "500"))
    deadline = time.monotonic() + max_wait_s
    clicked = False
    last_click_at = 0.0

    while time.monotonic() < deadline:
        if await _try_confirm_startale_sign_message_once(page):
            return True

        for target in _all_page_targets(page):
            try:
                # Не долбим кликом слишком часто: даём модалке Startale появиться.
                if clicked and (time.monotonic() - last_click_at) < 2.0:
                    continue
                sign, source = await _find_sign_trigger(target)
                if sign is None:
                    continue
                await sign.click(timeout=10_000)
                logger.debug(f"[ELHEXA] Sign trigger clicked ({source})")
                clicked = True
                last_click_at = time.monotonic()
                await page.wait_for_timeout(800)
            except Exception:
                continue
        await page.wait_for_timeout(poll_ms)

    if clicked:
        logger.debug("[ELHEXA] Sign clicked, modal not detected")
    else:
        logger.debug("[ELHEXA] Sign button not found")
    return clicked


async def _reload_elhexa_miniapp(page, app_url: str):
    """Перезагрузить miniapp ELHEXA и дождаться iframe игры."""
    logger.debug("[ELHEXA] reload miniapp для повторного поиска Sign")
    await page.goto(app_url, wait_until="domcontentloaded", timeout=120_000)
    await _goto_miniapps_if_needed(page, app_url)
    try:
        await page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:
        pass
    await page.wait_for_timeout(2000)
    await _try_explore_mini_apps_intro(page)
    logger.debug("[ELHEXA] ждём iframe game.elhexa.io после reload")
    return await _wait_elhexa_game_frame(page)


async def _ensure_elhexa_sign(page, app_url: str, game_frame):
    """
    Sign обязателен перед gift/check-in.
    Если кнопка/модалка не найдена, перезагружаем miniapp и повторяем попытку.
    """
    max_reload_attempts = int(os.environ.get("BONUS9_ELHEXA_SIGN_RELOAD_ATTEMPTS", "2"))

    for attempt in range(max_reload_attempts + 1):
        logger.debug("[ELHEXA] сначала Sign, потом gift/check-in")
        if await _try_sign_elhexa_login(page):
            await page.wait_for_timeout(1500)
            return game_frame
        if attempt >= max_reload_attempts:
            break
        logger.debug(
            f"[ELHEXA] Sign не найден, reload miniapp и повтор ({attempt + 1}/{max_reload_attempts})"
        )
        game_frame = await _reload_elhexa_miniapp(page, app_url)
        if not game_frame:
            logger.error("[ELHEXA] iframe game.elhexa.io не появился после reload")
            return None

    logger.error("[ELHEXA] Sign не найден после reload, останавливаем ELHEXA")
    return None


async def _wait_elhexa_game_frame(page, timeout_ms: int = 120_000):
    """Дождаться iframe с game.elhexa.io (Cocos canvas)."""
    deadline = time.monotonic() + timeout_ms / 1000.0
    while time.monotonic() < deadline:
        for fr in page.frames:
            try:
                if "game.elhexa.io" in (fr.url or ""):
                    return fr
            except Exception:
                continue
        await page.wait_for_timeout(500)
    return None


async def _canvas_click_fraction(canvas, rx: float, ry: float) -> bool:
    """Клик по canvas в долях ширины/высоты (0..1) через page.mouse с human-like движением."""
    try:
        await canvas.wait_for(state="visible", timeout=30_000)
        await canvas.scroll_into_view_if_needed(timeout=10_000)
        box = await canvas.bounding_box()
        if not box:
            return False
        page = canvas.page

        target_x = box["x"] + box["width"] * rx
        target_y = box["y"] + box["height"] * ry

        # Человек редко телепортирует курсор строго в точку: делаем подвод с небольшим разбросом.
        approach_x = target_x + random.uniform(-24, 24)
        approach_y = target_y + random.uniform(-16, 16)

        await page.mouse.move(approach_x, approach_y, steps=random.randint(8, 16))
        await page.wait_for_timeout(random.randint(40, 110))
        await page.mouse.move(target_x, target_y, steps=random.randint(10, 22))
        await page.wait_for_timeout(random.randint(25, 80))
        await page.mouse.down()
        await page.wait_for_timeout(random.randint(45, 120))
        await page.mouse.up()
        await page.wait_for_timeout(random.randint(60, 140))
        return True
    except Exception:
        return False


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


async def _page_has_tx_modal(page) -> bool:
    for target in _all_page_targets(page):
        try:
            if await _frame_has_tx_modal(target):
                return True
        except Exception:
            continue
    return False


async def _canvas_click_candidates(
    canvas,
    candidates: list[tuple[float, float]],
    *,
    page=None,
    stop_on_tx_modal: bool = False,
    pause_ms: int = 700,
    log_label: str | None = None,
) -> bool:
    for idx, (rx, ry) in enumerate(candidates, 1):
        ok = await _canvas_click_fraction(canvas, _clamp01(rx), _clamp01(ry))
        if not ok:
            continue
        if log_label:
            logger.debug(
                f"[ELHEXA] {log_label} click #{idx} @ ({_clamp01(rx):.3f}, {_clamp01(ry):.3f})"
            )
        if pause_ms > 0:
            await canvas.page.wait_for_timeout(pause_ms)
        if stop_on_tx_modal and page and await _page_has_tx_modal(page):
            return True
    return False


async def _flow_gift_airdrop_and_rewards(game_frame) -> None:
    """
    Gift Airdrop: тап по подарку → модалка REWARDS → Accept (всё на canvas).
    Используем одну и ту же безопасную точку для gift и Accept, чтобы не попадать по Day-tabs.
    """
    canvas = game_frame.locator("canvas").first
    ax = _env_float("BONUS9_ELHEXA_CANVAS_ACCEPT_X", 0.5)
    ay = _env_float("BONUS9_ELHEXA_CANVAS_ACCEPT_Y", 0.58)
    if await _canvas_click_fraction(canvas, ax, ay):
        logger.debug("[ELHEXA] canvas gift")
    await game_frame.page.wait_for_timeout(5000)
    if await _canvas_click_fraction(canvas, ax, ay):
        logger.debug("[ELHEXA] canvas Accept")
    await game_frame.page.wait_for_timeout(1500)


async def _flow_checkin_bonus_free_tier(game_frame) -> None:
    """CHECK-IN BONUS: бесплатный ряд в Cocos canvas, без ожидания DOM-кнопки."""
    canvas = game_frame.locator("canvas").first

    # Даём сцене дорисоваться после Gift/Rewards, затем кликаем по координате в canvas.
    wait_ms = int(float(os.environ.get("BONUS9_ELHEXA_CANVAS_CHECKIN_DELAY_S", "5")) * 1000)
    logger.debug("[ELHEXA] CHECK-IN $0 canvas-only, ждём сцену")
    await game_frame.page.wait_for_timeout(wait_ms)

    bx = _env_float("BONUS9_ELHEXA_CANVAS_CHECKIN_X", 0.75)
    by = _env_float("BONUS9_ELHEXA_CANVAS_CHECKIN_Y", 0.55)
    x_offsets = (0.0, 0.04, 0.08, -0.03, 0.11)
    y_offsets = (0.0, -0.03, 0.03, -0.05, 0.05)
    candidates: list[tuple[float, float]] = []
    for dx in x_offsets:
        for dy in y_offsets:
            pt = (_clamp01(bx + dx), _clamp01(by + dy))
            if pt not in candidates:
                candidates.append(pt)

    got_tx_modal = await _canvas_click_candidates(
        canvas,
        candidates,
        page=game_frame.page,
        stop_on_tx_modal=True,
        pause_ms=650,
        log_label="canvas $0",
    )
    if got_tx_modal:
        logger.debug("[ELHEXA] canvas $0 -> Approve transaction opened")
    else:
        logger.debug("[ELHEXA] canvas $0 clicked, modal not detected yet")
    await game_frame.page.wait_for_timeout(700)


async def _checkin_free_tier_claimed_in_canvas(game_frame) -> bool:
    """
    Fallback для Cocos canvas: смотрим визуальное состояние free action-кнопки.
    Если кнопка уже серая (Claimed), а слева виден зелёный claimed-mark, Confirm не ждём.
    """
    try:
        stats = await game_frame.evaluate(
            """(cfg) => {
                const canvas = document.querySelector('canvas');
                if (!canvas) return null;
                const ctx = canvas.getContext('2d', { willReadFrequently: true });
                if (!ctx) return null;

                function regionStats(cx, cy, rw, rh) {
                    const x = Math.max(0, Math.floor((cx - rw / 2) * canvas.width));
                    const y = Math.max(0, Math.floor((cy - rh / 2) * canvas.height));
                    const w = Math.max(1, Math.min(canvas.width - x, Math.floor(rw * canvas.width)));
                    const h = Math.max(1, Math.min(canvas.height - y, Math.floor(rh * canvas.height)));
                    const data = ctx.getImageData(x, y, w, h).data;

                    let rSum = 0, gSum = 0, bSum = 0;
                    let gray = 0, green = 0;
                    const total = data.length / 4 || 1;

                    for (let i = 0; i < data.length; i += 4) {
                        const r = data[i];
                        const g = data[i + 1];
                        const b = data[i + 2];
                        rSum += r;
                        gSum += g;
                        bSum += b;

                        if (Math.abs(r - g) <= 18 && Math.abs(g - b) <= 18 && r >= 70 && r <= 230) gray += 1;
                        if (g >= 110 && g - r >= 18 && g - b >= 8) green += 1;
                    }

                    return {
                        avg_r: rSum / total,
                        avg_g: gSum / total,
                        avg_b: bSum / total,
                        gray_ratio: gray / total,
                        green_ratio: green / total,
                    };
                }

                const rows = [];
                for (const rowY of cfg.row_ys) {
                    rows.push({
                        row_y: rowY,
                        action: regionStats(cfg.action_x, rowY, cfg.action_w, cfg.action_h),
                        badge: regionStats(cfg.badge_x, rowY, cfg.badge_w, cfg.badge_h),
                    });
                }
                return { rows };
            }""",
            {
                "action_x": _env_float("BONUS9_ELHEXA_CANVAS_FREE_ACTION_X", 0.72),
                "action_w": _env_float("BONUS9_ELHEXA_CANVAS_FREE_ACTION_W", 0.12),
                "action_h": _env_float("BONUS9_ELHEXA_CANVAS_FREE_ACTION_H", 0.08),
                "badge_x": _env_float("BONUS9_ELHEXA_CANVAS_CLAIMED_BADGE_X", 0.18),
                "badge_w": _env_float("BONUS9_ELHEXA_CANVAS_CLAIMED_BADGE_W", 0.12),
                "badge_h": _env_float("BONUS9_ELHEXA_CANVAS_CLAIMED_BADGE_H", 0.10),
                "row_ys": [
                    _env_float("BONUS9_ELHEXA_CANVAS_ROW_Y0", 0.44),
                    _env_float("BONUS9_ELHEXA_CANVAS_ROW_Y1", 0.48),
                    _env_float("BONUS9_ELHEXA_CANVAS_ROW_Y2", 0.52),
                    _env_float("BONUS9_ELHEXA_CANVAS_ROW_Y3", 0.56),
                    _env_float("BONUS9_ELHEXA_CANVAS_ROW_Y4", 0.60),
                    _env_float("BONUS9_ELHEXA_CANVAS_ROW_Y5", 0.64),
                ],
            },
        )
    except Exception as exc:
        logger.debug(f"[ELHEXA] claimed canvas fallback unavailable: {exc}")
        return False

    if not stats:
        return False

    best_row = None
    best_score = -1.0
    for row in stats.get("rows") or []:
        action = row.get("action") or {}
        badge = row.get("badge") or {}
        action_green = (
            float(action.get("green_ratio", 0.0)) >= 0.08
            or (float(action.get("avg_g", 0.0)) - float(action.get("avg_r", 0.0))) >= 12.0
        )
        action_gray = (
            float(action.get("gray_ratio", 0.0)) >= 0.08
            and abs(float(action.get("avg_r", 0.0)) - float(action.get("avg_g", 0.0))) <= 24.0
            and abs(float(action.get("avg_g", 0.0)) - float(action.get("avg_b", 0.0))) <= 24.0
        )
        badge_green = float(badge.get("green_ratio", 0.0)) >= 0.03
        score = float(action.get("gray_ratio", 0.0)) + float(badge.get("green_ratio", 0.0)) - float(action.get("green_ratio", 0.0))

        if score > best_score:
            best_score = score
            best_row = {
                "row_y": row.get("row_y"),
                "action_gray": float(action.get("gray_ratio", 0.0)),
                "action_green": float(action.get("green_ratio", 0.0)),
                "badge_green": float(badge.get("green_ratio", 0.0)),
            }

        if action_gray and badge_green and not action_green:
            logger.debug(
                "[ELHEXA] Claimed detected in canvas "
                f"(row_y={row.get('row_y')}, action gray={action.get('gray_ratio', 0):.2f}, "
                f"action green={action.get('green_ratio', 0):.2f}, badge green={badge.get('green_ratio', 0):.2f})"
            )
            return True

    if best_row:
        logger.debug(
            "[ELHEXA] canvas claimed probe best "
            f"(row_y={best_row['row_y']}, action gray={best_row['action_gray']:.2f}, "
            f"action green={best_row['action_green']:.2f}, badge green={best_row['badge_green']:.2f})"
        )
    return False


def _has_checkin_modal_context(text: str) -> bool:
    low = (text or "").lower()
    markers = (
        "check-in bonus",
        "check in bonus",
        "special 7-day on-chain check-in",
        "special 7-day on-chain check in",
        "7-day on-chain",
        "on-chain check-in",
        "on-chain check in",
        "check-in rules",
        "check in rules",
    )
    return any(marker in low for marker in markers) or bool(re.search(r"\bday\s*[1-7]\b", low))


def _has_zero_price_option(text: str) -> bool:
    # Важно: $0.29 / $0.99 — это платные опции, они не должны считаться бесплатным чек-ином.
    return bool(re.search(r"\$\s*0(?:[.,]0+)?(?![\d.,])", text or "", re.I))


async def _checkin_free_tier_claimed_in_ui(page, game_fr) -> bool:
    """
    Бесплатный день уже отмечен (Claimed) — модалка Approve/Confirm не появится.
    Реальный «Claimed» после чекина часто только на canvas (Cocos) — в DOM его может не быть;
    тогда успех ловим по порталу / tx из сети (_wait_checkin_outcome).
    """
    targets: list = []
    for target in _all_page_targets(page):
        if target not in targets:
            targets.append(target)
    if game_fr and game_fr not in targets:
        targets.append(game_fr)

    for t in targets:
        try:
            raw = await t.evaluate(
                """() => {
                  const b = document.body;
                  return b && b.innerText ? b.innerText : '';
                }"""
            )
            low = (raw or "").lower()
            if "claimed" in low and _has_checkin_modal_context(low) and not _has_zero_price_option(low):
                return True

            if await t.locator("text=/\\bClaimed\\b/i").count() > 0:
                if await t.locator(
                    "text=/CHECK-IN BONUS|Special 7-Day On-chain Check-?in|Check-In Rules|Day\\s*[1-7]|On-chain/i"
                ).count() > 0:
                    return True
        except Exception:
            continue
    try:
        html = (await page.content()).lower()
        if "claimed" in html and _has_checkin_modal_context(html) and not _has_zero_price_option(html):
            return True
    except Exception:
        pass
    return False


async def _checkin_free_tier_claimed(page, game_fr, checkin_state: dict | None = None) -> bool:
    if _checkin_api_says_claimed(checkin_state):
        logger.debug("[ELHEXA] Claimed detected by /api/v1/checkin")
        return True
    if await _checkin_free_tier_claimed_in_ui(page, game_fr):
        return True
    return await _checkin_free_tier_claimed_in_canvas(game_fr)


async def _check_claimed_after_accept(
    page,
    game_fr,
    checkin_state: dict | None = None,
    *,
    proxy: str | None = None,
    checkin_request_meta: dict | None = None,
    wait_s: float = 6.0,
    poll_ms: int = 500,
) -> bool:
    """Сразу после Gift/Rewards Accept даём UI/API/canvas несколько секунд показать Claimed."""
    deadline = time.monotonic() + wait_s
    next_refresh = 0.0
    while time.monotonic() < deadline:
        now = time.monotonic()
        if checkin_request_meta and now >= next_refresh:
            await _refresh_elhexa_checkin_state(
                proxy,
                checkin_request_meta,
                checkin_state if isinstance(checkin_state, dict) else {},
                reason="after Accept",
            )
            next_refresh = now + 1.5
        if await _checkin_free_tier_claimed(page, game_fr, checkin_state):
            logger.info("[ELHEXA] Claimed после Accept, пропуск")
            return True
        await page.wait_for_timeout(poll_ms)
    return False


async def _frame_has_tx_modal(fr) -> bool:
    """Признаки модалки подписи UserOp (Startale / ELHEXA check-in)."""
    try:
        if await fr.locator("text=/Approve transaction/i").count() > 0:
            return True
        if await fr.locator("text=/Contract interaction/i").count() > 0:
            return True
        if await fr.locator("text=/Soneium Mainnet/i").count() > 0:
            return True
        if await fr.locator("text=/0xb97d/i").count() > 0:
            return True
    except Exception:
        pass
    return False


async def _confirm_startale_transaction(page) -> bool:
    """
    Оверлей Startale «Approve transaction» → Confirm.
    Ищем во всех фреймах (Dynamic может быть в iframe); кнопка — по роли или тексту.
    """
    max_wait_s = int(os.environ.get("BONUS9_ELHEXA_CONFIRM_WAIT_S", "10"))
    poll_ms = int(os.environ.get("BONUS9_ELHEXA_CONFIRM_POLL_MS", "700"))
    deadline = time.monotonic() + max_wait_s

    await page.wait_for_timeout(2500)

    def _frames():
        out: list = [page]
        try:
            for fr in page.frames:
                if fr != page.main_frame:
                    out.append(fr)
        except Exception:
            pass
        return out

    last_log = 0.0
    while time.monotonic() < deadline:
        for fr in _frames():
            try:
                if not await _frame_has_tx_modal(fr):
                    continue
                btn = fr.get_by_role("button", name=re.compile(r"Confirm", re.I))
                if await btn.count() > 0:
                    await btn.first.click(timeout=20_000)
                    logger.debug("[ELHEXA] Confirm Approve")
                    return True
                alt = fr.locator("button").filter(has_text=re.compile(r"^\s*Confirm\s*$", re.I))
                if await alt.count() > 0:
                    await alt.first.click(timeout=20_000)
                    logger.debug("[ELHEXA] Confirm fallback")
                    return True
            except Exception:
                continue

        now = time.monotonic()
        if now - last_log > 4.0:
            last_log = now
            logger.debug(f"[ELHEXA] ждём Confirm (~{max(0, int(deadline - now))}s)")
        await page.wait_for_timeout(poll_ms)

    try:
        btn = page.get_by_role("button", name=re.compile(r"Confirm", re.I))
        if await btn.count() > 0:
            await btn.first.click(timeout=15_000)
            logger.debug("[ELHEXA] Confirm только main frame")
            return True
    except Exception:
        pass
    return False


async def _portal_elhexa_increased(
    address: str,
    proxy: str | None,
    before: int,
) -> bool:
    """Портал Soneium: счётчик elhexa вырос — чекин зачтён (работает и когда Claimed только на canvas)."""
    try:
        raw = await asyncio.to_thread(get_bonus_dapp_data, address, proxy)
    except Exception:
        return False
    if not raw:
        return False
    st = parse_account_status(raw)
    return int(st.get("elhexa", 0)) > before


async def _wait_checkin_outcome(
    page,
    tx_bucket: list[str],
    game_fr=None,
    max_wait_s: int = 60,
    *,
    address: str | None = None,
    proxy: str | None = None,
    portal_elhexa_before: int | None = None,
    checkin_state: dict | None = None,
) -> bool:
    """
    После Confirm: успех — хеш из ответов релея, «Claimed» в DOM (если есть), или рост elhexa на портале.
    """
    poll_ms = int(os.environ.get("BONUS9_ELHEXA_CLAIMED_POLL_MS", "800"))
    portal_first = float(os.environ.get("BONUS9_ELHEXA_PORTAL_FIRST_DELAY_S", "2"))
    portal_every = float(os.environ.get("BONUS9_ELHEXA_PORTAL_POLL_S", "3"))
    deadline = time.monotonic() + max_wait_s
    next_portal = time.monotonic() + portal_first
    while time.monotonic() < deadline:
        if tx_bucket:
            return True
        now = time.monotonic()
        if (
            address
            and portal_elhexa_before is not None
            and now >= next_portal
        ):
            try:
                if await _portal_elhexa_increased(address, proxy, portal_elhexa_before):
                    logger.debug("[ELHEXA] портал: elhexa вырос после Approve (canvas/UI без текста в DOM)")
                    return True
            except Exception:
                pass
            next_portal = now + portal_every
        try:
            if game_fr and await _checkin_free_tier_claimed(page, game_fr, checkin_state):
                logger.debug("[ELHEXA] Claimed после Approve (api/ui/canvas)")
                return True
        except Exception:
            pass
        try:
            html = (await page.content()).lower()
            if "claimed" in html and ("check-in" in html or "check in" in html or "bonus" in html):
                logger.debug("[ELHEXA] Claimed после Approve (DOM/HTML)")
                return True
        except Exception:
            pass
        await page.wait_for_timeout(poll_ms)
    return bool(tx_bucket)


async def _process_wallet(
    private_key: str,
    rpc_url: str,
    proxy: str | None,
    headless: bool,
    check_id: int,
    app_url: str,
    portal_elhexa_before: int,
    sa_address: str | None,
) -> bool:
    """
    Используем camoufox (Firefox) для SIWE авторизации как в startale_gm.
    """
    from camoufox.async_api import AsyncCamoufox
    from curl_cffi import requests as curl_requests
    import random
    
    acct = Account.from_key(private_key)
    address = acct.address
    wallet_uuid = str(uuid.uuid4())

    tx_bucket: list[str] = []
    checkin_state: dict = {}
    checkin_request_meta: dict = {}
    rpc = curl_requests.Session(impersonate="chrome124")
    if proxy:
        rpc.proxies = {"http": proxy, "https": proxy}

    px = _playwright_proxy(proxy)
    os_choice = random.choice(["windows", "macos"])
    
    # Тот же injected wallet и same-origin RPC endpoint, что и в startale_gm.
    wallet_script_inlined = MOCK_WALLET_SCRIPT.replace("%UUID%", wallet_uuid)

    async def _inject_wallet_route(route):
        try:
            response = await route.fetch(timeout=90_000)
            ct = response.headers.get("content-type", "")
            if "text/html" not in ct:
                await route.fulfill(response=response)
                return
            body = await response.text()
            injection = f"<script>{wallet_script_inlined}</script>"
            if "<head>" in body:
                body = body.replace("<head>", "<head>" + injection, 1)
            elif "<head " in body:
                idx = body.index("<head ")
                end = body.index(">", idx)
                body = body[:end + 1] + injection + body[end + 1:]
            else:
                body = injection + body
            await route.fulfill(response=response, body=body.encode("utf-8"))
        except Exception as _exc:
            msg = str(_exc).lower()
            if "request context disposed" in msg or "target page, context or browser has been closed" in msg:
                return
            logger.debug(f"[ELHEXA] route inject error: {_exc}")
            try:
                await route.continue_()
            except Exception:
                pass
    
    # RPC обработчик для эмуляции кошелька
    async def _rpc_handler(payload_json: str) -> str:
        try:
            data = json.loads(payload_json)
            method = data.get("method", "")
            params = data.get("params", [])

            result = None

            if method in ("eth_accounts", "eth_requestAccounts"):
                result = [address]

            elif method == "eth_chainId":
                result = CHAIN_HEX

            elif method == "wallet_requestPermissions":
                result = [{"parentCapability": "eth_accounts"}]

            elif method in ("wallet_revokePermissions", "wallet_addEthereumChain",
                            "wallet_switchEthereumChain"):
                result = None

            elif method == "wallet_getPermissions":
                result = []

            elif method == "personal_sign":
                msg_hex = params[0] if params else ""
                raw = bytes.fromhex(msg_hex[2:]) if msg_hex.startswith("0x") else msg_hex.encode()
                sig = acct.sign_message(encode_defunct(primitive=raw))
                result = "0x" + sig.signature.hex()

            elif method in ("eth_signTypedData_v4", "eth_signTypedData"):
                typed = params[1] if len(params) >= 2 else (params[0] if params else None)
                if typed is None:
                    raise RuntimeError("eth_signTypedData: missing typed data")
                if isinstance(typed, str):
                    typed = json.loads(typed)
                sig = acct.sign_typed_data(full_message=typed)
                result = "0x" + sig.signature.hex()

            elif method == "eth_sendTransaction":
                raise RuntimeError("eth_sendTransaction not supported — ELHEXA uses WaaS")

            else:
                resp = rpc.post(
                    rpc_url,
                    json={"jsonrpc": "2.0", "method": method, "params": params, "id": 1},
                    headers={"Content-Type": "application/json"},
                    timeout=60,
                )
                rdata = resp.json()
                if "error" in rdata:
                    raise RuntimeError(json.dumps(rdata["error"]))
                result = rdata.get("result")

            return json.dumps({"result": result})
        except Exception as exc:
            logger.debug(f"[ELHEXA RPC error] {exc}")
            return json.dumps({"error": str(exc)})
    
    async with AsyncCamoufox(
        headless=headless,
        os=os_choice,
        proxy=px,
        geoip=False,
    ) as browser:
        ctx = await browser.new_context()
        page = await ctx.new_page()

        await page.route(f"{APP_URL}/**", _inject_wallet_route)
        await page.route(APP_URL, _inject_wallet_route)

        async def _rpc_route(route):
            try:
                body = route.request.post_data
                item = json.loads(body) if body else {}
                result_json = await _rpc_handler(json.dumps(item))
                resp = json.loads(result_json)
                await route.fulfill(
                    status=200,
                    content_type="application/json",
                    body=json.dumps(resp),
                )
            except Exception as _rpc_exc:
                logger.debug(f"[ELHEXA] rpc_route error: {_rpc_exc}")
                try:
                    await route.fulfill(
                        status=200,
                        content_type="application/json",
                        body=json.dumps({"error": str(_rpc_exc)}),
                    )
                except Exception:
                    pass

        await page.route(RPC_ENDPOINT, _rpc_route)

        def _on_response(response) -> None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return
            loop.create_task(_capture_bundler_tx_hash(response, tx_bucket))
            loop.create_task(_capture_elhexa_checkin_state(response, checkin_state))
            loop.create_task(_capture_elhexa_checkin_request_meta(response, checkin_request_meta))

        page.on("response", _on_response)

        try:
            logger.debug(f"[ELHEXA] goto {APP_URL} для авторизации")
            await page.goto(APP_URL, wait_until="domcontentloaded", timeout=120_000)
            
            # Ждём редиректа на /sign-up — означает, что React SPA загрузился
            try:
                await page.wait_for_url("**/sign-up**", timeout=90_000)
                logger.debug("[ELHEXA] редирект на /sign-up выполнен")
            except Exception:
                logger.debug("[ELHEXA] /sign-up не дождались, продолжаем")

            # Ждём кнопки "Connect a wallet"
            connect_btn = page.locator(
                "button:has-text('Connect a wallet'), button:has-text('Connect Wallet')"
            ).first
            try:
                await connect_btn.wait_for(timeout=30_000)
            except Exception:
                logger.debug("[ELHEXA] кнопка Connect не появилась")

            if await connect_btn.count() > 0:
                logger.debug("[ELHEXA] connect wallet")
                await connect_btn.click()
                try:
                    await page.wait_for_selector(
                        "input[placeholder*='wallet'], input[placeholder*='Wallet'], "
                        "[placeholder*='Search'], input[type='search']",
                        timeout=8_000,
                    )
                    logger.debug("[ELHEXA] wallet modal opened")
                except Exception:
                    await page.wait_for_timeout(2000)

                mm_item = page.get_by_text("MetaMask", exact=True).first
                try:
                    await mm_item.wait_for(timeout=5_000)
                except Exception:
                    pass

                if await mm_item.count() > 0:
                    await mm_item.click()
                    logger.debug("[ELHEXA] MetaMask (fake) clicked")
                    # Ждём завершения SIWE-аутентификации: URL изменится с /sign-up
                    try:
                        await page.wait_for_url(
                            lambda url: "/sign-up" not in url,
                            timeout=30_000,
                        )
                        logger.debug(f"[ELHEXA] аутентифицирован, URL: {page.url}")
                    except Exception:
                        logger.debug("[ELHEXA] /sign-up timeout — ждём ещё")
                        await page.wait_for_timeout(5000)
                else:
                    logger.error("[ELHEXA] MetaMask не найден в модале")
                    return False
            else:
                logger.debug("[ELHEXA] Connect нет (уже подключено?)")
                await page.wait_for_timeout(3000)

            logger.debug("[ELHEXA] переходим на miniapps#elhexa")
            await page.goto(app_url, wait_until="domcontentloaded", timeout=120_000)

            await _goto_miniapps_if_needed(page, app_url)
            try:
                await page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                pass
            await page.wait_for_timeout(2000)

            await _try_explore_mini_apps_intro(page)

            logger.debug("[ELHEXA] ждём iframe game.elhexa.io")
            game_fr = await _wait_elhexa_game_frame(page)
            if not game_fr:
                logger.error("[ELHEXA] iframe game.elhexa.io не появился")
                return False

            game_fr = await _ensure_elhexa_sign(page, app_url, game_fr)
            if not game_fr:
                return False

            logger.debug("[ELHEXA] флоу gift→rewards→$0")
            skip_gift = os.environ.get("BONUS9_ELHEXA_SKIP_GIFT", "").strip().lower() in (
                "1",
                "true",
                "yes",
            )
            if not skip_gift:
                await _flow_gift_airdrop_and_rewards(game_fr)
                if await _check_claimed_after_accept(
                    page,
                    game_fr,
                    checkin_state,
                    proxy=proxy,
                    checkin_request_meta=checkin_request_meta,
                ):
                    db.mark_elhexa_done(address, period_id=current_period_id)
                    return True
            else:
                logger.debug("[ELHEXA] пропуск Gift Airdrop (BONUS9_ELHEXA_SKIP_GIFT)")
            await page.wait_for_timeout(2000)

            await _refresh_elhexa_checkin_state(
                proxy,
                checkin_request_meta,
                checkin_state,
                reason="before $0",
            )
            if await _checkin_free_tier_claimed(page, game_fr, checkin_state):
                logger.info("[ELHEXA] Claimed до $0, пропуск")
                db.mark_elhexa_done(address, period_id=current_period_id)
                return True
            await _flow_checkin_bonus_free_tier(game_fr)
            await page.wait_for_timeout(2000)

            await _refresh_elhexa_checkin_state(
                proxy,
                checkin_request_meta,
                checkin_state,
                reason="after $0",
            )
            if await _checkin_free_tier_claimed(page, game_fr, checkin_state):
                logger.info("[ELHEXA] Claimed после $0, пропуск")
                return True

            if not await _confirm_startale_transaction(page):
                await _refresh_elhexa_checkin_state(
                    proxy,
                    checkin_request_meta,
                    checkin_state,
                    reason="confirm missing",
                )
                if await _checkin_free_tier_claimed(page, game_fr, checkin_state):
                    logger.info("[ELHEXA] Claimed, Confirm не было — OK")
                    return True
                raw = get_bonus_dapp_data(address, proxy)
                if raw:
                    st2 = parse_account_status(raw)
                    p2 = int(st2.get("elhexa", 0))
                    if p2 > portal_elhexa_before:
                        logger.info(f"[ELHEXA] портал {portal_elhexa_before}→{p2}, OK (без Confirm)")
                        return True
                no_confirm_wait = int(os.environ.get("BONUS9_ELHEXA_NO_CONFIRM_OUTCOME_WAIT_S", "15"))
                logger.debug(f"[ELHEXA] Confirm не появился, коротко ждём outcome (~{no_confirm_wait}s)")
                success_wo_confirm = await _wait_checkin_outcome(
                    page,
                    tx_bucket,
                    game_fr=game_fr,
                    max_wait_s=no_confirm_wait,
                    address=address,
                    proxy=proxy,
                    portal_elhexa_before=portal_elhexa_before,
                    checkin_state=checkin_state,
                )
                if success_wo_confirm:
                    logger.info("[ELHEXA] outcome подтверждён без Confirm")
                    return True
                logger.error("[ELHEXA] нет Confirm")
                return False

            # После Confirm ждём пока кнопка станет Claimed
            logger.debug("[ELHEXA] ждём Claimed после Confirm...")
            deadline_claimed = time.monotonic() + 30
            while time.monotonic() < deadline_claimed:
                if await _checkin_free_tier_claimed(page, game_fr, checkin_state):
                    logger.info("[ELHEXA] Claimed найден!")
                    break
                await page.wait_for_timeout(1000)

            tx_wait = int(os.environ.get("BONUS9_ELHEXA_TX_WAIT", "120"))
            success = await _wait_checkin_outcome(
                page,
                tx_bucket,
                game_fr=game_fr,
                max_wait_s=tx_wait,
                address=address,
                proxy=proxy,
                portal_elhexa_before=portal_elhexa_before,
                checkin_state=checkin_state,
            )

            if success:
                logger.success("[ELHEXA] OK")
                th = _last_tx_hash(tx_bucket)
                if th:
                    logger.debug(f"[ELHEXA] tx из bundler: {th[:10]}…{th[-6:]}")
                if th and sa_address:
                    post_elhexa_checkin_verify(th, sa_address, check_id, proxy)
                return True

            logger.warning("[ELHEXA] таймаут после Confirm")
            return False

        except Exception as e:
            logger.error(f"[ELHEXA] {e}")
            return False


async def _do_elhexa_async(
    private_key: str,
    proxy: str | None,
    rpc_url: str,
    headless: bool,
    check_id: int,
    app_url: str,
    proxy_pool: list[str | None] | None = None,
) -> bool:
    acct = Account.from_key(private_key)
    eoa_address = acct.address

    # 1) Портал — источник истины о прогрессе квеста
    st = require_account_status(eoa_address, proxy, proxy_pool=proxy_pool)
    req = int(st.get("elhexa_required", 3))
    portal_cnt = int(st.get("elhexa", 0))
    el_done = bool(st.get("elhexa_done"))

    logger.info(
        f"[ELHEXA] {portal_cnt}/{req} done={el_done}"
    )

    if el_done or portal_cnt >= req:
        logger.info(f"[ELHEXA] квест закрыт {portal_cnt}/{req}, пропуск")
        return False

    # 2) Получаем SA свежим из portal mapping (без кеша БД)
    sa_list = _fetch_smart_accounts_from_mapping(eoa_address, proxy, proxy_pool)
    sa_address = sa_list[0] if sa_list else None

    if not sa_address:
        logger.warning("[ELHEXA] SA не найден в portal mapping, пропуск")
        return False

    # 3) On-chain: проверяем, был ли уже чекин сегодня.
    # Доверяем on-chain только если портал уже зафиксировал хотя бы один чекин для этого EOA:
    # при portal_cnt == 0 SA из mapping может быть не связан с этим EOA в системе портала.
    if portal_cnt > 0 and _is_checked_today_onchain(sa_address, rpc_url):
        logger.info(f"[ELHEXA] on-chain: already checked today ({sa_address}), браузер не запускаем")
        return False

    # 4) Контракт не на паузе?
    if is_paused(rpc_url):
        logger.warning("[ELHEXA] paused, пропуск")
        return False

    had_browser_proxies = bool(
        (proxy and str(proxy).strip()) or proxy_utils.nonempty_proxies(proxy_pool or [])
    )
    browser_proxy = _select_alive_browser_proxy(proxy, proxy_pool, app_url)
    if had_browser_proxies and not browser_proxy:
        return False
    proxy = browser_proxy

    logger.info(f"[ELHEXA] {eoa_address} camoufox")

    ok = await _process_wallet(
        private_key,
        rpc_url,
        proxy,
        headless,
        check_id,
        app_url,
        portal_elhexa_before=portal_cnt,
        sa_address=sa_address,
    )

    return ok


def do_elhexa_checkin(
    private_key: str,
    proxy: str | None = None,
    rpc_url: str = "https://soneium-rpc.publicnode.com",
    check_id: int = 1,
    headless: bool | None = None,
    proxy_pool: list[str | None] | None = None,
) -> bool:
    """
    Ежедневный чекин ELHEXA через мини-приложение Startale (Dynamic WaaS + bundler в браузере).

    Решение о запуске браузера принимается исключительно на основе:
    - статуса квеста с портала (require_account_status)
    - on-chain данных getCheckInStatus(SA) из portal mapping

    При падении с исключением — до 5 попыток с ротацией прокси из proxy_pool.
    """
    app_url = (os.environ.get("BONUS9_ELHEXA_APP_URL") or "").strip() or DEFAULT_APP_URL
    hl = headless
    if hl is None:
        raw = (
            os.environ.get("BONUS9_ELHEXA_HEADLESS", "")
            or os.environ.get("BONUS9_GM_HEADLESS", "1")
        ).strip().lower()
        hl = raw not in ("0", "false", "no", "off")

    max_attempts = int(os.environ.get("BONUS9_ELHEXA_MAX_ATTEMPTS", "5"))
    pool = proxy_utils.nonempty_proxies(proxy_pool or [])
    used_proxies: list[str] = []
    current_proxy = proxy

    for attempt in range(1, max_attempts + 1):
        try:
            return asyncio.run(
                _do_elhexa_async(
                    private_key, current_proxy, rpc_url, hl, check_id, app_url,
                    proxy_pool=proxy_pool,
                )
            )
        except Exception as exc:
            logger.warning(f"[ELHEXA] попытка {attempt}/{max_attempts} ошибка: {exc}")
            if attempt == max_attempts:
                logger.error("[ELHEXA] все попытки исчерпаны")
                return False
            if current_proxy and current_proxy not in used_proxies:
                used_proxies.append(current_proxy)
            next_proxy = proxy_utils.rotate_proxy(pool, current_proxy, exclude=used_proxies)
            if next_proxy:
                logger.info(f"[ELHEXA] ротация прокси → {proxy_utils.nonempty_proxies([next_proxy])}")
                current_proxy = next_proxy
            time.sleep(2)

    return False
