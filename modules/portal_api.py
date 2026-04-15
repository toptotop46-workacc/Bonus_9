"""Soneium Score Season 9 — Portal API."""

from __future__ import annotations

import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests
from tqdm import tqdm

from modules import logger, proxy_utils

PORTAL_URL = "https://portal.soneium.org/api/profile/bonus-dapp"

# Season 9 dapp IDs
DAPP_STARTALE    = "startale_9"
DAPP_SOUNDCHAINS = "soundchains_9"
DAPP_SUPERSTAKE  = "superstake_9"
DAPP_ELHEXA      = "elhexa_9"

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
]


def _headers() -> dict:
    return {
        "accept": "application/json, text/plain, */*",
        "accept-language": "ru,ru-RU;q=0.9,en-US;q=0.8,en;q=0.7",
        "referer": "https://portal.soneium.org/en/profile/",
        "user-agent": random.choice(_USER_AGENTS),
    }


# ── Low-level fetch ───────────────────────────────────────────────────────────

def get_bonus_dapp_data(
    address: str,
    proxy: str | None = None,
    *,
    retries: int | None = None,
    retry_delay: float | None = None,
    timeout: tuple[float, float] | None = None,
) -> list[Any] | None:
    """
    Сырой список dapp с портала. При обрыве/429/таймауте — повторы (как в require_account_status),
    иначе статус в меню «прыгает» между '-' и реальными значениями.
    """
    r = retries if retries is not None else max(
        1, int(os.environ.get("BONUS9_PORTAL_HTTP_RETRIES", "3"))
    )
    d = retry_delay if retry_delay is not None else float(
        os.environ.get("BONUS9_PORTAL_HTTP_RETRY_DELAY", "0.5")
    )
    req_timeout = timeout if timeout is not None else (
        float(os.environ.get("BONUS9_PORTAL_CONNECT_TIMEOUT", "6")),
        float(os.environ.get("BONUS9_PORTAL_READ_TIMEOUT", "18")),
    )
    for attempt in range(r):
        try:
            s = requests.Session()
            s.trust_env = False
            if proxy:
                s.proxies = {"http": proxy, "https": proxy}
            resp = s.get(
                PORTAL_URL,
                params={"address": address},
                headers=_headers(),
                timeout=req_timeout,
            )
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                try:
                    wait = min(60, int(float(ra))) if ra else None
                except ValueError:
                    wait = None
                if wait is None:
                    wait = min(30, int(2 ** attempt))
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            return None
        except Exception:
            if attempt < r - 1:
                time.sleep(d * (1.0 + 0.35 * attempt))
            continue
    return None


def _get_dapp(data: list[Any], dapp_id: str) -> dict | None:
    for item in data:
        if item.get("id") == dapp_id:
            return item
    return None


def _find_quest(dapp: dict, *keywords: str) -> dict | None:
    for q in (dapp.get("quests") or []):
        desc = (q.get("description") or "").lower()
        if any(kw.lower() in desc for kw in keywords):
            return q
    return None


# ── Status parsing ────────────────────────────────────────────────────────────

def parse_account_status(data: list[Any] | None) -> dict:
    """Parse all Season 9 quest fields from raw portal data into a flat dict."""
    result = {
        "swap_done":          False,
        "referral_done":      False,
        "gm":                 0,
        "gm_required":        5,
        "soundchains_done":   False,
        "superstake":         0,
        "superstake_required": 10,
        "elhexa":              0,
        "elhexa_required":     3,
        "elhexa_done":         False,
    }
    if not data:
        return result

    startale = _get_dapp(data, DAPP_STARTALE)
    if startale:
        q_swap = _find_quest(startale, "swap", "usdsc")
        if q_swap:
            result["swap_done"] = bool(q_swap.get("isDone"))
        q_ref = _find_quest(startale, "referral", "invite", "friend")
        if q_ref:
            result["referral_done"] = bool(q_ref.get("isDone"))
        q_gm = _find_quest(startale, "daily gm", "gm")
        if q_gm:
            result["gm"] = int(q_gm.get("completed", 0))
            result["gm_required"] = int(q_gm.get("required", 5))

    sc = _get_dapp(data, DAPP_SOUNDCHAINS)
    if sc:
        q = _find_quest(sc, "mint", "music", "track", "nft")
        result["soundchains_done"] = bool(q.get("isDone")) if q else bool(sc.get("isDone"))

    ss = _get_dapp(data, DAPP_SUPERSTAKE)
    if ss:
        q = _find_quest(ss, "claw", "round", "play", "game")
        if q:
            result["superstake"] = int(q.get("completed", 0))
            result["superstake_required"] = int(q.get("required", 10))

    elhexa = _get_dapp(data, DAPP_ELHEXA)
    if elhexa:
        q = _find_quest(elhexa, "check-in", "checkin", "daily")
        req_default = max(1, int(os.environ.get("BONUS9_ELHEXA_REQUIRED", "3")))
        if q:
            result["elhexa"] = int(q.get("completed", 0))
            result["elhexa_required"] = int(q.get("required", req_default))
            result["elhexa_done"] = bool(q.get("isDone")) or (
                result["elhexa"] >= result["elhexa_required"]
            )
        else:
            result["elhexa_required"] = req_default
            result["elhexa_done"] = bool(elhexa.get("isDone"))

    return result


def _next_portal_retry_proxy(
    pool: list[str | None] | None,
    failed: str | None,
) -> str | None:
    """Следующий прокси для повтора: случайный из pool, отличный от только что сбойного."""
    cleaned = proxy_utils.nonempty_proxies(pool) if pool else []
    if len(cleaned) <= 1:
        return failed
    others = [p for p in cleaned if p != failed]
    return random.choice(others if others else cleaned)


def require_account_status(
    address: str,
    proxy: str | None,
    *,
    proxy_pool: list[str | None] | None = None,
    retries: int | None = None,
    delay_sec: float | None = None,
) -> dict:
    """
    Свежий статус квестов с портала (parse_account_status) с повторами при сбое сети.
    Без успешного ответа модуль не должен выполнять on-chain / браузерные действия.
    Если передан proxy_pool с несколькими прокси, при каждой неудаче берётся другой случайный.
    """
    r = retries if retries is not None else max(
        1,
        int(
            os.environ.get(
                "BONUS9_PORTAL_RETRIES",
                os.environ.get("BONUS9_SWAP_PORTAL_RETRIES", "8"),
            )
        ),
    )
    d = delay_sec if delay_sec is not None else float(
        os.environ.get(
            "BONUS9_PORTAL_RETRY_SEC",
            os.environ.get("BONUS9_SWAP_PORTAL_RETRY_SEC", "2"),
        )
    )
    parsed: dict | None = None
    use_proxy: str | None = proxy
    for attempt in range(r):
        raw = get_bonus_dapp_data(address, use_proxy)
        if raw is not None:
            parsed = parse_account_status(raw)
            break
        if attempt < r - 1:
            logger.warning(
                f"[Portal] {address} нет ответа {attempt + 1}/{r} → {d:.0f}s"
            )
            time.sleep(d)
            use_proxy = _next_portal_retry_proxy(proxy_pool, use_proxy)
    if parsed is None:
        raise RuntimeError(
            f"[Portal] не удалось получить данные для {address} после {r} попыток"
        )
    return parsed


# ── Batch fetch ───────────────────────────────────────────────────────────────

def fetch_portal_data_batch(
    addresses: list[str],
    proxy_urls: list[str | None],
    batch_size: int = 50,
) -> dict[str, list[Any] | None]:
    """
    Параллельные запросы к порталу. Прокси привязан к индексу кошелька (match_proxy),
    а не random — иначе ответ то с одного endpoint, то с другого, статус «плавает».
    После первого прохода адреса с None дополнительно опрашиваются с другим прокси.
    """
    cleaned = proxy_utils.nonempty_proxies(proxy_urls)
    results: dict[str, list[Any] | None] = {}
    address_index = {addr: i for i, addr in enumerate(addresses)}
    bad_proxies: set[str] = set()
    bad_lock = threading.Lock()
    batch_timeout = (
        float(os.environ.get("BONUS9_PORTAL_BATCH_CONNECT_TIMEOUT", "3")),
        float(os.environ.get("BONUS9_PORTAL_BATCH_READ_TIMEOUT", "8")),
    )
    proxy_attempts = max(1, int(os.environ.get("BONUS9_PORTAL_BATCH_PROXY_ATTEMPTS", "3")))
    max_workers = max(
        1,
        int(
            os.environ.get(
                "BONUS9_PORTAL_BATCH_MAX_WORKERS",
                str(min(batch_size, 32)),
            )
        ),
    )

    def _mark_bad(proxy: str | None) -> None:
        if not proxy:
            return
        with bad_lock:
            bad_proxies.add(proxy)

    def _bad_snapshot() -> set[str]:
        with bad_lock:
            return set(bad_proxies)

    def _proxy_candidates(wallet_index: int) -> list[str | None]:
        if not cleaned:
            return [None]
        primary = proxy_utils.match_proxy(cleaned, wallet_index)
        blocked = _bad_snapshot()
        candidates: list[str | None] = []
        if primary not in blocked:
            candidates.append(primary)
        rotated = cleaned[(wallet_index % len(cleaned)) :] + cleaned[: (wallet_index % len(cleaned))]
        for proxy in rotated:
            if proxy in blocked or proxy in candidates:
                continue
            candidates.append(proxy)
            if len(candidates) >= proxy_attempts:
                break
        if primary and primary not in blocked and primary not in candidates:
            candidates.append(primary)
        return candidates[:proxy_attempts] if candidates else [None]

    def _fetch_one(address: str, wallet_index: int) -> tuple[str, list[Any] | None]:
        for attempt_i, proxy in enumerate(_proxy_candidates(wallet_index), 1):
            data = get_bonus_dapp_data(
                address,
                proxy,
                retries=1,
                retry_delay=0,
                timeout=batch_timeout,
            )
            if data is not None:
                return address, data
            _mark_bad(proxy)
            if proxy and attempt_i < proxy_attempts:
                logger.debug(f"[Portal batch] {address} proxy fail -> rotate ({attempt_i}/{proxy_attempts})")
        return address, None

    with tqdm(
        total=len(addresses),
        desc="Portal статус",
        unit="wallet",
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        colour="cyan",
    ) as pbar:
        for start in range(0, len(addresses), batch_size):
            chunk = addresses[start : start + batch_size]
            with ThreadPoolExecutor(max_workers=min(len(chunk), max_workers)) as executor:
                indexed = {
                    executor.submit(_fetch_one, addr, start + j): addr
                    for j, addr in enumerate(chunk)
                }
                for future in as_completed(indexed):
                    try:
                        addr, data = future.result(timeout=65)
                    except Exception:
                        addr = indexed[future]
                        data = None
                    results[addr] = data
                    pbar.update(1)

    extra = max(0, int(os.environ.get("BONUS9_PORTAL_BATCH_REEXTRA", "2")))
    for round_i in range(extra):
        missing = [a for a in addresses if results.get(a) is None]
        if not missing:
            break
        for addr in missing:
            idx = address_index[addr]
            for proxy in _proxy_candidates(idx + round_i + 1):
                data = get_bonus_dapp_data(
                    addr,
                    proxy,
                    retries=1,
                    retry_delay=0,
                    timeout=batch_timeout,
                )
                if data is not None:
                    results[addr] = data
                    break
                _mark_bad(proxy)

    return results


# ── Single-wallet print (legacy / status-only mode) ───────────────────────────

def print_portal_status(address: str, proxy: str | None = None) -> None:
    data   = get_bonus_dapp_data(address, proxy)
    status = parse_account_status(data)
    ok = lambda v: "✅" if v else "❌"
    logger.header(f"[Portal] {address}")
    logger.info(
        f"  swap={ok(status['swap_done'])} ref={ok(status['referral_done'])} "
        f"gm={status['gm']}/{status['gm_required']} sc={ok(status['soundchains_done'])} "
        f"ss={status['superstake']}/{status['superstake_required']} "
        f"elhexa={status['elhexa']}/{status['elhexa_required']}"
    )
