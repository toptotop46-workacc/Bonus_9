"""Register a new wallet with a referral code (task 1.2)."""

import os
import random
import secrets
import time
from urllib.parse import urlparse

from eth_account import Account

from modules import db, logger
from modules.portal_api import get_bonus_dapp_data, parse_account_status, require_account_status
from modules import proxy_utils
from modules.startale_auth import authenticate, get_or_create_user, get_referral_code


def _human_sleep(a: float, b: float) -> None:
    """Случайная пауза [a, b] секунд — имитация естественных промежутков между действиями."""
    time.sleep(random.uniform(a, b))


def _proxy_host_port(p: str | None) -> str:
    """Только host:port для логов (без логина/пароля)."""
    if not p:
        return "(нет)"
    s = p.strip()
    if s.startswith(("http://", "https://", "socks5://")):
        u = urlparse(s)
        if u.hostname:
            return f"{u.hostname}:{u.port}" if u.port else str(u.hostname)
    return s[:36] + "…" if len(s) > 36 else s


def _transient_proxy_error(exc: BaseException) -> bool:
    """Таймаут/обрыв прокси — можно сменить endpoint и повторить."""
    s = str(exc).lower()
    if "timed out" in s or "timeout" in s:
        return True
    if "connection refused" in s or "connection reset" in s:
        return True
    if "could not connect" in s or "failed to connect" in s:
        return True
    if "curl:" in s and "(28)" in s:
        return True
    if "curl:" in s and "(7)" in s:
        return True
    if "errno 110" in s or "errno 111" in s:
        return True
    return False


def _portal_referral_done(main_address: str, proxy: str | None) -> bool:
    raw = get_bonus_dapp_data(main_address, proxy=proxy)
    st = parse_account_status(raw)
    return bool(st.get("referral_done"))


def _wait_portal_referral(
    main_address: str,
    proxy: str | None,
    timeout_sec: int | None = None,
    interval_sec: int | None = None,
) -> bool:
    """Ждём, пока на портале у основного кошелька засчитается реферальный квест."""
    if timeout_sec is None:
        timeout_sec = int(os.environ.get("BONUS9_REFERRAL_PORTAL_TIMEOUT", "420"))
    if interval_sec is None:
        interval_sec = int(os.environ.get("BONUS9_REFERRAL_PORTAL_INTERVAL", "22"))

    deadline = time.time() + timeout_sec
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        if _portal_referral_done(main_address, proxy):
            return True
        jitter_lo = interval_sec * float(os.environ.get("BONUS9_REFERRAL_POLL_JITTER_LO", "0.75"))
        jitter_hi = interval_sec * float(os.environ.get("BONUS9_REFERRAL_POLL_JITTER_HI", "1.35"))
        wait_next = random.uniform(jitter_lo, jitter_hi)
        logger.info(f"[Referral] портал жду ~{wait_next:.0f}s #{attempt}")
        time.sleep(wait_next)
    return False


def do_referral(
    main_private_key: str,
    proxies: list[str | None],
    wallet_index: int,
) -> bool:
    """
    1. Авторизация основного кошелька → реф.код (прокси с ротацией при таймаутах).
    2. Новый кошелёк, пауза.
    3. Авторизация приглашённого — другой прокси; при обрыве перебираются запасные.
    4. Регистрация с referrer_code, ожидание портала.
    """
    cleaned = proxy_utils.nonempty_proxies(proxies)
    if len(cleaned) < 2:
        logger.error("[Referral] нужно ≥2 прокси в proxy.txt")
        return False

    max_main_tries = int(os.environ.get("BONUS9_REFERRAL_MAIN_PROXY_TRIES", "15"))
    max_ref_tries = int(os.environ.get("BONUS9_REFERRAL_REF_PROXY_TRIES", "15"))
    n = len(cleaned)

    main_addr = Account.from_key(main_private_key).address
    main_proxy_first = proxy_utils.match_proxy(cleaned, wallet_index)
    st = require_account_status(main_addr, main_proxy_first, proxy_pool=proxies)
    if st.get("referral_done"):
        logger.info(f"[Referral] {main_addr} портал OK → БД")
        db.mark_referral_done(main_addr, "")
        return True

    if db.is_referral_done(main_addr):
        logger.info(f"[Referral] {main_addr} уже в БД, пропуск")
        return True

    jwt_main: str | None = None
    user_id_main: str = ""
    main_proxy: str | None = None
    base_idx: int = 0

    for rot in range(min(max_main_tries, n)):
        pair = proxy_utils.pick_referral_proxy_pair(proxies, wallet_index, rotation=rot)
        if not pair or pair[0] is None or pair[1] is None:
            continue
        main_p, _ref_first = pair
        base_idx = (wallet_index + rot) % n
        logger.info(
            f"[Referral] прокси #{rot + 1} main={_proxy_host_port(main_p)} ref={_proxy_host_port(_ref_first)}"
        )

        _human_sleep(
            float(os.environ.get("BONUS9_REFERRAL_PRE_AUTH_MIN", "0.8")),
            float(os.environ.get("BONUS9_REFERRAL_PRE_AUTH_MAX", "2.8")),
        )

        logger.debug(f"[Referral] реф.код ← {main_addr}")

        try:
            jwt_main, _, user_id_main = authenticate(main_private_key, main_p)
            main_proxy = main_p
            break
        except Exception as e:
            if _transient_proxy_error(e) and rot < min(max_main_tries, n) - 1:
                logger.warning(f"[Referral] main proxy: {str(e)[:100]} → следующий")
                _human_sleep(0.4, 1.5)
                continue
            logger.error(f"[Referral] auth main: {e}")
            return False

    if not jwt_main or not main_proxy:
        logger.error("[Referral] auth main: все прокси исчерпаны")
        return False

    _human_sleep(0.35, 1.2)

    user_main = get_or_create_user(
        jwt_main, main_addr, proxy=main_proxy, known_user_id=user_id_main or None
    )
    if not user_id_main:
        user_id_main = (user_main or {}).get("id", "") or ""

    ref_code = get_referral_code(jwt_main, user_id_main, proxy=main_proxy)
    if not ref_code:
        ref_code = ((user_main or {}).get("referral") or {}).get("referral_code")
    if not ref_code:
        logger.error(f"[Referral] нет реф.кода {main_addr}")
        return False

    logger.info(f"[Referral] код {ref_code}")

    _human_sleep(0.6, 2.0)

    new_key = "0x" + secrets.token_hex(32)
    new_account = Account.from_key(new_key)
    new_addr = new_account.address
    logger.info(f"[Referral] invitee {new_addr}")

    gap = random.uniform(
        float(os.environ.get("BONUS9_REFERRAL_AUTH_GAP_MIN", "14")),
        float(os.environ.get("BONUS9_REFERRAL_AUTH_GAP_MAX", "42")),
    )
    logger.info(f"[Referral] пауза {gap:.0f}s → invitee")
    time.sleep(gap)

    ref_candidates = proxy_utils.referral_ref_alternatives(cleaned, base_idx, main_proxy)
    jwt_new: str | None = None
    ref_proxy: str | None = None

    for ref_i, ref_p in enumerate(ref_candidates[:max_ref_tries]):
        try:
            jwt_new, _, _ = authenticate(new_key, ref_p)
            ref_proxy = ref_p
            break
        except Exception as e:
            if _transient_proxy_error(e) and ref_i < min(len(ref_candidates), max_ref_tries) - 1:
                logger.warning(f"[Referral] ref proxy: {str(e)[:100]} → следующий")
                _human_sleep(0.4, 1.2)
                continue
            logger.error(f"[Referral] auth invitee: {e}")
            return False

    if not jwt_new or not ref_proxy:
        logger.error("[Referral] auth invitee: прокси исчерпаны")
        return False

    _human_sleep(0.4, 1.5)

    user_new = get_or_create_user(jwt_new, new_addr, referrer_code=ref_code, proxy=ref_proxy)

    used_code = (user_new.get("referral") or {}).get("referral_code_used", "")
    if used_code:
        logger.success(f"[Referral] API OK {ref_code} → {new_addr} used={used_code}")
    else:
        logger.info(f"[Referral] API рег. {new_addr} → жду портал")

    _human_sleep(1.5, 4.0)

    if not _wait_portal_referral(main_addr, main_proxy):
        logger.error("[Referral] портал: таймаут, квест не отмечен")
        return False

    logger.success(f"[Referral] портал OK {main_addr}")
    db.mark_referral_done(main_addr, new_addr)
    return True
