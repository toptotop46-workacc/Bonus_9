"""SIWE authentication against Startale App via DynamicAuth.

Логика совпадает с https://github.com/toptotop46-workacc/Easters (src/api.js):
undici ProxyAgent + fetch ≈ requests + прокси; SIWE как buildSIWEMessage; verify с signedMessage (0x).
"""

from __future__ import annotations

import base64
import json
import os
import time
import random
from datetime import datetime, timezone
from typing import Any

import requests
from curl_cffi import requests as curl_requests
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3

from modules import logger

DYNAMIC_ENV_ID = "740c1c57-7fa3-4da0-99f7-bae832bfe159"
DYNAMIC_BASE = f"https://app.dynamicauth.com/api/v0/sdk/{DYNAMIC_ENV_ID}"
API_BASE = "https://api-app.startale.com/api/v1"
CHAIN_ID = 1868
ORIGIN = "https://app.startale.com"

_CURL_IMPERSONATE = "chrome124"

USER_AGENTS = [
    # Windows — Chrome 130-135
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    # macOS 10.15 — Chrome 130-135
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # macOS 14 — Chrome 133-135
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    # Linux — Chrome 133-135
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    # Windows — Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
    # macOS — Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15",
    # Windows — Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
]

# Stable Chrome UA kept for curl_cffi sessions (impersonate=chrome124)
EASTERS_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"


def _http_timeout() -> int:
    return int(os.environ.get("BONUS9_STARTALE_HTTP_TIMEOUT", "60"))


def _http_client_mode() -> str:
    """
    requests — как Node fetch + undici ProxyAgent (Easters), стабильнее с HTTP-прокси.
    curl — curl_cffi chrome impersonate (если нужен обход WAF).
    """
    return os.environ.get("BONUS9_STARTALE_HTTP_CLIENT", "requests").strip().lower()


def _session(proxy: str | None) -> Any:
    """Все запросы к Dynamic/Startale только через переданный proxy (без системного прокси/VPN из env)."""
    mode = _http_client_mode()
    if mode == "curl":
        s = curl_requests.Session(impersonate=_CURL_IMPERSONATE)
        if proxy:
            s.proxies = {"http": proxy, "https": proxy}
        return s
    s = requests.Session()
    s.trust_env = False
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
    return s


def _require_proxy_for_startale(proxy: str | None) -> None:
    if os.environ.get("BONUS9_REQUIRE_STARTALE_PROXY", "").strip() in ("1", "true", "yes"):
        if not (proxy and str(proxy).strip()):
            raise RuntimeError(
                "Задан BONUS9_REQUIRE_STARTALE_PROXY: для Startale нужен прокси в proxy.txt (региональные ограничения)."
            )


def _request_with_retry(session: Any, method: str, url: str, **kwargs):
    """Запрос с повторами при 429 и 5xx (Dynamic часто режет частые вызовы)."""
    max_attempts = 8
    for attempt in range(max_attempts):
        resp = session.request(method, url, **kwargs)
        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            try:
                wait = int(float(ra)) if ra else None
            except ValueError:
                wait = None
            if wait is None:
                wait = min(120, 5 * (2 ** min(attempt, 5)))
            logger.warning(f"[Startale] 429 → пауза {wait}s ({attempt + 1}/{max_attempts})")
            time.sleep(wait)
            continue
        if resp.status_code >= 500 and attempt < max_attempts - 1:
            w = min(60, 2 ** attempt)
            logger.warning(f"[Startale] HTTP {resp.status_code} → пауза {w}s")
            time.sleep(w)
            continue
        return resp
    return resp


def _issued_at_iso8601_ms() -> str:
    """Аналог new Date().toISOString() в Easters (миллисекунды, Z)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _build_siwe_message(address: str, nonce: str) -> str:
    """Текст 1:1 с buildSIWEMessage() в Easters src/api.js."""
    issued_at = _issued_at_iso8601_ms()
    chain_id_str = str(CHAIN_ID)
    lines = [
        "app.startale.com wants you to sign in with your Ethereum account:",
        address,
        "",
        "Welcome to Startale. Signing is the only way we can truly know that you are the owner of the wallet you are connecting. Signing is a safe, gas-less transaction that does not in any way give Startale permission to perform any transactions with your wallet.",
        "",
        f"URI: {ORIGIN}/log-in",
        "Version: 1",
        f"Chain ID: {chain_id_str}",
        f"Nonce: {nonce}",
        f"Issued At: {issued_at}",
        f"Request ID: {DYNAMIC_ENV_ID}",
    ]
    return "\n".join(lines)


def _sign_message_eip191(private_key: str, message: str) -> str:
    """Как viem account.signMessage — в verify уходит hex с префиксом 0x."""
    msg = encode_defunct(text=message)
    signed = Account.sign_message(msg, private_key=private_key)
    h = signed.signature.hex()
    return h if h.startswith("0x") else "0x" + h


def _base_headers(ua: str | None = None) -> dict:
    return {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": ORIGIN,
        "Referer": f"{ORIGIN}/",
        "User-Agent": ua or random.choice(USER_AGENTS),
    }


def _authed_headers(jwt: str, ua: str | None = None) -> dict:
    h = _base_headers(ua)
    h["Authorization"] = f"Bearer {jwt}"
    return h


def _get_nonce(session: Any, ua: str) -> str:
    resp = _request_with_retry(
        session,
        "GET",
        f"{DYNAMIC_BASE}/nonce",
        headers=_base_headers(ua),
        timeout=_http_timeout(),
    )
    resp.raise_for_status()
    return resp.json()["nonce"]


def _connect(session: Any, address: str, ua: str) -> None:
    payload = {
        "address": address,
        "chain": "EVM",
        "provider": "browserExtension",
        "walletName": "metamask",
        "authMode": "connect-and-sign",
    }
    resp = _request_with_retry(
        session,
        "POST",
        f"{DYNAMIC_BASE}/connect",
        json=payload,
        headers=_base_headers(ua),
        timeout=_http_timeout(),
    )
    resp.raise_for_status()


def _verify(session: Any, address: str, message: str, signature_hex: str, ua: str) -> tuple[str, dict]:
    """POST /verify — как Easters; второй элемент — полный JSON (часто есть user / минтокен)."""
    payload = {
        "signedMessage": signature_hex,
        "messageToSign": message,
        "publicWalletAddress": address,
        "chain": "EVM",
        "walletName": "metamask",
        "walletProvider": "browserExtension",
        "network": str(CHAIN_ID),
        "additionalWalletAddresses": [],
    }
    resp = _request_with_retry(
        session,
        "POST",
        f"{DYNAMIC_BASE}/verify",
        json=payload,
        headers=_base_headers(ua),
        timeout=_http_timeout(),
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        data = {}
    jwt = data.get("jwt") or data.get("token") or (data.get("user") or {}).get("jwt")
    if not jwt:
        raise RuntimeError(f"No JWT in response: {data}")
    return jwt, data


def _jwt_payload_dict(jwt: str) -> dict[str, Any]:
    try:
        parts = jwt.split(".")
        if len(parts) < 2:
            return {}
        payload_b64 = parts[1]
        pad = "=" * (-len(payload_b64) % 4)
        raw = base64.urlsafe_b64decode(payload_b64 + pad)
        out = json.loads(raw.decode("utf-8"))
        return out if isinstance(out, dict) else {}
    except Exception:
        return {}


def _extract_user_id_from_claims(c: dict[str, Any]) -> str:
    """Ищем id пользователя Startale/Dynamic в claims JWT для GET /user/{id}."""
    for k in ("startale_user_id", "startaleUserId", "user_id", "userId"):
        v = c.get(k)
        if isinstance(v, str) and len(v) > 4:
            return v
    meta = c.get("metadata")
    if isinstance(meta, dict):
        for k in ("user_id", "userId", "id"):
            v = meta.get(k)
            if isinstance(v, str) and len(v) > 4:
                return v
    u = c.get("user")
    if isinstance(u, dict) and u.get("id"):
        return str(u["id"])
    sub = c.get("sub")
    if isinstance(sub, str) and len(sub) >= 32 and sub.count("-") >= 4:
        return sub
    return ""


def _parse_user_from_me_response(data: dict[str, Any]) -> dict | None:
    """Разбор ответа GET /user/me (разные обёртки)."""
    user = data.get("user")
    if isinstance(user, dict) and user:
        return user
    inner = data.get("data")
    if isinstance(inner, dict):
        user = inner.get("user")
        if isinstance(user, dict) and user:
            return user
    return None


def _get_user_by_id(session: Any, jwt: str, user_id: str, ua: str) -> dict | None:
    try:
        resp = _request_with_retry(
            session,
            "GET",
            f"{API_BASE}/user/{user_id}",
            headers=_authed_headers(jwt, ua),
            timeout=_http_timeout(),
        )
        if not resp.ok:
            return None
        user = resp.json().get("user")
        if isinstance(user, dict) and user:
            return user
    except Exception:
        pass
    return None


def _resolve_user_profile(session: Any, jwt: str, ua: str, verify_json: dict[str, Any] | None = None) -> dict | None:
    """GET /user/me с повторами, затем данные из verify, затем id из JWT → GET /user/{id}."""
    if verify_json:
        u = verify_json.get("user")
        if isinstance(u, dict) and u.get("id"):
            return u

    for attempt in range(10):
        try:
            resp = _request_with_retry(
                session,
                "GET",
                f"{API_BASE}/user/me",
                headers=_authed_headers(jwt, ua),
                timeout=_http_timeout(),
            )
            if resp.ok:
                body = resp.json()
                if isinstance(body, dict):
                    user = _parse_user_from_me_response(body)
                    if user:
                        return user
                    if attempt == 0:
                        logger.debug(f"[Startale] GET /user/me keys: {list(body.keys())[:20]}")
        except Exception:
            pass
        time.sleep(0.45 + 0.15 * attempt)

    claims = _jwt_payload_dict(jwt)
    uid = _extract_user_id_from_claims(claims)
    if uid:
        user = _get_user_by_id(session, jwt, uid, ua)
        if user:
            logger.debug(f"[Startale] профиль по JWT id={uid[:12]}…")
            return user

    return None


def authenticate(private_key: str, proxy: str | None = None) -> tuple[str, str, str]:
    """
    Full SIWE flow.
    Returns (jwt, address, user_id).
    """
    _require_proxy_for_startale(proxy)
    account = Account.from_key(private_key)
    address = account.address
    ua = random.choice(USER_AGENTS)
    session = _session(proxy)
    mode = _http_client_mode()

    logger.debug(f"[Startale] auth {address} ({mode})")

    nonce = _get_nonce(session, ua)
    _connect(session, address, ua)
    message = _build_siwe_message(address, nonce)
    sig = _sign_message_eip191(private_key, message)
    jwt, verify_json = _verify(session, address, message, sig, ua)

    user_id = ""
    prof = _resolve_user_profile(session, jwt, ua, verify_json=verify_json)
    if prof and prof.get("id"):
        user_id = str(prof["id"])

    logger.success(f"[Startale] Auth OK {address} user={user_id or '—'}")
    return jwt, address, user_id


def get_or_create_user(
    jwt: str,
    address: str,
    referrer_code: str | None = None,
    proxy: str | None = None,
    known_user_id: str | None = None,
) -> dict:
    _require_proxy_for_startale(proxy)
    session = _session(proxy)
    ua = random.choice(USER_AGENTS)

    if known_user_id and str(known_user_id).strip():
        u = _get_user_by_id(session, jwt, str(known_user_id).strip(), ua)
        if u:
            return u

    user = _resolve_user_profile(session, jwt, ua, verify_json=None)
    if user:
        return user

    payload: dict = {}
    if referrer_code:
        payload["referrer_code"] = referrer_code
    resp = _request_with_retry(
        session,
        "POST",
        f"{API_BASE}/user",
        json=payload,
        headers=_authed_headers(jwt, ua),
        timeout=_http_timeout(),
    )

    if resp.status_code == 409:
        logger.debug("[Startale] POST /user 409 → повторный профиль")
        user = _resolve_user_profile(session, jwt, ua, verify_json=None)
        if user:
            return user
        try:
            err_user = (resp.json() or {}).get("user")
            if isinstance(err_user, dict) and err_user:
                return err_user
        except Exception:
            pass
        raise RuntimeError(
            "409 Conflict на POST /user: профиль не удалось прочитать (GET /user/me и JWT id). "
            "Проверь прокси (все запросы к API должны идти через него)."
        )

    resp.raise_for_status()
    return (resp.json() or {}).get("user", {}) or {}


def get_linked_smart_account_address(user: dict) -> str | None:
    """
    Адрес смарт-аккаунта из GET /user (linked_accounts), как в приложении Startale.
    Отличается от computeAccountAddress(factory) — без этого квесты Soneium/ELHEXA не засчитывают tx.
    """
    for acc in user.get("linked_accounts") or []:
        if str(acc.get("type", "")).lower() != "smart_account":
            continue
        if acc.get("enabled") is False:
            continue
        addr = acc.get("address")
        if isinstance(addr, str) and addr.startswith("0x") and len(addr) >= 42:
            return Web3.to_checksum_address(addr)
    return None


def get_referral_code(jwt: str, user_id: str, proxy: str | None = None) -> str | None:
    _require_proxy_for_startale(proxy)
    if not user_id:
        return None
    session = _session(proxy)
    ua = EASTERS_UA
    try:
        resp = _request_with_retry(
            session,
            "GET",
            f"{API_BASE}/user/{user_id}",
            headers=_authed_headers(jwt, ua),
            timeout=_http_timeout(),
        )
        resp.raise_for_status()
        user = resp.json().get("user", {})
        code = (user.get("referral") or {}).get("referral_code")
        return code
    except Exception as e:
        logger.warning(f"[Startale] реф.код: {e}")
        return None
