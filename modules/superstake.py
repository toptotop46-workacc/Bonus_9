"""
Superstake Claw Machine — Play 10 rounds (task 3).
Flow: кошелёк → Privy SIWE (auth.privy.io) → POST /v1/users/login
      с телом { \"token\": "<Privy session JWT>" } (поле token из ответа /siwe/authenticate,
      не privy_access_token) → Firebase customToken → POST /game (gameType ip, betAmount строка «N» IP, по умолчанию 10) → createGame.

На backend ставка в единицах IP (строка). В createGame value и betAmount — из gameMetadata.betAmount (wei), часто 0 для IP.
"""

import os
import re
import random
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from web3 import Web3
from eth_account import Account
from eth_account.messages import encode_defunct
from modules import logger, db, web3_utils
from modules.portal_api import require_account_status

try:
    from curl_cffi import requests as http_requests
    _CURL_IMPERSONATE = "chrome"
except ImportError:
    import requests as http_requests
    _CURL_IMPERSONATE = None  # type: ignore[assignment]

# ── Addresses ────────────────────────────────────────────────────────────────
CASH_OR_CRASH_ADDR = Web3.to_checksum_address("0xa52B8E221a05886CA92709f7996578019709740C")
BACKEND_BASE       = "https://coc-backend-353453819043.us-east4.run.app/api"
FIREBASE_API_KEY: str = ""

SUPERSTAKE_ORIGIN  = "https://coolcats-soneium.superstake.fun"
# Privy (из бандла /_next/static/chunks/053557efade84935.js); coc-backend ждёт JWT из поля `token` после SIWE.
PRIVY_APP_ID_DEFAULT    = "cmg6d6xu100dpl20cua273r9w"
PRIVY_CLIENT_ID_DEFAULT = "client-WY6RQwUeiqmqSeQk3jtDKy5QYHT1XVVYwVffdEEUhzQE4"
PRIVY_AUTH_BASE         = "https://auth.privy.io/api"
SUPERSTAKE_SIWE_DOMAIN  = "coolcats-soneium.superstake.fun"
SONEIUM_CHAIN_ID        = 1868

# Публичный Web API key из клиента (если авто-скачивание с сайта недоступно).
_FIREBASE_KEY_FALLBACK = "AIzaSyAKb4yfAZKle3zNPvTBhQXteeng9MAw0H4"

FIREBASE_KEY_RE = re.compile(r"AIzaSy[0-9A-Za-z_-]{33}")
_CACHE_DIR = Path(".cache")
_CACHE_KEY_FILE = _CACHE_DIR / "superstake_firebase_key.txt"
_CACHE_MAX_AGE_SEC = 7 * 86400

USER_AGENTS = [
    # Windows — Chrome 130-135
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    # macOS 10.15 — Chrome 131-135
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
    # Windows — Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
]


def _curl_proxies(proxy: str | None) -> dict | None:
    if not proxy:
        return None
    p = proxy.strip()
    return {"http": p, "https": p}


def _http_kw(proxy: str | None, timeout: float) -> dict:
    kw: dict = {"proxies": _curl_proxies(proxy), "timeout": timeout}
    if _CURL_IMPERSONATE:
        kw["impersonate"] = _CURL_IMPERSONATE
    return kw


def _http_get(url: str, *, headers: dict | None = None, proxy: str | None = None,
              timeout: float = 60) -> object:
    return http_requests.get(url, headers=headers or {}, **_http_kw(proxy, timeout))


def _http_post(url: str, *, json: dict | None = None, headers: dict | None = None,
               proxy: str | None = None, timeout: float = 30) -> object:
    return http_requests.post(url, json=json, headers=headers or {}, **_http_kw(proxy, timeout))


def _privy_transient_network_error(exc: BaseException) -> bool:
    s = str(exc).lower()
    if "timed out" in s or "timeout" in s:
        return True
    if "(28)" in s and "curl" in s:
        return True
    if "connection" in s and any(
        x in s for x in ("refused", "reset", "aborted", "failed", "closed")
    ):
        return True
    return False


def _http_post_privy(
    url: str,
    *,
    json: dict | None = None,
    headers: dict | None = None,
    proxy: str | None = None,
    timeout: float = 60,
) -> object:
    """
    Запросы к auth.privy.io: многие HTTP-прокси режут или тормозят хост.
    SUPERSTAKE_PRIVY_DIRECT=1 — сразу без прокси; иначе при ошибке сети — повтор без прокси.
    """
    direct = os.environ.get("SUPERSTAKE_PRIVY_DIRECT", "").strip().lower() in ("1", "true", "yes")
    no_fb = os.environ.get("SUPERSTAKE_PRIVY_NO_FALLBACK", "").strip().lower() in ("1", "true", "yes")
    if direct:
        return _http_post(url, json=json, headers=headers, proxy=None, timeout=timeout)
    try:
        return _http_post(url, json=json, headers=headers, proxy=proxy, timeout=timeout)
    except Exception as e:
        if proxy and not no_fb and _privy_transient_network_error(e):
            logger.debug("[Superstake] Privy: повтор без прокси")
            return _http_post(url, json=json, headers=headers, proxy=None, timeout=timeout)
        raise


def _read_cached_firebase_key() -> str | None:
    try:
        if not _CACHE_KEY_FILE.is_file():
            return None
        if time.time() - _CACHE_KEY_FILE.stat().st_mtime > _CACHE_MAX_AGE_SEC:
            return None
        raw = _CACHE_KEY_FILE.read_text(encoding="utf-8").strip()
        if raw and FIREBASE_KEY_RE.fullmatch(raw):
            return raw
    except Exception:
        pass
    return None


def _write_cached_firebase_key(key: str) -> None:
    try:
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _CACHE_KEY_FILE.write_text(key.strip(), encoding="utf-8")
    except Exception as e:
        logger.debug(f"[Superstake] кэш ключа: {e}")


def discover_firebase_api_key(proxy: str | None) -> str | None:
    """
    Тянем главную страницу и бандлы /assets/*.js, ищем публичный apiKey (AIza...).
    curl_cffi + TLS fingerprint как у Chrome; при необходимости — прокси.
    """
    hdr = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        r = _http_get(SUPERSTAKE_ORIGIN + "/", headers=hdr, proxy=proxy, timeout=90)
        if not r.ok:
            logger.warning(f"[Superstake] главная HTTP {r.status_code}")
            return None
        html = r.text
    except Exception as e:
        logger.warning(f"[Superstake] главная: {e}")
        return None

    found: set[str] = set(FIREBASE_KEY_RE.findall(html))
    paths = set(re.findall(r'(?:src|href)=["\'](/assets/[^"\']+\.js)["\']', html))
    for path in sorted(paths)[:35]:
        if found:
            break
        url = SUPERSTAKE_ORIGIN.rstrip("/") + path
        try:
            jr = _http_get(url, proxy=proxy, timeout=90)
            if jr.ok:
                found.update(FIREBASE_KEY_RE.findall(jr.text))
        except Exception as e:
            logger.debug(f"[Superstake] asset {path}: {e}")

    if not found:
        return None
    return sorted(found)[0]


def ensure_firebase_api_key(override: str | None, proxy: str | None) -> None:
    """
    Порядок: config → env → кэш → встроенный публичный ключ клиента (без ожидания сети).
    Параллельно в фоне curl_cffi тянет бандлы с сайта и обновляет .cache/ для следующих запусков.
    """
    global FIREBASE_API_KEY
    if override and str(override).strip():
        FIREBASE_API_KEY = str(override).strip()
        logger.debug("[Superstake] Firebase key: config.toml")
        return
    env_k = os.environ.get("SUPERSTAKE_FIREBASE_API_KEY", "").strip()
    if env_k:
        FIREBASE_API_KEY = env_k
        logger.debug("[Superstake] Firebase key: env")
        return
    cached = _read_cached_firebase_key()
    if cached:
        FIREBASE_API_KEY = cached
        logger.debug("[Superstake] Firebase key: .cache")
        return
    FIREBASE_API_KEY = _FIREBASE_KEY_FALLBACK
    logger.info("[Superstake] Firebase fallback → фоновый discover")

    def _background_discover() -> None:
        try:
            k = discover_firebase_api_key(proxy)
            if k:
                _write_cached_firebase_key(k)
                logger.debug(f"[Superstake] Firebase key записан в кэш ({k[:12]}…)")
        except Exception as e:
            logger.debug(f"[Superstake] фоновый discover: {e}")

    threading.Thread(target=_background_discover, daemon=True).start()


def _firebase_identity_url(path: str) -> str:
    return f"https://identitytoolkit.googleapis.com/v1/{path}?key={FIREBASE_API_KEY}"


def _firebase_headers() -> dict:
    """Без Origin/Referer Google часто отклоняет ключ с ограничением по HTTP referrer."""
    return {
        "Content-Type": "application/json",
        "Accept":       "application/json",
        "Origin":       SUPERSTAKE_ORIGIN,
        "Referer":      SUPERSTAKE_ORIGIN + "/",
        "User-Agent":   random.choice(USER_AGENTS),
    }


def _firebase_post(path: str, payload: dict, proxy: str | None = None) -> dict:
    """POST identitytoolkit; при ошибке текст ответа Google попадает в исключение."""
    resp = _http_post(
        _firebase_identity_url(path),
        json=payload,
        headers=_firebase_headers(),
        proxy=proxy,
        timeout=35,
    )
    if not resp.ok:
        try:
            err_body = resp.json()
        except Exception:
            err_body = resp.text[:800]
        hint = ""
        if resp.status_code == 400 and isinstance(err_body, dict):
            em = str((err_body.get("error") or {}).get("message", ""))
            if "API_KEY" in em.upper() or "API key" in em:
                hint = " Проверь сеть/прокси или укажи superstake_firebase_api_key в config.toml."
        raise RuntimeError(f"Firebase {path} -> HTTP {resp.status_code}: {err_body}{hint}")
    return resp.json()

TOKEN_ID           = "63KU4l5Xtg6MePjxEiQF"  # ETH @ Soneium (1868), подтягивается из GET /v1/tokens
GAME_KIND          = "claw_machine"
# Раскладка рядов claw machine из фронта (1da3ab4575aa4c7d.js): l=[9,8,7,6,5,4,3,2] для gameKind claw_machine
CLAW_MACHINE_TILES_CONFIG: list[int] = [9, 8, 7, 6, 5, 4, 3, 2]

# Режим gameType: ip — на фронте useState(10) и betAmount: M.toString() → строка "10" (10 IP), не minBet токена в wei.
_SONEIUM_ETH_TOKEN_CACHE: dict | None = None

# ── ABI selectors ─────────────────────────────────────────────────────────────
# createGame(string, bytes32, string, string, address, bool, uint256, uint256, bytes)
CREATE_GAME_SEL = Web3.keccak(
    text="createGame(string,bytes32,string,string,address,bool,uint256,uint256,bytes)"
)[:4]


# ── ABI encoding helpers ──────────────────────────────────────────────────────

def _u256(v: int) -> bytes:
    return v.to_bytes(32, "big")

def _addr_enc(a: str) -> bytes:
    return bytes.fromhex(a.removeprefix("0x").lower().zfill(64))

def _bytes_enc(b: bytes) -> bytes:
    pad = (32 - len(b) % 32) % 32
    return _u256(len(b)) + b + b"\x00" * pad

def _str_enc(s: str) -> bytes:
    return _bytes_enc(s.encode("utf-8"))


def _encode_create_game(
    preliminary_game_id: str,
    game_seed_hash: str,        # hex string "0x..."
    algo_version: str,
    game_config: str,
    token_addr: str,
    is_native: bool,
    bet_amount: int,
    deadline: int,
    server_signature: bytes,
) -> bytes:
    """ABI-encode createGame() calldata."""
    # Dynamic types: string, bytes32(static), string, string, address(static),
    #                bool(static), uint256(static), uint256(static), bytes
    # Offsets for dynamic fields: slot 0 (string), slot 2 (string), slot 3 (string), slot 8 (bytes)

    # Static slots: bytes32, address, bool, uint256, uint256
    # Dynamic: string[0], string[2], string[3], bytes[8]

    pgi_enc    = _str_enc(preliminary_game_id)
    seed_bytes = bytes.fromhex(game_seed_hash.removeprefix("0x"))[:32].ljust(32, b"\x00")
    av_enc     = _str_enc(algo_version)
    gc_enc     = _str_enc(game_config)
    sig_enc    = _bytes_enc(server_signature)

    # 9 params, compute head (9 × 32 = 288 bytes)
    # Slot 0: offset of param[0] (string)
    # Slot 1: bytes32 (static)
    # Slot 2: offset of param[2] (string)
    # Slot 3: offset of param[3] (string)
    # Slot 4: address (static, 20 bytes, right-padded)
    # Slot 5: bool (static)
    # Slot 6: uint256 (static)
    # Slot 7: uint256 (static)
    # Slot 8: offset of param[8] (bytes)

    head_size = 9 * 32  # 288

    body = b""

    dynamics = [
        (0, pgi_enc),
        (2, av_enc),
        (3, gc_enc),
        (8, sig_enc),
    ]

    # build body and collect offsets
    running = head_size
    dyn_offsets = {}
    for slot, enc in dynamics:
        dyn_offsets[slot] = running
        running += len(enc)
        body += enc

    # build head
    head = b""
    for slot in range(9):
        if slot == 0:
            head += _u256(dyn_offsets[0])
        elif slot == 1:
            head += seed_bytes
        elif slot == 2:
            head += _u256(dyn_offsets[2])
        elif slot == 3:
            head += _u256(dyn_offsets[3])
        elif slot == 4:
            head += _addr_enc(token_addr)
        elif slot == 5:
            head += _u256(1 if is_native else 0)
        elif slot == 6:
            head += _u256(bet_amount)
        elif slot == 7:
            head += _u256(deadline)
        elif slot == 8:
            head += _u256(dyn_offsets[8])

    return CREATE_GAME_SEL + head + body


# ── Firebase Auth ─────────────────────────────────────────────────────────────

def firebase_custom_token_login(custom_token: str, proxy: str | None = None) -> str:
    """Обмен Firebase custom token (после wallet на сайте) на idToken."""
    data = _firebase_post(
        "accounts:signInWithCustomToken",
        {"token": custom_token, "returnSecureToken": True},
        proxy,
    )
    return data["idToken"]


def _privy_app_and_client() -> tuple[str, str]:
    app = os.environ.get("SUPERSTAKE_PRIVY_APP_ID", "").strip() or PRIVY_APP_ID_DEFAULT
    cid = os.environ.get("SUPERSTAKE_PRIVY_CLIENT_ID", "").strip() or PRIVY_CLIENT_ID_DEFAULT
    return app, cid


def _privy_auth_headers() -> dict:
    app, cid = _privy_app_and_client()
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Origin": SUPERSTAKE_ORIGIN,
        "Referer": SUPERSTAKE_ORIGIN + "/",
        "privy-app-id": app,
        "privy-client-id": cid,
        "User-Agent": random.choice(USER_AGENTS),
    }


def _issued_at_privy_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _build_privy_siwe_message(
    address: str,
    nonce: str,
    issued_at: str,
    *,
    domain: str = SUPERSTAKE_SIWE_DOMAIN,
    uri: str | None = None,
    chain_id: int = SONEIUM_CHAIN_ID,
) -> str:
    """Текст сообщения 1:1 с Privy JS SDK (loginWithSiwe / init)."""
    uri = uri or (SUPERSTAKE_ORIGIN + "/")
    statement = (
        "By signing, you are proving you own this wallet and logging in. "
        "This does not initiate a transaction or cost any fees."
    )
    chain_str = str(chain_id)
    return (
        f"{domain} wants you to sign in with your Ethereum account:\n"
        f"{address}\n"
        f"\n"
        f"{statement}\n"
        f"\n"
        f"URI: {uri}\n"
        f"Version: 1\n"
        f"Chain ID: {chain_str}\n"
        f"Nonce: {nonce}\n"
        f"Issued At: {issued_at}\n"
        f"Resources:\n"
        f"- https://privy.io"
    )


def privy_siwe_authenticate(private_key: str, proxy: str | None = None) -> dict:
    """
    SIWE через Privy API: POST /v1/siwe/init → EIP-191 подпись → POST /v1/siwe/authenticate.
    В ответе поле `token` — JWT для coc-backend; поле `privy_access_token` для coc не использовать.
    """
    account = Account.from_key(private_key)
    address = account.address
    r1 = _http_post_privy(
        f"{PRIVY_AUTH_BASE}/v1/siwe/init",
        json={"address": address},
        headers=_privy_auth_headers(),
        proxy=proxy,
        timeout=60,
    )
    if not r1.ok:
        raise RuntimeError(f"Privy siwe/init: HTTP {r1.status_code} {r1.text[:600]}")
    nonce = (r1.json() or {}).get("nonce")
    if not nonce:
        raise RuntimeError(f"Privy siwe/init: нет nonce: {r1.text[:400]}")

    issued_at = _issued_at_privy_iso()
    message = _build_privy_siwe_message(address, nonce, issued_at)
    msg = encode_defunct(text=message)
    sig_hex = Account.sign_message(msg, private_key=private_key).signature.hex()
    if not sig_hex.startswith("0x"):
        sig_hex = "0x" + sig_hex

    wct = os.environ.get("SUPERSTAKE_PRIVY_WALLET_CLIENT_TYPE", "privy").strip() or "privy"
    cct = os.environ.get("SUPERSTAKE_PRIVY_CONNECTOR_TYPE", "injected").strip() or "injected"
    mode = os.environ.get("SUPERSTAKE_PRIVY_SIWE_MODE", "login-or-sign-up").strip() or "login-or-sign-up"

    body = {
        "signature": sig_hex,
        "message": message,
        "chainId": str(SONEIUM_CHAIN_ID),
        "walletClientType": wct,
        "connectorType": cct,
        "mode": mode,
    }
    r2 = _http_post_privy(
        f"{PRIVY_AUTH_BASE}/v1/siwe/authenticate",
        json=body,
        headers=_privy_auth_headers(),
        proxy=proxy,
        timeout=60,
    )
    if not r2.ok:
        raise RuntimeError(f"Privy siwe/authenticate: HTTP {r2.status_code} {r2.text[:800]}")
    out = r2.json()
    if not isinstance(out, dict):
        raise RuntimeError("Privy siwe/authenticate: ответ не JSON-объект")
    return out


def coc_login_with_privy_session_jwt(privy_session_jwt: str, proxy: str | None = None) -> dict:
    """
    JWT из поля `token` ответа Privy /siwe/authenticate → coc-backend отдаёт customToken.
    Без Authorization — только { \"token\": \"<jwt>\" }.
    """
    headers = {
        "Content-Type": "application/json",
        "Accept":       "application/json",
        "Origin":       SUPERSTAKE_ORIGIN,
        "Referer":      SUPERSTAKE_ORIGIN + "/",
        "User-Agent":   random.choice(USER_AGENTS),
    }
    resp = _http_post(
        f"{BACKEND_BASE}/v1/users/login",
        json={"token": privy_session_jwt},
        headers=headers,
        proxy=proxy,
        timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(
            f"coc-backend POST /v1/users/login (token): HTTP {resp.status_code} {resp.text[:800]}"
        )
    out = resp.json()
    return out if isinstance(out, dict) else {}


def _extract_custom_token(raw: dict) -> str | None:
    if not raw:
        return None
    ct = raw.get("customToken")
    if ct:
        return str(ct)
    data = raw.get("data")
    if isinstance(data, dict):
        ct = data.get("customToken")
        if ct:
            return str(ct)
    return None


def get_firebase_token_via_wallet(private_key: str, proxy: str | None = None) -> str:
    """
    Вход через кошелёк: Privy SIWE -> JWT (`token`) -> POST /v1/users/login -> customToken -> Firebase idToken.
    """
    auth = privy_siwe_authenticate(private_key, proxy)
    session_jwt = auth.get("token")
    if not session_jwt:
        raise RuntimeError(
            f"Privy authenticate не вернул поле token (нужно для coc). Ключи ответа: {list(auth.keys())}"
        )
    raw = coc_login_with_privy_session_jwt(str(session_jwt), proxy)
    custom = _extract_custom_token(raw)
    if not custom:
        raise RuntimeError(f"coc-backend не вернул customToken: {raw}")
    id_tok = firebase_custom_token_login(custom, proxy)
    logger.info("[Superstake] Auth OK (Privy → coc → Firebase)")
    return id_tok


# ── Backend API ───────────────────────────────────────────────────────────────

def _backend_headers(id_token: str) -> dict:
    return {
        "Authorization": f"Bearer {id_token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "Origin":        "https://coolcats-soneium.superstake.fun",
        "Referer":       "https://coolcats-soneium.superstake.fun/",
        "User-Agent":    random.choice(USER_AGENTS),
    }


def get_soneium_eth_token(proxy: str | None) -> dict:
    """Кэш: ETH@Soneium из GET /v1/tokens — для POST /v1/game нужен tokenId (ставка IP задаётся отдельно, не minBet)."""
    global _SONEIUM_ETH_TOKEN_CACHE
    if _SONEIUM_ETH_TOKEN_CACHE:
        return _SONEIUM_ETH_TOKEN_CACHE
    try:
        resp = _http_get(
            f"{BACKEND_BASE}/v1/tokens",
            proxy=proxy,
            timeout=35,
        )
        if resp.ok:
            raw = resp.json()
            for tok in raw.get("tokens") or []:
                net = tok.get("network") or {}
                cid = net.get("chainId")
                try:
                    cid_int = int(cid) if cid is not None else 0
                except (TypeError, ValueError):
                    cid_int = 0
                if cid_int == SONEIUM_CHAIN_ID and str(tok.get("symbol", "")).upper() == "ETH":
                    _SONEIUM_ETH_TOKEN_CACHE = tok
                    logger.debug(
                        f"[Superstake] tokenId Soneium ETH: {str(tok.get('id'))[:14]}…"
                    )
                    return _SONEIUM_ETH_TOKEN_CACHE
    except Exception as e:
        logger.debug(f"[Superstake] get_soneium_eth_token: {e}")
    _SONEIUM_ETH_TOKEN_CACHE = {"id": TOKEN_ID}
    return _SONEIUM_ETH_TOKEN_CACHE


def _ip_game_bet_amount_str() -> str:
    """Строка ставки для gameType ip (как M.toString() на сайте, по умолчанию «10» IP)."""
    raw = os.environ.get("SUPERSTAKE_IP_BET_AMOUNT", "10").strip()
    if raw.isdigit() and int(raw) > 0:
        return raw
    return "10"


def create_game_backend(
    id_token: str,
    wallet_address: str,
    proxy: str | None = None,
) -> dict:
    """POST /v1/game — режим ip; betAmount — строка в «единицах IP» (на сайте по умолчанию «10»), не wei из токена."""
    addr = Web3.to_checksum_address(wallet_address)
    tok = get_soneium_eth_token(proxy)
    token_id = str(tok.get("id") or TOKEN_ID)
    bet_ip = _ip_game_bet_amount_str()
    # Фронт: mutateAsync({ data: { gameType, ... } }) → в HTTP уходит только внутренний объект (без ключа "data").
    payload = {
        "gameType":            "ip",
        "gameKind":            GAME_KIND,
        "betAmount":           bet_ip,
        "playerWalletAddress": addr,
        "tokenId":             token_id,
        "tilesConfig":         CLAW_MACHINE_TILES_CONFIG,
    }
    resp = _http_post(
        f"{BACKEND_BASE}/v1/game",
        json=payload,
        headers=_backend_headers(id_token),
        proxy=proxy,
        timeout=45,
    )
    try:
        resp.raise_for_status()
    except Exception as e:
        detail = resp.text[:500] if resp.text else str(e)
        try:
            detail = resp.json()
        except Exception:
            pass
        raise RuntimeError(f"POST /v1/game не удался ({resp.status_code}): {detail}") from e
    return resp.json()


# ── Play one round ────────────────────────────────────────────────────────────

def play_one_round(
    private_key: str,
    w3: Web3,
    id_token: str,
    proxy: str | None = None,
    gas_limit_multiplier: float = 1.3,
) -> bool:
    """
    Один раунд Claw Machine (gameType ip): POST /v1/game, затем on-chain createGame.
    value и bet в createGame = int(gameMetadata.betAmount) wei (если пусто — 0).
    Returns True on success.
    """
    account     = Account.from_key(private_key)
    eoa_address = account.address

    game_data = create_game_backend(id_token, eoa_address, proxy)

    # Extract fields
    game_id      = game_data.get("id", "")
    seed_hash    = game_data.get("gameSeedHash", "")
    metadata     = game_data.get("gameMetadata", {})
    game_config  = metadata.get("gameConfig", "")
    deadline     = int(metadata.get("deadline", time.time() + 300))
    server_sig   = bytes.fromhex(
        metadata.get("signature", "0x").removeprefix("0x")
    )

    meta_bet_raw = metadata.get("betAmount")
    if meta_bet_raw is not None and str(meta_bet_raw).strip() != "":
        chain_bet = int(str(meta_bet_raw).strip())
    else:
        chain_bet = 0

    if not game_id or not seed_hash or not server_sig:
        raise RuntimeError(f"Неполные данные игры от backend: {game_data}")

    logger.debug(
        f"[Superstake] game={game_id} deadline={deadline} value_wei={chain_bet}"
    )

    calldata = _encode_create_game(
        preliminary_game_id=game_id,
        game_seed_hash=seed_hash,
        algo_version="v1",
        game_config=game_config,
        token_addr="0x0000000000000000000000000000000000000000",
        is_native=True,
        bet_amount=chain_bet,
        deadline=deadline,
        server_signature=server_sig,
    )

    tx_hash = web3_utils.build_and_send_tx(
        w3=w3,
        private_key=private_key,
        to=CASH_OR_CRASH_ADDR,
        data=calldata,
        value=chain_bet,
        gas_limit_multiplier=gas_limit_multiplier,
    )

    db.add_superstake_round(eoa_address, tx_hash)
    rounds = db.get_superstake_rounds(eoa_address)
    logger.success(f"[Superstake] раунд {rounds} tx {tx_hash[:10]}…{tx_hash[-6:]}")
    return True


def _is_daily_ip_game_limit_error(exc: BaseException) -> bool:
    """HTTP 403 от POST /v1/game: дневной лимит IP-игр (не ошибка конфига)."""
    s = str(exc).lower()
    if "403" not in s and "daily" not in s:
        return False
    if "daily limit" in s and "ip" in s:
        return True
    if "reached the daily limit" in s:
        return True
    if "try again tomorrow" in s and ("ip" in s or "game" in s):
        return True
    return False


# ── Main entry ────────────────────────────────────────────────────────────────

def run_claw_machine(
    private_key: str,
    w3: Web3,
    proxy: str | None = None,
    rounds_required: int = 10,
    action_delay_min: float = 5,
    action_delay_max: float = 15,
    firebase_api_key: str | None = None,
    proxy_pool: list[str | None] | None = None,
) -> bool:
    """
    Claw Machine до rounds_required успешных on-chain createGame (режим ip на backend).
    В теле POST /v1/game поле betAmount — строка в единицах IP (по умолчанию «10»; env SUPERSTAKE_IP_BET_AMOUNT).
    Отдельный POST /v1/users/login с Firebase не вызывается — лимиты по ответам POST /v1/game.
    Если API вернёт дневной лимит IP — оставшиеся раунды для кошелька пропускаются без ошибки (return True).
    После подтверждения tx результат игры на backend не опрашивается.

    Аутентификация: кошелёк -> Privy SIWE -> coc-backend (JWT из поля token) -> Firebase.
    firebase_api_key: опционально переопределить Web API Key; иначе встроенный / кэш.
    """
    from eth_account import Account as _Acc
    eoa_address = _Acc.from_key(private_key).address

    st = require_account_status(eoa_address, proxy, proxy_pool=proxy_pool)
    req_p = int(st.get("superstake_required", rounds_required))
    done_p = int(st.get("superstake", 0))
    if done_p >= req_p:
        info = db.get_account_info(eoa_address) or {}
        cur = int(info.get("superstake_rounds", 0))
        if done_p > cur:
            db.upsert_account(eoa_address, superstake_rounds=done_p)
        logger.info(f"[Superstake] {eoa_address} портал {done_p}/{req_p} — выход")
        return True

    # Портал опережает локальную БД (например раунды с телефона или старый quest_results.json)
    db_rounds = db.get_superstake_rounds(eoa_address)
    if done_p > db_rounds:
        db.upsert_account(eoa_address, superstake_rounds=done_p)

    ensure_firebase_api_key(firebase_api_key, proxy)

    completed = db.get_superstake_rounds(eoa_address)
    if completed >= rounds_required:
        logger.info(f"[Superstake] {eoa_address} {completed}/{rounds_required} — выход")
        return True

    logger.info(f"[Superstake] {eoa_address} {completed}/{rounds_required} IP")

    id_token = get_firebase_token_via_wallet(private_key, proxy)

    # Play rounds (лимит IP / квест — по ответам POST /v1/game, без отдельного users/login)
    while completed < rounds_required:
        try:
            play_one_round(private_key, w3, id_token, proxy)
            completed = db.get_superstake_rounds(eoa_address)
        except Exception as e:
            if _is_daily_ip_game_limit_error(e):
                logger.info(
                    f"[Superstake] {eoa_address} лимит IP {completed}/{rounds_required} — стоп"
                )
                return True
            err_s = str(e).lower()
            logger.error(f"[Superstake] раунд {e}")
            if "401" in err_s or "unauthorized" in err_s:
                id_token = get_firebase_token_via_wallet(private_key, proxy)
            elif ("/v1/game" in err_s or "post /v1/game" in err_s) and any(
                x in err_s for x in ("429", "exhaust", "quota")
            ):
                logger.warning("[Superstake] /game 429 → 30s")
                time.sleep(30)
            else:
                time.sleep(10)

        if completed >= rounds_required:
            break

        delay = random.uniform(action_delay_min, action_delay_max)
        logger.info(f"[Superstake] пауза {delay:.0f}s")
        time.sleep(delay)

    logger.success(f"[Superstake] OK {rounds_required} {eoa_address}")
    return True
