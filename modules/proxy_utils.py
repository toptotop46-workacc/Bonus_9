"""Proxy file parsing. Format: host:port:user:pass or host:port"""

import random
import re
from pathlib import Path
from modules import logger

# Random offset set once per process run — spreads wallets across proxy pool differently each session
_SESSION_OFFSET: int = random.randint(0, 999)


def parse_proxy_line(line: str) -> str | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    # already full url
    if line.startswith(("http://", "https://", "socks5://")):
        return line
    parts = line.split(":")
    if len(parts) == 2:
        host, port = parts
        return f"http://{host}:{port}"
    if len(parts) == 4:
        host, port, user, password = parts
        # Только явные шаблоны из инструкций (не подстроки: у провайдеров логин бывает literal "privatekey")
        if host.upper() in ("IP", "HOST", "<HOST>") or user.upper() in ("LOGIN", "USER", "<USER>"):
            return None
        return f"http://{user}:{password}@{host}:{port}"
    return None


def load_proxies_from_file(path: Path) -> list[str]:
    if not path.exists():
        logger.warning(f"proxy.txt не найден: {path}")
        return []
    proxies = []
    for line in path.read_text(encoding="utf-8").splitlines():
        p = parse_proxy_line(line)
        if p:
            proxies.append(p)
    logger.info(f"Прокси: {len(proxies)}")
    return proxies


def match_proxy(proxies: list[str], index: int) -> str | None:
    if not proxies:
        return None
    return proxies[(index + _SESSION_OFFSET) % len(proxies)]


def rotate_proxy(
    proxies: list[str],
    current: str | None,
    exclude: list[str] | None = None,
) -> str | None:
    """Return a proxy different from *current* (for mid-session rotation on error)."""
    if not proxies:
        return None
    blocked = {current} if current else set()
    if exclude:
        blocked.update(exclude)
    candidates = [p for p in proxies if p not in blocked]
    return random.choice(candidates) if candidates else random.choice(proxies)


def nonempty_proxies(proxies: list[str | None]) -> list[str]:
    """Непустые строки прокси из списка (как в proxy.txt)."""
    return [p.strip() for p in proxies if p and str(p).strip()]


def pick_referral_proxy_pair(
    proxies: list[str | None],
    wallet_index: int,
    rotation: int = 0,
) -> tuple[str | None, str | None]:
    """
    Два разных прокси для реферала: основной аккаунт и регистрация приглашённого.
    rotation — сдвиг по списку (для смены пары при мёртвых прокси).
    """
    cleaned = nonempty_proxies(proxies)
    if len(cleaned) < 2:
        return None, None
    n = len(cleaned)
    base = (wallet_index + rotation) % n
    main_p = cleaned[base]
    for off in range(1, n + 1):
        ref_p = cleaned[(base + off) % n]
        if ref_p != main_p:
            return main_p, ref_p
    return None, None


def referral_ref_alternatives(
    cleaned: list[str],
    base_idx: int,
    main_p: str,
) -> list[str]:
    """Все отличные от main_p прокси по порядку от base_idx (для ротации приглашённого)."""
    n = len(cleaned)
    out: list[str] = []
    seen: set[str] = set()
    for k in range(1, n + 1):
        c = cleaned[(base_idx + k) % n]
        if c != main_p and c not in seen:
            seen.add(c)
            out.append(c)
    return out
