"""Daily GM (Startale app) через браузер: mock EIP-6963 + подпись в Python (как в рабочем gm.py)."""

from __future__ import annotations

import asyncio
import json
import os
import re
import uuid

from eth_account import Account
from eth_account.messages import encode_defunct

from modules import db, logger
from modules.portal_api import get_bonus_dapp_data, parse_account_status, require_account_status

# ── Сеть / приложение (Soneium) ───────────────────────────────────────────────
CHAIN_ID = 1868
CHAIN_HEX = hex(CHAIN_ID)
APP_URL = "https://app.startale.com"

CHROMIUM_ARGS = ["--disable-blink-features=AutomationControlled"]
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

MOCK_WALLET_INIT_SCRIPT = """
(function({uuid}) {
  function announce() {
    const provider = {
      request: async (req) => eip1193Request({ ...req, uuid }),
      on: () => {},
      removeListener: () => {},
    };
    const info = {
      uuid,
      name: "Mock Wallet",
      icon: "data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' width='24' height='24'><rect width='24' height='24' fill='%234F46E5' rx='4'/><text x='12' y='17' text-anchor='middle' fill='white' font-size='14'>M</text></svg>",
      rdns: "com.example.mock-wallet",
    };
    const evt = new CustomEvent("eip6963:announceProvider", {
      detail: Object.freeze({ info, provider }),
    });
    window.dispatchEvent(evt);
  }
  announce();
  window.addEventListener("eip6963:requestProvider", announce);
  window.addEventListener("DOMContentLoaded", announce);
})({uuid: "%UUID%"});
"""


def _playwright_proxy(proxy: str | None) -> dict | None:
    if not proxy:
        return None
    from urllib.parse import urlparse

    p = urlparse(proxy)
    if not p.hostname:
        return {"server": proxy}
    scheme = p.scheme or "http"
    port = p.port or (443 if scheme == "https" else 80)
    server = f"{scheme}://{p.hostname}:{port}"
    out: dict = {"server": server}
    if p.username:
        out["username"] = p.username
        out["password"] = p.password or ""
    return out


async def _process_wallet(
    pw,
    private_key: str,
    rpc_url: str,
    proxy: str | None,
    headless: bool,
) -> bool:
    from curl_cffi import requests as curl_requests

    acct = Account.from_key(private_key)
    address = acct.address
    wallet_uuid = str(uuid.uuid4())

    rpc = curl_requests.Session(impersonate="chrome124")
    if proxy:
        rpc.proxies = {"http": proxy, "https": proxy}

    async def eip1193_request(req):
        method = req.get("method", "")
        params = req.get("params", [])
        if os.environ.get("GM_DEBUG_RPC"):
            logger.debug(f"[GM RPC] {method}")

        if method in ("eth_accounts", "eth_requestAccounts"):
            return [address]

        if method == "eth_chainId":
            return CHAIN_HEX

        if method == "wallet_requestPermissions":
            return [{"parentCapability": "eth_accounts"}]

        if method == "wallet_revokePermissions":
            return None

        if method == "wallet_getPermissions":
            return []

        if method == "wallet_switchEthereumChain":
            return None

        if method == "personal_sign":
            msg_hex = params[0] if params else ""
            raw = bytes.fromhex(msg_hex[2:]) if msg_hex.startswith("0x") else msg_hex.encode()
            sig = acct.sign_message(encode_defunct(primitive=raw))
            return "0x" + sig.signature.hex()

        if method in ("eth_signTypedData_v4", "eth_signTypedData"):
            typed = None
            if len(params) >= 2:
                typed = params[1]
            elif params:
                typed = params[0]
            if typed is None:
                raise RuntimeError("eth_signTypedData: missing typed data")
            if isinstance(typed, str):
                typed = json.loads(typed)
            sig = acct.sign_typed_data(full_message=typed)
            return "0x" + sig.signature.hex()

        if method == "wallet_addEthereumChain":
            return None

        if method == "eth_sendTransaction":
            raise RuntimeError("eth_sendTransaction not supported — GM uses WaaS")

        resp = rpc.post(
            rpc_url,
            json={"jsonrpc": "2.0", "method": method, "params": params, "id": 1},
            headers={"Content-Type": "application/json"},
            timeout=60,
        )
        data = resp.json()
        if "error" in data:
            raise RuntimeError(json.dumps(data["error"]))
        return data.get("result")

    browser = await pw.chromium.launch(headless=headless, args=CHROMIUM_ARGS)
    ctx_kw: dict = {
        "viewport": {"width": 1280, "height": 900},
        "user_agent": USER_AGENT,
    }
    px = _playwright_proxy(proxy)
    if px:
        ctx_kw["proxy"] = px

    ctx = await browser.new_context(**ctx_kw)
    await ctx.expose_function("eip1193Request", eip1193_request)
    script = MOCK_WALLET_INIT_SCRIPT.replace("%UUID%", wallet_uuid)
    await ctx.add_init_script(script)

    page = await ctx.new_page()

    try:
        logger.debug("[GM] goto app.startale.com")
        await page.goto(APP_URL, wait_until="domcontentloaded")
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass
        await page.wait_for_timeout(2000)

        connect_btn = page.locator(
            "button:has-text('Connect a wallet'), button:has-text('Connect Wallet')"
        ).first
        if await connect_btn.count() > 0:
            logger.debug("[GM] connect wallet")
            await connect_btn.click()
            await page.wait_for_timeout(2000)
            mock_btn = page.locator("button:has-text('Mock Wallet')").first
            if await mock_btn.count() > 0:
                await mock_btn.click()
                logger.debug("[GM] mock wallet")
            else:
                logger.error("[GM] нет Mock Wallet")
                return False
        else:
            logger.debug("[GM] Кнопка Connect не найдена — возможно уже подключено")

        logger.debug("[GM] wait UI")
        await page.wait_for_timeout(10000)

        page_text = (await page.inner_text("body")).lower()
        if "next gm available" in page_text:
            logger.info("[GM] уже GM сегодня (next available)")
            db.mark_gm_done(address)
            return True

        gm_selectors = [
            "button:has-text('Send GM back')",
            "button:has-text('Send GM')",
            "button:has-text('GM')",
            "[role='button']:has-text('GM')",
        ]
        gm_btn = None
        for sel in gm_selectors:
            btn = page.locator(sel).first
            if await btn.count() > 0:
                disabled = await btn.get_attribute("disabled")
                if disabled is None:
                    gm_btn = btn
                    break
        if gm_btn is None:
            for pattern in (r"Send GM back", r"Send GM", r"^GM$"):
                btn = page.get_by_role("button", name=re.compile(pattern, re.I)).first
                if await btn.count() > 0:
                    disabled = await btn.get_attribute("disabled")
                    if disabled is None:
                        gm_btn = btn
                        break

        if gm_btn is None:
            logger.error("[GM] кнопка GM не найдена")
            try:
                labels = await page.evaluate(
                    """() => [...new Set([...document.querySelectorAll('button,[role=button],a[role=button]')]
                      .map(e => (e.innerText || '').trim()).filter(Boolean))].slice(0, 50)"""
                )
                if labels:
                    logger.debug("[GM] Кнопки UI: " + ", ".join(labels[:25]))
            except Exception:
                pass
            return False

        logger.debug("[GM] click GM")
        await gm_btn.click()

        success = False
        for _ in range(45):
            await page.wait_for_timeout(2000)
            text = (await page.inner_text("body")).lower()
            if "next gm available" in text:
                success = True
                break
            if "gm confirmed" in text or "transaction confirmed" in text:
                success = True
                break
            all_gone = True
            for sel in gm_selectors:
                if await page.locator(sel).first.count() > 0:
                    all_gone = False
                    break
            if all_gone:
                await page.wait_for_timeout(5000)
                success = True
                break

        if success:
            logger.success("[GM] OK")
            try:
                await asyncio.sleep(5)
                raw = await asyncio.to_thread(get_bonus_dapp_data, address, proxy)
                if raw:
                    st = parse_account_status(raw)
                    logger.debug(
                        f"[GM] portal GM {st['gm']}/{st['gm_required']}"
                    )
            except Exception:
                pass
            db.mark_gm_done(address)
            return True

        logger.warning("[GM] таймаут подтверждения")
        return False

    except Exception as e:
        logger.error(f"[GM] {e}")
        return False
    finally:
        await browser.close()


async def _do_gm_async(
    private_key: str,
    proxy: str | None,
    rpc_url: str,
    force: bool,
    headless: bool,
    proxy_pool: list[str | None] | None = None,
) -> bool:
    from playwright.async_api import async_playwright

    acct = Account.from_key(private_key)
    eoa_address = acct.address

    if not force:
        st = require_account_status(eoa_address, proxy, proxy_pool=proxy_pool)
        gm_req = int(st.get("gm_required", 5))
        if int(st.get("gm", 0)) >= gm_req:
            logger.info(f"[GM] {eoa_address} портал OK, пропуск")
            db.mark_gm_done(eoa_address)
            return False
        if db.is_gm_done_today(eoa_address):
            logger.info(f"[GM] {eoa_address} уже сегодня (БД)")
            return False

    logger.info(f"[GM] {eoa_address} playwright")

    async with async_playwright() as pw:
        ok = await _process_wallet(pw, private_key, rpc_url, proxy, headless)

    return ok


def do_gm(
    private_key: str,
    proxy: str | None = None,
    rpc_url: str = "https://soneium-rpc.publicnode.com",
    force: bool = False,
    headless: bool | None = None,
    proxy_pool: list[str | None] | None = None,
) -> bool:
    """
    Ежедневный GM через app.startale.com (Dynamic WaaS подписывает on-chain часть).
    """
    hl = headless if headless is not None else os.environ.get("BONUS9_GM_HEADLESS", "1") not in (
        "0",
        "false",
        "False",
    )
    return asyncio.run(
        _do_gm_async(private_key, proxy, rpc_url, force, hl, proxy_pool=proxy_pool)
    )


# ── Совместимость: ончейн-проверки по SA больше не используются в этом модуле ──

def is_checked_in_today(sa_address: str, rpc_url: str = "https://soneium-rpc.publicnode.com") -> bool:
    """Устарело для GM-модуля; оставлено для возможных внешних вызовов."""
    from web3 import Web3

    CHECKIN_CONTRACT = "0x0B9f730bF4C1Bf1c0D5B548556a239d5eC0A1D3e"
    selector = Web3.keccak(text="hasCheckedInToday(address)")[:4]
    call_data = selector + bytes.fromhex(sa_address.removeprefix("0x").zfill(64))
    import requests

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
    """Устарело для GM-модуля; оставлено для возможных внешних вызовов."""
    from web3 import Web3

    import requests

    CHECKIN_CONTRACT = "0x0B9f730bF4C1Bf1c0D5B548556a239d5eC0A1D3e"
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
