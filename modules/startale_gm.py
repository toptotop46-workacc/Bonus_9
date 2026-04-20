"""Daily GM (Startale app) через браузер: mock EIP-6963 + подпись в Python (как в рабочем gm.py)."""

from __future__ import annotations

import asyncio
import json
import os
import random
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

# Виртуальный same-origin эндпоинт для RPC: Playwright перехватит запрос,
# Python обработает подпись и вернёт ответ через route.fulfill().
# CSP connect-src 'self' разрешает запросы к собственному origin → никаких блокировок.
RPC_ENDPOINT = f"{APP_URL}/__gmrpc__"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
)

# ── Кошелёк (route-injected — работает в native page principal) ─────────────
# RPC-запросы идут через fetch к same-origin /__gmrpc__ — перехватываются Playwright.
# Никаких window-globals, никакого page.evaluate(), никаких compartment-барьеров.
# %UUID% заменяется перед инжекцией.
MOCK_WALLET_SCRIPT = """
(function() {
  'use strict';
  var _uuid = "%UUID%";
  var RPC_URL = 'https://app.startale.com/__gmrpc__';

  function rpc(method, params) {
    return fetch(RPC_URL, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({method: method, params: params || []})
    }).then(function(r) { return r.json(); }).then(function(resp) {
      if (resp.error) throw new Error(resp.error);
      return resp.result;
    });
  }

  var provider = {
    isMetaMask: true,
    selectedAddress: null,
    chainId: "0x74c",
    networkVersion: "1868",
    request: function(req) {
      console.log('[GM-wallet] request:', req.method);
      return rpc(req.method, req.params || []);
    },
    send: function(method, params) {
      if (typeof method === 'object') return rpc(method.method, method.params || []);
      return rpc(method, params || []);
    },
    sendAsync: function(req, cb) {
      rpc(req.method, req.params || []).then(function(r) {
        cb(null, {id: req.id, jsonrpc: '2.0', result: r});
      }).catch(function(e) { cb(e); });
    },
    on: function() { return this; },
    removeListener: function() { return this; },
    _metamask: { isUnlocked: function() { return Promise.resolve(true); } },
  };

  try {
    Object.defineProperty(window, 'ethereum', {
      value: provider, writable: true, configurable: true,
    });
  } catch(e) { window.ethereum = provider; }

  var info = {
    uuid: _uuid,
    name: "MetaMask",
    icon: "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCA0MDAgNDAwIj48cmVjdCB3aWR0aD0iNDAwIiBoZWlnaHQ9IjQwMCIgZmlsbD0iI0Y2ODUxQiIgcng9IjEwMCIvPjwvc3ZnPg==",
    rdns: "io.metamask",
  };
  function announce() {
    window.dispatchEvent(new CustomEvent("eip6963:announceProvider", {
      detail: {info: info, provider: provider},
      bubbles: true,
    }));
    console.log('[GM-wallet] eip6963:announceProvider dispatched');
  }
  window.addEventListener("eip6963:requestProvider", function() {
    console.log('[GM-wallet] eip6963:requestProvider received');
    announce();
  });
  announce();
  console.log('[GM-wallet] wallet injected (fetch mode)');
})();
"""

# Старое имя для совместимости
MOCK_WALLET_INIT_SCRIPT = MOCK_WALLET_SCRIPT


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

    # ── Python-обработчик RPC-запросов ────────────────────────────────────────
    # Вызывается из sandbox bridge через expose_function (не сеть → нет CSP).
    async def _rpc_handler(payload_json: str) -> str:
        try:
            data = json.loads(payload_json)
            method = data.get("method", "")
            params = data.get("params", [])
            if os.environ.get("GM_DEBUG_RPC"):
                logger.debug(f"[GM RPC] {method} params={str(params)[:120]}")

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
                raise RuntimeError("eth_sendTransaction not supported — GM uses WaaS")

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
            logger.debug(f"[GM RPC error] {exc}")
            return json.dumps({"error": str(exc)})

    return await _browser_session(
        acct, address, wallet_uuid, _rpc_handler, proxy, headless,
    )


async def _browser_session(
    acct, address, wallet_uuid, rpc_handler, proxy, headless,
) -> bool:
    from camoufox.async_api import AsyncCamoufox

    os_choice = random.choice(["windows", "macos"])
    px = _playwright_proxy(proxy)

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
            logger.debug(f"[GM] route inject error: {_exc}")
            await route.continue_()

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

        # ── Python RPC route handler ─────────────────────────────────────────
        # Wallet делает fetch('https://app.startale.com/__gmrpc__', {method:'POST',...}).
        # CSP разрешает connect-src 'self' → запрос идёт, Playwright перехватывает.
        # Никаких compartment-барьеров, никакого polling.
        async def _rpc_route(route):
            try:
                body = route.request.post_data
                item = json.loads(body) if body else {}
                method = item.get("method", "unknown")
                if os.environ.get("GM_DEBUG_RPC"):
                    logger.debug(f"[GM RPC] {method} params={str(item.get('params',''))[:120]}")
                result_json = await rpc_handler(json.dumps(item))
                resp = json.loads(result_json)
                await route.fulfill(
                    status=200,
                    content_type="application/json",
                    body=json.dumps(resp),
                )
            except Exception as _rpc_exc:
                logger.debug(f"[GM] rpc_route error: {_rpc_exc}")
                try:
                    await route.fulfill(
                        status=200,
                        content_type="application/json",
                        body=json.dumps({"error": str(_rpc_exc)}),
                    )
                except Exception:
                    pass

        await page.route(f"{APP_URL}/__gmrpc__", _rpc_route)

        def _on_console(msg):
            if os.environ.get("GM_DEBUG_RPC"):
                if msg.type in ("error", "warning") or "[GM-wallet]" in msg.text:
                    logger.debug(f"[GM browser:{msg.type}] {msg.text[:500]}")
        page.on("console", _on_console)
        page.on("pageerror", lambda e: logger.debug(f"[GM pageerror] {str(e)[:300]}") if os.environ.get("GM_DEBUG_RPC") else None)


        try:
            await page.goto(APP_URL, wait_until="domcontentloaded", timeout=60_000)

            # Ждём редиректа на /sign-up — означает, что React SPA загрузился
            try:
                await page.wait_for_url("**/sign-up**", timeout=90_000)
                logger.debug("[GM] редирект на /sign-up выполнен")
            except Exception:
                logger.debug("[GM] /sign-up не дождались, продолжаем на текущем URL")

            # Ждём кнопки "Connect a wallet"
            connect_btn = page.locator(
                "button:has-text('Connect a wallet'), button:has-text('Connect Wallet')"
            ).first
            try:
                await connect_btn.wait_for(timeout=30_000)
            except Exception:
                logger.debug("[GM] кнопка Connect не появилась")

            if await connect_btn.count() > 0:
                logger.debug("[GM] connect wallet")
                await connect_btn.click()
                try:
                    await page.wait_for_selector(
                        "input[placeholder*='wallet'], input[placeholder*='Wallet'], "
                        "[placeholder*='Search'], input[type='search']",
                        timeout=8_000,
                    )
                    logger.debug("[GM] wallet modal opened")
                except Exception:
                    await page.wait_for_timeout(2000)

                mm_item = page.get_by_text("MetaMask", exact=True).first
                try:
                    await mm_item.wait_for(timeout=5_000)
                except Exception:
                    pass

                if await mm_item.count() > 0:
                    await mm_item.click()
                    logger.debug("[GM] MetaMask (fake) clicked")
                    # Ждём завершения SIWE-аутентификации: URL изменится с /sign-up
                    try:
                        await page.wait_for_url(
                            lambda url: "/sign-up" not in url,
                            timeout=30_000,
                        )
                        logger.debug(f"[GM] аутентифицирован, URL: {page.url}")
                    except Exception:
                        logger.debug("[GM] /sign-up timeout — ждём ещё")
                        await page.wait_for_timeout(5000)
                else:
                    all_shadow_text = await page.evaluate("""
                        () => {
                            const texts = [];
                            function walk(el) {
                                if (el.shadowRoot) {
                                    texts.push(el.shadowRoot.textContent.slice(0, 100));
                                    for (const c of el.shadowRoot.children) walk(c);
                                }
                                for (const c of el.children) walk(c);
                            }
                            walk(document.documentElement);
                            return texts.filter(t => t.trim()).slice(0, 5);
                        }
                    """)
                    logger.debug(f"[GM] shadow DOM texts: {all_shadow_text}")
                    await page.screenshot(path="modal_debug.png", full_page=True)
                    logger.error("[GM] нет MetaMask в модале")
                    return False
            else:
                logger.debug("[GM] Кнопка Connect не найдена — возможно уже подключено")
                await page.wait_for_timeout(3000)

            logger.debug("[GM] wait UI")
            # Дать странице время отрендерить GM-контент после авторизации
            await page.wait_for_timeout(3000)

            page_text = (await page.inner_text("body")).lower()
            if "next gm available in" in page_text:
                logger.info("[GM] уже GM сегодня (next available in)")
                db.mark_gm_done(address)
                return True

            gm_selectors = [
                "button:has-text('Send GM back')",
                "button:has-text('Send GM')",
                "button:has-text('GM')",
                "[role='button']:has-text('GM')",
            ]
            gm_btn = None

            # Ждём появления GM кнопки (страница могла ещё не прогрузить секцию)
            try:
                await page.wait_for_selector(
                    ", ".join(gm_selectors), timeout=15_000
                )
            except Exception:
                # Проверим ещё раз после таймаута — может быть "Next GM available in"
                page_text = (await page.inner_text("body")).lower()
                if "next gm available in" in page_text:
                    logger.info("[GM] уже GM сегодня (next available in после таймаута)")
                    db.mark_gm_done(address)
                    return True
                pass

            for sel in gm_selectors:
                btn = page.locator(sel).first
                cnt = await btn.count()
                if cnt > 0:
                    try:
                        is_dis = await btn.is_disabled()
                    except Exception:
                        is_dis = False
                    if os.environ.get("GM_DEBUG_RPC"):
                        logger.debug(f"[GM btn] {sel!r}: count={cnt} disabled={is_dis}")
                    if not is_dis:
                        gm_btn = btn
                        break

            # Fallback: get_by_role
            if gm_btn is None:
                for text in ("Send GM back", "Send GM", "GM"):
                    btn = page.get_by_role("button", name=re.compile(re.escape(text), re.I)).first
                    cnt = await btn.count()
                    if cnt > 0:
                        try:
                            is_dis = await btn.is_disabled()
                        except Exception:
                            is_dis = False
                        if os.environ.get("GM_DEBUG_RPC"):
                            logger.debug(f"[GM btn role] {text!r}: count={cnt} disabled={is_dis}")
                        if not is_dis:
                            gm_btn = btn
                            break

            # Крайний fallback: get_by_text (любой элемент с текстом)
            if gm_btn is None:
                for text in ("Send GM back", "Send GM"):
                    btn = page.get_by_text(text, exact=False).first
                    if await btn.count() > 0:
                        if os.environ.get("GM_DEBUG_RPC"):
                            logger.debug(f"[GM btn text] '{text}' found via get_by_text")
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
                    else:
                        logger.debug("[GM] Кнопки UI: (пусто)")
                except Exception as ex:
                    logger.debug(f"[GM] evaluate error: {ex}")
                try:
                    await page.screenshot(path="gm_debug.png", full_page=True)
                    logger.debug("[GM] скриншот → gm_debug.png")
                    content = await page.content()
                    with open("gm_debug.html", "w", encoding="utf-8") as f:
                        f.write(content)
                    logger.debug(f"[GM] HTML ({len(content)} bytes) → gm_debug.html")
                    logger.debug(f"[GM] URL: {page.url}")
                except Exception as ex:
                    logger.debug(f"[GM] screenshot/html error: {ex}")
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


async def _do_gm_async(
    private_key: str,
    proxy: str | None,
    rpc_url: str,
    force: bool,
    headless: bool,
    proxy_pool: list[str | None] | None = None,
) -> bool:
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

    logger.info(f"[GM] {eoa_address} camoufox")

    return await _process_wallet(private_key, rpc_url, proxy, headless)


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
