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
import re
import time
import uuid
from pathlib import Path

import requests
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3

from modules import db, logger
from modules.elhexa_period import (
    apply_elhexa_config_env,
    elhexa_current_period_id,
    elhexa_next_reset_msk_str,
)
from modules.portal_api import get_bonus_dapp_data, parse_account_status, require_account_status
from modules.startale_gm import (
    CHAIN_HEX,
    CHROMIUM_ARGS,
    MOCK_WALLET_INIT_SCRIPT,
    USER_AGENT,
    _playwright_proxy,
)

# ELHEXA Checkin contract (USDSC в конфиге игры; для тестов UserOp / совместимости)
ELHEXA_CONTRACT = "0xb97DDf414748d1DBEF846fc2Fe74391f7Bc8A715"
ELHEXA_API_BASE = "https://api.elhexa.io/api/v1"

# checkin(uint256 id, uint256 amount) selector
CHECKIN_SELECTOR = bytes.fromhex("7c21bd5a")

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_APP_URL = "https://app.startale.com/miniapps#elhexa"


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


async def _try_sign_elhexa_login(page) -> None:
    """Подпись входа в elhexa.io (Sign message) — кнопка Sign в оверлее Startale."""
    try:
        sign = page.get_by_role("button", name=re.compile(r"^Sign$", re.I))
        if await sign.count() > 0:
            await sign.first.click(timeout=15_000)
            logger.debug("[ELHEXA] Sign (elhexa.io)")
            await page.wait_for_timeout(2500)
    except Exception:
        pass


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
    """Клик по canvas в долях ширины/высоты (0..1)."""
    try:
        await canvas.wait_for(state="visible", timeout=30_000)
        box = await canvas.bounding_box()
        if not box:
            return False
        await canvas.click(
            position={"x": box["width"] * rx, "y": box["height"] * ry},
            timeout=15_000,
        )
        return True
    except Exception:
        return False


async def _flow_gift_airdrop_and_rewards(game_frame) -> None:
    """
    Gift Airdrop: тап по подарку → модалка REWARDS → Accept (всё на canvas).
    Координаты как в отладочном проходе Playwright MCP.
    """
    canvas = game_frame.locator("canvas").first
    gx = _env_float("BONUS9_ELHEXA_CANVAS_GIFT_X", 0.5)
    gy = _env_float("BONUS9_ELHEXA_CANVAS_GIFT_Y", 0.32)
    if await _canvas_click_fraction(canvas, gx, gy):
        logger.debug("[ELHEXA] canvas gift")
    await game_frame.page.wait_for_timeout(5000)
    for ry in (0.42, 0.38, 0.48, 0.52):
        await _canvas_click_fraction(canvas, 0.5, ry)
        await game_frame.page.wait_for_timeout(400)
    ax = _env_float("BONUS9_ELHEXA_CANVAS_ACCEPT_X", 0.5)
    ay = _env_float("BONUS9_ELHEXA_CANVAS_ACCEPT_Y", 0.58)
    if await _canvas_click_fraction(canvas, ax, ay):
        logger.debug("[ELHEXA] canvas Accept")
    await game_frame.page.wait_for_timeout(1500)


async def _flow_checkin_bonus_free_tier(game_frame) -> None:
    """CHECK-IN BONUS: бесплатный ряд — кнопка $0 (canvas)."""
    canvas = game_frame.locator("canvas").first
    bx = _env_float("BONUS9_ELHEXA_CANVAS_CHECKIN_X", 0.72)
    by = _env_float("BONUS9_ELHEXA_CANVAS_CHECKIN_Y", 0.52)
    if await _canvas_click_fraction(canvas, bx, by):
        logger.debug("[ELHEXA] canvas $0")
    await game_frame.page.wait_for_timeout(1000)


async def _checkin_free_tier_claimed_in_ui(page, game_fr) -> bool:
    """
    Бесплатный день уже отмечен (Claimed) — модалка Approve/Confirm не появится.
    Реальный «Claimed» после чекина часто только на canvas (Cocos) — в DOM его может не быть;
    тогда успех ловим по порталу / tx из сети (_wait_checkin_outcome).
    """
    targets: list = [page, game_fr]
    for t in targets:
        try:
            if await t.locator("text=/\\bClaimed\\b/i").count() > 0:
                if await t.locator("text=/CHECK-IN|check-in|Day\\s*1|7-Day|On-chain/i").count() > 0:
                    return True
            raw = await t.evaluate(
                """() => {
                  const b = document.body;
                  return b && b.innerText ? b.innerText : '';
                }"""
            )
            low = (raw or "").lower()
            if "claimed" in low and (
                "check-in" in low
                or "check in" in low
                or "day 1" in low
                or "check-in bonus" in low
            ):
                return True
        except Exception:
            continue
    try:
        html = (await page.content()).lower()
        if "claimed" in html and ("check-in" in html or "check in bonus" in html):
            return True
    except Exception:
        pass
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
    max_wait_s = int(os.environ.get("BONUS9_ELHEXA_CONFIRM_WAIT_S", "120"))
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
        if now - last_log > 12.0:
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
            if game_fr and await _checkin_free_tier_claimed_in_ui(page, game_fr):
                logger.debug("[ELHEXA] Claimed после Approve (UI)")
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
    pw,
    private_key: str,
    rpc_url: str,
    proxy: str | None,
    headless: bool,
    check_id: int,
    app_url: str,
    portal_elhexa_before: int,
    current_period_id: str,
) -> bool:
    from curl_cffi import requests as curl_requests

    acct = Account.from_key(private_key)
    address = acct.address
    wallet_uuid = str(uuid.uuid4())

    rpc = curl_requests.Session(impersonate="chrome124")
    if proxy:
        rpc.proxies = {"http": proxy, "https": proxy}

    tx_bucket: list[str] = []

    async def eip1193_request(req):
        method = req.get("method", "")
        params = req.get("params", [])
        if os.environ.get("ELHEXA_DEBUG_RPC"):
            logger.debug(f"[ELHEXA RPC] {method}")

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

        if method == "wallet_sendCalls":
            logger.debug("[ELHEXA] wallet_sendCalls stub → bundler")
            return "0x" + "00" * 32

        if method == "wallet_getCallsStatus":
            return {"status": 200}

        if method == "eth_sendTransaction":
            raise RuntimeError("eth_sendTransaction not supported — мини-приложение использует WaaS / bundler")

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

    def _on_response(response) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(_capture_bundler_tx_hash(response, tx_bucket))

    page.on("response", _on_response)

    try:
        logger.debug(f"[ELHEXA] goto {app_url}")
        await page.goto(app_url, wait_until="domcontentloaded", timeout=120_000)
        try:
            await page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception:
            pass
        await page.wait_for_timeout(2000)

        connect_btn = page.locator(
            "button:has-text('Connect a wallet'), button:has-text('Connect Wallet')"
        ).first
        if await connect_btn.count() > 0:
            logger.debug("[ELHEXA] connect wallet")
            await connect_btn.click()
            await page.wait_for_timeout(2000)
            mock_btn = page.locator("button:has-text('Mock Wallet')").first
            if await mock_btn.count() > 0:
                await mock_btn.click()
                logger.debug("[ELHEXA] mock wallet")
            else:
                logger.error("[ELHEXA] нет Mock Wallet")
                return False
        else:
            logger.debug("[ELHEXA] Connect нет (уже?)")

        await _goto_miniapps_if_needed(page, app_url)
        try:
            await page.wait_for_load_state("networkidle", timeout=15_000)
        except Exception:
            pass
        await page.wait_for_timeout(2000)

        await _try_explore_mini_apps_intro(page)
        await _try_sign_elhexa_login(page)

        logger.debug("[ELHEXA] ждём iframe game.elhexa.io")
        game_fr = await _wait_elhexa_game_frame(page)
        if not game_fr:
            logger.error("[ELHEXA] iframe game.elhexa.io не появился")
            return False

        logger.debug("[ELHEXA] флоу gift→rewards→$0")
        skip_gift = os.environ.get("BONUS9_ELHEXA_SKIP_GIFT", "").strip().lower() in (
            "1",
            "true",
            "yes",
        )
        if not skip_gift:
            await _flow_gift_airdrop_and_rewards(game_fr)
        else:
            logger.debug("[ELHEXA] пропуск Gift Airdrop (BONUS9_ELHEXA_SKIP_GIFT)")
        await page.wait_for_timeout(2000)

        if await _checkin_free_tier_claimed_in_ui(page, game_fr):
            logger.info("[ELHEXA] Claimed в UI до $0, пропуск")
            db.mark_elhexa_done(address, period_id=current_period_id)
            return True

        await _flow_checkin_bonus_free_tier(game_fr)
        await page.wait_for_timeout(2000)

        if await _checkin_free_tier_claimed_in_ui(page, game_fr):
            logger.info("[ELHEXA] Claimed в UI после $0, пропуск")
            db.mark_elhexa_done(address, period_id=current_period_id)
            return True

        if not await _confirm_startale_transaction(page):
            if await _checkin_free_tier_claimed_in_ui(page, game_fr):
                logger.info("[ELHEXA] Claimed в UI, Confirm не было — OK")
                db.mark_elhexa_done(address, period_id=current_period_id)
                return True
            raw = get_bonus_dapp_data(address, proxy)
            if raw:
                st2 = parse_account_status(raw)
                p2 = int(st2.get("elhexa", 0))
                if p2 > portal_elhexa_before:
                    logger.info(f"[ELHEXA] портал {portal_elhexa_before}→{p2}, OK (без Confirm)")
                    db.upsert_account(
                        address,
                        elhexa_last_period=current_period_id,
                        elhexa_last_date=current_period_id,
                        elhexa_total=p2,
                    )
                    return True
            logger.error("[ELHEXA] нет Confirm")
            return False

        tx_wait = int(os.environ.get("BONUS9_ELHEXA_TX_WAIT", "120"))
        success = await _wait_checkin_outcome(
            page,
            tx_bucket,
            game_fr=game_fr,
            max_wait_s=tx_wait,
            address=address,
            proxy=proxy,
            portal_elhexa_before=portal_elhexa_before,
        )

        if success:
            logger.success("[ELHEXA] OK")
            th = _last_tx_hash(tx_bucket)
            if th:
                logger.debug(f"[ELHEXA] tx из bundler: {th[:10]}…{th[-6:]}")
            sa = db.get_smart_account(address)
            if th and sa:
                post_elhexa_checkin_verify(th, sa, check_id, proxy)
            db.mark_elhexa_done(address, period_id=current_period_id)
            return True

        logger.warning("[ELHEXA] таймаут после Confirm")
        return False

    except Exception as e:
        logger.error(f"[ELHEXA] {e}")
        return False
    finally:
        await browser.close()


async def _do_elhexa_async(
    private_key: str,
    proxy: str | None,
    rpc_url: str,
    headless: bool,
    check_id: int,
    app_url: str,
    proxy_pool: list[str | None] | None = None,
) -> bool:
    from playwright.async_api import async_playwright

    acct = Account.from_key(private_key)
    eoa_address = acct.address
    period_id = elhexa_current_period_id()
    next_reset = elhexa_next_reset_msk_str()

    # 1) Сначала портал — без этого не открываем браузер и не делаем on-chain попытку.
    st = require_account_status(eoa_address, proxy, proxy_pool=proxy_pool)
    req = int(st.get("elhexa_required", 3))
    portal_cnt = int(st.get("elhexa", 0))
    el_done = bool(st.get("elhexa_done"))
    info = db.get_account_info(eoa_address) or {}
    db_total = int(info.get("elhexa_total", 0))
    cnt = max(portal_cnt, db_total)

    logger.info(
        f"[ELHEXA] {portal_cnt}/{req} БД={db_total} done={el_done} | "
        f"период {period_id} МСК | сброс ~{next_reset}"
    )
    logger.debug(f"[ELHEXA] шаги {min(portal_cnt, req)}/{req}")

    if el_done or cnt >= req:
        logger.info(f"[ELHEXA] квест закрыт {cnt}/{req}, пропуск")
        if portal_cnt > db_total:
            db.upsert_account(eoa_address, elhexa_total=portal_cnt)
        return False

    if portal_cnt > db_total:
        old_db = db_total
        db.upsert_account(eoa_address, elhexa_total=portal_cnt)
        logger.info(f"[ELHEXA] синхр БД {old_db}→{portal_cnt}")
        force_browser = os.environ.get(
            "BONUS9_ELHEXA_FORCE_BROWSER_AFTER_SYNC", ""
        ).strip().lower() in ("1", "true", "yes", "on")
        if not force_browser:
            db.upsert_account(
                eoa_address,
                elhexa_last_period=period_id,
                elhexa_last_date=period_id,
            )
            logger.info(
                f"[ELHEXA] skip браузер: портал впереди, период {period_id} ок"
            )
            logger.debug(
                "[ELHEXA] принудительно: BONUS9_ELHEXA_FORCE_BROWSER_AFTER_SYNC=1"
            )
            return False
        logger.warning("[ELHEXA] FORCE_BROWSER_AFTER_SYNC=1, playwright")

    db_total = int(db.get_account_info(eoa_address).get("elhexa_total", 0))

    if db.is_elhexa_done_this_period(eoa_address, period_id):
        logger.debug(f"[ELHEXA] портал {portal_cnt} vs БД {db_total}")
        logger.info(
            f"[ELHEXA] период {period_id} уже в БД | {portal_cnt}/{req} | next ~{next_reset}"
        )
        return False

    if is_paused(rpc_url):
        logger.warning("[ELHEXA] paused, пропуск")
        return False

    logger.info(f"[ELHEXA] {eoa_address} playwright")

    async with async_playwright() as pw:
        ok = await _process_wallet(
            pw,
            private_key,
            rpc_url,
            proxy,
            headless,
            check_id,
            app_url,
            portal_elhexa_before=portal_cnt,
            current_period_id=period_id,
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

    Перед запуском Playwright всегда вызывается портал (require_account_status): если квест
    по ELHEXA уже закрыт (completed/required), браузер не открывается.

    Если портал показывает больший счётчик, чем БД (например 1/3 при elhexa_total=0), после
    синхронизации браузер по умолчанию не запускается — чекин уже зачтён на портале.
    Принудительно: BONUS9_ELHEXA_FORCE_BROWSER_AFTER_SYNC=1.

    Игровой день: граница по Europe/Moscow (по умолчанию 22:00), см. config
    `elhexa_reset_hour_msk` или env BONUS9_ELHEXA_RESET_HOUR_MSK.
    """
    try:
        import toml

        cfg_path = PROJECT_ROOT / "config.toml"
        if cfg_path.exists():
            apply_elhexa_config_env(toml.load(cfg_path))
    except Exception:
        pass

    app_url = (os.environ.get("BONUS9_ELHEXA_APP_URL") or "").strip() or DEFAULT_APP_URL
    hl = headless
    if hl is None:
        raw = (
            os.environ.get("BONUS9_ELHEXA_HEADLESS", "")
            or os.environ.get("BONUS9_GM_HEADLESS", "1")
        ).strip().lower()
        hl = raw not in ("0", "false", "no", "off")

    return asyncio.run(
        _do_elhexa_async(
            private_key, proxy, rpc_url, hl, check_id, app_url, proxy_pool=proxy_pool
        )
    )
