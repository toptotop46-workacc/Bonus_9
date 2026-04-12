#!/usr/bin/env python3
"""
Bonus_9 — Soneium Score Season 9 automation
Entry point
"""

from __future__ import annotations

import random
import re
import sys
import time
from pathlib import Path
from typing import Optional

import toml
import questionary

from modules import logger, db, crypto_utils, proxy_utils
from modules.elhexa_period import apply_elhexa_config_env, elhexa_current_period_id
from modules.web3_utils import get_w3, get_eoa_address
from modules.portal_api import (
    fetch_portal_data_batch,
    parse_account_status,
    print_portal_status,
)

PROJECT_ROOT = Path(__file__).parent


# ── Banner ────────────────────────────────────────────────────────────────────

def show_banner() -> None:
    print("\033[96m")
    print("  ╔══════════════════════════════════════════════════════════╗")
    print("  ║          BONUS_9 — Soneium Score Season 9                ║")
    print("  ║              Автоматизация бонусных квестов              ║")
    print("  ╠══════════════════════════════════════════════════════════╣")
    print("  ║  1. Startale: Swap ETH→USDSC, Referral, Daily GM         ║")
    print("  ║  2. SoundChains: Mint Music NFT (прямой вызов контракта) ║")
    print("  ║  3. Superstake: Claw Machine 10 rounds                   ║")
    print("  ║  4. ELHEXA: Daily Check-In (3 чекина)                    ║")
    print("  ╚══════════════════════════════════════════════════════════╝")
    print("\033[0m")


# ── Config ────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    cfg_path = PROJECT_ROOT / "config.toml"
    if not cfg_path.exists():
        logger.error("config.toml не найден!")
        sys.exit(1)
    return toml.load(cfg_path)


# ── Wallets ───────────────────────────────────────────────────────────────────

def load_wallets() -> list[tuple[str, str]]:
    """Returns list of (private_key, eoa_address)."""
    keys = crypto_utils.load_keys_plaintext(PROJECT_ROOT)
    if not keys:
        logger.error("Нет ключей!")
        sys.exit(1)
    wallets = [(k, get_eoa_address(k)) for k in keys]
    logger.info(f"Кошельки: {len(wallets)}")
    return wallets


# ── Status table ──────────────────────────────────────────────────────────────

def show_status(
    wallets: list[tuple[str, str]],
    proxies: list[Optional[str]],
) -> None:
    """Print progress table for all wallets to stdout."""
    addresses = [addr for _, addr in wallets]
    portal_raw = fetch_portal_data_batch(addresses, [p for p in proxies if p])

    header = (
        f"{'#':<4} {'Адрес':<44} "
        f"{'Swap':<6} {'Ref':<5} {'GM':<7} "
        f"{'Chains':<8} {'Superstake':<12} {'ELHEXA':<10}"
    )
    sep = "─" * len(header)
    print(header)
    print(sep)

    for i, (_, addr) in enumerate(wallets):
        status = parse_account_status(portal_raw.get(addr))
        info   = db.get_account_info(addr) or {}

        swap  = "ok" if (status["swap_done"]        or info.get("swap_done"))        else "-"
        ref   = "ok" if (status["referral_done"]     or info.get("referral_done"))    else "-"
        gm    = f"{status['gm']}/{status['gm_required']}"
        sc    = "ok" if (status["soundchains_done"]  or info.get("soundchains_done")) else "-"
        ss_p  = max(status["superstake"], info.get("superstake_rounds", 0))
        ss    = f"{ss_p}/{status['superstake_required']}"
        elh_p = max(status["elhexa"], info.get("elhexa_total", 0))
        elh   = f"{elh_p}/{status['elhexa_required']}"

        print(
            f"{i+1:<4} {addr:<44} "
            f"{swap:<6} {ref:<5} {gm:<7} "
            f"{sc:<8} {ss:<12} {elh:<7}"
        )

    print(sep)
    print(f"Всего кошельков: {len(wallets)}")


def _today() -> str:
    import datetime
    return datetime.date.today().isoformat()


# ── Menu ─────────────────────────────────────────────────────────────────────

ALL_KEY  = "__all__"
STAT_KEY = "__status__"

MODULES = [
    ("1.1 Startale: Swap ETH→USDSC",            "swap"),
    ("1.2 Startale: Реферал",                    "referral"),
    ("1.3 Startale: Daily GM",                   "gm"),
    ("2.  SoundChains: Mint NFT",                "soundchains"),
    ("3.  Superstake: Claw Machine (10 rounds)", "superstake"),
    ("4.  ELHEXA: Daily Check-In (3 чекина)",    "elhexa"),
]

_MENU_STYLE = questionary.Style([
    ("highlighted", "fg:cyan bold"),
    ("selected",    "fg:green"),
    ("pointer",     "fg:cyan bold"),
])


def ask_modules() -> list[str]:
    choices = [
        questionary.Choice("🔄 Все модули подряд", value=ALL_KEY),
        questionary.Choice("📊 Показать статус",   value=STAT_KEY),
        questionary.Separator("──── Отдельные модули ────"),
    ] + [questionary.Choice(label, value=key) for label, key in MODULES]

    selected = questionary.checkbox(
        "Выбери действие (↑↓ — навигация, пробел — выбор, Enter — запуск):",
        choices=choices,
        style=_MENU_STYLE,
    ).ask()

    if selected is None:
        print("\nОтменено.")
        sys.exit(0)
    if not selected:
        logger.warning("Ничего не выбрано")
        sys.exit(0)

    return selected


# ── Portal pre-check ──────────────────────────────────────────────────────────

def _is_already_done(
    module: str,
    addr: str,
    portal_statuses: dict[str, dict],
) -> bool:
    """True if quest is already done (portal is source of truth, db is fallback)."""
    status = portal_statuses.get(addr) or {}
    info   = db.get_account_info(addr) or {}

    if module == "swap":
        return status.get("swap_done", False) or bool(info.get("swap_done"))

    if module == "referral":
        return status.get("referral_done", False) or bool(info.get("referral_done"))

    if module == "gm":
        done_portal = status.get("gm", 0) >= status.get("gm_required", 5)
        return done_portal or db.is_gm_done_today(addr)

    if module == "soundchains":
        return status.get("soundchains_done", False) or bool(info.get("soundchains_done"))

    if module == "superstake":
        done_portal = status.get("superstake", 0) >= status.get("superstake_required", 10)
        done_db     = info.get("superstake_rounds", 0) >= 10
        return done_portal or done_db

    if module == "elhexa":
        req = status.get("elhexa_required", 3)
        cnt = max(status.get("elhexa", 0), info.get("elhexa_total", 0))
        quest_complete = status.get("elhexa_done", False) or cnt >= req
        if quest_complete:
            return True
        period_id = elhexa_current_period_id()
        if db.is_elhexa_done_this_period(addr, period_id):
            return True
        return False

    return False


# ── Task runners ──────────────────────────────────────────────────────────────

def _run_single_task(
    module: str,
    i_orig: int,
    pk: str,
    addr: str,
    proxies: list[Optional[str]],
    cfg: dict,
    rpc_url: str,
    portal_statuses: dict[str, dict],
) -> bool:
    """Execute one module for one wallet. Returns True if ran, False if skipped."""
    if _is_already_done(module, addr, portal_statuses):
        logger.info(f"[{addr}] {module} пропуск")
        return False

    if module == "referral":
        from modules.startale_referral import do_referral
        if len(proxy_utils.nonempty_proxies(proxies)) < 2:
            logger.error("Реферал: нужно ≥2 разных прокси (main/ref разные IP)")
            return False
        do_referral(pk, proxies, i_orig)
        return True

    proxy = proxy_utils.match_proxy(proxies, i_orig)
    w3 = get_w3(rpc_url, proxy, cfg.get("disable_ssl", False))

    if module == "swap":
        from modules.startale_swap import swap_eth_to_usdsc
        swap_eth_to_usdsc(
            pk,
            w3,
            gas_limit_multiplier=cfg.get("gas_limit_multiplier", 1.2),
            proxy=proxy,
            proxy_pool=proxies,
        )

    elif module == "gm":
        from modules.startale_gm import do_gm
        do_gm(pk, proxy=proxy, rpc_url=rpc_url, proxy_pool=proxies)

    elif module == "soundchains":
        from modules.soundchains import run_soundchains
        run_soundchains(pk, w3, proxy=proxy, proxy_pool=proxies)

    elif module == "superstake":
        from modules.superstake import run_claw_machine
        raw_fb = cfg.get("superstake_firebase_api_key")
        fb_key = str(raw_fb).strip() if raw_fb not in (None, "") else None
        run_claw_machine(
            private_key=pk, w3=w3, proxy=proxy,
            rounds_required=int(cfg.get("superstake_rounds_required", 10)),
            action_delay_min=cfg.get("action_delay_min", 3),
            action_delay_max=cfg.get("action_delay_max", 10),
            firebase_api_key=fb_key,
            proxy_pool=proxies,
        )

    elif module == "elhexa":
        from modules.elhexa import do_elhexa_checkin
        do_elhexa_checkin(pk, proxy=proxy, rpc_url=rpc_url, proxy_pool=proxies)

    return True


# ── Graceful shutdown ─────────────────────────────────────────────────────────

import signal
_shutdown = False

def _handle_signal(sig, frame):
    global _shutdown
    logger.warning("SIG: стоп после текущей задачи")
    _shutdown = True

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── Main ──────────────────────────────────────────────────────────────────────

MODULE_ORDER = ("swap", "referral", "gm", "soundchains", "superstake", "elhexa")


def main() -> None:
    show_banner()

    selected = ask_modules()

    cfg     = load_config()
    apply_elhexa_config_env(cfg)
    logger.apply_config(cfg)
    rpc_url = cfg.get("rpc_url", "https://soneium-rpc.publicnode.com")

    db.init_db()
    wallets = load_wallets()
    proxies = proxy_utils.load_proxies_from_file(PROJECT_ROOT / "proxy.txt")

    run_all    = ALL_KEY  in selected
    run_status = STAT_KEY in selected

    if run_status:
        show_status(wallets, proxies)
        if not run_all and set(selected) <= {STAT_KEY}:
            return

    selected_modules = [m for m in MODULE_ORDER if run_all or m in selected]
    if not selected_modules:
        logger.warning("Нет выбранных модулей")
        return

    # Batch portal pre-check
    logger.info(f"Портал: {len(wallets)} адр.")
    portal_raw = fetch_portal_data_batch(
        [addr for _, addr in wallets],
        [p for p in proxies if p],
    )
    portal_statuses = {
        addr: parse_account_status(portal_raw.get(addr))
        for _, addr in wallets
    }

    # Build flat task list and shuffle (anti-sybil)
    tasks = [
        (module, i, pk, addr)
        for module in selected_modules
        for i, (pk, addr) in enumerate(wallets)
    ]
    random.shuffle(tasks)

    logger.info(f"Задачи: {len(tasks)} ({len(wallets)}×{len(selected_modules)}), shuffle")

    delay_min = cfg.get("delay_min", 30)
    delay_max = cfg.get("delay_max", 120)

    for idx, (module, i_orig, pk, addr) in enumerate(tasks, 1):
        if _shutdown:
            logger.warning("Остановка")
            break

        logger.header(f"[{idx}/{len(tasks)}] {addr} — {module}")

        try:
            ran = _run_single_task(
                module, i_orig, pk, addr,
                proxies, cfg, rpc_url, portal_statuses,
            )
        except KeyboardInterrupt:
            logger.warning("KeyboardInterrupt")
            break
        except Exception as e:
            logger.error(f"[{addr}] {module}: {e}")
            ran = True  # still count as executed for delay purposes

        if ran and idx < len(tasks) and not _shutdown:
            delay = random.uniform(delay_min, delay_max)
            logger.info(f"пауза {delay:.0f}s")
            time.sleep(delay)

    logger.success("Готово (статус — пункт «Показать статус»)")


if __name__ == "__main__":
    main()
