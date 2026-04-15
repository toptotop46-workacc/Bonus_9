"""JSON flat-file state store: quest_results.json"""

import json
import datetime
from pathlib import Path

DB_PATH = Path("quest_results.json")


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def init_db() -> None:
    if not DB_PATH.exists():
        DB_PATH.write_text("{}", encoding="utf-8")


def _load() -> dict:
    try:
        return json.loads(DB_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save(data: dict) -> None:
    DB_PATH.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def get_account_info(address: str) -> dict:
    return _load().get(address.lower(), {})


def upsert_account(address: str, **kwargs) -> None:
    data = _load()
    key  = address.lower()
    rec  = data.get(key, {})
    for k, v in kwargs.items():
        if v is not None:
            rec[k] = v
    rec["updated_at"] = _now_iso()
    data[key] = rec
    _save(data)


# ---- helpers ----

def is_gm_done_today(address: str) -> bool:
    info = get_account_info(address)
    last = info.get("gm_last_date")
    today = datetime.date.today().isoformat()
    return last == today


def mark_gm_done(address: str) -> None:
    today = datetime.date.today().isoformat()
    upsert_account(address, gm_last_date=today,
                   gm_total=get_account_info(address).get("gm_total", 0) + 1)


def is_swap_done(address: str) -> bool:
    return bool(get_account_info(address).get("swap_done"))


def mark_swap_done(address: str, tx_hash: str) -> None:
    upsert_account(address, swap_done=True, swap_tx=tx_hash)


def is_referral_done(address: str) -> bool:
    return bool(get_account_info(address).get("referral_done"))


def mark_referral_done(address: str, ref_wallet: str) -> None:
    upsert_account(address, referral_done=True, referral_wallet=ref_wallet)


def get_superstake_rounds(address: str) -> int:
    return int(get_account_info(address).get("superstake_rounds", 0))


def add_superstake_round(address: str, tx_hash: str) -> None:
    rounds = get_superstake_rounds(address) + 1
    history = get_account_info(address).get("superstake_txs", [])
    history.append(tx_hash)
    upsert_account(address, superstake_rounds=rounds, superstake_txs=history)


def is_soundchains_done(address: str) -> bool:
    return bool(get_account_info(address).get("soundchains_done"))


def mark_soundchains_done(address: str, tx_hash: str) -> None:
    upsert_account(address, soundchains_done=True, soundchains_tx=tx_hash)


def _elhexa_last_period_stored(info: dict) -> str | None:
    last = info.get("elhexa_last_period")
    if last and isinstance(last, str) and len(last) >= 10:
        return last[:10]
    last = info.get("elhexa_last_date")
    if last and isinstance(last, str) and len(last) >= 10:
        return last[:10]
    return None


def is_elhexa_done_this_period(address: str, period_id: str) -> bool:
    """True, если для текущего игрового периода ELHEXA уже зафиксирован чекин в БД."""
    info = get_account_info(address)
    last = _elhexa_last_period_stored(info)
    if not last:
        return False
    return last == period_id


def is_elhexa_done_today(address: str) -> bool:
    """Устар.: используйте is_elhexa_done_this_period с period_id из elhexa_period."""
    from modules.elhexa_period import elhexa_current_period_id

    return is_elhexa_done_this_period(address, elhexa_current_period_id())


def touch_elhexa_period(address: str, *, period_id: str | None = None) -> None:
    """
    Зафиксировать текущий игровой период ELHEXA без изменения elhexa_total.
    Полезно, когда on-chain check-in уже найден, а портал ещё не успел проиндексировать шаг.
    """
    from modules.elhexa_period import elhexa_current_period_id

    pid = period_id if period_id is not None else elhexa_current_period_id()
    upsert_account(
        address,
        elhexa_last_period=pid,
        elhexa_last_date=pid,
    )


def mark_elhexa_done(address: str, *, period_id: str | None = None) -> None:
    from modules.elhexa_period import elhexa_current_period_id

    pid = period_id if period_id is not None else elhexa_current_period_id()
    upsert_account(
        address,
        elhexa_last_period=pid,
        elhexa_last_date=pid,
        elhexa_total=get_account_info(address).get("elhexa_total", 0) + 1,
    )


def get_startale_user_id(address: str) -> str | None:
    return get_account_info(address).get("startale_user_id")


def set_startale_user_id(address: str, user_id: str) -> None:
    upsert_account(address, startale_user_id=user_id)


def get_smart_account(address: str) -> str | None:
    return get_account_info(address).get("smart_account_address")


def set_smart_account(address: str, sa: str) -> None:
    upsert_account(address, smart_account_address=sa)


def get_soundchains_token(address: str) -> str | None:
    return get_account_info(address).get("soundchains_token")


def set_soundchains_token(address: str, token: str) -> None:
    upsert_account(address, soundchains_token=token)
