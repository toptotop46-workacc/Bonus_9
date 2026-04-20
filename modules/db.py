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
