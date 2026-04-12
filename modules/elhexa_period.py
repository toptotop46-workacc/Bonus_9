"""Игровой период ELHEXA: граница суток по часу в Europe/Moscow (по умолчанию 22:00)."""

from __future__ import annotations

import datetime
import os
from zoneinfo import ZoneInfo

MSK = ZoneInfo("Europe/Moscow")


def apply_elhexa_config_env(cfg: dict) -> None:
    """Подставить час сброса из config.toml, если env ещё не задан."""
    if os.environ.get("BONUS9_ELHEXA_RESET_HOUR_MSK", "").strip():
        return
    v = cfg.get("elhexa_reset_hour_msk")
    if v is not None:
        os.environ["BONUS9_ELHEXA_RESET_HOUR_MSK"] = str(int(v))


def elhexa_reset_hour_msk() -> int:
    raw = os.environ.get("BONUS9_ELHEXA_RESET_HOUR_MSK", "").strip()
    if raw:
        try:
            h = int(raw)
            if 0 <= h <= 23:
                return h
        except ValueError:
            pass
    return 22


def elhexa_current_period_id(
    now: datetime.datetime | None = None,
    *,
    reset_hour: int | None = None,
) -> str:
    """
    Стабильный ID периода (дата YYYY-MM-DD по Москве): интервал
    [день D в reset_hour, день D+1 в reset_hour) мапится на period_id = D (календарная дата MSK).
    """
    if now is None:
        now = datetime.datetime.now(datetime.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=datetime.timezone.utc)
    rh = elhexa_reset_hour_msk() if reset_hour is None else reset_hour
    msk = now.astimezone(MSK)
    d = msk.date()
    if msk.hour < rh:
        d = d - datetime.timedelta(days=1)
    return d.isoformat()


def elhexa_next_reset_utc(
    now: datetime.datetime | None = None,
    *,
    reset_hour: int | None = None,
) -> datetime.datetime:
    """Следующий момент сброса (reset_hour по Москве), в UTC."""
    if now is None:
        now = datetime.datetime.now(datetime.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=datetime.timezone.utc)
    rh = elhexa_reset_hour_msk() if reset_hour is None else reset_hour
    msk = now.astimezone(MSK)
    cand = msk.replace(hour=rh, minute=0, second=0, microsecond=0)
    if cand <= msk:
        cand = cand + datetime.timedelta(days=1)
    return cand.astimezone(datetime.timezone.utc)


def elhexa_next_reset_msk_str(
    now: datetime.datetime | None = None,
    *,
    reset_hour: int | None = None,
) -> str:
    """Строка для логов: время следующего сброса в МСК."""
    u = elhexa_next_reset_utc(now=now, reset_hour=reset_hour)
    msk = u.astimezone(MSK)
    return msk.strftime("%Y-%m-%d %H:%M MSK")
