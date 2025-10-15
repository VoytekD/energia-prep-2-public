# src/energia_prep2/tasks/date_dim.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone, date
from typing import List, Tuple, Dict, Optional

from ..cfg import settings
from ..db import get_conn_app
from ..log import log

# ───────────────────────── helpers: TZ & holidays (łagodne fallbacki)
try:
    from zoneinfo import ZoneInfo  # py>=3.9
    PL_TZ = ZoneInfo("Europe/Warsaw")
except Exception:
    PL_TZ = None  # jeśli brak tzdata w obrazie, policzymy meta po UTC

try:
    import holidays
    PL_HOLIDAYS = holidays.country_holidays("PL")
except Exception:
    PL_HOLIDAYS = None  # jeśli brak pakietu "holidays", święta będą wyłączone

# ───────────────────────── meta PL (bez ts_local w tabeli)
def _dow_pg(ts: datetime) -> int:
    """Postgres EXTRACT(DOW): 0=Sun..6=Sat; Python weekday(): 0=Mon..6=Sun"""
    # mapowanie: Mon(0)->1, Tue->2, ..., Sat->6, Sun(6)->0
    return (ts.weekday() + 1) % 7

def _as_pl_time(ts_utc: datetime) -> datetime:
    if PL_TZ is not None:
        return ts_utc.astimezone(PL_TZ)
    return ts_utc  # fallback: bez konwersji (UTC)

def _is_holiday_pl(d: date) -> tuple[bool, Optional[str]]:
    if PL_HOLIDAYS is None:
        return False, None
    name = PL_HOLIDAYS.get(d)
    return (name is not None), (str(name) if name else None)

def _meta_from_ts(ts_utc: datetime) -> Dict[str, object]:
    ts_pl = _as_pl_time(ts_utc)
    is_hol, hol_name = _is_holiday_pl(ts_pl.date())
    is_workday = (ts_pl.weekday() < 5) and (not is_hol)
    return {
        "ts_local": ts_pl.replace(tzinfo=None),
        "year": ts_pl.year,
        "month": ts_pl.month,
        "day": ts_pl.day,
        "dow": _dow_pg(ts_pl),
        "hour": ts_pl.hour,
        "is_workday": is_workday,
        "is_holiday": is_hol,
        "holiday_name": hol_name,
        "granularity": "H",
        "dt_h": 1.0,
    }

def _hour_range_closed(start_utc: datetime, end_utc: datetime):
    """Generator godzin (UTC) domknięty: [start, end], krok 1h."""
    cur = start_utc
    step = timedelta(hours=1)
    while cur <= end_utc:
        yield cur
        cur = cur + step

# ───────────────────────── DDL ensure
def _ensure_table():
    sql = """
    CREATE SCHEMA IF NOT EXISTS input;

    CREATE TABLE IF NOT EXISTS input.date_dim (
      ts_utc        timestamptz PRIMARY KEY,
      ts_local      timestamp    NOT NULL,  -- DODANE: lokalny czas (Europe/Warsaw)
      year          integer NOT NULL,
      month         integer NOT NULL,
      day           integer NOT NULL,
      dow           integer NOT NULL,      -- 0=Sun..6=Sat (zgodne z PG)
      hour          integer NOT NULL,      -- 0..23 w CZASIE PL (meta), ale oś jest po UTC
      is_workday    boolean NOT NULL,
      is_holiday    boolean NOT NULL,
      holiday_name  text,
      granularity   text NOT NULL DEFAULT 'H',
      dt_h          double precision NOT NULL DEFAULT 1.0
    );

    CREATE INDEX IF NOT EXISTS ix_date_dim_ts       ON input.date_dim (ts_utc);
    CREATE INDEX IF NOT EXISTS ix_date_dim_ts_local ON input.date_dim (ts_local);
    """
    with get_conn_app() as con, con.cursor() as cur:
        cur.execute(sql)
        con.commit()

# ───────────────────────── budowa/UPSERT
def _rows_for_range(start_utc: datetime, end_utc: datetime) -> List[Tuple]:
    rows: List[Tuple] = []
    for ts in _hour_range_closed(start_utc, end_utc):
        m = _meta_from_ts(ts)
        rows.append((
            ts,                              # ts_utc (timestamptz)
            m["ts_local"],                   # DODANE: ts_local (timestamp)
            int(m["year"]),
            int(m["month"]),
            int(m["day"]),
            int(m["dow"]),
            int(m["hour"]),
            bool(m["is_workday"]),
            bool(m["is_holiday"]),
            m["holiday_name"],
            str(m["granularity"]),
            float(m["dt_h"]),
        ))
    return rows

def build() -> None:
    """
    Buduje/uzupełnia input.date_dim dla zakresu:
      settings.DATE_START .. settings.DATE_END (oba włącznie), co 1h (UTC-only).
    Meta kalendarzowa liczona w PL + zapisujemy ts_local.
    """
    d0 = settings.DATE_START
    d1 = settings.DATE_END
    if d0 > d1:
        d0, d1 = d1, d0

    start_utc = datetime(d0.year, d0.month, d0.day, 0, 0, tzinfo=timezone.utc)
    end_utc   = datetime(d1.year, d1.month, d1.day, 23, 0, tzinfo=timezone.utc)

    log.info("date_dim: buduję oś UTC %s → %s (1h, inclusive)", start_utc.isoformat(), end_utc.isoformat())

    _ensure_table()
    rows = _rows_for_range(start_utc, end_utc)

    if not rows:
        log.warning("date_dim: brak wierszy do wstawienia (pusty zakres?)")
        return

    upsert_sql = """
        INSERT INTO input.date_dim (
          ts_utc, ts_local, year, month, day, dow, hour, is_workday, is_holiday, holiday_name, granularity, dt_h
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (ts_utc) DO UPDATE SET
          ts_local = EXCLUDED.ts_local,
          year = EXCLUDED.year,
          month = EXCLUDED.month,
          day = EXCLUDED.day,
          dow = EXCLUDED.dow,
          hour = EXCLUDED.hour,
          is_workday = EXCLUDED.is_workday,
          is_holiday = EXCLUDED.is_holiday,
          holiday_name = EXCLUDED.holiday_name,
          granularity = EXCLUDED.granularity,
          dt_h = EXCLUDED.dt_h
    """

    with get_conn_app() as con, con.cursor() as cur:
        cur.executemany(upsert_sql, rows)
        affected = cur.rowcount
        con.commit()

    log.info("date_dim: OK — upsert=%d; zakres=%s → %s; dt_h=1.0",
             affected, rows[0][0].isoformat(), rows[-1][0].isoformat())

if __name__ == "__main__":
    build()
