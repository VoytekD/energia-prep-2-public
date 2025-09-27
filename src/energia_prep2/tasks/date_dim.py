from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, Tuple, Dict

import holidays
from zoneinfo import ZoneInfo

from ..cfg import settings
from ..db import get_conn_app
from ..log import log


PL_TZ = ZoneInfo(settings.TZ)  # "Europe/Warsaw"


def _hour_range_local_inclusive(start_date: datetime, end_date: datetime) -> Iterable[Tuple[datetime, datetime]]:
    """Generuje (ts_utc, ts_local) co godzinę dla zakresu [start_date .. end_date] w strefie PL_TZ."""
    cur_local = datetime.combine(start_date.date(), datetime.min.time(), PL_TZ)
    end_local = datetime.combine(end_date.date(), datetime.max.time(), PL_TZ).replace(minute=0, second=0, microsecond=0)
    while cur_local <= end_local:
        ts_local = cur_local
        ts_utc = ts_local.astimezone(timezone.utc)
        yield ts_utc.replace(tzinfo=None), ts_local.replace(tzinfo=None)
        cur_local += timedelta(hours=1)


def _ensure_date_dim_base_schema() -> None:
    """
    Dokłada brakujące kolumny w input.date_dim, jeśli ich nie ma (ts_utc, ts_local, year, ...).
    Tworzy też unikalne indeksy po ts_utc/ts_local, jeśli brak.
    """
    with get_conn_app() as conn, conn.cursor() as cur:
        cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='input' AND table_name='date_dim' AND column_name='ts_utc')
            THEN EXECUTE 'ALTER TABLE input.date_dim ADD COLUMN ts_utc timestamptz NOT NULL'; END IF;

            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='input' AND table_name='date_dim' AND column_name='ts_local')
            THEN EXECUTE 'ALTER TABLE input.date_dim ADD COLUMN ts_local timestamp'; END IF;

            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='input' AND table_name='date_dim' AND column_name='year')
            THEN EXECUTE 'ALTER TABLE input.date_dim ADD COLUMN year integer'; END IF;

            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='input' AND table_name='date_dim' AND column_name='month')
            THEN EXECUTE 'ALTER TABLE input.date_dim ADD COLUMN month smallint'; END IF;

            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='input' AND table_name='date_dim' AND column_name='day')
            THEN EXECUTE 'ALTER TABLE input.date_dim ADD COLUMN day smallint'; END IF;

            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='input' AND table_name='date_dim' AND column_name='hour')
            THEN EXECUTE 'ALTER TABLE input.date_dim ADD COLUMN hour smallint'; END IF;

            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='input' AND table_name='date_dim' AND column_name='dow')
            THEN EXECUTE 'ALTER TABLE input.date_dim ADD COLUMN dow smallint'; END IF;

            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='input' AND table_name='date_dim' AND column_name='is_workday')
            THEN EXECUTE 'ALTER TABLE input.date_dim ADD COLUMN is_workday boolean'; END IF;

            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='input' AND table_name='date_dim' AND column_name='is_holiday')
            THEN EXECUTE 'ALTER TABLE input.date_dim ADD COLUMN is_holiday boolean'; END IF;

            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='input' AND table_name='date_dim' AND column_name='holiday_name')
            THEN EXECUTE 'ALTER TABLE input.date_dim ADD COLUMN holiday_name text'; END IF;

            IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname='input' AND tablename='date_dim' AND indexname='ix_date_dim_ts_utc')
            THEN EXECUTE 'CREATE UNIQUE INDEX ix_date_dim_ts_utc ON input.date_dim(ts_utc)'; END IF;

            IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname='input' AND tablename='date_dim' AND indexname='ix_date_dim_ts_local')
            THEN EXECUTE 'CREATE UNIQUE INDEX ix_date_dim_ts_local ON input.date_dim(ts_local)'; END IF;
        END $$;
        """)
        conn.commit()


def _introspect_extra_columns() -> Dict[str, bool]:
    """
    Sprawdza, czy input.date_dim ma dodatkowe, wymagane kolumny, które musimy zasilać w INSERT:
    - granularity  → wstawiamy 'H'
    - is_active    → wstawiamy TRUE
    - day_of_week  → wstawiamy to samo co 'dow' (1..7)
    """
    result = {"granularity": False, "is_active": False, "day_of_week": False}
    with get_conn_app() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='input' AND table_name='date_dim'
              AND column_name IN ('granularity','is_active','day_of_week')
        """)
        for (col,) in cur.fetchall():
            result[col] = True
    return result


def build() -> None:
    log.info(
        "date_dim: buduję zakres start=%s end=%s tz=%s",
        settings.DATE_START, settings.DATE_END, settings.TZ
    )

    _ensure_date_dim_base_schema()
    extras = _introspect_extra_columns()

    pl_holidays = holidays.Poland(years=range(settings.DATE_START.year, settings.DATE_END.year + 1))

    rows = []
    for ts_utc, ts_local in _hour_range_local_inclusive(
        datetime.combine(settings.DATE_START, datetime.min.time(), PL_TZ),
        datetime.combine(settings.DATE_END, datetime.min.time(), PL_TZ),
    ):
        y = ts_local.year
        m = ts_local.month
        d = ts_local.day
        h = ts_local.hour
        dow = ts_local.isoweekday()  # 1-7
        is_holiday = (ts_local.date() in pl_holidays)
        is_workday = (dow <= 5) and (not is_holiday)
        holiday_name = pl_holidays.get(ts_local.date(), None)

        base = [ts_utc, ts_local, y, m, d, h, dow, is_workday, is_holiday, holiday_name]
        # dodatkowe kolumny
        if extras["granularity"]:
            base.append("H")        # hourly
        if extras["is_active"]:
            base.append(True)       # aktywny rekord
        if extras["day_of_week"]:
            base.append(dow)        # to samo co 'dow'

        rows.append(tuple(base))

    # dynamiczna lista kolumn wstawianych
    cols = [
        "ts_utc", "ts_local", "year", "month", "day", "hour", "dow",
        "is_workday", "is_holiday", "holiday_name"
    ]
    if extras["granularity"]:
        cols.append("granularity")
    if extras["is_active"]:
        cols.append("is_active")
    if extras["day_of_week"]:
        cols.append("day_of_week")

    update_cols = [
        "ts_local", "year", "month", "day", "hour", "dow",
        "is_workday", "is_holiday", "holiday_name"
    ]
    if extras["granularity"]:
        update_cols.append("granularity")
    if extras["is_active"]:
        update_cols.append("is_active")
    if extras["day_of_week"]:
        update_cols.append("day_of_week")

    placeholders = ",".join(["%s"] * len(cols))
    set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

    upsert_sql = f"""
        INSERT INTO input.date_dim ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (ts_utc) DO UPDATE SET {set_clause}
    """

    with get_conn_app() as conn, conn.cursor() as cur:
        BATCH = 1000
        for i in range(0, len(rows), BATCH):
            cur.executemany(upsert_sql, rows[i:i+BATCH])
        conn.commit()

    log.info("date_dim: OK (wiersze=%s, kolumny_wstawione=%s)", len(rows), cols)
