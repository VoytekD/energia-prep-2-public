# -*- coding: utf-8 -*-
"""
calc.__init__
==============
Wspólna konfiguracja: logger, stałe (schematy/tabele/kanały), narzędzia DB.

UWAGA:
- Zgodność z nowym nazewnictwem tabel etapów: output."00_stage_ingest", ..."03_stage_pricing".
- Kolejka zadań: output.calc_job_queue (status: queued|running|done|failed|skipped).
"""

from __future__ import annotations

import os
import json
import logging
from typing import Any, Optional

# Public API tego modułu
__all__ = [
    # stałe
    "CHANNEL_REBUILD",
    "SCHEMA_OUTPUT",
    "TABLE_JOBS",
    "TABLE_STAGE_00",
    "TABLE_STAGE_01",
    "TABLE_STAGE_02",
    "TABLE_STAGE_03",
    # narzędzia
    "log",
    "build_pg_dsn",
    "create_async_pool",
    "ensure_db_objects",
    "parse_notify_payload",
]

# ──────────────────────────────────────────────────────────────────────────────
# Logger
# ──────────────────────────────────────────────────────────────────────────────

def _build_logger() -> logging.Logger:
    """
    Buduje spójne loggery bazowe:
      - "calc"                 (używany w listenerze)
      - "energia-prep-2.calc"  (rodzic dla ingest_00/runner/commit_02/io/validate)
    """
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    def ensure(name: str) -> logging.Logger:
        lg = logging.getLogger(name)
        if not lg.handlers:
            lg.setLevel(level)
            h = logging.StreamHandler()
            h.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
            lg.addHandler(h)
            lg.propagate = False
        return lg

    lg_calc = ensure("calc")
    # NOWE: rodzic dla wszystkich podloggerów "energia-prep-2.calc.*"
    ensure("energia-prep-2.calc")
    return lg_calc

log = _build_logger()

# ──────────────────────────────────────────────────────────────────────────────
# Stałe: kanały / schematy / tabele
# ──────────────────────────────────────────────────────────────────────────────

CHANNEL_REBUILD = os.getenv("CHANNEL_REBUILD", "ch_energy_rebuild")

SCHEMA_OUTPUT = os.getenv("SCHEMA_OUTPUT", "output")

# Kolejka zadań (job queue)
TABLE_JOBS = f'{SCHEMA_OUTPUT}.calc_job_queue'

# Etapy procesu – nowe nazewnictwo
TABLE_STAGE_00 = f'{SCHEMA_OUTPUT}."00_stage_ingest"'
TABLE_STAGE_01 = f'{SCHEMA_OUTPUT}."01_stage_proposer"'
TABLE_STAGE_02 = f'{SCHEMA_OUTPUT}."02_stage_commit"'
TABLE_STAGE_03 = f'{SCHEMA_OUTPUT}."03_stage_pricing"'

# ──────────────────────────────────────────────────────────────────────────────
# DB utils
# ──────────────────────────────────────────────────────────────────────────────

def build_pg_dsn(
    host: Optional[str] = None,
    port: Optional[int] = None,
    db: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
) -> str:
    """
    Składa DSN do Postgresa. Używa PG* albo DB_* z env.
    Priorytet: argumenty funkcji > PG* > DB_* > domyślne.
    """
    host = host or os.getenv("PGHOST") or os.getenv("DB_HOST") or "localhost"
    port = int(port or os.getenv("PGPORT") or os.getenv("DB_PORT") or 5432)
    db   = db   or os.getenv("PGDATABASE") or os.getenv("DB_NAME") or "postgres"
    user = user or os.getenv("PGUSER") or os.getenv("DB_USER") or "postgres"

    # ← tu dodane aliasy: DB_PASSWORD, POSTGRES_PASSWORD
    password = (
        password
        or os.getenv("PGPASSWORD")
        or os.getenv("DB_PASS")
        or os.getenv("DB_PASSWORD")
        or os.getenv("POSTGRES_PASSWORD")
        or ""
    )

    parts = [f"host={host}", f"port={port}", f"dbname={db}", f"user={user}"]
    if password:
        parts.append(f"password={password}")
    # keepalives dla długich listenerów
    parts += ["keepalives=1", "keepalives_idle=30", "keepalives_interval=10", "keepalives_count=5"]
    return " ".join(parts)

async def create_async_pool(dsn: Optional[str] = None):
    """
    Tworzy AsyncConnectionPool (psycopg_pool). Ustawia rozsądne limity.
    Zwraca instancję puli – NIECZYNNEJ, należy wywołać await pool.open().
    """
    from psycopg_pool import AsyncConnectionPool  # leniwy import
    dsn = dsn or build_pg_dsn()
    min_size = int(os.getenv("POOL_MIN_SIZE", "1"))
    max_size = int(os.getenv("POOL_MAX_SIZE", "10"))
    pool = AsyncConnectionPool(
        conninfo=dsn,
        min_size=min_size,
        max_size=max_size,
        kwargs={"autocommit": True},
        open=False,  # ← kluczowa zmiana: nie otwieraj w konstruktorze
    )
    return pool

async def ensure_db_objects(pool_or_con: Any) -> None:
    """
    Idempotentne DDL:
    - schema output
    - extensions (bez błędów, jeśli brak uprawnień)
    - tabela kolejki zadań (z dozwolonym statusem 'skipped')
    """
    async def _run_ddl(con) -> None:
        async with con.cursor() as cur:
            # Schemat
            await cur.execute(f'CREATE SCHEMA IF NOT EXISTS {SCHEMA_OUTPUT};')

            # Ekstensje — best-effort
            for ext in ('pgcrypto', 'uuid-ossp', 'btree_gist'):
                try:
                    await cur.execute(f'CREATE EXTENSION IF NOT EXISTS "{ext}";')
                except Exception:
                    # brak uprawnień → ignorujemy
                    pass

            # Kolejka zadań
            await cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_JOBS} (
                job_id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                params_ts    timestamptz NOT NULL,
                status       text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','done','failed','skipped')),
                created_at   timestamptz NOT NULL DEFAULT now(),
                started_at   timestamptz,
                finished_at  timestamptz,
                error        text,
                last_heartbeat timestamptz
            );
            """)
            await cur.execute(f'CREATE INDEX IF NOT EXISTS {SCHEMA_OUTPUT}_calc_job_queue_params_ts_idx ON {TABLE_JOBS}(params_ts);')
            await cur.execute(f'CREATE INDEX IF NOT EXISTS {SCHEMA_OUTPUT}_calc_job_queue_status_idx ON {TABLE_JOBS}(status);')

    # Obsługa: albo dostaliśmy pulę, albo pojedyncze połączenie
    try:
        from psycopg_pool import AsyncConnectionPool
    except Exception:
        AsyncConnectionPool = None  # type: ignore

    if AsyncConnectionPool and isinstance(pool_or_con, AsyncConnectionPool):
        async with pool_or_con.connection() as con:
            await _run_ddl(con)
    else:
        con = pool_or_con
        await _run_ddl(con)

# ──────────────────────────────────────────────────────────────────────────────
# Inne utilsy
# ──────────────────────────────────────────────────────────────────────────────

def parse_notify_payload(payload: str) -> dict[str, Any]:
    """
    Odbiera payload z NOTIFY. Oczekuje JSON-a z co najmniej:
      { "params_ts": "...", "action": "rebuild" }
    Zwraca słownik. Nie rzuca — w razie śmieci daje {"raw": payload}.
    """
    try:
        obj = json.loads(payload)
        if not isinstance(obj, dict):
            return {"raw": payload}
        # normalizacja kluczy
        if "paramsTs" in obj and "params_ts" not in obj:
            obj["params_ts"] = obj["paramsTs"]
        if "action" not in obj:
            obj["action"] = "rebuild"
        return obj
    except Exception:
        return {"raw": payload}
