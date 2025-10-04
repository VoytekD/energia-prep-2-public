# Calc.__init__ — wspólna konfiguracja i utilsy dla modułów z katalogu `calc/`.
# Bezpieczne importy (leniwe dla psycopg), logger gotowy od razu.

from __future__ import annotations

import os
import sys
import json
import logging
from typing import Optional, Any

# ──────────────────────────────────────────────────────────────────────────────
# Logger (gotowy natychmiast — zanim dotkniemy czegokolwiek innego)
# ──────────────────────────────────────────────────────────────────────────────

def _build_logger() -> logging.Logger:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger("energia-prep-2.calc")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)s | calc | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger

log = _build_logger()

# ──────────────────────────────────────────────────────────────────────────────
# Stałe i nazwy obiektów
# ──────────────────────────────────────────────────────────────────────────────

# Kanał LISTEN/NOTIFY do wyzwalania przeliczeń
CHANNEL_REBUILD = "ch_energy_rebuild"

# Schemat wynikowy
SCHEMA_OUTPUT = os.getenv("SCHEMA_OUTPUT", "output")

# Kolejka jobów (Form API enqueue) — ***POPRAWIONE: output.calc_job_queue***
TABLE_JOBS = "output.calc_job_queue"

# Nazwy tabel wynikowych (nowy pipeline: stage00/01/02 + final)
TABLE_DETAIL_FINAL   = f"{SCHEMA_OUTPUT}.stage04_report_calc"
TABLE_STAGE00_INGEST = f"{SCHEMA_OUTPUT}.stage00_ingest_calc"
TABLE_STAGE01_PROP   = f"{SCHEMA_OUTPUT}.stage01_proposer_calc"
TABLE_STAGE02_COMMIT = f"{SCHEMA_OUTPUT}.stage02_commit_calc"

# ──────────────────────────────────────────────────────────────────────────────
# DSN i pula połączeń (leniwe importy psycopg/psycopg_pool)
# ──────────────────────────────────────────────────────────────────────────────

def build_pg_dsn(appname: str = "energia-prep-2:calc") -> str:
    """
    Składa DSN Postgresa na podstawie zmiennych środowisk (spójnie z resztą projektu).
    """
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db = os.getenv("DB_NAME", "energia")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "")
    sslmode = os.getenv("DB_SSLMODE", "disable")

    return (
        f"host={host} port={port} dbname={db} user={user} password={password} "
        f"sslmode={sslmode} application_name={appname}"
    )

def create_async_pool(appname: str = "energia-prep-2:calc") -> Any:
    """
    Tworzy AsyncConnectionPool (psycopg_pool). Import leniwy, żeby sam import pakietu
    nie wywracał się, gdy środowisko nie ma jeszcze zainstalowanych dodatków.
    """
    try:
        from psycopg_pool import AsyncConnectionPool  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "Brak zależności psycopg_pool. Dodaj do pyproject: psycopg_pool>=3.2,<3.3"
        ) from e

    dsn = build_pg_dsn(appname=appname)
    pool = AsyncConnectionPool(
        dsn,
        min_size=1,
        max_size=int(os.getenv("DB_POOL_MAX", "10")),
        open=False,
    )
    return pool
# ──────────────────────────────────────────────────────────────────────────────
# DDL minimalny: listener wymaga tylko schematu `output` (statusy tworzy io.ensure_calc_status_structures)
# ──────────────────────────────────────────────────────────────────────────────

DDL_SCHEMA_AND_EXT = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_OUTPUT};
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
"""

async def ensure_db_objects(pool: Any) -> None:
    """
    Minimalny zestaw obiektów dla listenera:
      - schemat `output` + rozszerzenia UUID.
    Tabele statusowe i kolejkę tworzymy w io.ensure_calc_status_structures().
    """
    try:
        import psycopg  # leniwy import
    except Exception as e:
        raise RuntimeError("Brak zależności psycopg>=3.1,<3.2") from e

    async with pool.connection() as con:  # type: ignore
        async with con.cursor() as cur:
            await cur.execute(DDL_SCHEMA_AND_EXT)
        await con.commit()
    log.info("DDL (schema output + extensions) zastosowany (idempotentnie).")

# ──────────────────────────────────────────────────────────────────────────────
# Utilsy pomocnicze (np. dekodowanie payloadu NOTIFY)
# ──────────────────────────────────────────────────────────────────────────────

def parse_notify_payload(payload: Optional[str]) -> dict:
    """
    Odbieramy payload z NOTIFY. Oczekiwana forma:
      {"cmd":"rebuild", "params_ts":"2025-01-01T00:00:00Z"}
    Zwracamy dict; brak/parsowanie błędne -> pusty dict.
    """
    if not payload:
        return {}
    try:
        d = json.loads(payload)
        if not isinstance(d, dict):
            return {}
        return d
    except Exception:
        return {}

__all__ = [
    "CHANNEL_REBUILD",
    "SCHEMA_OUTPUT",
    "TABLE_JOBS",
    "TABLE_DETAIL_FINAL",
    "TABLE_STAGE00_INGEST",
    "TABLE_STAGE01_PROP",
    "TABLE_STAGE02_COMMIT",
    "log",
    "build_pg_dsn",
    "create_async_pool",
    "ensure_db_objects",
    "parse_notify_payload",
]
