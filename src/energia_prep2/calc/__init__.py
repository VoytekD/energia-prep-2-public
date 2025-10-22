# -*- coding: utf-8 -*-
"""
calc.__init__
==============
Wspólne narzędzia i stałe dla modułów `energia_prep2.calc`:

- Logger `log`
- Budowanie DSN do Postgresa: `build_pg_dsn()` — WYŁĄCZNIE DB_* z .env
- Fabryka asynchronicznej puli: `create_async_pool(...)` (bez auto-open)
- DDL: `ensure_db_objects(pool)` — NO-OP (DDL robi bootstrap SQL)
- Kanał LISTEN/NOTIFY i nazwy tabel
- Parser payloadu NOTIFY
- Helpery do normy:
    - `get_snapshot_norm()` — zwraca całe `norm` z output.snapshot
    - `get_snapshot_consolidated_norm()` — zwraca `norm['consolidated']` JEŚLI istnieje, w przeciwnym razie całe `norm`
    - `get_consolidated_norm()` — alias do `get_snapshot_consolidated_norm()`

Status błędu w kolejce: 'failed' (spójnie).
"""

from __future__ import annotations

import os
import json
import logging
from typing import Any, Optional

import psycopg
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import dict_row

# ─────────────────────────────────────────────────────────────────────────────
# Logger pakietu
# ─────────────────────────────────────────────────────────────────────────────
log = logging.getLogger("calc")

# ─────────────────────────────────────────────────────────────────────────────
# Stałe / identyfikatory
# ─────────────────────────────────────────────────────────────────────────────
SCHEMA_OUTPUT = "output"
TABLE_JOBS = f"{SCHEMA_OUTPUT}.calc_job_queue"
CHANNEL_REBUILD = "ch_energy_rebuild"

__all__ = [
    "log",
    "build_pg_dsn",
    "create_async_pool",
    "ensure_db_objects",
    "CHANNEL_REBUILD",
    "TABLE_JOBS",
    "parse_notify_payload",
    "get_snapshot_norm",
    "get_snapshot_consolidated_norm",
    "get_consolidated_norm",
]

# ─────────────────────────────────────────────────────────────────────────────
# Walidacja ENV (BEZ aliasów, tylko DB_*)
# ─────────────────────────────────────────────────────────────────────────────
def _require_env(name: str) -> str:
    v = os.getenv(name)
    if v is None or v == "":
        raise RuntimeError(f"ENV missing: {name}")
    return v

# ─────────────────────────────────────────────────────────────────────────────
# DSN / Pool (tylko DB_*)
# ─────────────────────────────────────────────────────────────────────────────
def build_pg_dsn() -> str:
    """
    Składa DSN **wyłącznie** z DB_*:
      DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    Opcjonalnie:
      DB_SSLMODE (domyślnie 'prefer'), DB_CONNECT_TIMEOUT (sekundy; domyślnie 5)
    """
    host = _require_env("DB_HOST")
    port = _require_env("DB_PORT")
    db   = _require_env("DB_NAME")
    user = _require_env("DB_USER")
    pwd  = _require_env("DB_PASSWORD")

    sslmode = os.getenv("DB_SSLMODE", "prefer")
    connect_timeout = os.getenv("DB_CONNECT_TIMEOUT", "5")

    parts = [
        f"host={host}",
        f"port={port}",
        f"dbname={db}",
        f"user={user}",
        f"connect_timeout={connect_timeout}",
    ]
    if pwd:
        parts.append(f"password={pwd}")
    if sslmode:
        parts.append(f"sslmode={sslmode}")

    dsn = " ".join(parts) + " "

    # Diagnostyka (bez hasła)
    try:
        safe = f"host={host} port={port} dbname={db} user={user} sslmode={sslmode} connect_timeout={connect_timeout}"
        log.info("PG DSN: %s", safe)
    except Exception:
        pass

    return dsn


def create_async_pool(
    dsn: Optional[str] = None,
    *,
    min_size: int = 1,
    max_size: int = 10,
    timeout: float = 30.0,
) -> AsyncConnectionPool:
    """
    Tworzy pulę połączeń psycopg (psycopg_pool.AsyncConnectionPool).
    - Pula NIE jest otwierana automatycznie (open=False).
    """
    if dsn is None:
        dsn = build_pg_dsn()

    pool = AsyncConnectionPool(
        conninfo=dsn,
        min_size=min_size,
        max_size=max_size,
        open=False,
        timeout=timeout,
        kwargs={},
    )
    return pool

# ─────────────────────────────────────────────────────────────────────────────
# DDL: NO-OP (zarządza nim bootstrap SQL)
# ─────────────────────────────────────────────────────────────────────────────
async def ensure_db_objects(pool: AsyncConnectionPool) -> None:
    """
    NO-OP: wszystkie obiekty DB tworzy bootstrap SQL.
    Zostawiamy lekki ping do DB dla wczesnej detekcji problemów z połączeniem.
    """
    async with pool.connection() as con:
        async with con.cursor() as cur:
            await cur.execute("SELECT 1")
        await con.commit()
    log.info("DDL skipped (managed by bootstrap SQL).")

# ─────────────────────────────────────────────────────────────────────────────
# NOTIFY payload parser
# ─────────────────────────────────────────────────────────────────────────────
def parse_notify_payload(payload: Optional[str]) -> dict:
    """
    Akceptuje:
      - None/"" → {"raw": None} / {"raw": ""}
      - JSON (np. {"job_id":"...", "action":"rebuild", "params_ts":"..."})
      - dowolny tekst → {"raw": "..."}.
    Normalizuje klucz "paramsTs" → "params_ts" i uzupełnia "action" domyślnie jako "rebuild".
    Zwraca słownik. Nie rzuca – w razie śmieci daje {"raw": payload}.
    """
    if payload is None:
        return {"raw": None}
    try:
        obj = json.loads(payload)
        if not isinstance(obj, dict):
            return {"raw": payload}
        if "paramsTs" in obj and "params_ts" not in obj:
            obj["params_ts"] = obj["paramsTs"]
        if "action" not in obj:
            obj["action"] = "rebuild"
        return obj
    except Exception:
        return {"raw": payload}

# ─────────────────────────────────────────────────────────────────────────────
# Helpery: odczyt normy z output.snapshot
# ─────────────────────────────────────────────────────────────────────────────
async def get_snapshot_norm(con: psycopg.AsyncConnection, *, calc_id: str) -> dict[str, Any]:
    """
    Zwraca całe `norm` z output.snapshot dla wskazanego calc_id.
    Rzuca błąd, jeśli nie znaleziono lub jeśli kolumna nie jest poprawnym JSON-em.
    """
    con.row_factory = dict_row
    q = "SELECT norm FROM output.snapshot WHERE calc_id = %s"
    async with con.cursor() as cur:
        await cur.execute(q, (str(calc_id),))
        row = await cur.fetchone()

    if not row:
        log.error("get_snapshot_norm: no row for calc_id=%s", calc_id)
        raise RuntimeError("output.snapshot: brak wiersza dla calc_id")

    norm = row[0] if not isinstance(row, dict) else row.get("norm")

    # JSONB może przyjść jako dict lub str – ujednól
    if isinstance(norm, str):
        import json as _json
        norm = _json.loads(norm)

    if not isinstance(norm, dict):
        log.error("get_snapshot_norm: 'norm' is not a JSON object (type=%s)", type(norm))
        raise RuntimeError("output.snapshot: kolumna 'norm' nie jest JSON-em")

    try:
        log.debug("get_snapshot_norm: keys=%d", len(norm.keys()))
    except Exception:
        pass

    return dict(norm)


async def get_snapshot_consolidated_norm(con: psycopg.AsyncConnection, *, calc_id: str) -> dict[str, Any]:
    """
    Zwraca `norm['consolidated']` z output.snapshot jeśli istnieje,
    w przeciwnym razie zwraca całe `norm`.
    """
    norm = await get_snapshot_norm(con, calc_id=calc_id)

    section = None
    if isinstance(norm, dict):
        section = norm.get("consolidated")

    if isinstance(section, str):
        import json as _json
        try:
            section = _json.loads(section)
        except Exception:
            log.error("get_snapshot_consolidated_norm: 'consolidated' is not valid JSON string")
            raise RuntimeError("output.snapshot.norm.consolidated: niepoprawny JSON (string)")

    if section is not None and not isinstance(section, dict):
        log.error("get_snapshot_consolidated_norm: 'consolidated' is not an object (type=%s)", type(section))
        raise RuntimeError("output.snapshot.norm.consolidated: nie jest obiektem JSON")

    # gdy brak 'consolidated' → zwróć całe norm (nowy kształt snapshotu)
    return dict(section) if isinstance(section, dict) else dict(norm)


# Alias funkcyjny: stara nazwa zwraca „consolidated jeśli jest, inaczej całe norm”
async def get_consolidated_norm(con: psycopg.AsyncConnection, *, calc_id: str) -> dict[str, Any]:
    return await get_snapshot_consolidated_norm(con, calc_id=calc_id)
