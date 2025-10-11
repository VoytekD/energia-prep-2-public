# src/energia_prep2/tasks/params_api.py
from __future__ import annotations

import json
import logging
import os
import re
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, Optional, Tuple

import psycopg
from fastapi import APIRouter, FastAPI, HTTPException, Request
from psycopg import sql
from psycopg.rows import dict_row
import yaml

# -----------------------------------------------------------------------------
# ENV / DB
# -----------------------------------------------------------------------------
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME", "energia")
DB_USER     = os.getenv("DB_USER", "voytek")
DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS", "")
DB_SSLMODE  = os.getenv("DB_SSLMODE", "prefer")
DB_TZ       = os.getenv("DB_TIMEZONE", "UTC")

FORM_CONFIG = os.getenv("FORM_CONFIG", "/app/config_form.yaml")

router = APIRouter()
SAFE_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------
def _dsn() -> str:
    return (
        f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
        f"user={DB_USER} password={DB_PASSWORD} sslmode={DB_SSLMODE}"
    )

def _connect():
    conn = psycopg.connect(_dsn(), autocommit=True, connect_timeout=8)
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SET TIME ZONE {}").format(sql.Literal(DB_TZ)))
    return conn

def _execute(conn, query: str, params: Optional[tuple] = None):
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, params or ())
        if cur.description:
            return cur.fetchall()
        return None

def _execute_sql(conn, s: sql.Composed, params: Optional[tuple] = None):
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(s, params or ())
        if cur.description:
            return cur.fetchall()
        return None

# -----------------------------------------------------------------------------
# BOOTSTRAP obiektów kolejkowania (util.enqueue_calc + kolejka + trigger)
# -----------------------------------------------------------------------------
UTIL_ENQUEUE_DDL = """
CREATE SCHEMA IF NOT EXISTS util;
CREATE SCHEMA IF NOT EXISTS output;

-- Kolejka jobów w OUTPUT (spójnie z listenerem)
CREATE TABLE IF NOT EXISTS output.calc_job_queue (
  job_id      uuid PRIMARY KEY
              DEFAULT (md5(random()::text || clock_timestamp()::text)::uuid),
  params_ts   timestamptz      NOT NULL,
  status      text             NOT NULL DEFAULT 'queued'
                                  CHECK (status IN ('queued','running','done','error')),
  created_at  timestamptz      NOT NULL DEFAULT now(),
  started_at  timestamptz,
  finished_at timestamptz,
  error       text
);

CREATE INDEX IF NOT EXISTS ix_calc_job_queue_status_created
  ON output.calc_job_queue (status, created_at DESC);

-- Enqueue do OUTPUT + NOTIFY na kanał, którego słucha listener
CREATE OR REPLACE FUNCTION util.enqueue_calc(p_params_ts timestamptz DEFAULT now())
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
  v_job_id uuid;
BEGIN
  INSERT INTO output.calc_job_queue (params_ts, status)
  VALUES (COALESCE(p_params_ts, now()), 'queued')
  RETURNING job_id INTO v_job_id;

  PERFORM pg_notify(
    'ch_energy_rebuild',
    json_build_object(
        'job_id', v_job_id::text,
        'status', 'queued'
    )::text
  );

  RETURN v_job_id;
END;
$$;
"""

def _ensure_util_enqueue_objects():
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(UTIL_ENQUEUE_DDL)
        log.info("bootstrap: util.enqueue_calc + params.calc_job_queue OK")
    except Exception as e:
        log.warning("bootstrap: util enqueue objects failed: %s", e)

# -----------------------------------------------------------------------------
# Config loading
# -----------------------------------------------------------------------------
def _load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def _build_from_config(cfg: Dict[str, Any]) -> Tuple[Dict[str, Tuple[str, str]], Dict[str, Dict[str, Any]]]:
    params_tables = cfg.get("params_tables", {}) or {}
    fields: Dict[str, Dict[str, Any]] = {}
    tables: Dict[str, Tuple[str, str]] = {}

    for form_name, tdef in params_tables.items():
        schema = tdef.get("schema", "params")
        table  = tdef.get("table", form_name)
        tables[form_name] = (schema, table)

    for form_name in tables.keys():
        fsec = cfg.get(form_name, {}) or {}
        fields[form_name] = fsec

    return tables, fields

_CONFIG: Dict[str, Any] = {}
TABLES: Dict[str, Tuple[str, str]] = {}
FORM_FIELDS: Dict[str, Dict[str, Any]] = {}

def _reload_config():
    global _CONFIG, TABLES, FORM_FIELDS
    _CONFIG      = _load_config(FORM_CONFIG)
    TABLES, FORM_FIELDS = _build_from_config(_CONFIG)
    log.info("params_api: forms loaded: %s", list(TABLES.keys()))

# -----------------------------------------------------------------------------
# Spec parsing + column creation
# -----------------------------------------------------------------------------
def _parse_type(spec: Any) -> Tuple[str, Optional[int]]:
    if isinstance(spec, (list, tuple)):
        return "array", None
    if isinstance(spec, bool):   return "boolean", None
    if isinstance(spec, int):    return "integer", None
    if isinstance(spec, float):  return "number", None
    if not isinstance(spec, str): return "text", None

    right = spec.split(";", 1)[1].strip().lower() if ";" in spec else "text"
    if "," in right:
        t, p = right.split(",", 1)
        t = t.strip()
        try:    p = int(p.strip())
        except: p = None
    else:
        t, p = right.strip(), None

    if t == "integer": return "integer", None
    if t == "number":  return "number", p
    if t == "boolean": return "boolean", None
    if t in ("array", "json"):  return "array", None
    return "text", None

def _sql_type(t: str, p: Optional[int]) -> str:
    if t == "integer": return "integer"
    if t == "number":
        scale = 6 if p is None else max(0, int(p))
        return f"numeric(20,{scale})"
    if t == "boolean": return "boolean"
    if t == "array":   return "jsonb"
    return "text"

def _ensure_table_columns(schema: str, table: str, fields: Dict[str, Any], conn):
    rows = _execute(conn, """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
    """, (schema, table))
    existing = {r["column_name"] for r in (rows or [])}

    for col, spec in (fields or {}).items():
        if col in existing:
            continue
        t, p = _parse_type(spec)
        col_type = _sql_type(t, p)
        s = sql.SQL("ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS {} {}").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.Identifier(col),
            sql.SQL(col_type),
        )
        _execute_sql(conn, s)
        log.info("params_api: added column %s.%s.%s %s", schema, table, col, col_type)

# -----------------------------------------------------------------------------
# Payload helpers
# -----------------------------------------------------------------------------
def _latest_payload(conn, schema: str, table: str) -> Dict[str, Any]:
    s = sql.SQL("""
        SELECT payload
        FROM {}.{}
        ORDER BY updated_at DESC
        LIMIT 1
    """).format(sql.Identifier(schema), sql.Identifier(table))
    rows = _execute_sql(conn, s)
    return (rows[0]["payload"] if rows else {}) or {}

def _merge_payload(old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(old or {})
    merged.update(new or {})
    return merged

ARRAY_KEYS_BY_FORM = {
    "form_par_arbitrazu": {
        "bonus_hrs_ch", "bonus_hrs_dis", "bonus_hrs_ch_free", "bonus_hrs_dis_free"
    },
}

def _typed_from_input(input_data: Dict[str, Any], form: str) -> Dict[str, Any]:
    fsec = FORM_FIELDS.get(form, {}) or {}
    array_keys = ARRAY_KEYS_BY_FORM.get(form, set())
    out: Dict[str, Any] = {}
    for k, spec in fsec.items():
        if k not in input_data:
            continue
        t, d = _parse_type(spec)
        v = input_data[k]

        if k in array_keys:
            try:
                if isinstance(v, str):
                    v = json.loads(v)
                if isinstance(v, (list, tuple)):
                    v = [int(x) for x in v if str(x).lstrip("+-").isdigit()]
                else:
                    v = []
            except Exception:
                v = []
            out[k] = v
            continue

        try:
            if t == "integer":
                v = int(Decimal(str(v)).to_integral_value(rounding=ROUND_HALF_UP))
            elif t == "number":
                dv = Decimal(str(v))
                if d is not None:
                    dv = dv.quantize(Decimal(1).scaleb(-d), rounding=ROUND_HALF_UP)
                v = float(dv)
            elif t == "boolean":
                v = str(v).strip().lower() in ("1","true","yes","y","on")
            elif t == "array":
                if isinstance(v, str):
                    try:
                        v = json.loads(v)
                    except Exception:
                        v = []
                if isinstance(v, (list, tuple)):
                    vv = []
                    for x in v:
                        sx = str(x).lstrip("+-")
                        if sx.isdigit():
                            vv.append(int(x))
                        else:
                            vv.append(x)
                    v = vv
                else:
                    v = []
            else:
                v = str(v)
        except Exception:
            pass
        out[k] = v
    return out

# -----------------------------------------------------------------------------
# API endpoints
# -----------------------------------------------------------------------------
@router.get("/_health/status.json")
def health():
    try:
        with _connect() as conn:
            _execute(conn, "SELECT 1")
        return {"status": "OK"}
    except Exception as e:
        return {"status": "ERROR", "detail": str(e)}

@router.post("/form/{form_name}")
async def post_form(form_name: str, request: Request):
    log.info("API: POST /form/%s — start", form_name)

    if form_name not in TABLES:
        log.warning("API: unknown form '%s'", form_name)
        raise HTTPException(status_code=404, detail=f"Unknown form: {form_name}")
    schema, table = TABLES[form_name]

    if not SAFE_IDENT_RE.match(schema) or not SAFE_IDENT_RE.match(table):
        log.error("API: invalid schema/table: %s.%s", schema, table)
        raise HTTPException(status_code=400, detail="Invalid schema/table")

    try:
        body = await request.json()
        log.info("API: RAW request body for %s → %s", form_name, body)
    except Exception:
        log.exception("API: invalid JSON body for %s", form_name)
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    payload_in = body.get("payload") if isinstance(body, dict) and "payload" in body else body
    if not isinstance(payload_in, dict):
        log.error("API: body not an object for %s: %r", form_name, payload_in)
        raise HTTPException(status_code=400, detail="Body must be an object or {'payload': {...}}")

    log.info("API: input payload for %s → %s", form_name, payload_in)

    typed = _typed_from_input(payload_in, form_name)
    log.info("API: typed payload for %s → %s", form_name, typed)

    try:
        with _connect() as conn:
            # upewnij się, że mamy obiekty util/calc_job_queue (idempotentnie)
            try:
                with conn.cursor() as cur:
                    cur.execute(UTIL_ENQUEUE_DDL)
            except Exception as e:
                log.warning("API: util enqueue bootstrap during POST failed: %s", e)

            # 1) kolumny wg configu
            log.debug("API: ensure columns for %s.%s", schema, table)
            _ensure_table_columns(schema, table, FORM_FIELDS.get(form_name, {}), conn)

            # 2) seed istnieje?
            exists_sql = sql.SQL("SELECT 1 FROM {}.{} LIMIT 1").format(
                sql.Identifier(schema), sql.Identifier(table)
            )
            with conn.cursor() as cur:
                cur.execute(exists_sql)
                has_seed = cur.fetchone() is not None
            log.info("API: seed exists for %s.%s → %s", schema, table, has_seed)
            if not has_seed:
                log.error("API: missing seed in %s.%s", schema, table)
                raise HTTPException(
                    status_code=409,
                    detail=f"No seed row in {schema}.{table}. Run bootstrap/config seed first."
                )

            # 3) scalenie z poprzednim payloadem
            old = _latest_payload(conn, schema, table)
            log.info("API: old payload (top row) for %s.%s → %s", schema, table, old)
            merged_payload = _merge_payload(old, typed)
            log.info("API: merged payload for %s.%s → %s", schema, table, merged_payload)

            # 4) INSERT tylko payload
            s = sql.SQL("INSERT INTO {}.{} (payload) VALUES (%s)").format(
                sql.Identifier(schema),
                sql.Identifier(table),
            )
            log.debug("API: SQL to execute for %s.%s → INSERT payload", schema, table)

            job_id = None
            with _connect() as conn2:
                with conn2.cursor(row_factory=dict_row) as cur2:
                    cur2.execute(s, [json.dumps(merged_payload, ensure_ascii=False)])
                    log.info("API: DB INSERT done for %s.%s, rowcount=%s", schema, table, getattr(cur2, "rowcount", "?"))

                    # enqueue → util.enqueue_calc
                    try:
                        cur2.execute("SELECT util.enqueue_calc(now()) AS job_id")
                        r = cur2.fetchone()
                        job_id = r["job_id"] if r and "job_id" in r else None
                        log.info("API: util.enqueue_calc issued, job_id=%s", job_id)
                    except Exception as e:
                        log.warning("API: util.enqueue_calc failed (params saved ok): %s", e)

        resp = {"ok": True, "form": form_name, "inserted_keys": list(typed.keys())}
        if job_id:
            resp["job_id"] = str(job_id)
        log.info("API: response to client for %s → %s", form_name, resp)
        return resp

    except HTTPException:
        raise
    except Exception as e:
        log.exception("API: post_form failed for %s: %s", form_name, e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------------------------
# Public helper(s) for app_server.py
# -----------------------------------------------------------------------------
def ensure_columns_from_config(form_name: Optional[str] = None) -> None:
    _reload_config()
    with _connect() as conn:
        if form_name is None:
            for form, (schema, table) in TABLES.items():
                _ensure_table_columns(schema, table, FORM_FIELDS.get(form, {}), conn)
        else:
            if form_name not in TABLES:
                raise ValueError(f"Unknown form: {form_name}")
            schema, table = TABLES[form_name]
            _ensure_table_columns(schema, table, FORM_FIELDS.get(form_name, {}), conn)

# -----------------------------------------------------------------------------
# App factory
# -----------------------------------------------------------------------------
def create_app() -> FastAPI:
    # Bootstrap util.enqueue_calc + kolejka na starcie
    _ensure_util_enqueue_objects()

    try:
        ensure_columns_from_config(None)
        log.info("startup: ensure_columns_from_config OK")
    except Exception as e:
        log.warning("startup: ensure_columns_from_config() failed: %s", e)

    app = FastAPI(title="energia-prep-2 Params API", version="1.0.0")

    @app.on_event("startup")
    def _on_startup():
        # jeszcze raz, idempotentnie (jeśli kontener się podnosi zanim DB będzie gotowe)
        _ensure_util_enqueue_objects()
        try:
            ensure_columns_from_config(None)
            log.info("startup(event): ensure_columns_from_config OK")
        except Exception as e:
            log.warning("startup(event): ensure_columns_from_config() failed: %s", e)

    app.include_router(router)
    return app

app = create_app()
