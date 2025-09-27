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
# minimalna poprawka: dopuszczamy DB_PASS jako alias
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
    return "text", None

def _sql_type(t: str, p: Optional[int]) -> str:
    if t == "integer": return "integer"
    if t == "number":
        scale = 6 if p is None else max(0, int(p))
        return f"numeric(20,{scale})"
    if t == "boolean": return "boolean"
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
    rows = _execute(conn, f"""
        SELECT payload
        FROM {schema}.{table}
        ORDER BY updated_at DESC
        LIMIT 1
    """)
    return (rows[0]["payload"] if rows else {}) or {}

def _merge_payload(old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(old or {})
    merged.update(new or {})
    return merged

def _typed_from_input(input_data: Dict[str, Any], form: str) -> Dict[str, Any]:
    fsec = FORM_FIELDS.get(form, {}) or {}
    out: Dict[str, Any] = {}
    for k, spec in fsec.items():
        if k not in input_data:
            continue
        t, d = _parse_type(spec)
        v = input_data[k]
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
    if form_name not in TABLES:
        raise HTTPException(status_code=404, detail=f"Unknown form: {form_name}")
    schema, table = TABLES[form_name]

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    payload_in = body.get("payload") if isinstance(body, dict) and "payload" in body else body
    if not isinstance(payload_in, dict):
        raise HTTPException(status_code=400, detail="Body must be an object or {'payload': {...}}")

    typed = _typed_from_input(payload_in, form_name)

    try:
        with _connect() as conn:
            # 1) dopilnuj kolumn wg configu
            _ensure_table_columns(schema, table, FORM_FIELDS.get(form_name, {}), conn)

            # 2) scal z poprzednim payloadem z tej tabeli
            old = _latest_payload(conn, schema, table)
            merged_payload = _merge_payload(old, typed)

            # 3) WSTAW KOMPLET WARTOŚCI DLA WSZYSTKICH POLI Z CONFIGU
            cfg_cols = list(FORM_FIELDS.get(form_name, {}).keys())
            to_insert_raw = {c: merged_payload.get(c) for c in cfg_cols}
            to_insert = _typed_from_input(to_insert_raw, form_name)

            # 4) INSERT: payload + wszystkie kolumny z configu
            cols = [sql.Identifier("payload")] + [sql.Identifier(c) for c in cfg_cols]
            vals = [sql.Placeholder()] + [sql.Placeholder() for _ in cfg_cols]
            args = [json.dumps(merged_payload, ensure_ascii=False)] + [to_insert[c] for c in cfg_cols]

            s = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(", ").join(cols),
                sql.SQL(", ").join(vals),
            )
            with _connect() as conn2:
                with conn2.cursor() as cur:
                    cur.execute(s, args)

        return {"ok": True, "form": form_name, "inserted_keys": list(typed.keys())}
    except Exception as e:
        log.exception("post_form failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------------------------
# Public helper(s) for app_server.py
# -----------------------------------------------------------------------------
def ensure_columns_from_config(form_name: Optional[str] = None) -> None:
    """
    Wywoływany przez app_server.py (bez argumentu) lub ręcznie dla jednej tabeli.
    """
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
    # spróbuj na starcie dołożyć kolumny wg configu (loguj błąd, ale nie przerywaj startu)
    try:
        ensure_columns_from_config(None)
        log.info("startup: ensure_columns_from_config OK")
    except Exception as e:
        log.warning("startup: ensure_columns_from_config() failed: %s", e)

    app = FastAPI(title="energia-prep-2 Params API", version="1.0.0")

    @app.on_event("startup")
    def _on_startup():
        try:
            ensure_columns_from_config(None)
            log.info("startup(event): ensure_columns_from_config OK")
        except Exception as e:
            log.warning("startup(event): ensure_columns_from_config() failed: %s", e)

    app.include_router(router)
    return app

app = create_app()
