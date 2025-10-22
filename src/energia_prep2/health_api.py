# src/energia_prep2/health_api.py
from __future__ import annotations

import html
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

# ─────────────────────────────────────────────────────────────────────────────
# Konfiguracja / ENV

APP_NAME = os.getenv("APP_NAME", "energia-prep-2")
APP_VERSION = os.getenv("APP_VERSION", "1.3.0")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "energia")
DB_USER = os.getenv("DB_USER", "voytek")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SSLMODE = os.getenv("DB_SSLMODE", "prefer")
DB_TIMEZONE = os.getenv("DB_TIMEZONE", "UTC")  # trzymajmy UTC, prezentację robimy w UI

# Kluczowe tabele wejściowe (PREP)
INPUT_TABLES: List[Tuple[str, str]] = [
    ("input", "date_dim"),
    ("input", "konsumpcja"),
    ("input", "produkcja"),
]

# Kandydaci na źródło cen (legacy + docelowe)
PRICES_CANDIDATES: List[Tuple[str, str]] = [
    ("input", "ceny_godzinowe"),  # docelowo
    ("tge", "prices"),            # legacy (jeśli istnieje)
]

# ─────────────────────────────────────────────────────────────────────────────
# Narzędzia

def _dsn() -> str:
    return (
        f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} "
        f"user={DB_USER} password={DB_PASSWORD} sslmode={DB_SSLMODE}"
    )

def _connect() -> psycopg.Connection:
    conn = psycopg.connect(_dsn(), autocommit=True, connect_timeout=8)
    with conn.cursor() as cur:
        # Prezentacja timestamptz w wybranej strefie
        cur.execute(sql.SQL("SET TIME ZONE {}").format(sql.Literal(DB_TIMEZONE)))
    return conn

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def _utcnow_iso() -> str:
    return _utcnow().isoformat()

def _query_one(conn: psycopg.Connection, sql_text: str, params: Optional[tuple] = None) -> Any:
    with conn.cursor() as cur:
        cur.execute(sql_text, params or ())
        row = cur.fetchone()
    return row[0] if row else None

def _query_dicts(conn: psycopg.Connection, sql_text: str, params: Optional[tuple] = None) -> List[dict]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql_text, params or ())
        return list(cur.fetchall())

def _exists_table(conn: psycopg.Connection, schema: str, table: str) -> bool:
    return bool(
        _query_one(
            conn,
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s
            LIMIT 1
            """,
            (schema, table),
        )
    )

def _detect_prices_table(conn: psycopg.Connection) -> Optional[str]:
    for sch, tab in PRICES_CANDIDATES:
        if _exists_table(conn, sch, tab):
            return f"{sch}.{tab}"
    return None

def _humanize_seconds(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    seconds = int(round(seconds))
    neg = seconds < 0
    seconds = abs(seconds)
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    out = []
    if d:
        out.append(f"{d}d")
    if d or h:
        out.append(f"{h}h")
    if d or h or m:
        out.append(f"{m}m")
    out.append(f"{s:02d}s")
    txt = " ".join(out)
    return f"-{txt}" if neg else txt

def _fmt_ts(ts: Optional[datetime]) -> str:
    if not ts:
        return "—"
    return ts.replace(microsecond=0).isoformat()

def _safe_json(obj: Any) -> str:
    def _default(o):
        if isinstance(o, datetime):
            return _fmt_ts(o)
        return str(o)
    try:
        return json.dumps(obj, ensure_ascii=False, default=_default)
    except Exception:
        return "{}"

def _overall(*statuses: str) -> str:
    if any(s == "down" for s in statuses):
        return "down"
    if any(s == "degraded" for s in statuses):
        return "degraded"
    return "up"

# ─────────────────────────────────────────────────────────────────────────────
# Grupy (Overview / wspólne)

def _group_runtime() -> Dict[str, Any]:
    return {
        "status": "up",
        "app": f"{APP_NAME} {APP_VERSION}",
        "checked_at": _utcnow_iso(),
    }

def _group_db() -> Dict[str, Any]:
    started = time.perf_counter()
    try:
        with _connect() as conn:
            now_db = _query_one(conn, "SELECT now()")
        dur = time.perf_counter() - started
        return {
            "status": "up",
            "now_db": _fmt_ts(now_db),
            "duration_s": round(dur, 4),
            "duration_human": _humanize_seconds(dur),
            "error": None,
            "checked_at": _utcnow_iso(),
        }
    except Exception as e:
        dur = time.perf_counter() - started
        return {
            "status": "down",
            "now_db": None,
            "duration_s": round(dur, 4),
            "duration_human": _humanize_seconds(dur),
            "error": str(e),
            "checked_at": _utcnow_iso(),
        }

def _group_schema(conn: psycopg.Connection) -> Dict[str, Any]:
    missing_tables: List[str] = []
    for sch, tab in INPUT_TABLES:
        if not _exists_table(conn, sch, tab):
            missing_tables.append(f"{sch}.{tab}")
    detected_prices = _detect_prices_table(conn)
    if not detected_prices:
        missing_tables.append("(prices) input.ceny_godzinowe")
    status = "up" if not missing_tables else "degraded"
    return {
        "status": status,
        "missing": {"tables": missing_tables},
        "detected_prices_table": detected_prices,
        "checked_at": _utcnow_iso(),
    }

def _table_stats(conn: psycopg.Connection, schema: str, table: str) -> Dict[str, Any]:
    try:
        rows = _query_dicts(
            conn,
            f"""
            SELECT
              MIN(ts_utc) AS min_ts_utc,
              MAX(ts_utc) AS max_ts_utc,
              COUNT(*)   AS cnt
            FROM {schema}.{table}
            """,
        )
        r = rows[0] if rows else {}
        min_ts = r.get("min_ts_utc")
        max_ts = r.get("max_ts_utc")
        cnt = int(r.get("cnt") or 0)
        age = (_utcnow() - (max_ts or _utcnow())).total_seconds() if max_ts else None
        return {
            "count": cnt,
            "min_ts_utc": _fmt_ts(min_ts) if min_ts else None,
            "max_ts_utc": _fmt_ts(max_ts) if max_ts else None,
            "age_seconds": age,
            "age_human": _humanize_seconds(age),
        }
    except Exception as e:
        return {"error": str(e), "count": 0, "min_ts_utc": None, "max_ts_utc": None, "age_seconds": None, "age_human": "—"}

def _group_data(conn: psycopg.Connection) -> Dict[str, Any]:
    stats: Dict[str, Any] = {}
    for sch, tab in INPUT_TABLES:
        stats[f"{sch}.{tab}"] = _table_stats(conn, sch, tab)
    prices_tbl = _detect_prices_table(conn)
    if prices_tbl:
        sch, tab = prices_tbl.split(".", 1)
        stats[prices_tbl] = _table_stats(conn, sch, tab)
    else:
        stats["prices"] = {"error": "prices table not found"}
    degraded = any(("error" in v) or (v.get("count", 0) == 0) for v in stats.values())
    status = "degraded" if degraded else "up"

    # Formularz (skrót) – jeśli istnieje
    form_latest = {"present": False, "inserted_at": None, "payload": None}
    try:
        rows = _query_dicts(
            conn,
            """
            SELECT inserted_at, payload
            FROM params.form_zmienne
            ORDER BY inserted_at DESC
            LIMIT 1
            """,
        )
        if rows:
            form_latest["present"] = True
            form_latest["inserted_at"] = _fmt_ts(rows[0]["inserted_at"])
            form_latest["payload"] = rows[0]["payload"]
    except Exception:
        pass

    return {"status": status, "stats": stats, "form_zmienne_latest": form_latest, "checked_at": _utcnow_iso()}

# ─────────────────────────────────────────────────────────────────────────────
# Pasek nawigacji (menu)

def _nav(active: str) -> str:
    tabs = [("Overview", "/"), ("Prep", "/prep"), ("Pipeline", "/pipeline")]
    out = []
    for label, href in tabs:
        sel = " style='color:#93c5fd; font-weight:700;'" if label == active else ""
        out.append(f"<a href='{href}'{sel}>{html.escape(label)}</a>")
    return "<div style='display:flex; gap:14px; margin:8px 0 18px'>" + " · ".join(out) + "</div>"

# ─────────────────────────────────────────────────────────────────────────────
# Pipeline (scope='calc') — odczyt z output.health_service

def _group_pipeline(conn: psycopg.Connection) -> Dict[str, Any]:
    rows = _query_dicts(
        conn,
        """
        SELECT calc_id, params_ts, overall_status, sections, summary, created_at
        FROM output.health_service
        WHERE scope='calc'
        ORDER BY created_at DESC
        LIMIT 1
        """,
    )
    if not rows:
        return {"status": "degraded", "present": False, "info": "Brak snapshotu CALC w output.health_service", "snapshot": {}}
    row = rows[0]
    snap = {
        "calc_id": str(row.get("calc_id") or ""),
        "params_ts": row.get("params_ts"),
        "overall_status": row.get("overall_status"),
        "stages": row.get("sections") or {},
        "summary": row.get("summary"),
        "created_at": row.get("created_at"),
    }
    return {"status": row["overall_status"], "present": True, "snapshot": snap}

# ─────────────────────────────────────────────────────────────────────────────
# Prep (scope='prep') — snapshot lub fallback live

def _prep_live(conn: psycopg.Connection) -> Dict[str, Any]:
    schema = _group_schema(conn)
    data = _group_data(conn)
    input_status = data.get("status", "degraded")
    forms_status = "up" if data.get("form_zmienne_latest", {}).get("present") else "degraded"
    overall = _overall(schema.get("status", "degraded"), input_status, forms_status)
    return {
        "status": overall,
        "input": {"status": input_status, "stats": data.get("stats", {})},
        "params": {"status": forms_status, "form_zmienne_latest": data.get("form_zmienne_latest", {})},
        "created_at": _utcnow_iso(),
        "summary": "Live PREP (fallback) — brak snapshotu w health_service.",
    }

def _group_prep(conn: psycopg.Connection) -> Dict[str, Any]:
    rows = _query_dicts(
        conn,
        """
        SELECT overall_status, sections, summary, created_at
        FROM output.health_service
        WHERE scope='prep'
        ORDER BY created_at DESC
        LIMIT 1
        """,
    )
    if rows:
        r = rows[0]
        sec = r.get("sections") or {}
        return {
            "status": r.get("overall_status", "degraded"),
            "input": sec.get("input") or {},
            "params": sec.get("params") or {},
            "created_at": _fmt_ts(r.get("created_at")),
            "summary": r.get("summary") or "",
        }
    return _prep_live(conn)

# ─────────────────────────────────────────────────────────────────────────────
# FastAPI

app = FastAPI(title=f"{APP_NAME} — Health", version=APP_VERSION)

@app.get("/status.json", response_class=JSONResponse)
def health_json():
    groups, overall = _compute_overview()
    return {"status": overall, "checked_at": _utcnow_iso(), "groups": groups}

@app.get("/", response_class=HTMLResponse)
def health_html():
    groups, overall = _compute_overview()
    return _page_overview(groups, overall)

@app.get("/pipeline.json", response_class=JSONResponse)
def pipeline_json():
    try:
        with _connect() as conn:
            pipe = _group_pipeline(conn)
        return {"status": pipe["status"], "checked_at": _utcnow_iso(), "pipeline": pipe}
    except Exception as e:
        return JSONResponse({"status": "down", "error": str(e)}, status_code=500)

@app.get("/pipeline", response_class=HTMLResponse)
def pipeline_html():
    try:
        with _connect() as conn:
            pipe = _group_pipeline(conn)
        return _pipeline_page(pipe)
    except Exception as e:
        err_html = f"<html><body>{_nav('Pipeline')}<h1>Pipeline</h1><p style='color:#e74c3c'>{html.escape(str(e))}</p></body></html>"
        return HTMLResponse(err_html, status_code=500)

@app.get("/prep.json", response_class=JSONResponse)
def prep_json():
    try:
        with _connect() as conn:
            prep = _group_prep(conn)
        return {"status": prep["status"], "checked_at": _utcnow_iso(), "prep": prep}
    except Exception as e:
        return JSONResponse({"status": "down", "error": str(e)}, status_code=500)

@app.get("/prep", response_class=HTMLResponse)
def prep_html():
    try:
        with _connect() as conn:
            prep = _group_prep(conn)
        return _prep_page(prep)
    except Exception as e:
        err_html = f"<html><body>{_nav('Prep')}<h1>Prep</h1><p style='color:#e74c3c'>{html.escape(str(e))}</p></body></html>"
        return HTMLResponse(err_html, status_code=500)

# ─────────────────────────────────────────────────────────────────────────────
# Overview — compute + HTML

def _compute_overview() -> Tuple[Dict[str, Any], str]:
    runtime = _group_runtime()
    db = _group_db()
    try:
        with _connect() as conn:
            schema = _group_schema(conn)
            data = _group_data(conn)
    except Exception as e:
        schema = {"status": "down", "error": str(e), "missing": {"tables": []}}
        data = {"status": "down", "error": str(e), "stats": {}}
    forms_status = "up" if data.get("form_zmienne_latest", {}).get("present") else "degraded"
    forms = {"status": forms_status, "info": data.get("form_zmienne_latest", {})}
    groups = {"runtime": runtime, "database": db, "schema": schema, "data": data, "forms": forms}
    overall = _overall(db["status"], schema["status"], data["status"], forms["status"])
    return groups, overall

def _chip(status: str) -> str:
    color = {"up": "#2ecc71", "degraded": "#f39c12", "down": "#e74c3c"}.get(status, "#7f8c8d")
    text = status.upper()
    return f'<span class="chip" style="background:{color}">{html.escape(text)}</span>'

def _kpi(title: str, value: str, sub: str = "") -> str:
    sub_html = f'<div class="kpi-sub">{html.escape(sub)}</div>' if sub else ""
    return f"""
    <div class="kpi">
      <div class="kpi-title">{html.escape(title)}</div>
      <div class="kpi-value">{html.escape(value)}</div>
      {sub_html}
    </div>
    """

def _row(cells: List[str]) -> str:
    tds = "".join(f"<td>{c}</td>" for c in cells)
    return f"<tr>{tds}</tr>"

def _fmt_count(v: Any) -> str:
    try:
        return f"{int(v):,}".replace(",", " ")
    except Exception:
        return str(v)

def _page_overview(groups: Dict[str, Any], overall: str) -> str:
    now_txt = _fmt_ts(datetime.fromisoformat(groups["runtime"]["checked_at"]))
    stats = groups["data"].get("stats", {})
    dd = stats.get("input.date_dim", {})
    kons = stats.get("input.konsumpcja", {})
    prod = stats.get("input.produkcja", {})
    prices_key = next((k for k in stats.keys() if k.endswith("ceny_godzinowe") or k.endswith("prices")), None)
    ceny = stats.get(prices_key) if prices_key else {}
    forms_info = groups["data"].get("form_zmienne_latest", {})
    forms_present = "TAK" if forms_info.get("present") else "NIE"

    kpi_html = "".join([
        _kpi("Status systemu", overall.upper(), f"sprawdzone: {now_txt}"),
        _kpi("Ceny godzinowe – rekordów", _fmt_count(ceny.get("count", 0)),
             f"ostatni: {ceny.get('max_ts_utc') or '—'} • wiek: {ceny.get('age_human','—')}"),
        _kpi("Konsumpcja – rekordów", _fmt_count(kons.get("count", 0)),
             f"zakres: {kons.get('min_ts_utc','—')} → {kons.get('max_ts_utc','—')}"),
        _kpi("Produkcja – rekordów", _fmt_count(prod.get("count", 0)),
             f"zakres: {prod.get('min_ts_utc','—')} → {prod.get('max_ts_utc','—')}"),
        _kpi("Formularz (params.form_zmienne)", forms_present,
             f"inserted_at: {forms_info.get('inserted_at','—')}"),
    ])

    runtime = groups["runtime"]
    runtime_table = "".join([
        _row(["App", f"{html.escape(runtime.get('app',''))} {_chip(runtime.get('status','up'))}"]),
    ])

    db = groups["database"]
    db_table = "".join([
        _row(["now_db", html.escape(db.get("now_db","—") or "—")]),
        _row(["connect_time", f"{html.escape(db.get('duration_human','—'))} ({db.get('duration_s','—')}s)"]),
        _row(["error", html.escape(db.get("error") or "—")]),
    ])

    schema = groups["schema"]
    missing = (schema.get("missing") or {}).get("tables", [])
    detected_prices = schema.get("detected_prices_table", "—")
    schema_rows = []
    schema_rows.append(_row(["detected prices table", html.escape(str(detected_prices))]))
    schema_rows.append(_row(["missing tables", html.escape(", ".join(missing) if missing else "—")]))
    schema_table = "".join(schema_rows)

    def _stats_table(title: str, s: Dict[str, Any]) -> str:
        return "".join([
            _row(["table", html.escape(title)]),
            _row(["count", _fmt_count(s.get("count", 0))]),
            _row(["min_ts", html.escape(s.get("min_ts_utc", "—") or "—")]),
            _row(["max_ts", html.escape(s.get("max_ts_utc", "—") or "—")]),
            _row(["age", html.escape(s.get("age_human", "—") or "—")]),
        ])

    dd_table = _stats_table("input.date_dim", dd or {})
    kons_table = _stats_table("input.konsumpcja", kons or {})
    prod_table = _stats_table("input.produkcja", prod or {})
    ceny_table = _stats_table(prices_key or "prices", ceny or {})

    return f"""
<!doctype html>
<html lang="pl">
<head>
<meta charset="utf-8"/>
<title>Status — {html.escape(APP_NAME)}</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
  :root {{
    --bg: #0f172a;
    --panel: #111827;
    --text: #e5e7eb;
    --muted: #9ca3af;
    --accent: #60a5fa;
    --ok: #2ecc71;
    --warn: #f39c12;
    --err: #e74c3c;
    --chip: #1f2937;
    --kpi: #0b1220;
    --border: #1f2937;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0;
    font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial;
    color: var(--text);
    background: radial-gradient(1200px 600px at 20% -20%, #0b1220, #0f172a);
  }}
  .wrap {{ max-width: 1200px; margin: 24px auto; padding: 0 16px; }}
  h1 {{ font-size: 22px; margin: 0 0 8px 0; }}
  .subtitle {{ color: var(--muted); font-size: 12px; margin-bottom: 16px; }}
  .grid {{ display: grid; gap: 14px; grid-template-columns: repeat(2, minmax(0, 1fr)); }}
  .panel {{
    background: linear-gradient(180deg, #0d1220, #0b0f1a);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 14px;
  }}
  .panel h2 {{ font-size: 14px; margin: 0 0 10px 0; display:flex; align-items:center; gap:8px; }}
  .panel h2 .chip {{ margin-left: 6px; }}
  table {{ width: 100%; border-collapse: collapse; }}
  td {{ padding: 6px 8px; border-top: 1px solid #1e293b; font-size: 13px; vertical-align: top; }}
  td:first-child {{ color: var(--muted); width: 32%; }}
  .chip {{
    display:inline-block; padding:2px 8px; border-radius:999px; font-size:11px;
    color:#0b0f1a; font-weight:700;
  }}
  .kpis {{ display:grid; gap:12px; grid-template-columns: repeat(5, minmax(0, 1fr)); margin: 12px 0 6px; }}
  .kpi {{
    background: var(--kpi);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 12px;
    min-height: 96px;
  }}
  .kpi-title {{ font-size:12px; color:var(--muted); margin-bottom:6px; }}
  .kpi-value {{ font-size:20px; font-weight:800; letter-spacing:0.2px; }}
  .kpi-sub {{ margin-top:4px; font-size:12px; color:var(--muted); }}
  @media (max-width: 1100px) {{ .kpis {{ grid-template-columns: repeat(2, 1fr); }} }}
  @media (max-width: 700px) {{
    .grid {{ grid-template-columns: 1fr; }}
    .kpis {{ grid-template-columns: 1fr; }}
  }}
  a {{ color:#93c5fd; text-decoration:none; }}
</style>
</head>
<body>
  <div class="wrap">
    {_nav("Overview")}
    <h1>Status systemu — {html.escape(APP_NAME)}</h1>
    <div class="subtitle">Sprawdzone: {now_txt}</div>

    <div class="kpis">{kpi_html}</div>

    <div class="grid">
      <div class="panel">
        <h2>Runtime {_chip(runtime.get('status','up'))}</h2>
        <table>{runtime_table}</table>
      </div>

      <div class="panel">
        <h2>Database {_chip(groups['database'].get('status','down'))}</h2>
        <table>{db_table}</table>
      </div>

      <div class="panel">
        <h2>Schema {_chip(groups['schema'].get('status','down'))}</h2>
        <table>{schema_table}</table>
      </div>

      <div class="panel">
        <h2>Forms {_chip(groups['forms'].get('status','degraded'))}</h2>
        <table>
          <tr><td>present</td><td>{"TAK" if forms_info.get("present") else "NIE"}</td></tr>
          <tr><td>inserted_at</td><td>{html.escape(forms_info.get("inserted_at") or "—")}</td></tr>
          <tr><td>payload (preview)</td><td>{html.escape((_safe_json(forms_info.get("payload") or {})[:350]))}</td></tr>
        </table>
      </div>

      <div class="panel" style="grid-column: 1 / -1;">
        <h2>Data {_chip(groups['data'].get('status','down'))}</h2>
        <table>
          <tr><td colspan="2" style="color:#93c5fd; font-weight:700; padding-top:10px;">input.date_dim</td></tr>
          {dd_table}
          <tr><td colspan="2" style="color:#93c5fd; font-weight:700; padding-top:10px;">input.konsumpcja</td></tr>
          {kons_table}
          <tr><td colspan="2" style="color:#93c5fd; font-weight:700; padding-top:10px;">input.produkcja</td></tr>
          {prod_table}
          <tr><td colspan="2" style="color:#93c5fd; font-weight:700; padding-top:10px;">{html.escape(prices_key or 'prices')}</td></tr>
          {ceny_table}
        </table>
      </div>
    </div>
  </div>
</body>
</html>
    """

# ─────────────────────────────────────────────────────────────────────────────
# HTML — zakładka Pipeline

def _pipeline_page(pipe: Dict[str, Any]) -> str:
    status = pipe.get("status", "degraded")
    snap = pipe.get("snapshot") or {}
    present = pipe.get("present", False)

    calc_id = snap.get("calc_id", "-")
    created_at = snap.get("created_at")
    params_ts = snap.get("params_ts")
    stages = snap.get("stages") or {}
    summary = snap.get("summary") or ""

    created_txt = _fmt_ts(created_at) if created_at else "—"
    params_txt = _fmt_ts(params_ts) if params_ts else "—"

    def _stage_card(k: str, payload: Dict[str, Any]) -> str:
        st = (payload or {}).get("status", "degraded")
        chip = _chip(st)
        # Wybór kilku metryk do pokazania, jeśli istnieją
        keys_to_show = [
            "rows", "null_new_cols", "dup_ts", "neg_flow", "ch_cap_violation", "dis_cap_violation",
            "over_energy", "balance", "soc_bounds", "time", "grid", "flows", "soc_delta",
            "cycles_over", "cashflow_mismatch", "null_price_contrib_nonzero", "direction",
        ]
        items = []
        for key in keys_to_show:
            if key in (payload or {}):
                val = payload[key]
                val_txt = html.escape(_safe_json(val) if isinstance(val, (dict, list)) else str(val))
                items.append(f"<div style='margin:2px 0'><b>{html.escape(key)}:</b> {val_txt}</div>")
        extra = "".join(items) if items else "<div style='opacity:.7'>brak dodatkowych metryk</div>"
        return f"""
        <div class="card" style="min-width:320px; flex:1">
          <div class="card-title">Etap {html.escape(k)} {chip}</div>
          <div class="card-content">{extra}</div>
        </div>
        """

    stage_cards = "".join(_stage_card(k, v) for k, v in stages.items())

    top = f"""
    <div style="display:flex; gap:12px; align-items:center; margin-bottom:16px;">
      <h1 style="margin:0; font-size:20px;">Pipeline</h1>
      {_chip(status)}
      <div style="opacity:.8">calc_id: <code>{html.escape(calc_id)}</code></div>
    </div>
    <div style="display:flex; gap:16px; flex-wrap:wrap; margin-bottom:10px;">
      <div class="kpi"><div class="kpi-title">Snapshot utworzony</div><div class="kpi-value">{html.escape(created_txt)}</div></div>
      <div class="kpi"><div class="kpi-title">Params TS</div><div class="kpi-value">{html.escape(params_txt)}</div></div>
      <div class="kpi"><div class="kpi-title">Dane</div><div class="kpi-value">{"obecne" if present else "brak"}</div></div>
    </div>
    """

    summ = f"<div class='card'><div class='card-title'>Podsumowanie</div><div class='card-content'>{html.escape(summary) or '—'}</div></div>"

    return f"""
<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><title>Health — Pipeline</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
  body {{ margin:0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, "Helvetica Neue", Arial; background: #0f172a; color:#e5e7eb; }}
  a {{ color:#93c5fd; text-decoration:none; }}
  .wrap {{ max-width:1200px; margin:24px auto; padding:0 16px; }}
  .card {{ background:#0b1220; border:1px solid #1f2937; border-radius:12px; padding:14px; }}
  .card-title {{ font-weight:700; margin-bottom:6px; color:#93c5fd; }}
  .card-content {{ font-size:14px; }}
  .grid {{ display:flex; flex-wrap:wrap; gap:12px; }}
  .kpi {{ background:#0b1220; border:1px solid #1f2937; border-radius:12px; padding:12px 14px; min-width:220px; }}
  .chip {{ display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; }}
</style>
</head>
<body>
  <div class="wrap">
    {_nav("Pipeline")}
    {top}
    <div class="grid">{stage_cards or '<div class="card"><div class="card-content">Brak danych etapów.</div></div>'}</div>
    <div style="margin-top:12px;">{summ}</div>
  </div>
</body>
</html>
    """

# ─────────────────────────────────────────────────────────────────────────────
# HTML — zakładka Prep

def _prep_page(prep: Dict[str, Any]) -> str:
    st = prep.get("status", "degraded")
    inp = prep.get("input", {})
    prm = prep.get("params", {})

    stats = inp.get("stats", {})
    def _stat_row(name: str) -> str:
        s = stats.get(name, {}) or {}
        return f"<tr><td>{html.escape(name)}</td><td>{s.get('count','—')}</td><td>{html.escape(s.get('min_ts_utc','—') or '—')} → {html.escape(s.get('max_ts_utc','—') or '—')}</td></tr>"

    prices_key = next((k for k in stats.keys() if k.endswith("ceny_godzinowe") or k.endswith("prices")), "prices")
    stats_tbl = "".join([
        _stat_row("input.date_dim"),
        _stat_row("input.konsumpcja"),
        _stat_row("input.produkcja"),
        _stat_row(prices_key),
    ])

    def _kv_table(d: Dict[str, Any]) -> str:
        rows = []
        for k, v in (d or {}).items():
            txt = html.escape(_safe_json(v) if isinstance(v, (dict, list)) else str(v))
            rows.append(f"<tr><td>{html.escape(str(k))}</td><td>{txt}</td></tr>")
        return "".join(rows) or "<tr><td colspan='2' style='opacity:.7'>brak</td></tr>"

    return f"""
<!doctype html><html><head>
<meta charset="utf-8"><title>Health — Prep</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
 body {{ margin:0; font-family: ui-sans-serif, system-ui, Segoe UI, Roboto, Arial; background:#0f172a; color:#e5e7eb; }}
 .wrap {{ max-width:1200px; margin:24px auto; padding:0 16px; }}
 .panel {{ background:#0b1220; border:1px solid #1f2937; border-radius:12px; padding:14px; margin-bottom:12px; }}
 table {{ width:100%; border-collapse: collapse; }}
 td,th {{ border-top:1px solid #1e293b; padding:6px 8px; font-size:14px; vertical-align:top; }}
 th {{ text-align:left; color:#93c5fd; font-weight:700; }}
 .chip {{ display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; }}
</style>
</head>
<body>
<div class="wrap">
  {_nav("Prep")}
  <div style="display:flex; align-items:center; gap:10px; margin-bottom:10px">
    <h1 style="margin:0">Energy-Prep {_chip(st)}</h1>
    <div style="opacity:.8">ostatnie sprawdzenie: {html.escape(prep.get('created_at','—'))}</div>
  </div>

  <div class="panel">
    <h2 style="margin:0 0 8px 0">Input {_chip(inp.get('status','degraded'))}</h2>
    <table>
      <tr><th>tabela</th><th>liczba wierszy</th><th>zakres czasowy</th></tr>
      {stats_tbl}
    </table>
  </div>

  <div class="panel">
    <h2 style="margin:0 0 8px 0">Params {_chip(prm.get('status','degraded'))}</h2>
    <table>{_kv_table(prm.get('form_zmienne_latest') or prm)}</table>
  </div>

  <div class="panel">
    <h2 style="margin:0 0 8px 0">Werdykt</h2>
    <div>{html.escape(prep.get('summary') or '—')}</div>
  </div>
</div>
</body></html>
    """

# ─────────────────────────────────────────────────────────────────────────────
# Umożliwia: uvicorn energia_prep2.health_api:app
