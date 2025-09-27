# src/energia_prep2/health_api.py
from __future__ import annotations

import html
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg
from psycopg import sql
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from psycopg.rows import dict_row

# ─────────────────────────────────────────────────────────────────────────────
# Konfiguracja/ENV
APP_NAME = os.getenv("APP_NAME", "energia-prep-2")
APP_VERSION = os.getenv("APP_VERSION", "1.2.0")
PY_VERSION = os.getenv("PY_VERSION", "")  # opcjonalnie przekazywane z entrypointa

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "energia")
DB_USER = os.getenv("DB_USER", "voytek")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SSLMODE = os.getenv("DB_SSLMODE", "prefer")
DB_TIMEZONE = os.getenv("DB_TIMEZONE", "UTC")  # spójne UTC (odporne na DST)

# Kluczowe tabele/widoki
INPUT_TABLES = [
    ("input", "date_dim"),
    ("input", "konsumpcja"),
    ("input", "produkcja"),
]
PRICES_CANDIDATES = [
    ("input", "ceny_godzinowe"),  # docelowo tu jest TGE
    ("tge", "prices"),            # legacy – jeśli kiedyś było
]
VIEWS = [
    ("output", "full"),
    ("output", "delta_brutto"),
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
    # wymuś UTC na sesji (prezentacja timestamptz) — bez placeholderów
    with conn.cursor() as cur:
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
            """,
            (schema, table),
        )
    )

def _exists_view(conn: psycopg.Connection, schema: str, view: str) -> bool:
    return bool(
        _query_one(
            conn,
            """
            SELECT 1
            FROM information_schema.views
            WHERE table_schema=%s AND table_name=%s
            """,
            (schema, view),
        )
    )

def _humanize_seconds(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    seconds = int(round(seconds))
    negative = seconds < 0
    seconds = abs(seconds)
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m, s = divmod(rem, 60)
    out = []
    if d: out.append(f"{d}d")
    if d or h: out.append(f"{h}h")
    if d or h or m: out.append(f"{m}m")
    out.append(f"{s:02d}s")
    text = " ".join(out)
    return f"-{text}" if negative else text

def _fmt_ts(ts: Optional[datetime]) -> str:
    if not ts:
        return "—"
    # ISO w UTC, skrócone bez mikrosekund
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
# Grupy

def _group_runtime() -> Dict[str, Any]:
    return {
        "status": "up",
        "app": f"{APP_NAME} {APP_VERSION}",
        "python_psycopg": f"{os.getenv('PYTHON_VERSION', '')} / {os.getenv('PSYCOPG_VERSION', '')}".strip(" /"),
        "db_dsn": f"{DB_HOST}:{DB_PORT} / {DB_NAME} / {DB_USER} (prefer)",
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

def _detect_prices_table(conn: psycopg.Connection) -> Optional[str]:
    for sch, tab in PRICES_CANDIDATES:
        if _exists_table(conn, sch, tab):
            return f"{sch}.{tab}"
    return None

def _group_schema(conn: psycopg.Connection) -> Dict[str, Any]:
    missing_schemas: List[str] = []
    missing_tables: List[str] = []
    missing_views: List[str] = []

    # sprawdzamy INPUT
    for sch, tab in INPUT_TABLES:
        if not _exists_table(conn, sch, tab):
            missing_tables.append(f"{sch}.{tab}")

    # prices
    detected_prices = _detect_prices_table(conn)
    if not detected_prices:
        missing_tables.append("(prices) input.ceny_godzinowe")

    # widoki
    for sch, v in VIEWS:
        if not _exists_view(conn, sch, v):
            missing_views.append(f"{sch}.{v}")

    status = "up" if not (missing_schemas or missing_tables or missing_views) else "degraded"
    return {
        "status": status,
        "missing": {
            "schemas": missing_schemas,
            "tables": missing_tables,
            "views": missing_views,
        },
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

    # status = up jeśli brak błędów i wszystkie mają count>0
    degraded = any(("error" in v) or (v.get("count", 0) == 0) for v in stats.values())
    status = "degraded" if degraded else "up"

    # forms: najnowszy rekord form_zmienne
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

    return {
        "status": status,
        "stats": stats,
        "form_zmienne_latest": form_latest,
        "checked_at": _utcnow_iso(),
    }

def _group_views(conn: psycopg.Connection) -> Dict[str, Any]:
    samples: Dict[str, Any] = {}
    for sch, v in VIEWS:
        name = f"{sch}.{v}"
        try:
            rows = _query_dicts(conn, f"SELECT * FROM {sch}.{v} ORDER BY 1 DESC LIMIT 3")
            # ucywilizuj datetime -> iso
            for r in rows:
                for k, val in list(r.items()):
                    if isinstance(val, datetime):
                        r[k] = _fmt_ts(val)
            samples[name] = rows
        except Exception as e:
            samples[name] = [{"error": str(e)}]
    status = "up"
    return {"status": status, "samples": samples, "checked_at": _utcnow_iso()}

# ─────────────────────────────────────────────────────────────────────────────
# Obliczenie całości

def _compute() -> (Dict[str, Any], str):
    runtime = _group_runtime()
    db = _group_db()
    try:
        with _connect() as conn:
            schema = _group_schema(conn)
            data = _group_data(conn)
            views = _group_views(conn)
    except Exception as e:
        schema = {"status": "down", "error": str(e)}
        data = {"status": "down", "error": str(e)}
        views = {"status": "down", "error": str(e)}

    # „Forms” jako osobny wskaźnik (na podstawie _group_data)
    forms_status = "up" if data.get("form_zmienne_latest", {}).get("present") else "degraded"
    forms = {"status": forms_status, "info": data.get("form_zmienne_latest", {})}

    groups = {
        "runtime": runtime,
        "database": db,
        "schema": schema,
        "data": data,
        "views": views,
        "forms": forms,
    }
    overall = _overall(db["status"], schema["status"], data["status"], views["status"], forms["status"])
    return groups, overall

# ─────────────────────────────────────────────────────────────────────────────
# FastAPI

app = FastAPI(title=f"{APP_NAME} — Health", version=APP_VERSION)

@app.get("/status.json", response_class=JSONResponse)
def health_json():
    groups, overall = _compute()
    return {
        "status": overall,
        "checked_at": _utcnow_iso(),
        "groups": groups,
    }

@app.get("/", response_class=HTMLResponse)
def health_html():
    groups, overall = _compute()
    return _page(groups, overall)

# ─────────────────────────────────────────────────────────────────────────────
# HTML (KPI + tabele)

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

def _page(groups: Dict[str, Any], overall: str) -> str:
    now_txt = _fmt_ts(datetime.fromisoformat(groups["runtime"]["checked_at"]))
    # KPI – z danych
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
             f"ostatni rekord: {ceny.get('max_ts_utc') or '—'}  •  wiek: {ceny.get('age_human','—')}"),
        _kpi("Konsumpcja – rekordów", _fmt_count(kons.get("count", 0)),
             f"zakres: {kons.get('min_ts_utc','—')} → {kons.get('max_ts_utc','—')}"),
        _kpi("Produkcja – rekordów", _fmt_count(prod.get("count", 0)),
             f"zakres: {prod.get('min_ts_utc','—')} → {prod.get('max_ts_utc','—')}"),
        _kpi("Formularz (params.form_zmienne)", forms_present,
             f"inserted_at: {forms_info.get('inserted_at','—')}"),
    ])

    # Tabele – Runtime
    runtime = groups["runtime"]
    runtime_table = "".join([
        _row(["App", f"{html.escape(runtime.get('app',''))} {_chip(runtime.get('status','up'))}"]),
        _row(["Python / Psycopg", html.escape(runtime.get("python_psycopg",""))]),
        _row(["DB", html.escape(runtime.get("db_dsn",""))]),
    ])

    # DB
    db = groups["database"]
    db_table = "".join([
        _row(["now_db", html.escape(db.get("now_db","—"))]),
        _row(["connect_time", f"{html.escape(db.get('duration_human','—'))} ({db.get('duration_s','—')}s)"]),
        _row(["error", html.escape(db.get("error") or "—")]),
    ])

    # Schema
    schema = groups["schema"]
    missing = schema.get("missing", {})
    detected_prices = schema.get("detected_prices_table", "—")
    schema_rows = []
    schema_rows.append(_row(["detected prices table", html.escape(str(detected_prices))]))
    for mtype in ("schemas", "tables", "views"):
        items = missing.get(mtype, [])
        schema_rows.append(_row([mtype, html.escape(", ".join(items) if items else "—")]))
    schema_table = "".join(schema_rows)

    # Data (zbiorczo)
    def _stats_table(title: str, s: Dict[str, Any]) -> str:
        return "".join([
            _row(["table", html.escape(title)]),
            _row(["count", _fmt_count(s.get("count", 0))]),
            _row(["min_ts", html.escape(s.get("min_ts_utc", "—") or "—")]),
            _row(["max_ts", html.escape(s.get("max_ts_utc", "—") or "—")]),
            _row(["age", html.escape(s.get("age_human", "—"))]),
        ])

    dd_table = _stats_table("input.date_dim", dd or {})
    kons_table = _stats_table("input.konsumpcja", kons or {})
    prod_table = _stats_table("input.produkcja", prod or {})
    ceny_table = _stats_table(prices_key or "prices", ceny or {})

    # Views (tylko próbki)
    samples = groups["views"].get("samples", {})
    sample_rows = []
    for vname, rows in samples.items():
        preview = _safe_json(rows)[:350]
        sample_rows.append(_row([html.escape(vname), html.escape(preview + ("…" if len(preview) == 350 else ""))]))
    views_table = "".join(sample_rows) or _row(["—", "—"])

    # Forms panel
    forms = groups.get("forms", {"status": "degraded", "info": {}})
    forms_info2 = forms.get("info", {})
    forms_payload_preview = _safe_json(forms_info2.get("payload") or {})[:350]
    forms_table = "".join([
        _row(["present", "TAK" if forms_info2.get("present") else "NIE"]),
        _row(["inserted_at", html.escape(forms_info2.get("inserted_at") or "—")]),
        _row(["payload (preview)", html.escape(forms_payload_preview + ("…" if len(forms_payload_preview) == 350 else ""))]),
    ])

    # CSS/HTML
    return f"""
<!doctype html>
<html lang="pl">
<head>
<meta charset="utf-8"/>
<title>Status systemu — {html.escape(APP_NAME)}</title>
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
    background: linear-gradient(180deg, #0b1220, #0a1020);
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
</style>
</head>
<body>
  <div class="wrap">
    <h1>Status systemu — {html.escape(APP_NAME)}</h1>
    <div class="subtitle">Sprawdzone: {now_txt}</div>

    <div class="kpis">
      {kpi_html}
    </div>

    <div class="grid">
      <div class="panel">
        <h2>Runtime {_chip(runtime.get('status','up'))}</h2>
        <table>{runtime_table}</table>
      </div>

      <div class="panel">
        <h2>Database {_chip(db.get('status','down'))}</h2>
        <table>{db_table}</table>
      </div>

      <div class="panel">
        <h2>Schema {_chip(schema.get('status','down'))}</h2>
        <table>{schema_table}</table>
      </div>

      <div class="panel">
        <h2>Forms {_chip(forms.get('status','degraded'))}</h2>
        <table>{forms_table}</table>
      </div>

      <div class="panel">
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

      <div class="panel" style="grid-column: 1 / -1;">
        <h2>Views {_chip(groups['views'].get('status','down'))}</h2>
        <table>
          <tr><td>view</td><td>sample (truncated)</td></tr>
          {views_table}
        </table>
      </div>
    </div>
  </div>
</body>
</html>
    """

# Umożliwia: uvicorn energia_prep2.health_api:app
