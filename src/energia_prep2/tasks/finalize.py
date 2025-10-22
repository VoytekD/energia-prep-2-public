from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
import json

from ..db import get_conn_app
from ..log import log

# ─────────────────────────────────────────────────────────────────────────────
# Konfiguracja (dopasuj, jeśli masz inne tabele wejściowe/parametry)

INPUT_TABLES: List[Tuple[str, str]] = [
    ("input", "date_dim"),
    ("input", "konsumpcja"),
    ("input", "produkcja"),
]

PRICES_CANDIDATES: List[Tuple[str, str]] = [
    ("input", "ceny_godzinowe"),  # docelowo
    ("tge", "prices"),            # legacy (jeśli istnieje)
]

PARAM_FORMS: Dict[str, List[str]] = {
    "params.form_zmienne":      ["emax", "moc_pv_pp", "moc_pv_wz", "moc_wiatr", "procent_arbitrazu"],
    "params.form_bess_param":   ["p_ch_max_mw", "p_dis_max_mw", "eff_ch", "eff_dis", "bess_lambda_month"],
    "params.form_par_arbitrazu":["soc_low_threshold", "soc_high_threshold", "cycles_per_day", "arbi_dis_to_load"],
}

EPS = 1e-9

# ─────────────────────────────────────────────────────────────────────────────
# Modele i utilsy

@dataclass
class Check:
    ok: bool
    detail: str

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _exists_table(cur, schema: str, table: str) -> bool:
    cur.execute("""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema=%s AND table_name=%s
        LIMIT 1
    """, (schema, table))
    return cur.fetchone() is not None

def _detect_prices_table(cur) -> Optional[str]:
    for s, t in PRICES_CANDIDATES:
        if _exists_table(cur, s, t):
            return f"{s}.{t}"
    return None

def _table_stats(cur, schema: str, table: str) -> Dict[str, Any]:
    cur.execute(f"SELECT MIN(ts_utc), MAX(ts_utc), COUNT(*) FROM {schema}.{table}")
    min_ts, max_ts, cnt = cur.fetchone()
    age = None
    if max_ts:
        age = (datetime.now(timezone.utc) - max_ts).total_seconds()
    return {
        "count": int(cnt or 0),
        "min_ts_utc": min_ts.replace(microsecond=0).isoformat() if min_ts else None,
        "max_ts_utc": max_ts.replace(microsecond=0).isoformat() if max_ts else None,
        "age_seconds": age,
    }

def _dup_count(cur, full_table: str) -> Optional[int]:
    s, t = full_table.split(".", 1)
    if not _exists_table(cur, s, t):
        return None
    cur.execute(f"""
        SELECT COUNT(*) FROM (
          SELECT ts_utc, COUNT(*) c
          FROM {s}.{t}
          GROUP BY ts_utc
          HAVING COUNT(*)>1
        ) d
    """)
    return int(cur.fetchone()[0] or 0)

def _ensure_health_service(cur) -> None:
    cur.execute("""
    CREATE SCHEMA IF NOT EXISTS output;
    CREATE TABLE IF NOT EXISTS output.health_service (
      id              bigserial PRIMARY KEY,
      scope           text NOT NULL CHECK (scope IN ('prep','calc')),
      calc_id         uuid NULL,
      params_ts       timestamptz NULL,
      overall_status  text NOT NULL CHECK (overall_status IN ('up','degraded','down')),
      sections        jsonb NOT NULL,
      summary         text NULL,
      created_at      timestamptz NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_health_service_scope_created
      ON output.health_service (scope, created_at DESC);
    """)

def _merge_status(*statuses: str) -> str:
    if any(s == "down" for s in statuses):
        return "down"
    if any(s == "degraded" for s in statuses):
        return "degraded"
    return "up"

# ─────────────────────────────────────────────────────────────────────────────
# Główna logika PREP snapshot

def _build_prep_sections(cur) -> Dict[str, Any]:
    # Legacy „smoke” (zachowujemy dotychczasowe checki finalize)
    legacy_checks: List[Check] = []

    # 1) date_dim > 0
    cur.execute("SELECT COUNT(*) FROM input.date_dim")
    n_date_dim = int(cur.fetchone()[0] or 0)
    legacy_checks.append(Check(ok=(n_date_dim > 0), detail=f"input.date_dim count={n_date_dim}"))

    # 2) tge.prices istnieje (legacy; dopuszczamy też input.ceny_godzinowe)
    cur.execute("""
        SELECT EXISTS (
          SELECT 1 FROM information_schema.tables
          WHERE table_schema='tge' AND table_name='prices'
        )
    """)
    exists_tge = bool(cur.fetchone()[0])
    legacy_checks.append(Check(ok=exists_tge, detail="tge.prices exists (legacy)"))

    # 3) widok output.vw_energy_calc_input (jeśli wciąż używany)
    cur.execute("""
        SELECT EXISTS (
          SELECT 1 FROM information_schema.views
          WHERE table_schema='output' AND table_name='vw_energy_calc_input'
        )
    """)
    exists_vw = bool(cur.fetchone()[0])
    legacy_checks.append(Check(ok=exists_vw, detail="output.vw_energy_calc_input exists (legacy)"))

    # Sekcja INPUT
    stats: Dict[str, Any] = {}
    issues: List[str] = []
    for s, t in INPUT_TABLES:
        if not _exists_table(cur, s, t):
            issues.append(f"missing table {s}.{t}")
            stats[f"{s}.{t}"] = {"error": "table not found", "count": 0}
        else:
            stats[f"{s}.{t}"] = _table_stats(cur, s, t)

    prices_tbl = _detect_prices_table(cur)
    if not prices_tbl:
        issues.append("prices table not found")
        stats["prices"] = {"error": "prices table not found", "count": 0}
        ceny_bad_flags = None
    else:
        s, t = prices_tbl.split(".", 1)
        stats[prices_tbl] = _table_stats(cur, s, t)
        cur.execute(f"""
            SELECT COUNT(*) FROM {prices_tbl}
            WHERE price_pln_mwh::text IN ('NaN','Infinity','-Infinity')
        """)
        ceny_bad_flags = int(cur.fetchone()[0] or 0)

    dups = {
        "input.konsumpcja": _dup_count(cur, "input.konsumpcja"),
        "input.produkcja": _dup_count(cur, "input.produkcja"),
    }
    if prices_tbl:
        dups[prices_tbl] = _dup_count(cur, prices_tbl)

    hard_errors = []
    for key, st in stats.items():
        if st.get("count", 0) == 0 or st.get("error"):
            hard_errors.append(key)
    bad_dups = any((v or 0) > 0 for v in dups.values() if v is not None)
    bad_nan = (ceny_bad_flags or 0) > 0 if ceny_bad_flags is not None else False

    input_status = "up"
    if hard_errors or bad_dups or bad_nan or issues:
        input_status = "degraded"

    # Sekcja PARAMS – ostatnie rekordy i zakresy
    forms_detail: Dict[str, Any] = {}
    forms_status = "up"
    for tbl, cols in PARAM_FORMS.items():
        schema, table = tbl.split(".", 1)
        present = _exists_table(cur, schema, table)
        missing_cols: List[str] = []
        range_issues: List[str] = []
        row_out: Dict[str, Any] = {}
        if present:
            cur.execute(f"SELECT * FROM {tbl} ORDER BY updated_at DESC NULLS LAST, inserted_at DESC NULLS LAST LIMIT 1")
            row = cur.fetchone()
            if row is None:
                present = False
            else:
                # row jako dict
                cols_names = [desc.name for desc in cur.description]
                row_out = dict(zip(cols_names, row))
                # brakujące kolumny
                for c in cols:
                    if row_out.get(c) is None:
                        missing_cols.append(c)
                # zakresy podst.
                def _v(x): return row_out.get(x)
                if _v("emax") is not None and (_v("emax") <= 0): range_issues.append("emax<=0")
                if _v("eff_ch") is not None and not (0 < _v("eff_ch") <= 1): range_issues.append("eff_ch∉(0,1]")
                if _v("eff_dis") is not None and not (0 < _v("eff_dis") <= 1): range_issues.append("eff_dis∉(0,1]")
                if _v("soc_low_threshold") is not None and _v("soc_high_threshold") is not None:
                    lo, hi = _v("soc_low_threshold"), _v("soc_high_threshold")
                    if not (0 <= lo <= 1 and 0 <= hi <= 1 and lo < hi):
                        range_issues.append("SOC thresholds invalid")
        st = "up" if present and not missing_cols and not range_issues else "degraded"
        if st != "up":
            forms_status = "degraded"
        forms_detail[tbl] = {
            "status": st,
            "present": present,
            "missing_cols": missing_cols,
            "range_issues": range_issues,
            "latest_row": row_out if present else None,
        }

    # Sekcja LEGACY (stare checki finalize)
    legacy = [{"ok": c.ok, "detail": c.detail} for c in legacy_checks]
    legacy_status = "up" if all(c["ok"] for c in legacy) else "degraded"

    overall = _merge_status(input_status, forms_status, legacy_status)

    sections = {
        "input": {
            "status": input_status,
            "stats": stats,
            "dups": dups,
            "ceny_bad_flags": ceny_bad_flags,
            "issues": issues,
            "prices_table": prices_tbl or None,
        },
        "params": {
            "status": forms_status,
            "forms": forms_detail,
        },
        "legacy": {
            "status": legacy_status,
            "checks": legacy,
        },
        "meta": {
            "generated_at": _utcnow_iso(),
        }
    }
    summary = (
        f"Input={input_status}, Params={forms_status}, Legacy={legacy_status}. "
        f"ceny_bad_flags={ceny_bad_flags}, dups={{{k:v for k,v in dups.items()}}}"
    )
    return overall, sections, summary

# ─────────────────────────────────────────────────────────────────────────────
# Public API

def run() -> None:
    """
    Walidacja PREP po imporcie + zapis snapshotu do output.health_service (scope='prep').
    Zachowuje stare logi finalize (OK/ERR) i podnosi wyjątek, gdy którykolwiek check jest nie-OK.
    """
    checks: list[Check] = []

    with get_conn_app() as conn, conn.cursor() as cur:
        # ZACHOWANIE STARYCH LOGÓW (jak w oryginale)
        cur.execute("SELECT count(*) FROM input.date_dim")
        n = int(cur.fetchone()[0] or 0)
        checks.append(Check(ok=(n > 0), detail=f"input.date_dim count={n}"))

        cur.execute("""
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables WHERE table_schema='tge' AND table_name='prices'
            )
        """)
        exists = bool(cur.fetchone()[0])
        checks.append(Check(ok=exists, detail="tge.prices exists"))

        cur.execute("""
            SELECT EXISTS (
              SELECT 1 FROM information_schema.views WHERE table_schema='output' AND table_name='vw_energy_calc_input'
            )
        """)
        exists_v = bool(cur.fetchone()[0])
        checks.append(Check(ok=exists_v, detail="output.vw_energy_calc_input exists"))

        # NOWE: zbuduj sekcje PREP i zapisz snapshot do output.health_service
        _ensure_health_service(cur)
        overall, sections, summary = _build_prep_sections(cur)
        cur.execute("""
          INSERT INTO output.health_service (scope, overall_status, sections, summary)
          VALUES ('prep', %s, %s::jsonb, %s)
        """, (overall, json.dumps(sections, ensure_ascii=False), summary))

    any_fail = False
    for c in checks:
        level = "OK" if c.ok else "ERR"
        log.info("finalize: [%s] %s", level, c.detail)
        if not c.ok:
            any_fail = True

    log.info("finalize: PREP snapshot written to output.health_service (scope='prep')")
    if any_fail:
        raise RuntimeError("finalize: walidacja nie przeszła — sprawdź logi powyżej.")
    log.info("finalize: ALL GREEN")
