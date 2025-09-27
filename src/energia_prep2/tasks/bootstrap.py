# src/energia_prep2/tasks/bootstrap.py
from __future__ import annotations

import atexit
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple

import psycopg
from psycopg import sql
from psycopg.errors import UndefinedTable, RaiseException

LOG = logging.getLogger("energia-prep-2")

def _ensure_logging():
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=os.getenv("LOG_LEVEL", "INFO"),
            format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        )
    LOG.setLevel(os.getenv("LOG_LEVEL", "INFO"))

_ensure_logging()

# ── ENV ───────────────────────────────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "energia")
DB_USER = os.getenv("DB_USER", "voytek")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SSLMODE = os.getenv("DB_SSLMODE", "prefer")
DB_SUPERUSER = os.getenv("DB_SUPERUSER", "postgres")
# <= TZ z env ma pierwszeństwo jako domyślna strefa DB
DB_TIMEZONE = os.getenv("DB_TIMEZONE") or os.getenv("TZ", "UTC")

SQL_DIR = Path(os.getenv("SQL_DIR", "/app/sql")).resolve()
FORM_CONFIG = os.getenv("FORM_CONFIG", "/app/config_form.yaml")
BAD_SUFFIXES = (".swp", ".swo", ".bak", ".tmp", "~")

# Auto-odpalanie widoków po zakończeniu procesu (po całym pipeline/ETL)
AUTO_RUN_VIEWS_AT_EXIT = os.getenv("AUTO_RUN_VIEWS_AT_EXIT", "0") in ("1", "true", "TRUE", "yes", "YES")

# Pliki wymagające superuser (event trigger, globalne GRANTy, itp.)
SUPERUSER_FILES = {
    "40_triggers/02_event_triggers.sql",
    "90_finalize/01_post_grants.sql",
}

# Kolejność ETAP 1 (bez widoków!)
ORDER_BOOTSTRAP = [
    "00_init/00_prechecks.sql",
    "00_init/01_util_schema.sql",
    "00_init/02_util_grants.sql",
    "10_schemas/01_bootstrap_schemas.sql",
    "20_tables/01_input_tables.sql",
    "20_tables/02_tge_tables.sql",
    "20_tables/03_params_tables.sql",
    "20_tables/04_support_tables.sql",
    # triggers (opcjonalnie – jeśli są)
    "40_triggers/01_row_triggers.sql",
    "40_triggers/02_event_triggers.sql",
    # grants/validate po etapie 1
    "90_finalize/01_post_grants.sql",
    "90_finalize/02_validate.sql",
]

# Kolejność ETAP 2 (widoki + finalize)
ORDER_VIEWS = [
    "30_views/00_output_energy_base.sql",
    "30_views/01_output_energy_broker_detail.sql",
    "30_views/02_output_energy_soc_oze.sql",
    "30_views/03_output_energy_soc_arbi.sql",
    "30_views/04_output_energy_store_summary.sql",
    # rozszerzenie o Twoje 05a/05b/05c/06:
    "30_views/05a_output_obliczenia_pln_mag_i_oze.sql",
    "30_views/05b_output_obliczenia_pln_oze.sql",
    "30_views/05c_output_obliczenia_pln_grid.sql",
    "30_views/06_output_obliczenia_pln_summary.sql",
    # finalize po widokach
    "90_finalize/01_post_grants.sql",
    "90_finalize/02_validate.sql",
]

# ── helpers ───────────────────────────────────────────────────────────────────
def _dsn(user: str | None = None) -> str:
    u = user or DB_USER
    return f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={u} password={DB_PASSWORD} sslmode={DB_SSLMODE}"

def _connect(user: str | None = None):
    conn = psycopg.connect(_dsn(user), autocommit=True, connect_timeout=8)
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SET TIME ZONE {}").format(sql.Literal(DB_TIMEZONE)))
    return conn

def _rel(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(SQL_DIR)).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")

def _is_view_path(path: Path) -> bool:
    r = _rel(path)
    return r.startswith("30_views/") or "/30_views/" in r

def _iter_exact_paths(order_list: list[str]) -> Iterable[Path]:
    for rel in order_list:
        p = (SQL_DIR / rel).resolve()
        if p.exists() and p.is_file() and not any(str(p).endswith(s) for s in BAD_SUFFIXES):
            yield p
        else:
            if not p.exists():
                LOG.warning("SQL pominięto — brak pliku: %s", p)
            elif not p.is_file():
                LOG.warning("SQL pominięto — nie plik: %s", p)
            else:
                LOG.warning("SQL pominięto — zły sufiks: %s", p)

def _banner(title: str):
    bar = "─" * 78
    LOG.info("[%s]\n%s\n%s\n%s", title, bar, title, bar)

def _log_env(stage: str):
    LOG.info(
        "ENV (%s): DB_HOST=%s DB_PORT=%s DB_NAME=%s DB_USER=%s DB_SUPERUSER=%s TZ=%s SQL_DIR=%s FORM_CONFIG=%s file=%s",
        stage, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_SUPERUSER, DB_TIMEZONE, str(SQL_DIR), FORM_CONFIG, __file__,
    )

def _scan_views_dir():
    vd = SQL_DIR / "30_views"
    if not vd.exists():
        LOG.warning("30_views: brak katalogu (%s)", vd)
        return
    files = sorted([p for p in vd.glob("*.sql") if p.is_file()])
    LOG.info("30_views: %d plików", len(files))
    names = {}
    for p in files:
        LOG.info("30_views file: %s", _rel(p))
        names[p.name] = names.get(p.name, 0) + 1
    dups = [n for n, c in names.items() if c > 1]
    if dups:
        LOG.warning("30_views: duplikaty nazw: %s", ", ".join(dups))

# ── drop views ────────────────────────────────────────────────────────────────
def _list_all_views(cur) -> List[Tuple[str, str, bool]]:
    q = """
    SELECT n.nspname, c.relname, c.relkind = 'm' AS is_mat
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('v','m')
      AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
    ORDER BY n.nspname, c.relname;
    """
    cur.execute(q)
    return [(r[0], r[1], r[2]) for r in cur.fetchall()]

def _drop_all_views():
    with _connect() as conn, conn.cursor() as cur:
        views = _list_all_views(cur)
        if not views:
            LOG.info("drop-all-views: brak widoków do zrzucenia")
            return
        LOG.info("drop-all-views: %d widoków/matview do usunięcia", len(views))
        for schema, name, is_mat in views:
            ident = f"{schema}.{name}"
            if is_mat:
                cur.execute(sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {}.{} CASCADE")
                            .format(sql.Identifier(schema), sql.Identifier(name)))
                LOG.info("drop-all-views: MATVIEW usunięty → %s", ident)
            else:
                cur.execute(sql.SQL("DROP VIEW IF EXISTS {}.{} CASCADE")
                            .format(sql.Identifier(schema), sql.Identifier(name)))
                LOG.info("drop-all-views: VIEW usunięty → %s", ident)

# ── seed GUC ──────────────────────────────────────────────────────────────────
def _read_config_json() -> dict:
    try:
        import yaml
        with open(FORM_CONFIG, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        LOG.warning("FORM_CONFIG nieczytelny (%s): %s", FORM_CONFIG, e)
        return {}

def _canon_val(v: str):
    s = str(v)
    val = s.split(";", 1)[0].strip() if ";" in s else s
    try:
        if isinstance(val, str) and val.lower() in ("true", "false"):
            return val.lower() == "true"
        if re.match(r"^-?\d+\.\d+$", val):
            return float(val)
        if re.match(r"^-?\d+$", val):
            return int(val)
        return val
    except Exception:
        return val

def _set_params_seed_gucs(cur):
    cfg = _read_config_json()
    sections = [
        "form_zmienne",
        "form_bess_param",
        "form_parametry_klienta",
        "form_oplaty_dystrybucyjne",
        "form_oplaty_dyst_sched",
        "form_oplaty_systemowe",
        "form_oplaty_sys_sched",
        "form_oplaty_sys_kparam",
        "form_par_kontraktu",
        "form_oplaty_fiskalne",
        "form_lcoe",
        "form_par_arbitrazu",
    ]
    for sec in sections:
        raw = cfg.get(sec) or {}
        data = {k: _canon_val(v) for k, v in raw.items() if isinstance(k, str)}
        js = json.dumps(data, ensure_ascii=False)
        guc = f"energia.{sec}_json"
        cur.execute(sql.SQL("SET SESSION {} = {}").format(sql.SQL(guc), sql.Literal(js)))
        LOG.info("params seed: %s → %d keys", sec, len(data))

# ── SQL executor ──────────────────────────────────────────────────────────────
@dataclass
class RunResult:
    path: Path
    rel: str
    superuser: bool
    seconds: float
    ok: bool

def _should_use_superuser(path: Path) -> bool:
    return _rel(path) in SUPERUSER_FILES

def _extract_error_context(sql_text: str, err: Exception) -> str:
    msg = str(err)
    m = re.search(r"LINE\s+(\d+):", msg)
    if not m:
        return "(brak kontekstu linii w błędzie)"
    ln = int(m.group(1))
    lines = sql_text.splitlines()
    start = max(0, ln - 8)
    end = min(len(lines), ln + 7)
    snippet = "\n".join(f"{i+1:5d}: {lines[i]}" for i in range(start, end))
    return f"\nSQL kontekst (linie {start+1}..{end}):\n{snippet}\n       ↑ błąd w okolicy tej linii"

def _run_sql_file(path: Path) -> RunResult:
    rel = _rel(path)
    sql_text = path.read_text(encoding="utf-8")
    superuser = _should_use_superuser(path)
    user = DB_SUPERUSER if superuser else DB_USER

    LOG.info("RUN  → %s | user=%s | bytes=%d", rel, user, len(sql_text))
    t0 = time.perf_counter()
    ok = False
    try:
        if path.name == "03_params_tables.sql":
            # ⬇️ TA SAMA SESJA: seed GUC + wykonanie SQL jednym cur
            with _connect(user) as conn, conn.cursor() as cur:
                _set_params_seed_gucs(cur)
                cur.execute(sql_text)
        else:
            with _connect(user) as conn, conn.cursor() as cur:
                cur.execute(sql_text)

        ok = True
        return RunResult(path, rel, superuser, time.perf_counter() - t0, ok=True)
    except Exception as e:
        dt = time.perf_counter() - t0
        LOG.error("FAIL → %s | %.3fs | %s: %s", rel, dt, e.__class__.__name__, e)
        LOG.error("%s", _extract_error_context(sql_text, e))
        if isinstance(e, UndefinedTable):
            LOG.error("Sugestia: wymagana relacja nie istnieje (kolejność?).")
        if isinstance(e, RaiseException):
            LOG.error("Sugestia: RAISE w SQL (kontrola zależności).")
        raise
    finally:
        if ok:
            LOG.info("OK   → %s | %.3fs%s", rel, time.perf_counter() - t0, " | superuser" if superuser else "")

# ── asercje ───────────────────────────────────────────────────────────────────
def _assert_relation_exists(schema: str, name: str, user: str | None = None):
    q = """
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = %s AND c.relname = %s
    LIMIT 1;
    """
    with _connect(user) as conn, conn.cursor() as cur:
        cur.execute(q, (schema, name))
        row = cur.fetchone()
    if not row:
        raise RuntimeError(f"Brak wymaganej relacji {schema}.{name}")

# ── PUBLIC API ────────────────────────────────────────────────────────────────
def run():
    """
    ETAP 1 (bootstrap): BEZ 30_views.
    """
    _banner("BOOTSTRAP START")
    _log_env("BOOTSTRAP")
    LOG.info("bootstrap: guard active (no 30_views in run) | file=%s", __file__)

    _scan_views_dir()

    # na starcie usuń ewentualne stare widoki (żeby nie blokowały DDL tabel)
    try:
        _drop_all_views()
    except Exception as e:
        LOG.warning("drop-all-views: warning: %s", e)

    # Fail-fast: ORDER_BOOTSTRAP nie może zawierać 30_views
    accidental_views = []
    for p in _iter_exact_paths(ORDER_BOOTSTRAP):
        if _is_view_path(p):
            accidental_views.append(_rel(p))
    if accidental_views:
        raise RuntimeError(
            "Guard/FAIL: ORDER_BOOTSTRAP zawiera pliki z 30_views (to błąd). "
            f"Usuń je z etapu run(). Lista: {accidental_views}"
        )

    # wykonanie etapu 1
    for f in _iter_exact_paths(ORDER_BOOTSTRAP):
        _run_sql_file(f)

    _banner("BOOTSTRAP DONE")

    # Rejestracja auto-startu widoków na koniec całego procesu (po ETL)
    if AUTO_RUN_VIEWS_AT_EXIT:
        LOG.info("AUTO_RUN_VIEWS_AT_EXIT=1 → zarejestrowano auto-start widoków przy atexit.")
        atexit.register(_views_atexit_runner)

def _views_atexit_runner():
    """
    Handler uruchamiany przy zakończeniu procesu (po całym pipeline/ETL).
    Odpala run_views_after_etl() i loguje ewentualne błędy z pełnym kontekstem.
    """
    try:
        _banner("ATEIXT: VIEWS START")
        run_views_after_etl()
        _banner("ATEIXT: VIEWS DONE")
    except Exception as e:
        LOG.error("ATEIXT: VIEWS FAIL — %s", e)
        # nie podnosimy wyjątku dalej, bo to atexit

def run_views_after_etl():
    """
    ETAP 2 (widoki): TYLKO 30_views (+ 90_finalize). Kolejność 00→01→…→06.
    Po 00_ sprawdzam istnienie output.energy_base (preflight).
    """
    _banner("VIEWS START")
    _log_env("VIEWS")
    _scan_views_dir()

    for f in _iter_exact_paths(ORDER_VIEWS):
        rel = _rel(f)
        if not (_is_view_path(f) or rel.startswith("90_finalize/")):
            LOG.warning("SKIP (views): %s nie jest w 30_views ani 90_finalize", rel)
            continue

        _run_sql_file(f)

        if rel.endswith("30_views/00_output_energy_base.sql"):
            _assert_relation_exists("output", "energy_base")
            LOG.info("Preflight OK: output.energy_base istnieje.")

    _banner("VIEWS DONE")
