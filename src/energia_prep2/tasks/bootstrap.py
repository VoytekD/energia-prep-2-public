# src/energia_prep2/tasks/bootstrap.py
from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

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

# Pliki wymagające superuser (event trigger, globalne GRANTy, itp.)
SUPERUSER_FILES = {
    "40_triggers/02_event_triggers.sql",
    "90_finalize/01_post_grants.sql",
}

# Kolejność BOOTSTRAP (bazowa)
ORDER_BOOTSTRAP = [
    "00_init/00_prechecks.sql",
    "00_init/01_util_schema.sql",
    "00_init/02_util_grants.sql",
    "10_schemas/01_bootstrap_schemas.sql",
    "20_tables/01_input_tables.sql",
    "20_tables/02_tge_tables.sql",
    "20_tables/03_params_tables.sql",
    "20_tables/04_support_tables.sql",
    "25_functions/01_run_after_wait.sql",
    "40_triggers/01_notify_autotriggers.sql",
    "90_finalize/01_post_grants.sql",
]
# Pliki „na koniec” — uruchamiane po ETL (po zasileniu input.*)
ORDER_BOOTSTRAP_END = [
    "90_finalize/02_validate.sql"
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

def _list_sql_in_dir(rel_dir: str) -> list[str]:
    """
    Zwraca posortowaną listę ścieżek (relatywnych do SQL_DIR) dla wszystkich plików .sql
    w katalogu rel_dir (bez rekurencji).
    """
    d = (SQL_DIR / rel_dir).resolve()
    if not d.exists() or not d.is_dir():
        LOG.warning("Katalog SQL pominięto (brak/nie katalog): %s", d)
        return []
    files = []
    for p in sorted(d.iterdir()):
        if p.is_file() and p.suffix.lower() == ".sql" and not any(str(p).endswith(s) for s in BAD_SUFFIXES):
            files.append(str(p.resolve().relative_to(SQL_DIR)).replace("\\", "/"))
    if not files:
        LOG.info("Katalog SQL pusty: %s", d)
    return files

def _banner(title: str):
    bar = "─" * 78
    LOG.info("[%s]\n%s\n%s\n%s", title, bar, title, bar)

def _log_env(stage: str):
    LOG.info(
        "ENV (%s): DB_HOST=%s DB_PORT=%s DB_NAME=%s DB_USER=%s DB_SUPERUSER=%s TZ=%s SQL_DIR=%s FORM_CONFIG=%s file=%s",
        stage, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_SUPERUSER, DB_TIMEZONE, str(SQL_DIR), FORM_CONFIG, __file__,
    )

# ── seed GUC (dla params.*) ───────────────────────────────────────────────────

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
        # NIE przerabiamy list/dict przez _canon_val — zostają natywne typy
        data = {
            k: (_canon_val(v) if not isinstance(v, (list, tuple, dict)) else v)
            for k, v in raw.items() if isinstance(k, str)
        }
        seed_text = json.dumps(data, ensure_ascii=False)
        guc = f"energia.{sec}_json"
        # set_config(guc, <czysty JSON>, false)
        cur.execute("SELECT set_config(%s, %s, false)", (guc, seed_text))
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
    _banner("BOOTSTRAP START")
    _log_env("BOOTSTRAP")
    for f in _iter_exact_paths(ORDER_BOOTSTRAP):
        _run_sql_file(f)

    _banner("BOOTSTRAP DONE")

def run_end():
    """Runner plików post-ETL (utrzymujemy jeden standard wykonywania jak dla BOOTSTRAP)."""
    if not ORDER_BOOTSTRAP_END:
        LOG.info("BOOTSTRAP_END: brak plików do uruchomienia.")
        return
    _banner("BOOTSTRAP END (POST-ETL)")
    _log_env("BOOTSTRAP_END")
    for f in _iter_exact_paths(ORDER_BOOTSTRAP_END):
        _run_sql_file(f)
    _banner("BOOTSTRAP END DONE")
