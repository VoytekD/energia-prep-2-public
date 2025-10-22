#!/usr/bin/env bash
set -Eeuo pipefail

log()  { echo "[entrypoint] $*"; }
fail() { log "ERROR: $*"; exit 1; }

# ──────────────────────────────────────────────────────────────────────────────
# Banner (bez hasła)
# ──────────────────────────────────────────────────────────────────────────────
log "──────────────────────────────────────────────────────────────────────────────"
log "energia_prep2: START"
log "──────────────────────────────────────────────────────────────────────────────"
log "TZ            = ${TZ:-}"
log "LOG_LEVEL     = ${LOG_LEVEL:-}"
log "DB_HOST       = ${DB_HOST:-}"
log "DB_PORT       = ${DB_PORT:-}"
log "DB_NAME       = ${DB_NAME:-}"
log "DB_USER       = ${DB_USER:-}"
log "DB_ADMIN_DB   = ${DB_ADMIN_DB:-postgres}"
log "DB_SUPERUSER  = ${DB_SUPERUSER:-postgres}"
log "SQL_DIR       = ${SQL_DIR:-/app/sql}"
log "DATA_DIR      = ${DATA_DIR:-/app/data}"
log "FORM_CONFIG   = ${FORM_CONFIG:-/app/config_form.yaml}"
log "PYTHONPATH    = ${PYTHONPATH:-/app/src}"
log "──────────────────────────────────────────────────────────────────────────────"

# ──────────────────────────────────────────────────────────────────────────────
# Wymagane zmienne
# ──────────────────────────────────────────────────────────────────────────────
: "${DB_HOST:?Missing DB_HOST}"
: "${DB_PORT:?Missing DB_PORT}"
: "${DB_NAME:?Missing DB_NAME}"
: "${DB_USER:?Missing DB_USER}"
: "${DB_PASSWORD:?Missing DB_PASSWORD}"

# Mapowanie SSL mode (jeśli potrzebujesz SSL, ustaw DB_SSLMODE w .env; domyślnie 'prefer')
export DB_SSLMODE="${DB_SSLMODE:-prefer}"

# ──────────────────────────────────────────────────────────────────────────────
# DB probe (Python/psycopg — brak zależności od `psql`)
# ──────────────────────────────────────────────────────────────────────────────
log "DB probe: psycopg connect to ${DB_HOST}:${DB_PORT}/${DB_NAME} as ${DB_USER}"

python - <<'PY'
import os, sys, time
import psycopg

host = os.environ["DB_HOST"]
port = int(os.environ["DB_PORT"])
dbname = os.environ["DB_NAME"]
user = os.environ["DB_USER"]
password = os.environ["DB_PASSWORD"]
sslmode = os.environ.get("DB_SSLMODE", "prefer")

max_tries = int(os.environ.get("DB_MAX_TRIES", "30"))
sleep_secs = float(os.environ.get("DB_SLEEP_SECS", "1"))

dsn = f"host={host} port={port} dbname={dbname} user={user} password={password} sslmode={sslmode}"

first_error_printed = False
for attempt in range(1, max_tries + 1):
    try:
        with psycopg.connect(dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT now();")
                cur.fetchone()
        print("[entrypoint] DB probe: OK")
        sys.exit(0)
    except Exception as e:
        if not first_error_printed:
            # pokaż tylko pierwszy pełny komunikat błędu
            print(f"[entrypoint] DB probe error (attempt {attempt}/{max_tries}): {e}", file=sys.stderr)
            first_error_printed = True
        if attempt == max_tries:
            print(f"[entrypoint] ERROR: DB probe FAILED after {max_tries} attempts "
                  f"(host={host} port={port} db={dbname} user={user}). Sprawdź dostęp/hasło/pg_hba/SSL.",
                  file=sys.stderr)
            sys.exit(2)
        print(f"[entrypoint] DB not ready yet (attempt {attempt}/{max_tries})… retrying in {sleep_secs}s")
        time.sleep(sleep_secs)
PY

# ──────────────────────────────────────────────────────────────────────────────
# Pipeline
# ──────────────────────────────────────────────────────────────────────────────
log "──────────────────────────────────────────────────────────────────────────────"
log "Uruchamiam pipeline: python -m energia_prep2.main"
set +e
python -m energia_prep2.main
PIPELINE_RC=$?
set -e

if [ $PIPELINE_RC -ne 0 ]; then
  log "pipeline: FAILED — pomijam start watchera i crona oraz API"
  # Jeśli wolisz twardo ubijać kontener po failu:
  # exit $PIPELINE_RC
  # Zostaw kontener aktywny do debugowania logów:
  tail -f /dev/null
  exit 0
fi

log "pipeline: OK"

# ──────────────────────────────────────────────────────────────────────────────
# CSV watcher + Scheduler (tylko po sukcesie pipeline)
# ──────────────────────────────────────────────────────────────────────────────
log "CSV watcher: start (energia_prep2.tasks.csv_import --watch)"
if command -v setsid >/dev/null 2>&1; then
  setsid python -m energia_prep2.tasks.csv_import --watch >/dev/null 2>&1 &
else
  nohup python -m energia_prep2.tasks.csv_import --watch >/dev/null 2>&1 &
fi

if [ "${ENABLE_CRON:-1}" != "0" ]; then
  log "Scheduler: uruchamiam cron (fetch TGE co godzinę)"
  # ...tu ewentualne uruchomienie crona...
else
  log "Scheduler: pominięty (ENABLE_CRON=0)"
fi

# ──────────────────────────────────────────────────────────────────────────────
# Start API (foreground)
# ──────────────────────────────────────────────────────────────────────────────
log "──────────────────────────────────────────────────────────────────────────────"
log "Startuję API (uvicorn) w przodzie"
log "──────────────────────────────────────────────────────────────────────────────"
exec uvicorn energia_prep2.app_server:app --host 0.0.0.0 --port 8003

