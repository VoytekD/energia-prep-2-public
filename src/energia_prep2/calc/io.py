"""
calc/io.py — I/O snapshotów parametrów i konsolidacja do params.__consolidated__
Styl jak w oryginale (f-stringi, Identifier.as_string), ale bez aliasów i fallbacków.
Brak pola → twardy RuntimeError. Procenty walidowane (0..1), listy bonusów mogą być puste.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple, Mapping

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Json

log = logging.getLogger("calc")
log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

SNAPSHOT_TABLE = os.getenv("SNAPSHOT_TABLE", "output.params_snapshot_calc")
_SCHEMA, _TABLE = SNAPSHOT_TABLE.split(".", 1)

STAGE_TABLE = "output.calc_job_stage"
JOB_QUEUE_TABLE = os.getenv("TABLE_JOBS", "output.calc_job_queue")

# ──────────────────────────────────────────────────────────────────────────────
# Helpers — oryginalna składnia + twarde wymagania
# ──────────────────────────────────────────────────────────────────────────────

_PERCENT_NAME_HINTS = re.compile(
    r"(percent|pct|_pct$|_percent$|procent|eta|eff|sprawn|lambda|lam\b)",
    re.I,
)

def _as_num(v, default=None):
    try:
        return float(v)
    except Exception:
        return default

def _as_bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.strip().lower() in {"1", "t", "true", "y", "yes"}
    return False

def _as_frac(name: str, v, default=None):
    """
    Jak w oryginale: auto-normalizacja procentów do ułamka, przycięcie do [0,1].
    """
    x = _as_num(v, default)
    if x is None:
        return None
    name_l = (name or "").lower()
    looks_like_percent = (
        (x > 1 and x <= 100)
        and (_PERCENT_NAME_HINTS.search(name_l) or name_l in {"bess_lambda_month", "procent_arbitrazu", "bess_soc_start",
                                                              "soc_low_threshold", "soc_high_threshold"})
    )
    if looks_like_percent:
        x = x / 100.0
    if x < 0.0:
        x = 0.0
    if x > 1.0:
        x = 1.0
    return x

def _lambda_month_to_hour(lam_month_frac: float | None, hours_in_month: float = 730.0):
    if lam_month_frac is None:
        return None
    try:
        lam = float(lam_month_frac)
    except Exception:
        return None
    if lam <= 0.0:
        return 0.0
    if lam >= 1.0:
        return 1.0
    return 1.0 - (1.0 - lam) ** (1.0 / float(hours_in_month or 730.0))

# Twarde wersje (brak → błąd)
def _req_num(name: str, v) -> float:
    x = _as_num(v, None)
    if x is None:
        raise RuntimeError(f"[IO] Wymagane pole numeryczne '{name}' w formularzach params.form_* — brak lub zły typ.")
    return float(x)

def _req_frac(name: str, v) -> float:
    x = _as_frac(name, v, None)
    if x is None:
        raise RuntimeError(f"[IO] Wymagane pole procent/frac '{name}' w formularzach params.form_* — brak lub zły typ.")
    # _as_frac gwarantuje [0,1]
    return float(x)

def _req_bool(name: str, v) -> bool:
    if v is None:
        raise RuntimeError(f"[IO] Wymagane pole bool '{name}' w formularzach params.form_* — brak.")
    return _as_bool(v)

def _req_str(name: str, v) -> str:
    s = (v or "").strip() if isinstance(v, str) else ""
    if not s:
        raise RuntimeError(f"[IO] Wymagane pole tekstowe '{name}' — nie może być puste.")
    return s

def _int_list_allow_empty(name: str, v) -> List[int]:
    if v is None:
        return []
    if not isinstance(v, (list, tuple)):
        raise RuntimeError(f"[IO] '{name}' musi być listą godzin (int).")
    out: List[int] = []
    for i, it in enumerate(v):
        try:
            out.append(int(it))
        except Exception:
            raise RuntimeError(f"[IO] '{name}[{i}]' nie jest liczbą całkowitą (godzina).")
    return out

# ──────────────────────────────────────────────────────────────────────────────
# Struktury statusowe / kolejka
# ──────────────────────────────────────────────────────────────────────────────

async def ensure_calc_status_structures(con: psycopg.AsyncConnection) -> None:
    """
    Struktura statusów etapów przeliczeń.
    """
    stage_qualified = sql.Identifier(*STAGE_TABLE.split(".", 1)).as_string(con)
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS output;

    CREATE TABLE IF NOT EXISTS {stage_qualified} (
      id           bigserial PRIMARY KEY,
      job_id       uuid,
      calc_id      uuid        NOT NULL,
      stage        text        NOT NULL CHECK (stage IN ('00','01','02','03','04')),
      status       text        NOT NULL CHECK (status IN ('pending','running','done','error')),
      created_at   timestamptz NOT NULL DEFAULT now(),
      started_at   timestamptz,
      finished_at  timestamptz,
      error        text,
      UNIQUE(calc_id, stage)
    );

    CREATE INDEX IF NOT EXISTS ix_calc_job_stage_calc_stage
      ON {stage_qualified} (calc_id, stage, status, finished_at DESC);
    """
    async with con.cursor() as cur:
        await cur.execute(ddl)

async def ensure_calc_job_queue(con: psycopg.AsyncConnection) -> None:
    """
    Kolejka jobów (bez czyszczenia historii).
    """
    qualified = sql.Identifier(*JOB_QUEUE_TABLE.split(".", 1)).as_string(con)
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS output;

    CREATE TABLE IF NOT EXISTS {qualified} (
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
      ON {qualified} (status, created_at DESC);
    """
    async with con.cursor() as cur:
        await cur.execute(ddl)

# ──────────────────────────────────────────────────────────────────────────────
# Tabela snapshotów
# ──────────────────────────────────────────────────────────────────────────────

async def ensure_params_snapshot_table(con: psycopg.AsyncConnection) -> None:
    """
    Tworzy schemat + tabelę snapshot (jeśli nie istnieje) oraz indeks po (calc_id, created_at).
    """
    index_name = f"idx_{_TABLE}_calc_created"

    qualified = sql.Identifier(_SCHEMA, _TABLE).as_string(con)
    schema_quoted = sql.Identifier(_SCHEMA).as_string(con)
    idx_quoted = sql.Identifier(index_name).as_string(con)

    ddl = f"""
        CREATE SCHEMA IF NOT EXISTS {schema_quoted};

        CREATE TABLE IF NOT EXISTS {qualified} (
            calc_id      uuid        NOT NULL,
            params_ts    timestamptz NOT NULL,
            source_table text        NOT NULL,
            row          jsonb       NOT NULL,
            created_at   timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (calc_id, source_table)
        );

        CREATE INDEX IF NOT EXISTS {idx_quoted}
        ON {qualified} (calc_id, created_at DESC);
    """

    async with con.cursor() as cur:
        await cur.execute(ddl)

# ──────────────────────────────────────────────────────────────────────────────
# Narzędzia: enumeracja tabel params.*, pobór ostatniego rekordu jako JSON
# ──────────────────────────────────────────────────────────────────────────────

async def list_params_tables(con: psycopg.AsyncConnection) -> List[Tuple[str, str]]:
    con.row_factory = dict_row
    q = """
      SELECT table_schema, table_name
      FROM information_schema.tables
      WHERE table_schema = 'params'
        AND table_type = 'BASE TABLE'
        AND table_name LIKE 'form_%'
      ORDER BY table_name
    """
    async with con.cursor() as cur:
        await cur.execute(q)
        rows = await cur.fetchall()
    return [(r["table_schema"], r["table_name"]) for r in rows]

async def _detect_order_by(con: psycopg.AsyncConnection, schema: str, table: str) -> str:
    con.row_factory = dict_row
    q = """
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = %s AND table_name = %s
    """
    async with con.cursor() as cur:
        await cur.execute(q, (schema, table))
        cols = [r["column_name"] for r in await cur.fetchall()]

    lower = [c.lower() for c in cols]
    if "updated_at" in lower:
        return "updated_at DESC NULLS LAST"
    if "created_at" in lower:
        return "created_at DESC NULLS LAST"

    qpk = """
      SELECT a.attname AS col
      FROM pg_index i
      JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
      WHERE i.indrelid = %s::regclass AND i.indisprimary
    """
    regclass = f"{schema}.{table}"
    async with con.cursor() as cur:
        await cur.execute(qpk, (regclass,))
        pkcols = [r["col"] for r in await cur.fetchall()]
    if len(pkcols) == 1:
        return f"{pkcols[0]} DESC"

    # deterministycznie: pierwsza kolumna
    return f"{cols[0]} DESC"

async def fetch_last_row_as_json(con: psycopg.AsyncConnection, schema: str, table: str) -> Optional[dict]:
    order_by = await _detect_order_by(con, schema, table)
    con.row_factory = dict_row
    qualified = sql.Identifier(schema, table).as_string(con)  # "schema"."table"
    q = f"SELECT row_to_json(t) AS j FROM (SELECT * FROM {qualified} ORDER BY {order_by}) t LIMIT 1"
    async with con.cursor() as cur:
        await cur.execute(q)
        r = await cur.fetchone()

    if not r:
        return None
    row = r["j"]
    if isinstance(row, (bytes, bytearray, memoryview)):
        row = bytes(row).decode("utf-8", errors="ignore")
    if isinstance(row, str):
        try:
            row = json.loads(row)
        except Exception:
            pass
    return row if isinstance(row, dict) else None

# ──────────────────────────────────────────────────────────────────────────────
# Dump snapshot: params.form_* → output.params_snapshot_calc
# ──────────────────────────────────────────────────────────────────────────────

async def dump_params_snapshot(con: psycopg.AsyncConnection, calc_id, params_ts) -> None:
    await ensure_params_snapshot_table(con)

    qualified = sql.Identifier(_SCHEMA, _TABLE).as_string(con)
    async with con.cursor() as cur:
        await cur.execute(f"DELETE FROM {qualified} WHERE calc_id = %s", (str(calc_id),))

    tables = await list_params_tables(con)
    if not tables:
        raise RuntimeError("Brak tabel formularzy w schemacie 'params' (form_*) — nie można zbudować snapshotu.")

    inserted = 0
    async with con.cursor() as cur:
        for schema, table in tables:
            row = await fetch_last_row_as_json(con, schema, table)
            if row is None:
                log.warning("Snapshot: tabela %s.%s pusta — pomijam.", schema, table)
                continue
            st = f"{schema}.{table}"
            await cur.execute(
                f"INSERT INTO {qualified} (calc_id, params_ts, source_table, row) VALUES (%s, %s, %s, %s)",
                (str(calc_id), params_ts, st, Json(row)),
            )
            inserted += 1
    log.info("Snapshot: zapisano %d wierszy z params.form_*", inserted)

# ──────────────────────────────────────────────────────────────────────────────
# Konsolidacja: params.__consolidated__ (payload.source + payload.norm)
# ──────────────────────────────────────────────────────────────────────────────

async def _snapshot_rows_as_map(con: psycopg.AsyncConnection, calc_id) -> Dict[str, dict]:
    con.row_factory = dict_row
    qualified = sql.Identifier(_SCHEMA, _TABLE).as_string(con)
    like_pattern = "params.%"
    consolidated = "params.__consolidated__"

    q = f"""
      SELECT source_table, row
      FROM {qualified}
      WHERE calc_id = %s
        AND source_table LIKE %s
        AND source_table <> %s
    """

    out: Dict[str, dict] = {}

    async with con.cursor() as cur:
        await cur.execute(q, (str(calc_id), like_pattern, consolidated))
        for rec in await cur.fetchall():
            st = rec["source_table"]
            row = rec["row"]
            if isinstance(row, (bytes, bytearray, memoryview)):
                row = bytes(row).decode("utf-8", errors="ignore")
            if isinstance(row, str):
                try:
                    row = json.loads(row)
                except Exception:
                    pass
            if isinstance(row, dict):
                pay = row.get("payload") if st.startswith("params.form_") else row
                if not isinstance(pay, dict):
                    raise RuntimeError(f"[SNAPSHOT] {st} nie zawiera payload (jsonb).")
                out[st] = pay
    return out

# wymagane klucze (00 + 01) – dopisujemy local_tz
REQUIRED_NORM_KEYS_00 = {"fac_pv_pp", "fac_pv_wz", "fac_wind", "fac_load", "local_tz"}
REQUIRED_NORM_KEYS_01 = {
    "emax_total_mwh","emax_oze_mwh","emax_arbi_mwh",
    "eta_ch_frac","eta_dis_frac","bess_lambda_h_frac",
    "c_ch_h","c_dis_h","p_ch_max_mw","p_dis_max_mw",
    "moc_umowna_mw","arbi_dis_to_load",
    "base_min_profit_pln_mwh","cycles_per_day",
    "force_order","allow_carry_over",
    "bonus_ch_window","bonus_dis_window",
    "bonus_low_soc_ch","bonus_high_soc_dis",
    "bonus_hrs_ch","bonus_hrs_dis","bonus_hrs_ch_free","bonus_hrs_dis_free",
    "soc_low_threshold","soc_high_threshold",
    "procent_arbitrazu_frac",
    "bess_soc_start_frac",
}
REQUIRED_NORM_KEYS_ALL = REQUIRED_NORM_KEYS_00 | REQUIRED_NORM_KEYS_01

def _assert_norm_keys(norm: dict):
    missing = [k for k in REQUIRED_NORM_KEYS_ALL if k not in norm]
    if missing:
        raise RuntimeError(f"[CONSOLIDATED] Brak kluczy w norm: {missing}")

async def build_params_snapshot_consolidated(con: psycopg.AsyncConnection, calc_id, params_ts) -> None:
    """
    Twarda konsolidacja — brak aliasów i brak wartości domyślnych.
    Pola muszą istnieć i być poprawne (poza listami godzin bonusowych, które mogą być puste/brak).
    """
    await ensure_params_snapshot_table(con)
    source = await _snapshot_rows_as_map(con, calc_id)

    required_tables = [
        "params.form_zmienne",
        "params.form_bess_param",
        "params.form_parametry_klienta",
        "params.form_par_arbitrazu",
    ]
    for t in required_tables:
        if t not in source:
            raise RuntimeError(f"[CONSOLIDATED] Brak w snapshot.source tabeli: {t}")

    zm   = source["params.form_zmienne"]
    bess = source["params.form_bess_param"]
    kli  = source["params.form_parametry_klienta"]
    arbi = source["params.form_par_arbitrazu"]

    # Strefa czasowa (twardo wymagane)
    local_tz = _req_str("local_tz", kli.get("local_tz"))

    # Zmienne (mnożniki + konsumpcja) — brak aliasów, brak defaultów
    emax_total_mwh     = _req_num("emax", zm.get("emax"))
    procent_arbi_frac  = _req_frac("procent_arbitrazu", zm.get("procent_arbitrazu"))
    fac_pv_pp          = _req_num("moc_pv_pp", zm.get("moc_pv_pp"))
    fac_pv_wz          = _req_num("moc_pv_wz", zm.get("moc_pv_wz"))
    fac_wind           = _req_num("moc_wiatr", zm.get("moc_wiatr"))
    zm_kons            = _req_num("zmiany_konsumpcji", zm.get("zmiany_konsumpcji"))
    fac_load           = 1.0 + (zm_kons / 100.0)

    # BESS — brak aliasów, brak defaultów
    eta_ch_frac        = _req_frac("bess_charge_eff",    bess.get("bess_charge_eff"))
    eta_dis_frac       = _req_frac("bess_discharge_eff", bess.get("bess_discharge_eff"))
    lam_month_frac     = _req_frac("bess_lambda_month",  bess.get("bess_lambda_month"))
    bess_lambda_h_frac = _lambda_month_to_hour(lam_month_frac)
    if bess_lambda_h_frac is None:
        raise RuntimeError("[CONSOLIDATED] Nie udało się przeliczyć bess_lambda_month → bess_lambda_h_frac.")

    c_ch_h             = _req_num("bess_c_rate_charge",    bess.get("bess_c_rate_charge"))
    c_dis_h            = _req_num("bess_c_rate_discharge", bess.get("bess_c_rate_discharge"))
    if c_ch_h <= 0.0 or c_dis_h <= 0.0:
        raise RuntimeError("[CONSOLIDATED] C-rate (bess_c_rate_charge/discharge) muszą być > 0 (godziny).")

    p_ch_max_mw        = round(emax_total_mwh / c_ch_h, 6)
    p_dis_max_mw       = round(emax_total_mwh / c_dis_h, 6)

    bess_soc_start_frac = _req_frac("bess_soc_start", bess.get("bess_soc_start"))

    emax_oze_mwh       = round(emax_total_mwh * (1.0 - procent_arbi_frac), 6)
    emax_arbi_mwh      = round(emax_total_mwh * procent_arbi_frac, 6)
    soc_oze_start_mwh  = round(emax_oze_mwh  * bess_soc_start_frac, 6)
    soc_arbi_start_mwh = round(emax_arbi_mwh * bess_soc_start_frac, 6)

    # Kontrakt/klient
    moc_umowna_mw      = _req_num("klient_moc_umowna", kli.get("klient_moc_umowna"))

    # Arbitraż
    arbi_dis_to_load   = _req_bool("arbi_dis_to_load", arbi.get("arbi_dis_to_load"))
    base_min_profit_pln_mwh = _req_num("base_min_profit_pln_mwh", arbi.get("base_min_profit_pln_mwh"))
    cycles_per_day          = int(_req_num("cycles_per_day",       arbi.get("cycles_per_day")))
    if cycles_per_day < 0:
        raise RuntimeError("[CONSOLIDATED] cycles_per_day nie może być ujemne.")
    allow_carry_over        = _req_bool("allow_carry_over", arbi.get("allow_carry_over"))
    force_order             = _req_bool("force_order",      arbi.get("force_order"))

    bonus_ch_window         = _req_num("bonus_ch_window",   arbi.get("bonus_ch_window"))
    bonus_dis_window        = _req_num("bonus_dis_window",  arbi.get("bonus_dis_window"))
    bonus_low_soc_ch        = _req_num("bonus_low_soc_ch",  arbi.get("bonus_low_soc_ch"))
    bonus_high_soc_dis      = _req_num("bonus_high_soc_dis",arbi.get("bonus_high_soc_dis"))

    soc_low_threshold       = _req_frac("soc_low_threshold",  arbi.get("soc_low_threshold"))
    soc_high_threshold      = _req_frac("soc_high_threshold", arbi.get("soc_high_threshold"))

    # Listy godzin — mogą być puste (gdy brak klucza → [])
    bonus_hrs_ch            = _int_list_allow_empty("bonus_hrs_ch",       arbi.get("bonus_hrs_ch"))
    bonus_hrs_dis           = _int_list_allow_empty("bonus_hrs_dis",      arbi.get("bonus_hrs_dis"))
    bonus_hrs_ch_free       = _int_list_allow_empty("bonus_hrs_ch_free",  arbi.get("bonus_hrs_ch_free"))
    bonus_hrs_dis_free      = _int_list_allow_empty("bonus_hrs_dis_free", arbi.get("bonus_hrs_dis_free"))

    norm: Dict[str, Any] = {
        "local_tz": local_tz,

        "emax_total_mwh": emax_total_mwh,
        "procent_arbitrazu_frac": procent_arbi_frac,
        "emax_oze_mwh": emax_oze_mwh,
        "emax_arbi_mwh": emax_arbi_mwh,

        "eta_ch_frac": eta_ch_frac,
        "eta_dis_frac": eta_dis_frac,
        "bess_lambda_h_frac": bess_lambda_h_frac,

        "c_ch_h": c_ch_h,
        "c_dis_h": c_dis_h,
        "p_ch_max_mw": p_ch_max_mw,
        "p_dis_max_mw": p_dis_max_mw,

        "moc_umowna_mw": moc_umowna_mw,
        "arbi_dis_to_load": arbi_dis_to_load,

        "fac_pv_pp": fac_pv_pp,
        "fac_pv_wz": fac_pv_wz,
        "fac_wind":  fac_wind,
        "fac_load":  fac_load,

        "base_min_profit_pln_mwh": base_min_profit_pln_mwh,
        "cycles_per_day": cycles_per_day,
        "allow_carry_over": allow_carry_over,
        "force_order": force_order,

        "bonus_ch_window": bonus_ch_window,
        "bonus_dis_window": bonus_dis_window,
        "bonus_low_soc_ch": bonus_low_soc_ch,
        "bonus_high_soc_dis": bonus_high_soc_dis,

        "soc_low_threshold":  soc_low_threshold,
        "soc_high_threshold": soc_high_threshold,

        "bonus_hrs_ch":       bonus_hrs_ch,
        "bonus_hrs_dis":      bonus_hrs_dis,
        "bonus_hrs_ch_free":  bonus_hrs_ch_free,
        "bonus_hrs_dis_free": bonus_hrs_dis_free,

        "bess_soc_start_frac": bess_soc_start_frac,
        "soc_oze_start_mwh":  soc_oze_start_mwh,
        "soc_arbi_start_mwh": soc_arbi_start_mwh,
    }

    _assert_norm_keys(norm)

    payload = {"source": source, "norm": norm}

    # Zapis do params.__consolidated__ + echo do snapshotu (source_table='params.__consolidated__')
    async with con.cursor() as cur:
        await cur.execute("""
            CREATE SCHEMA IF NOT EXISTS params;
            CREATE TABLE IF NOT EXISTS params.__consolidated__ (
              params_ts   timestamptz NOT NULL,
              calc_id     uuid        NOT NULL,
              payload     jsonb       NOT NULL,
              updated_at  timestamptz NOT NULL DEFAULT now(),
              PRIMARY KEY (params_ts, calc_id)
            );
        """)

        await cur.execute(
            """
            INSERT INTO params.__consolidated__ (params_ts, calc_id, payload, updated_at)
            VALUES (%s, %s, %s::jsonb, now())
            ON CONFLICT (params_ts, calc_id)
            DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at
            """,
            (params_ts, str(calc_id), Json(payload)),
        )

        qualified = sql.Identifier(_SCHEMA, _TABLE).as_string(con)
        await cur.execute(
            f"""
            INSERT INTO {qualified} (calc_id, params_ts, source_table, row, created_at)
            VALUES (%s, %s, %s, %s::jsonb, now())
            ON CONFLICT (calc_id, source_table)
            DO UPDATE SET row = excluded.row, params_ts = excluded.params_ts, created_at = now()
            """,
            (str(calc_id), params_ts, "params.__consolidated__", Json({"norm": norm})),
        )

    log.info("[CONSOLIDATED] Zapisano params.__consolidated__ i snapshot (calc_id=%s).", calc_id)

# ──────────────────────────────────────────────────────────────────────────────
# Sanity przed 00
# ──────────────────────────────────────────────────────────────────────────────

async def assert_consolidated_norm_ready(con: psycopg.AsyncConnection, calc_id) -> None:
    con.row_factory = dict_row
    qualified = sql.Identifier(_SCHEMA, _TABLE).as_string(con)
    q = f"""
        SELECT row
        FROM {qualified}
        WHERE calc_id=%s AND source_table='params.__consolidated__'
        ORDER BY created_at DESC
        LIMIT 1
    """
    async with con.cursor() as cur:
        await cur.execute(q, (str(calc_id),))
        r = await cur.fetchone()

    if not r:
        raise RuntimeError("[RUNNER] Brak snapshotu 'params.__consolidated__' dla tego calc_id.")
    row = r["row"]
    if not isinstance(row, Mapping) or "norm" not in row or not isinstance(row["norm"], Mapping):
        raise RuntimeError("[RUNNER] Snapshot consolidated nie zawiera poprawnego 'norm'.")
    norm = dict(row["norm"])
    _assert_norm_keys(norm)
    if not str(norm.get("local_tz") or "").strip():
        raise RuntimeError("[RUNNER] 'local_tz' w norm jest wymagane (np. 'Europe/Warsaw').")
