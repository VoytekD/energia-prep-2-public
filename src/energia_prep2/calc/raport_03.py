# calc/raport_03.py — RECREATE_MODE: none | truncate | drop

from __future__ import annotations
from typing import Any, Dict, Sequence, Optional
import uuid
import numpy as np
import psycopg
import os
import asyncio
import logging
import datetime as _dt
import io as _io  # COPY CSV buffer

# ─────────────────────────────────────────────────────────────────────────────
# Przełącznik czyszczenia (ENV: RECREATE_MODE)
# none/append/off → doklejanie wierszy
# truncate        → TRUNCATE wszystkich stage’ów (szybkie wyzerowanie)
# drop            → DROP & CREATE stage'ów (atomowo, najcięższe)
_RECREATE_MODE_RAW = (os.getenv("CALC_RECREATE_MODE", "truncate") or "").strip().lower()

if _RECREATE_MODE_RAW in {"", "none", "append", "off"}:
    RECREATE_MODE = "none"
elif _RECREATE_MODE_RAW in {"truncate", "trunc", "clean"}:
    RECREATE_MODE = "truncate"
elif _RECREATE_MODE_RAW in {"drop", "ddl", "recreate"}:
    RECREATE_MODE = "drop"
else:
    # cokolwiek innego → traktujemy jak domyślne 'truncate'
    RECREATE_MODE = "truncate"

# Próg przełączenia na COPY (ENV: CALC_COPY_THRESHOLD, domyślnie 5000)
COPY_THRESHOLD = int(os.getenv("CALC_COPY_THRESHOLD", "5000") or "5000")

# ─────────────────────────────────────────────────────────────────────────────
# Nazwy tabel
TABLE_STAGE_00 = "output.stage00_ingest_calc"
TABLE_STAGE_01 = "output.stage01_proposer_calc"
TABLE_STAGE_02 = "output.stage02_commit_calc"
TABLE_STAGE_04 = "output.stage04_report_calc"

# ─────────────────────────────────────────────────────────────────────────────
# DDL — STAGE 00: dokładnie kolumny z uzgodnionej listy
DDL_STAGE_00 = f"""
CREATE TABLE IF NOT EXISTS {TABLE_STAGE_00} (
  -- Identyfikatory
  params_ts timestamptz NOT NULL,
  calc_id   uuid        NOT NULL,
  ts_hour   int         NOT NULL CHECK (ts_hour >= 0),

  -- Czas
  ts_local  timestamp without time zone NOT NULL,
  ts_utc    timestamptz                 NOT NULL,

  -- Kalendarz (z lokalnego)
  year      int NOT NULL CHECK (year BETWEEN 2000 AND 2100),
  month     int NOT NULL CHECK (month BETWEEN 1 AND 12),
  day       int NOT NULL CHECK (day BETWEEN 1 AND 31),
  dow       int NOT NULL CHECK (dow BETWEEN 0 AND 6),
  hour      int NOT NULL CHECK (hour BETWEEN 0 AND 23),
  is_workday boolean NOT NULL,
  dt_h      date NOT NULL,

  -- Wejścia / moc/energia
  p_pv_pp_mw     double precision NOT NULL DEFAULT 0,
  p_pv_wz_mw     double precision NOT NULL DEFAULT 0,
  p_wind_mw      double precision NOT NULL DEFAULT 0,
  p_gen_total_mw double precision GENERATED ALWAYS AS (p_pv_pp_mw + p_pv_wz_mw + p_wind_mw) STORED,
  p_load_mw      double precision NOT NULL DEFAULT 0,
  e_surplus_mwh  double precision NOT NULL DEFAULT 0 CHECK (e_surplus_mwh >= 0),
  e_deficit_mwh  double precision NOT NULL DEFAULT 0 CHECK (e_deficit_mwh >= 0),
  CHECK (NOT (e_surplus_mwh > 0 AND e_deficit_mwh > 0)),

  -- Limity / parametry
  emax_oze_mwh       double precision NOT NULL CHECK (emax_oze_mwh >= 0),
  emax_arbi_mwh      double precision NOT NULL CHECK (emax_arbi_mwh >= 0),
  p_ch_max_mw        double precision NOT NULL CHECK (p_ch_max_mw >= 0),
  p_dis_max_mw       double precision NOT NULL CHECK (p_dis_max_mw >= 0),
  eta_ch_frac        double precision NOT NULL CHECK (eta_ch_frac > 0 AND eta_ch_frac <= 1),
  eta_dis_frac       double precision NOT NULL CHECK (eta_dis_frac > 0 AND eta_dis_frac <= 1),
  bess_lambda_h_frac double precision NOT NULL CHECK (bess_lambda_h_frac >= 0 AND bess_lambda_h_frac <= 1),
  moc_umowna_mw      double precision NOT NULL CHECK (moc_umowna_mw >= 0),
  arbi_dis_to_load   boolean NOT NULL,

  -- SOC
  soc_oze_start_mwh       double precision NOT NULL CHECK (soc_oze_start_mwh >= 0),
  soc_arbi_start_mwh      double precision NOT NULL CHECK (soc_arbi_start_mwh >= 0),
  soc_oze_after_idle_mwh  double precision NOT NULL CHECK (soc_oze_after_idle_mwh >= 0),
  soc_arbi_after_idle_mwh double precision NOT NULL CHECK (soc_arbi_after_idle_mwh >= 0),
  -- (opcjonalne %) do debug/BI; mogą być NULL
  soc_oze_pct_of_oze      double precision,
  soc_arbi_pct_of_arbi    double precision,
  soc_total_pct           double precision,

  -- Ceny
  price_tge_pln_mwh double precision,

  PRIMARY KEY (params_ts, calc_id, ts_hour)
);
"""

# ─────────────────────────────────────────────────────────────────────────────
# DDL — STAGE 01/02/04
DDL_STAGE_01 = f"""
CREATE TABLE IF NOT EXISTS {TABLE_STAGE_01} (
  params_ts timestamptz NOT NULL,
  calc_id   uuid        NOT NULL,
  ts_hour   int         NOT NULL,

  ts_local  timestamp without time zone NOT NULL,
  dt_h      date NOT NULL,

  prop_arbi_ch_from_grid_ac_mwh double precision NOT NULL DEFAULT 0 CHECK (prop_arbi_ch_from_grid_ac_mwh >= 0),
  prop_arbi_dis_to_grid_ac_mwh  double precision NOT NULL DEFAULT 0 CHECK (prop_arbi_dis_to_grid_ac_mwh >= 0),

  thr_low  double precision NOT NULL DEFAULT 0,
  thr_high double precision NOT NULL DEFAULT 0,

  dec_ch_base boolean NOT NULL DEFAULT false,
  dec_dis_base boolean NOT NULL DEFAULT false,

  PRIMARY KEY (params_ts, calc_id, ts_hour)
);
"""

DDL_STAGE_02 = f"""
CREATE TABLE IF NOT EXISTS {TABLE_STAGE_02} (
  params_ts timestamptz NOT NULL,
  calc_id   uuid        NOT NULL,
  ts_hour   int         NOT NULL,

  ts_local  timestamp without time zone NOT NULL,
  ts_utc    timestamptz NOT NULL,
  dt_h      date NOT NULL,

  charge_from_surplus_mwh      double precision NOT NULL DEFAULT 0 CHECK (charge_from_surplus_mwh >= 0),
  charge_from_grid_mwh         double precision NOT NULL DEFAULT 0 CHECK (charge_from_grid_mwh >= 0),
  discharge_to_load_mwh        double precision NOT NULL DEFAULT 0 CHECK (discharge_to_load_mwh >= 0),
  discharge_to_grid_mwh        double precision NOT NULL DEFAULT 0 CHECK (discharge_to_grid_mwh >= 0),

  loss_charge_from_grid_mwh    double precision NOT NULL DEFAULT 0,
  loss_charge_from_surplus_mwh double precision NOT NULL DEFAULT 0,
  loss_discharge_to_load_mwh   double precision NOT NULL DEFAULT 0,
  loss_idle_oze_mwh            double precision NOT NULL DEFAULT 0,
  loss_idle_arbi_mwh           double precision NOT NULL DEFAULT 0,

  e_import_mwh                 double precision NOT NULL DEFAULT 0,
  e_export_mwh                 double precision NOT NULL DEFAULT 0,
  e_curtailment_mwh            double precision NOT NULL DEFAULT 0,

  soc_oze_before_mwh           double precision NOT NULL DEFAULT 0,
  soc_arbi_before_mwh          double precision NOT NULL DEFAULT 0,
  soc_oze_after_idle_mwh       double precision NOT NULL DEFAULT 0,
  soc_arbi_after_idle_mwh      double precision NOT NULL DEFAULT 0,

  PRIMARY KEY (params_ts, calc_id, ts_hour)
);
"""

# >>> zmiana w 04: rozbicie na soc_oze_pct_of_total i soc_arbi_pct_of_total
DDL_STAGE_04 = f"""
CREATE TABLE IF NOT EXISTS {TABLE_STAGE_04} (
  params_ts timestamptz NOT NULL,
  calc_id   uuid        NOT NULL,
  ts_hour   int         NOT NULL,

  -- kopia metadanych czasu/kalendarza z 00
  ts_local  timestamp without time zone NOT NULL,
  ts_utc    timestamptz NOT NULL,
  year      int NOT NULL,
  month     int NOT NULL,
  day       int NOT NULL,
  dow       int NOT NULL,
  hour      int NOT NULL,
  is_workday boolean NOT NULL,
  dt_h      date NOT NULL,

  -- limity z 00 (dla raportów)
  emax_oze_mwh  double precision NOT NULL,
  emax_arbi_mwh double precision NOT NULL,
  moc_umowna_mw double precision NOT NULL,

  -- akcje/straty/bilans z 02
  charge_from_surplus_mwh      double precision NOT NULL,
  charge_from_grid_mwh         double precision NOT NULL,
  discharge_to_load_mwh        double precision NOT NULL,
  discharge_to_grid_mwh        double precision NOT NULL,

  loss_charge_from_grid_mwh    double precision NOT NULL,
  loss_charge_from_surplus_mwh double precision NOT NULL,
  loss_discharge_to_load_mwh   double precision NOT NULL,
  loss_idle_oze_mwh            double precision NOT NULL,
  loss_idle_arbi_mwh           double precision NOT NULL,

  e_import_mwh                 double precision NOT NULL,
  e_export_mwh                 double precision NOT NULL,
  e_curtailment_mwh            double precision NOT NULL,

  -- SOC (z 02) + % względem części i całości (liczone w locie)
  soc_oze_after_idle_mwh       double precision NOT NULL,
  soc_arbi_after_idle_mwh      double precision NOT NULL,
  soc_oze_pct_of_oze           double precision,
  soc_arbi_pct_of_arbi         double precision,
  soc_oze_pct_of_total         double precision,
  soc_arbi_pct_of_total        double precision,

  PRIMARY KEY (params_ts, calc_id, ts_hour)
);
"""

IDX_SQL = """
CREATE INDEX IF NOT EXISTS ix_s00_key ON output.stage00_ingest_calc (params_ts, calc_id, ts_hour);
CREATE INDEX IF NOT EXISTS ix_s01_key ON output.stage01_proposer_calc (params_ts, calc_id, ts_hour);
CREATE INDEX IF NOT EXISTS ix_s02_key ON output.stage02_commit_calc   (params_ts, calc_id, ts_hour);
CREATE INDEX IF NOT EXISTS ix_s04_key ON output.stage04_report_calc   (params_ts, calc_id, ts_hour);
"""

# ─────────────────────────────────────────────────────────────────────────────
# Pomocnicze (bez „plastrów”)
def _as_arr(H: Dict[str, Any], key: str, dtype=None) -> np.ndarray:
    if key not in H:
        raise KeyError(f"[03] Brak H['{key}'] — napraw w 00.")
    v = H[key]
    if isinstance(v, np.ndarray):
        return v.astype(dtype) if dtype is not None else v
    return np.array(v, dtype=dtype)

def _as_arr_P(P: Dict[str, Any], key: str, dtype=None) -> np.ndarray:
    if key not in P:
        raise KeyError(f"[03] Brak P['{key}'] — napraw w 01.")
    v = P[key]
    if isinstance(v, np.ndarray):
        return v.astype(dtype) if dtype is not None else v
    return np.array(v, dtype=dtype)

def _nan_to_none(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    try:
        return None if np.isnan(x) else float(x)
    except Exception:
        return float(x)

def _npdt64_to_py(x) -> Optional[object]:
    if x is None:
        return None
    if hasattr(x, "tolist"):  # np.datetime64 -> datetime
        return x.tolist()
    return x

async def _executemany(con: psycopg.AsyncConnection, sql: str, rows: Sequence[Sequence[object]]) -> None:
    async with con.cursor() as cur:
        await cur.executemany(sql, rows)
    # UWAGA: bez commit — transakcją sterujemy wyżej, w persist()

async def _copy_rows_csv(con: psycopg.AsyncConnection, table_fqn: str, columns: Sequence[str], rows: Sequence[Sequence[object]]) -> None:
    r"""
    COPY FROM STDIN w trybie TEXT z delim=TAB. Payload budujemy ręcznie:
    - None -> \\N
    - \t, \n, \\ -> escapowane odpowiednio jako \\t, \\n, \\\\
    """
    import io as _io

    buf = _io.StringIO()
    for r in rows:
        line = []
        for v in r:
            if v is None:
                line.append(r'\N')
            else:
                s = str(v)
                # TE ESCAPY są WŁAŚCIWE dla FORMAT text
                s = s.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n')
                line.append(s)
        buf.write('\t'.join(line) + '\n')
    payload = buf.getvalue()

    cols = ", ".join(columns)
    copy_sql = (
        f"COPY {table_fqn} ({cols}) "
        "FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
    )

    async with con.cursor() as cur:
        async with cur.copy(copy_sql) as copy:
            await copy.write(payload)


# ─────────────────────────────────────────────────────────────────────────────
# Hard-checks (egzekwujemy poprawność u źródła)
_EXEMPT_LEN_KEYS = {"bonus_hrs_ch", "bonus_hrs_dis", "bonus_hrs_ch_free", "bonus_hrs_dis_free"}

_REQUIRED_00_KEYS = [
    "ts_hour", "ts_local", "ts_utc",
    "year", "month", "day", "dow", "hour", "is_workday", "dt_h",
    "p_pv_pp_mw", "p_pv_wz_mw", "p_wind_mw", "p_load_mw",
    "e_surplus_mwh", "e_deficit_mwh",
    "emax_oze_mwh", "emax_arbi_mwh", "p_ch_max_mw", "p_dis_max_mw",
    "eta_ch_frac", "eta_dis_frac", "bess_lambda_h_frac", "moc_umowna_mw", "arbi_dis_to_load",
    "soc_oze_start_mwh", "soc_arbi_start_mwh", "soc_oze_after_idle_mwh", "soc_arbi_after_idle_mwh",
]

def _hard_len(arrs: dict):
    lens = {k: len(v) for k, v in arrs.items()}
    L = max(lens.values()) if lens else None
    bad = [k for k, n in lens.items() if n != L]
    if bad:
        raise ValueError(f"[03][H] Nierówne długości wektorów: {bad} (expect {L}). Napraw w 00.")

def _ensure_date_array(name: str, arr):
    for i, v in enumerate(arr):
        if not isinstance(v, _dt.date) or isinstance(v, _dt.datetime):
            raise TypeError(
                f"[03][H] {name}[{i}] ma typ {type(v).__name__}, oczekuję date. "
                f"Napraw w 00 (np. ts_local[i].date())."
            )

def _ensure_naive_dt_array(name: str, arr):
    for i, v in enumerate(arr):
        if not isinstance(v, _dt.datetime) or v.tzinfo is not None:
            raise TypeError(
                f"[03][H] {name}[{i}] oczekuję datetime **bez** tz (naive, lokalny). Masz: {repr(v)}. "
                f"Napraw w 00: buduj ts_local jako naive (Europe/Warsaw)."
            )

def _ensure_aware_dt_array(name: str, arr):
    for i, v in enumerate(arr):
        if not isinstance(v, _dt.datetime) or v.tzinfo is None:
            raise TypeError(
                f"[03][H] {name}[{i}] oczekuję datetime **z** tz (aware, UTC). Masz: {repr(v)}. "
                f"Napraw w 00: buduj ts_utc jako timestamptz (UTC)."
            )

def _ensure_nonneg(name: str, arr):
    for i, v in enumerate(arr):
        if v is None:
            raise ValueError(f"[03][H] {name}[{i}] = NULL — napraw w 00.")
        if v < 0:
            raise ValueError(f"[03][H] {name}[{i}] ujemne ({v}) — napraw w 00.")

def _hardcheck_stage00(H: dict):
    missing = [k for k in _REQUIRED_00_KEYS if k not in H]
    if missing:
        raise KeyError(f"[03][H] Brak wymaganych pól: {missing}. Napraw w 00.")

    vecs = {
        k: H[k]
        for k in H
        if (isinstance(H[k], (list, tuple)) or "ndarray" in type(H[k]).__name__)
        and k not in _EXEMPT_LEN_KEYS
    }
    _hard_len(vecs)

    _ensure_naive_dt_array("ts_local", H["ts_local"])
    _ensure_aware_dt_array("ts_utc",   H["ts_utc"])
    _ensure_date_array("dt_h",         H["dt_h"])

    _ensure_nonneg("e_surplus_mwh", H["e_surplus_mwh"])
    _ensure_nonneg("e_deficit_mwh", H["e_deficit_mwh"])
    for i, (s, d) in enumerate(zip(H["e_surplus_mwh"], H["e_deficit_mwh"])):
        if s > 0 and d > 0:
            raise ValueError(f"[03][H] XOR violation @i={i}: surplus i deficit > 0. Napraw w 00.")

    for name in ("eta_ch_frac", "eta_dis_frac", "bess_lambda_h_frac"):
        arr = H[name]
        for i, v in enumerate(arr):
            if v is None:
                raise ValueError(f"[03][H] {name}[{i}] = NULL — napraw w 00.")
            if name != "bess_lambda_h_frac" and not (0 < v <= 1):
                raise ValueError(f"[03][H] {name}[{i}]={v} poza (0,1] — napraw w 00.")
            if name == "bess_lambda_h_frac" and not (0 <= v <= 1):
                raise ValueError(f"[03][H] {name}[{i}]={v} poza [0,1] — napraw w 00.")

# ─────────────────────────────────────────────────────────────────────────────
# DDL helpers
async def _drop_stages(con) -> None:
    drop_sql = """
    DO $$
    BEGIN
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='output' AND table_name='stage04_report_calc') THEN
        EXECUTE 'DROP TABLE output.stage04_report_calc';
      END IF;
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='output' AND table_name='stage02_commit_calc') THEN
        EXECUTE 'DROP TABLE output.stage02_commit_calc';
      END IF;
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='output' AND table_name='stage01_proposer_calc') THEN
        EXECUTE 'DROP TABLE output.stage01_proposer_calc';
      END IF;
      IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='output' AND table_name='stage00_ingest_calc') THEN
        EXECUTE 'DROP TABLE output.stage00_ingest_calc';
      END IF;
    END$$;
    """
    await con.execute("CREATE SCHEMA IF NOT EXISTS output;")
    await con.execute(drop_sql)

async def _create_stages(con) -> None:
    await con.execute("CREATE SCHEMA IF NOT EXISTS output;")
    await con.execute(DDL_STAGE_00)
    await con.execute(DDL_STAGE_01)
    await con.execute(DDL_STAGE_02)
    await con.execute(DDL_STAGE_04)
    await con.execute(IDX_SQL)

async def _rebuild_stages_atomic(con) -> None:
    async with con.transaction():
        await _drop_stages(con)
        await _create_stages(con)

async def _truncate_stages(con) -> None:
    # TRUNCATE czyści najszybciej, bez zmiany struktury i uprawnień
    sql = """
    TRUNCATE TABLE
      output.stage04_report_calc,
      output.stage02_commit_calc,
      output.stage01_proposer_calc,
      output.stage00_ingest_calc;
    """
    await con.execute(sql)
    # brak commit – zostanie objęte transakcją persist()

async def _ensure_all_tables(con: psycopg.AsyncConnection) -> None:
    async with con.cursor() as cur:
        await cur.execute("CREATE SCHEMA IF NOT EXISTS output;")
        await cur.execute(DDL_STAGE_00)
        await cur.execute(DDL_STAGE_01)
        await cur.execute(DDL_STAGE_02)
        await cur.execute(DDL_STAGE_04)
        await cur.execute(IDX_SQL)
    # brak commit – zostanie objęte transakcją persist()

# ─────────────────────────────────────────────────────────────────────────────
# INSERT: stage_00 (H)
async def _insert_stage_00(con: psycopg.AsyncConnection, params_ts, calc_id, H: Dict[str, Any]) -> None:
    sql = f"""
    INSERT INTO {TABLE_STAGE_00} (
      params_ts, calc_id, ts_hour,
      ts_local, ts_utc,
      year, month, day, dow, hour, is_workday, dt_h,
      p_pv_pp_mw, p_pv_wz_mw, p_wind_mw, p_load_mw,
      e_surplus_mwh, e_deficit_mwh,
      emax_oze_mwh, emax_arbi_mwh, p_ch_max_mw, p_dis_max_mw,
      eta_ch_frac, eta_dis_frac, bess_lambda_h_frac, moc_umowna_mw, arbi_dis_to_load,
      soc_oze_start_mwh, soc_arbi_start_mwh,
      soc_oze_after_idle_mwh, soc_arbi_after_idle_mwh,
      soc_oze_pct_of_oze, soc_arbi_pct_of_arbi, soc_total_pct,
      price_tge_pln_mwh
    ) VALUES (
      %s,%s,%s,
      %s,%s,
      %s,%s,%s,%s,%s,%s,%s,
      %s,%s,%s,%s,
      %s,%s,
      %s,%s,%s,%s,
      %s,%s,%s,%s,%s,
      %s,%s,
      %s,%s,
      %s,%s,%s,
      %s
    )
    """
    ts_hour  = _as_arr(H, "ts_hour",  dtype=np.int64)

    ts_local = _as_arr(H, "ts_local", dtype=object)
    ts_utc   = _as_arr(H, "ts_utc",   dtype=object)

    year    = _as_arr(H, "year",      np.int64)
    month   = _as_arr(H, "month",     np.int64)
    day     = _as_arr(H, "day",       np.int64)
    dow     = _as_arr(H, "dow",       np.int64)
    hour    = _as_arr(H, "hour",      np.int64)
    is_work = _as_arr(H, "is_workday", bool)
    dt_h    = _as_arr(H, "dt_h",      dtype=object)  # date

    p_pv_pp = _as_arr(H, "p_pv_pp_mw", float)
    p_pv_wz = _as_arr(H, "p_pv_wz_mw", float)
    p_wind  = _as_arr(H, "p_wind_mw",  float)
    p_load  = _as_arr(H, "p_load_mw",  float)
    e_surplus = _as_arr(H, "e_surplus_mwh", float)
    e_deficit = _as_arr(H, "e_deficit_mwh", float)

    emax_oze  = _as_arr(H, "emax_oze_mwh",  float)
    emax_arbi = _as_arr(H, "emax_arbi_mwh", float)
    p_ch_max  = _as_arr(H, "p_ch_max_mw",   float)
    p_dis_max = _as_arr(H, "p_dis_max_mw",  float)
    eta_ch    = _as_arr(H, "eta_ch_frac",   float)
    eta_dis   = _as_arr(H, "eta_dis_frac",  float)
    lambda_h  = _as_arr(H, "bess_lambda_h_frac", float)
    moc_umowna= _as_arr(H, "moc_umowna_mw", float)
    arbi_to_ld= _as_arr(H, "arbi_dis_to_load",   bool)

    soc_oze_start  = _as_arr(H, "soc_oze_start_mwh",        float)
    soc_arbi_start = _as_arr(H, "soc_arbi_start_mwh",       float)
    soc_oze_ai     = _as_arr(H, "soc_oze_after_idle_mwh",   float)
    soc_arbi_ai    = _as_arr(H, "soc_arbi_after_idle_mwh",  float)
    soc_oze_pct    = H.get("soc_oze_pct_of_oze",   np.array([None]*len(ts_hour), dtype=object))
    soc_arbi_pct   = H.get("soc_arbi_pct_of_arbi", np.array([None]*len(ts_hour), dtype=object))
    soc_total_pct  = H.get("soc_total_pct",        np.array([None]*len(ts_hour), dtype=object))

    price = H.get("price_tge_pln_mwh", np.array([None]*len(ts_hour), dtype=object))

    rows = []
    N = len(ts_hour)
    for i in range(N):
        rows.append((
            params_ts, str(calc_id), int(ts_hour[i]),
            _npdt64_to_py(ts_local[i]), _npdt64_to_py(ts_utc[i]),
            int(year[i]), int(month[i]), int(day[i]), int(dow[i]), int(hour[i]), bool(is_work[i]), dt_h[i],
            _nan_to_none(p_pv_pp[i]), _nan_to_none(p_pv_wz[i]), _nan_to_none(p_wind[i]), _nan_to_none(p_load[i]),
            _nan_to_none(e_surplus[i]), _nan_to_none(e_deficit[i]),
            _nan_to_none(emax_oze[i]), _nan_to_none(emax_arbi[i]), _nan_to_none(p_ch_max[i]), _nan_to_none(p_dis_max[i]),
            _nan_to_none(eta_ch[i]), _nan_to_none(eta_dis[i]), _nan_to_none(lambda_h[i]), _nan_to_none(moc_umowna[i]), bool(arbi_to_ld[i]),
            _nan_to_none(soc_oze_start[i]), _nan_to_none(soc_arbi_start[i]),
            _nan_to_none(soc_oze_ai[i]),    _nan_to_none(soc_arbi_ai[i]),
            _nan_to_none(soc_oze_pct[i]),   _nan_to_none(soc_arbi_pct[i]), _nan_to_none(soc_total_pct[i]),
            _nan_to_none(price[i]),
        ))
    # AUTO: COPY jeśli wsad duży, inaczej executemany
    if len(rows) >= COPY_THRESHOLD:
        await _copy_rows_csv(con, TABLE_STAGE_00, [
            "params_ts","calc_id","ts_hour",
            "ts_local","ts_utc",
            "year","month","day","dow","hour","is_workday","dt_h",
            "p_pv_pp_mw","p_pv_wz_mw","p_wind_mw","p_load_mw",
            "e_surplus_mwh","e_deficit_mwh",
            "emax_oze_mwh","emax_arbi_mwh","p_ch_max_mw","p_dis_max_mw",
            "eta_ch_frac","eta_dis_frac","bess_lambda_h_frac","moc_umowna_mw","arbi_dis_to_load",
            "soc_oze_start_mwh","soc_arbi_start_mwh",
            "soc_oze_after_idle_mwh","soc_arbi_after_idle_mwh",
            "soc_oze_pct_of_oze","soc_arbi_pct_of_arbi","soc_total_pct",
            "price_tge_pln_mwh"
        ], rows)
    else:
        await _executemany(con, sql, rows)

# ─────────────────────────────────────────────────────────────────────────────
# INSERT: stage_01 (P + H)
async def _insert_stage_01(con: psycopg.AsyncConnection, params_ts, calc_id, H: Dict[str, Any], P: Dict[str, Any]) -> None:
    sql = f"""
    INSERT INTO {TABLE_STAGE_01} (
      params_ts, calc_id, ts_hour,
      ts_local, dt_h,
      prop_arbi_ch_from_grid_ac_mwh, prop_arbi_dis_to_grid_ac_mwh,
      thr_low, thr_high,
      dec_ch_base, dec_dis_base
    ) VALUES (
      %s,%s,%s,
      %s,%s,
      %s,%s,
      %s,%s,
      %s,%s
    )
    """
    ts_hour = _as_arr(H, "ts_hour", np.int64)
    ts_local = _as_arr(H, "ts_local", dtype=object)
    dt_h     = _as_arr(H, "dt_h",      dtype=object)

    prop_ch = _as_arr_P(P, "prop_arbi_ch_from_grid_ac_mwh", float)
    prop_dis= _as_arr_P(P, "prop_arbi_dis_to_grid_ac_mwh",  float)

    d = P.get("diag", {})
    if not d:
        raise KeyError("[03] Brak P['diag'] (thr_low, thr_high). Napraw w 01.")
    thr_low  = _as_arr(d, "thr_low",  float)
    thr_high = _as_arr(d, "thr_high", float)

    dec_ch_base  = _as_arr_P(P, "dec_ch_base",  bool)
    dec_dis_base = _as_arr_P(P, "dec_dis_base", bool)

    rows = []
    N = len(ts_hour)
    for i in range(N):
        rows.append((
            params_ts, str(calc_id), int(ts_hour[i]),
            _npdt64_to_py(ts_local[i]), dt_h[i],
            _nan_to_none(prop_ch[i]), _nan_to_none(prop_dis[i]),
            _nan_to_none(thr_low[i]), _nan_to_none(thr_high[i]),
            bool(dec_ch_base[i]), bool(dec_dis_base[i]),
        ))
    if len(rows) >= COPY_THRESHOLD:
        await _copy_rows_csv(con, TABLE_STAGE_01, [
            "params_ts","calc_id","ts_hour",
            "ts_local","dt_h",
            "prop_arbi_ch_from_grid_ac_mwh","prop_arbi_dis_to_grid_ac_mwh",
            "thr_low","thr_high",
            "dec_ch_base","dec_dis_base"
        ], rows)
    else:
        await _executemany(con, sql, rows)

# ─────────────────────────────────────────────────────────────────────────────
# INSERT: stage_02 (C + H)
async def _insert_stage_02(con: psycopg.AsyncConnection, params_ts, calc_id, H: Dict[str, Any], C: Dict[str, Any]) -> None:
    sql = f"""
    INSERT INTO {TABLE_STAGE_02} (
      params_ts, calc_id, ts_hour,
      ts_local, ts_utc, dt_h,
      charge_from_surplus_mwh, charge_from_grid_mwh,
      discharge_to_load_mwh, discharge_to_grid_mwh,
      loss_charge_from_grid_mwh, loss_charge_from_surplus_mwh,
      loss_discharge_to_load_mwh, loss_idle_oze_mwh, loss_idle_arbi_mwh,
      e_import_mwh, e_export_mwh, e_curtailment_mwh,
      soc_oze_before_mwh, soc_arbi_before_mwh,
      soc_oze_after_idle_mwh, soc_arbi_after_idle_mwh
    ) VALUES (
      %s,%s,%s,
      %s,%s,%s,
      %s,%s,
      %s,%s,
      %s,%s,
      %s,%s,%s,
      %s,%s,%s,
      %s,%s,
      %s,%s
    )
    """
    ts_hour = _as_arr(H, "ts_hour", np.int64)
    ts_local= _as_arr(H, "ts_local", dtype=object)
    ts_utc  = _as_arr(H, "ts_utc",   dtype=object)
    dt_h    = _as_arr(H, "dt_h",     dtype=object)

    ch_sur = _as_arr(C, "charge_from_surplus_mwh", float)
    ch_grid= _as_arr(C, "charge_from_grid_mwh",   float)
    dis_ld = _as_arr(C, "discharge_to_load_mwh",  float)
    dis_gr = _as_arr(C, "discharge_to_grid_mwh",  float)

    loss_ch_g= _as_arr(C, "loss_charge_from_grid_mwh",    float)
    loss_ch_s= _as_arr(C, "loss_charge_from_surplus_mwh", float)
    loss_dis_ld = _as_arr(C, "loss_discharge_to_load_mwh", float)
    loss_idle_o = _as_arr(C, "loss_idle_oze_mwh",          float)
    loss_idle_a = _as_arr(C, "loss_idle_arbi_mwh",         float)

    e_import = _as_arr(C, "e_import_mwh",      float)
    e_export = _as_arr(C, "e_export_mwh",      float)
    e_curt   = _as_arr(C, "e_curtailment_mwh", float)

    soc_oze_b  = _as_arr(C, "soc_oze_before_mwh",  float)
    soc_arbi_b = _as_arr(C, "soc_arbi_before_mwh", float)

    soc_oze_ai = _as_arr(C, "soc_oze_after_idle_mwh",  float)
    soc_arbi_ai= _as_arr(C, "soc_arbi_after_idle_mwh", float)

    rows = []
    N = len(ts_hour)
    for i in range(N):
        rows.append((
            params_ts, str(calc_id), int(ts_hour[i]),
            _npdt64_to_py(ts_local[i]), _npdt64_to_py(ts_utc[i]), dt_h[i],
            _nan_to_none(ch_sur[i]), _nan_to_none(ch_grid[i]),
            _nan_to_none(dis_ld[i]), _nan_to_none(dis_gr[i]),
            _nan_to_none(loss_ch_g[i]), _nan_to_none(loss_ch_s[i]),
            _nan_to_none(loss_dis_ld[i]), _nan_to_none(loss_idle_o[i]), _nan_to_none(loss_idle_a[i]),
            _nan_to_none(e_import[i]), _nan_to_none(e_export[i]), _nan_to_none(e_curt[i]),
            _nan_to_none(soc_oze_b[i]), _nan_to_none(soc_arbi_b[i]),
            _nan_to_none(soc_oze_ai[i]), _nan_to_none(soc_arbi_ai[i]),
        ))
    if len(rows) >= COPY_THRESHOLD:
        await _copy_rows_csv(con, TABLE_STAGE_02, [
            "params_ts","calc_id","ts_hour",
            "ts_local","ts_utc","dt_h",
            "charge_from_surplus_mwh","charge_from_grid_mwh",
            "discharge_to_load_mwh","discharge_to_grid_mwh",
            "loss_charge_from_grid_mwh","loss_charge_from_surplus_mwh",
            "loss_discharge_to_load_mwh","loss_idle_oze_mwh","loss_idle_arbi_mwh",
            "e_import_mwh","e_export_mwh","e_curtailment_mwh",
            "soc_oze_before_mwh","soc_arbi_before_mwh",
            "soc_oze_after_idle_mwh","soc_arbi_after_idle_mwh"
        ], rows)
    else:
        await _executemany(con, sql, rows)

# ─────────────────────────────────────────────────────────────────────────────
# INSERT: stage_04 (join 00+02 → H + C)
async def _insert_stage_04(con: psycopg.AsyncConnection, params_ts, calc_id, H: Dict[str, Any], C: Dict[str, Any]) -> None:
    sql = f"""
    INSERT INTO {TABLE_STAGE_04} (
      params_ts, calc_id, ts_hour,
      ts_local, ts_utc, year, month, day, dow, hour, is_workday, dt_h,
      emax_oze_mwh, emax_arbi_mwh, moc_umowna_mw,
      charge_from_surplus_mwh, charge_from_grid_mwh,
      discharge_to_load_mwh, discharge_to_grid_mwh,
      loss_charge_from_grid_mwh, loss_charge_from_surplus_mwh,
      loss_discharge_to_load_mwh, loss_idle_oze_mwh, loss_idle_arbi_mwh,
      e_import_mwh, e_export_mwh, e_curtailment_mwh,
      soc_oze_after_idle_mwh, soc_arbi_after_idle_mwh,
      soc_oze_pct_of_oze,  soc_arbi_pct_of_arbi,
      soc_oze_pct_of_total, soc_arbi_pct_of_total
    ) VALUES (
      %s,%s,%s,
      %s,%s,%s,%s,%s,%s,%s,%s,%s,
      %s,%s,%s,
      %s,%s,
      %s,%s,
      %s,%s,
      %s,%s,%s,
      %s,%s,%s,
      %s,%s,
      %s,%s,
      %s,%s
    )
    """
    ts_hour = _as_arr(H, "ts_hour", np.int64)
    ts_local = _as_arr(H, "ts_local", dtype=object)
    ts_utc   = _as_arr(H, "ts_utc",   dtype=object)
    year  = _as_arr(H, "year",  np.int64)
    month = _as_arr(H, "month", np.int64)
    day   = _as_arr(H, "day",   np.int64)
    dow   = _as_arr(H, "dow",   np.int64)
    hour  = _as_arr(H, "hour",  np.int64)
    is_work = _as_arr(H, "is_workday", bool)
    dt_h    = _as_arr(H, "dt_h", dtype=object)

    emax_oze  = _as_arr(H, "emax_oze_mwh",  float)
    emax_arbi = _as_arr(H, "emax_arbi_mwh", float)
    moc_umowna= _as_arr(H, "moc_umowna_mw", float)

    ch_sur = _as_arr(C, "charge_from_surplus_mwh", float)
    ch_grid= _as_arr(C, "charge_from_grid_mwh",   float)
    dis_ld = _as_arr(C, "discharge_to_load_mwh",  float)
    dis_gr = _as_arr(C, "discharge_to_grid_mwh",  float)

    loss_ch_g= _as_arr(C, "loss_charge_from_grid_mwh",    float)
    loss_ch_s= _as_arr(C, "loss_charge_from_surplus_mwh", float)
    loss_dis_ld = _as_arr(C, "loss_discharge_to_load_mwh", float)
    loss_idle_o = _as_arr(C, "loss_idle_oze_mwh",          float)
    loss_idle_a = _as_arr(C, "loss_idle_arbi_mwh",         float)
    e_import = _as_arr(C, "e_import_mwh",      float)
    e_export = _as_arr(C, "e_export_mwh",      float)
    e_curt   = _as_arr(C, "e_curtailment_mwh", float)

    # SOC po idle — ŹRÓDŁO: C (02)
    soc_oze_ai  = _as_arr(C, "soc_oze_after_idle_mwh",  float)
    soc_arbi_ai = _as_arr(C, "soc_arbi_after_idle_mwh", float)

    with np.errstate(divide="ignore", invalid="ignore"):
        soc_oze_pct_part  = np.where(emax_oze  > 0, 100.0 * (soc_oze_ai  / emax_oze),  np.nan)
        soc_arbi_pct_part = np.where(emax_arbi > 0, 100.0 * (soc_arbi_ai / emax_arbi), np.nan)
        emax_sum = emax_oze + emax_arbi
        soc_oze_pct_total  = np.where(emax_sum > 0, 100.0 * (soc_oze_ai  / emax_sum), np.nan)
        soc_arbi_pct_total = np.where(emax_sum > 0, 100.0 * (soc_arbi_ai / emax_sum), np.nan)

    rows = []
    N = len(ts_hour)
    for i in range(N):
        rows.append((
            params_ts, str(calc_id), int(ts_hour[i]),
            _npdt64_to_py(ts_local[i]), _npdt64_to_py(ts_utc[i]),
            int(year[i]), int(month[i]), int(day[i]), int(dow[i]), int(hour[i]), bool(is_work[i]), dt_h[i],
            _nan_to_none(emax_oze[i]), _nan_to_none(emax_arbi[i]), _nan_to_none(moc_umowna[i]),
            _nan_to_none(ch_sur[i]), _nan_to_none(ch_grid[i]),
            _nan_to_none(dis_ld[i]), _nan_to_none(dis_gr[i]),
            _nan_to_none(loss_ch_g[i]), _nan_to_none(loss_ch_s[i]),
            _nan_to_none(loss_dis_ld[i]), _nan_to_none(loss_idle_o[i]), _nan_to_none(loss_idle_a[i]),
            _nan_to_none(e_import[i]), _nan_to_none(e_export[i]), _nan_to_none(e_curt[i]),
            _nan_to_none(soc_oze_ai[i]), _nan_to_none(soc_arbi_ai[i]),
            _nan_to_none(soc_oze_pct_part[i]), _nan_to_none(soc_arbi_pct_part[i]),
            _nan_to_none(soc_oze_pct_total[i]), _nan_to_none(soc_arbi_pct_total[i]),
        ))
    if len(rows) >= COPY_THRESHOLD:
        await _copy_rows_csv(con, TABLE_STAGE_04, [
            "params_ts","calc_id","ts_hour",
            "ts_local","ts_utc","year","month","day","dow","hour","is_workday","dt_h",
            "emax_oze_mwh","emax_arbi_mwh","moc_umowna_mw",
            "charge_from_surplus_mwh","charge_from_grid_mwh",
            "discharge_to_load_mwh","discharge_to_grid_mwh",
            "loss_charge_from_grid_mwh","loss_charge_from_surplus_mwh",
            "loss_discharge_to_load_mwh","loss_idle_oze_mwh","loss_idle_arbi_mwh",
            "e_import_mwh","e_export_mwh","e_curtailment_mwh",
            "soc_oze_after_idle_mwh","soc_arbi_after_idle_mwh",
            "soc_oze_pct_of_oze","soc_arbi_pct_of_arbi",
            "soc_oze_pct_of_total","soc_arbi_pct_of_total"
        ], rows)
    else:
        await _executemany(con, sql, rows)

# ─────────────────────────────────────────────────────────────────────────────
# API
async def persist(
    con: psycopg.AsyncConnection,
    H_buf: Dict[str, Any],
    P_buf: Dict[str, Any],
    C_buf: Dict[str, Any],
) -> None:
    log = logging.getLogger("calc")

    # Identyfikatory batcha (spójne dla wszystkich buforów)
    params_ts = H_buf.get("params_ts") or P_buf.get("params_ts") or C_buf.get("params_ts")
    calc_id = uuid.UUID(str(H_buf.get("calc_id") or P_buf.get("calc_id") or C_buf.get("calc_id")))

    # Najpierw ewentualny pełny DROP/CREATE (osobna transakcja wewnątrz helpera)
    if RECREATE_MODE == "drop":
        log.info("[DDL] RECREATE_MODE=drop → DROP & CREATE stage'ów (atomowo).")
        await _rebuild_stages_atomic(con)

    # Hard-check H_buf (zero fallbacków) — przed rozpoczęciem transakcji wstawek
    _hardcheck_stage00(H_buf)

    # Jedna transakcja na cały persist (opcjonalny TRUNCATE + wszystkie inserty)
    async with con.transaction():
        async with con.cursor() as cur:
            await cur.execute("SET LOCAL synchronous_commit = OFF")

        # Ensure tabel zawsze (gdy świeża baza)
        await _ensure_all_tables(con)

        if RECREATE_MODE == "truncate":
            log.info("[DDL] RECREATE_MODE=truncate → TRUNCATE wszystkich stage’ów (szybkie czyszczenie).")
            await _truncate_stages(con)
        else:
            log.info("[DDL] RECREATE_MODE=%s → bez czyszczenia (doklejanie).", RECREATE_MODE)

        # Wstawki (wszystko objęte jedną transakcją)
        await _insert_stage_00(con, params_ts, calc_id, H_buf)
        await _insert_stage_01(con, params_ts, calc_id, H_buf, P_buf)
        await _insert_stage_02(con, params_ts, calc_id, H_buf, C_buf)
        await _insert_stage_04(con, params_ts, calc_id, H_buf, C_buf)
