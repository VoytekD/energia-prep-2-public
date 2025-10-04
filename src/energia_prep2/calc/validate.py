# validate.py — szybki post-check po przebiegu + zrzut sum do logów (bez ENV)
from __future__ import annotations

import logging
from typing import List, Mapping, Any
import psycopg
from psycopg import sql
from psycopg.rows import dict_row

log = logging.getLogger("energia-prep-2.calc")

TBL_00 = "output.stage00_ingest_calc"
TBL_01 = "output.stage01_proposer_calc"
TBL_02 = "output.stage02_commit_calc"
TBL_F  = "output.stage04_report_calc"

# ─────────────────────────────────────────────────────────────────────────────
# Helpers

def _get_count(row: Any, key: str = "n_bad") -> int:
    """Bezpiecznie wyciąga COUNT(*) zarówno z tuple, jak i dict-row."""
    if row is None:
        return 0
    if isinstance(row, Mapping):
        return int(row.get(key, 0) or 0)
    try:
        return int(row[0] or 0)
    except Exception:
        return 0

def _get_val(row: Any, key: str, idx: int) -> float:
    """Wyciąga wartość po nazwie (dict-row) lub indeksie (tuple)."""
    if row is None:
        return 0
    if isinstance(row, Mapping):
        return float(row.get(key, 0) or 0)
    try:
        return float(row[idx] or 0)
    except Exception:
        return 0

# ─────────────────────────────────────────────────────────────────────────────
async def _counts(con: psycopg.AsyncConnection, params_ts) -> List[str]:
    con.row_factory = dict_row
    q = f"""
    WITH c AS (
      SELECT
        (SELECT COUNT(*) FROM {TBL_00} WHERE params_ts=%s) AS n00,
        (SELECT COUNT(*) FROM {TBL_01} WHERE params_ts=%s) AS n01,
        (SELECT COUNT(*) FROM {TBL_02} WHERE params_ts=%s) AS n02,
        (SELECT COUNT(*) FROM {TBL_F}  WHERE params_ts=%s) AS nf
    )
    SELECT * FROM c
    """
    async with con.cursor() as cur:
        await cur.execute(q, (params_ts, params_ts, params_ts, params_ts))
        r = await cur.fetchone()
    msgs = []
    for k in ("n00","n01","n02"):
        if r[k] != r["nf"]:
            msgs.append(f"COUNT: {k}={r[k]} != final={r['nf']}")
    log.info("[VALIDATE] counts: stage00=%s stage01=%s stage02=%s final=%s", r["n00"], r["n01"], r["n02"], r["nf"])
    return msgs

async def _time_integrity(con: psycopg.AsyncConnection, params_ts) -> List[str]:
    con.row_factory = dict_row
    msgs: List[str] = []
    async with con.cursor() as cur:
        await cur.execute(
            f"SELECT COUNT(*) AS n FROM {TBL_F} WHERE params_ts=%s AND ts_utc IS NULL",
            (params_ts,),
        )
        n_null = (await cur.fetchone())["n"]
        await cur.execute(
            f"""
            SELECT COUNT(*) AS n FROM (
              SELECT ts_utc FROM {TBL_F}
              WHERE params_ts=%s
              GROUP BY ts_utc HAVING COUNT(*)>1
            ) x
            """,
            (params_ts,),
        )
        n_dup = (await cur.fetchone())["n"]
    if n_null: msgs.append(f"TIME/NULL: {n_null} rows with NULL ts_utc")
    if n_dup:  msgs.append(f"TIME/DUPS: {n_dup} duplicate ts_utc")
    log.info("[VALIDATE] time: nulls=%s dups=%s", n_null, n_dup)
    return msgs

async def _exclusivity(con: psycopg.AsyncConnection, params_ts) -> List[str]:
    con.row_factory = dict_row
    async with con.cursor() as cur:
        await cur.execute(
            f"""SELECT COUNT(*) AS n
                FROM {TBL_00}
                WHERE params_ts=%s AND e_surplus_mwh>0 AND e_deficit_mwh>0""",
            (params_ts,),
        )
        n = (await cur.fetchone())["n"]
    log.info("[VALIDATE] surplus & deficit both>0 rows=%s", n)
    return [f"EXCLUSIVITY: {n} rows have both surplus and deficit > 0"] if n else []

# ─────────────────────────────────────────────────────────────────────────────
async def _soc_pct_bounds(con: psycopg.AsyncConnection, params_ts) -> List[str]:
    """
    Sprawdza, czy procenty SOC są w [0,100].
    Nie polegamy na kolumnach *_of_total; wszystko liczymy w locie z SOC i emax.
    """
    q = sql.SQL("""
    WITH base AS (
      SELECT
        soc_oze_pct_of_oze,
        soc_arbi_pct_of_arbi,
        soc_oze_after_idle_mwh,
        soc_arbi_after_idle_mwh,
        emax_oze_mwh,
        emax_arbi_mwh
      FROM output.stage04_report_calc
      WHERE params_ts = %s
    ),
    calc AS (
      SELECT
        -- pct-of-total liczone w locie; NULL gdy suma emax = 0
        (100.0 * soc_oze_after_idle_mwh)
          / NULLIF(emax_oze_mwh + emax_arbi_mwh, 0)  AS soc_oze_pct_of_total_calc,
        (100.0 * soc_arbi_after_idle_mwh)
          / NULLIF(emax_oze_mwh + emax_arbi_mwh, 0)  AS soc_arbi_pct_of_total_calc,
        (100.0 * (soc_oze_after_idle_mwh + soc_arbi_after_idle_mwh))
          / NULLIF(emax_oze_mwh + emax_arbi_mwh, 0)  AS soc_total_pct_calc,
        -- pct względem części (z tabeli)
        soc_oze_pct_of_oze,
        soc_arbi_pct_of_arbi
      FROM base
    )
    SELECT
      SUM( (COALESCE(calc.soc_oze_pct_of_oze   < 0 OR calc.soc_oze_pct_of_oze   > 100, FALSE))::int ) AS bad_oze_of_oze,
      SUM( (COALESCE(calc.soc_arbi_pct_of_arbi < 0 OR calc.soc_arbi_pct_of_arbi > 100, FALSE))::int ) AS bad_arbi_of_arbi,
      SUM( (COALESCE(calc.soc_total_pct_calc   < 0 OR calc.soc_total_pct_calc   > 100, FALSE))::int ) AS bad_total_calc,
      SUM( (COALESCE(calc.soc_oze_pct_of_total_calc  < 0 OR calc.soc_oze_pct_of_total_calc  > 100, FALSE))::int ) AS bad_oze_of_total,
      SUM( (COALESCE(calc.soc_arbi_pct_of_total_calc < 0 OR calc.soc_arbi_pct_of_total_calc > 100, FALSE))::int ) AS bad_arbi_of_total
    FROM calc;
    """)

    issues: List[str] = []
    async with con.cursor() as cur:
        await cur.execute(q, (params_ts,))
        r = await cur.fetchone()

    if not r:
        return issues

    # Pobranie wyników odporne na dict-row/tuple
    bad_oze_of_oze   = _get_val(r, "bad_oze_of_oze",   0)
    bad_arbi_of_arbi = _get_val(r, "bad_arbi_of_arbi", 1)
    bad_total_calc   = _get_val(r, "bad_total_calc",   2)
    bad_oze_of_total = _get_val(r, "bad_oze_of_total", 3)
    bad_arbi_of_total= _get_val(r, "bad_arbi_of_total",4)

    if bad_oze_of_oze:
        issues.append(f"[SOC] {int(bad_oze_of_oze)} godz. z soc_oze_pct_of_oze poza [0,100].")
    if bad_arbi_of_arbi:
        issues.append(f"[SOC] {int(bad_arbi_of_arbi)} godz. z soc_arbi_pct_of_arbi poza [0,100].")
    if bad_total_calc:
        issues.append(f"[SOC] {int(bad_total_calc)} godz. z wyliczonym soc_total_pct (calc) poza [0,100].")
    if bad_oze_of_total:
        issues.append(f"[SOC] {int(bad_oze_of_total)} godz. z wyliczonym soc_oze_pct_of_total poza [0,100].")
    if bad_arbi_of_total:
        issues.append(f"[SOC] {int(bad_arbi_of_total)} godz. z wyliczonym soc_arbi_pct_of_total poza [0,100].")

    return issues

# ─────────────────────────────────────────────────────────────────────────────
async def _export_cap_violations(con: psycopg.AsyncConnection, params_ts) -> List[str]:
    """
    Sprawdza, czy w którejkolwiek godzinie eksport energii do sieci
    przekracza limit wynikający z mocy umownej (MW * 1h).
    Uwaga: dt_h to data doby, NIE mnożymy przez nią. Limit jest godzinowy.
    """
    q = sql.SQL("""
        SELECT COUNT(*) AS n_bad
        FROM output.stage02_commit_calc c
        JOIN output.stage00_ingest_calc h
          ON  h.params_ts = c.params_ts
          AND h.calc_id   = c.calc_id
          AND h.ts_hour   = c.ts_hour
        WHERE c.params_ts = %s
          -- tolerancja numeryczna 1e-9 MWh
          AND c.e_export_mwh > (GREATEST(0, h.moc_umowna_mw) * 1.0 + 1e-9)
    """)

    async with con.cursor() as cur:
        await cur.execute(q, (params_ts,))
        row = await cur.fetchone()

    n_bad = _get_count(row, "n_bad")
    issues: List[str] = []
    if n_bad > 0:
        issues.append(
            f"[EXPORT_CAP] {n_bad} godz. z e_export_mwh > moc_umowna_mw * 1h (limit godzinowy mocy umownej)."
        )
    return issues

# ─────────────────────────────────────────────────────────────────────────────
async def _dump_sums(con: psycopg.AsyncConnection, params_ts) -> None:
    """
    Zrzut najważniejszych sum do logów: import/export, straty, główne przepływy.
    Kolumny zgodne ze stage02_commit_calc.
    """
    con.row_factory = dict_row
    q = f"""
    SELECT
      COUNT(*) AS n,
      SUM(e_import_mwh)                  AS sum_import_mwh,
      SUM(e_export_mwh)                  AS sum_export_mwh,

      SUM(charge_from_surplus_mwh)       AS sum_ch_from_surplus_mwh,
      SUM(charge_from_grid_mwh)          AS sum_ch_from_grid_mwh,
      SUM(discharge_to_load_mwh)         AS sum_dis_to_load_mwh,
      SUM(discharge_to_grid_mwh)         AS sum_dis_to_grid_mwh,

      SUM(e_curtailment_mwh)             AS sum_curtailment_mwh,
      SUM(loss_charge_from_grid_mwh)     AS sum_loss_ch_from_grid_mwh,
      SUM(loss_charge_from_surplus_mwh)  AS sum_loss_ch_from_surplus_mwh,
      SUM(loss_discharge_to_load_mwh)    AS sum_loss_dis_to_load_mwh,
      SUM(loss_idle_oze_mwh + loss_idle_arbi_mwh) AS sum_loss_idle_mwh
    FROM {TBL_02}
    WHERE params_ts=%s
    """
    async with con.cursor() as cur:
        await cur.execute(q, (params_ts,))
        r = await cur.fetchone()

    log.info(
        "[VALIDATE][SUMS] rows=%s import=%.6f export=%.6f | ch_surplus=%.6f ch_grid=%.6f | "
        "dis_load=%.6f dis_grid=%.6f | losses: curtail=%.6f ch_grid=%.6f ch_surplus=%.6f dis_load=%.6f idle=%.6f",
        r["n"] or 0,
        (r["sum_import_mwh"] or 0.0),
        (r["sum_export_mwh"] or 0.0),
        (r["sum_ch_from_surplus_mwh"] or 0.0),
        (r["sum_ch_from_grid_mwh"] or 0.0),
        (r["sum_dis_to_load_mwh"] or 0.0),
        (r["sum_dis_to_grid_mwh"] or 0.0),
        (r["sum_curtailment_mwh"] or 0.0),
        (r["sum_loss_ch_from_grid_mwh"] or 0.0),
        (r["sum_loss_ch_from_surplus_mwh"] or 0.0),
        (r["sum_loss_dis_to_load_mwh"] or 0.0),
        (r["sum_loss_idle_mwh"] or 0.0),
    )

# ─────────────────────────────────────────────────────────────────────────────
async def run(con: psycopg.AsyncConnection, params_ts) -> None:
    # spójny tryb zwracania wierszy do dalszych zapytań
    con.row_factory = dict_row

    log.info("[VALIDATE] start (params_ts=%s)", params_ts)

    issues: List[str] = []
    issues += await _counts(con, params_ts)
    issues += await _time_integrity(con, params_ts)
    issues += await _exclusivity(con, params_ts)
    issues += await _soc_pct_bounds(con, params_ts)
    issues += await _export_cap_violations(con, params_ts)

    # zawsze zrzucamy sumy – niezależnie od issues
    await _dump_sums(con, params_ts)

    if issues:
        for m in issues:
            log.warning("[VALIDATE] %s", m)
        log.info("[VALIDATE] end (params_ts=%s) — %d issue(s) flagged", params_ts, len(issues))
    else:
        log.info("[VALIDATE] OK — brak problemów krytycznych")
        log.info("[VALIDATE] end (params_ts=%s) — 0 issues", params_ts)
