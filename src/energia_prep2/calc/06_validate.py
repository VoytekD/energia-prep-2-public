# src/energia_prep2/calc/06_validate.py
from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional, Set

from psycopg import AsyncConnection
from psycopg.rows import dict_row

log = logging.getLogger(__name__)

# ── stałe tolerancji ─────────────────────────────────────────────────────────
EPS = 1e-9
EPS_EN = 1e-6


# ── UTIL ─────────────────────────────────────────────────────────────────────

async def _tbl_exists(cur, schema: str, table: str) -> bool:
    """Sprawdza obecność obiektu (tabela/widok). Działa z dict_row."""
    await cur.execute(
        "SELECT to_regclass(%s) IS NOT NULL AS present",
        (f'{schema}."{table}"',),
    )
    row = await cur.fetchone()
    return bool(row["present"])


async def _cols(cur, schema: str, table: str) -> List[str]:
    """Zwraca listę kolumn z information_schema. Działa z dict_row."""
    await cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    rows_ = await cur.fetchall()
    return [r["column_name"] for r in rows_]


def _require_columns(found: Iterable[str], required: Iterable[str], ctx: str) -> None:
    found_set: Set[str] = set(found)
    missing = [c for c in required if c not in found_set]
    if missing:
        raise ValueError(
            f"[VALIDATE] W {ctx} brakuje kolumn: {', '.join(missing)}. "
            f"Dostępne: {', '.join(sorted(found_set))}"
        )


def _finite_sql(expr: str) -> str:
    # liczba skończona (bez NaN, ±Inf)
    return f"({expr} IS NOT NULL AND {expr} = {expr} AND {expr} < 'Infinity'::float8 AND {expr} > '-Infinity'::float8)"


async def _one_row(cur, sql: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    await cur.execute(sql, args or {})
    row = await cur.fetchone()
    if row is None:
        raise RuntimeError("Zapytanie nie zwróciło wiersza.")
    return row


# ── CHECKS ───────────────────────────────────────────────────────────────────

async def _check_02_proposer(cur, calc_id: str) -> Dict[str, Any]:
    """
    OUTPUT._02_proposer — wg COLS_02:
      - prop_arbi_ch_from_grid_ac_mwh >= 0
      - prop_arbi_dis_to_grid_ac_mwh  >= 0
      - brak NaN/Inf w ww. polach
      - brak sprzeczności flag dec_ch & dec_dis w tej samej godzinie
    """
    log.info("[VALIDATE/02] start")

    schema, table = "output", "_02_proposer"
    if not await _tbl_exists(cur, schema, table):
        raise ValueError(f"[VALIDATE/02] Brak tabeli {schema}.\"{table}\"")

    cols = await _cols(cur, schema, table)
    _require_columns(
        cols,
        ["prop_arbi_ch_from_grid_ac_mwh", "prop_arbi_dis_to_grid_ac_mwh", "dec_ch", "dec_dis"],
        f'{schema}."{table}"',
    )

    sql = f"""
    WITH x AS (
      SELECT
        COALESCE(prop_arbi_ch_from_grid_ac_mwh, 0)::double precision AS ch_grid_ac,
        COALESCE(prop_arbi_dis_to_grid_ac_mwh,  0)::double precision AS dis_grid_ac,
        COALESCE(dec_ch, false)  AS dec_ch,
        COALESCE(dec_dis, false) AS dec_dis
      FROM {schema}."{table}"
      WHERE calc_id = %(calc_id)s
    )
    SELECT
      COUNT(*)                                   AS n,
      COUNT(*) FILTER (WHERE ch_grid_ac < 0)     AS neg_ch_grid_ac,
      COUNT(*) FILTER (WHERE dis_grid_ac < 0)    AS neg_dis_grid_ac,

      COUNT(*) FILTER (WHERE NOT {_finite_sql('ch_grid_ac')})  AS badnum_ch_grid_ac,
      COUNT(*) FILTER (WHERE NOT {_finite_sql('dis_grid_ac')}) AS badnum_dis_grid_ac,

      COUNT(*) FILTER (WHERE dec_ch AND dec_dis) AS both_flags
    FROM x;
    """

    row = await _one_row(cur, sql, {"calc_id": calc_id})
    log.info("[VALIDATE/02] result: %s", row)

    issues = []
    for k in ("neg_ch_grid_ac", "neg_dis_grid_ac", "badnum_ch_grid_ac", "badnum_dis_grid_ac", "both_flags"):
        if row.get(k, 0) > 0:
            issues.append(f"{k}={row[k]}")

    if issues:
        raise ValueError(f"[VALIDATE/02] Błędy w proposer: {', '.join(issues)}")

    return row


async def _check_03_commit(cur, calc_id: str) -> Dict[str, Any]:
    """
    OUTPUT._03_commit — kluczowe pola wg COLS_03:
      - SOC_*_pct_of_total w [0,1] i skończone
      - brak NaN/Inf w rev_arbi_to_grid_pln, cost_grid_to_arbi_pln, cashflow_arbi_pln
    """
    log.info("[VALIDATE/03] start")

    schema, table = "output", "_03_commit"
    if not await _tbl_exists(cur, schema, table):
        raise ValueError(f"[VALIDATE/03] Brak tabeli {schema}.\"{table}\"")

    cols = await _cols(cur, schema, table)
    _require_columns(
        cols,
        [
            "soc_oze_pct_of_total",
            "soc_arbi_pct_of_total",
            "rev_arbi_to_grid_pln",
            "cost_grid_to_arbi_pln",
            "cashflow_arbi_pln",
        ],
        f'{schema}."{table}"',
    )

    sql = f"""
    SELECT
      COUNT(*) AS n,
      COUNT(*) FILTER (WHERE NOT {_finite_sql('soc_oze_pct_of_total')})  AS badnum_soc_oze,
      COUNT(*) FILTER (WHERE NOT {_finite_sql('soc_arbi_pct_of_total')}) AS badnum_soc_arbi,
      COUNT(*) FILTER (WHERE soc_oze_pct_of_total < 0 OR soc_oze_pct_of_total > 1)   AS out_of_range_soc_oze,
      COUNT(*) FILTER (WHERE soc_arbi_pct_of_total < 0 OR soc_arbi_pct_of_total > 1) AS out_of_range_soc_arbi,

      COUNT(*) FILTER (WHERE NOT {_finite_sql('rev_arbi_to_grid_pln')})   AS badnum_rev_arbi_to_grid,
      COUNT(*) FILTER (WHERE NOT {_finite_sql('cost_grid_to_arbi_pln')})  AS badnum_cost_grid_to_arbi,
      COUNT(*) FILTER (WHERE NOT {_finite_sql('cashflow_arbi_pln')})      AS badnum_cashflow_arbi
    FROM {schema}."{table}"
    WHERE calc_id = %(calc_id)s;
    """

    row = await _one_row(cur, sql, {"calc_id": calc_id})
    log.info("[VALIDATE/03] result: %s", row)

    issues = []
    for k in (
        "badnum_soc_oze",
        "badnum_soc_arbi",
        "out_of_range_soc_oze",
        "out_of_range_soc_arbi",
        "badnum_rev_arbi_to_grid",
        "badnum_cost_grid_to_arbi",
        "badnum_cashflow_arbi",
    ):
        if row.get(k, 0) > 0:
            issues.append(f"{k}={row[k]}")

    if issues:
        raise ValueError(f"[VALIDATE/03] Błędy w commit: {', '.join(issues)}")

    return row


async def _check_04_pricing(cur, calc_id: str) -> Dict[str, Any]:
    """
    OUTPUT._04_pricing — wg COLS_04:
      - skończone: cashflow_net_pln
    """
    log.info("[VALIDATE/04] start")

    schema, table = "output", "_04_pricing"
    if not await _tbl_exists(cur, schema, table):
        raise ValueError(f"[VALIDATE/04] Brak tabeli {schema}.\"{table}\"")

    cols = await _cols(cur, schema, table)
    _require_columns(cols, ["cashflow_net_pln"], f'{schema}."{table}"')

    sql = f"""
    SELECT
      COUNT(*) AS n,
      COUNT(*) FILTER (WHERE NOT {_finite_sql('cashflow_net_pln')}) AS badnum_cashflow_net
    FROM {schema}."{table}"
    WHERE calc_id = %(calc_id)s;
    """

    row = await _one_row(cur, sql, {"calc_id": calc_id})
    log.info("[VALIDATE/04] result: %s", row)

    if row.get("badnum_cashflow_net", 0) > 0:
        raise ValueError(f"[VALIDATE/04] NaN/Inf w cashflow_net_pln: {row['badnum_cashflow_net']}")

    return row


async def _check_04b_pricing_dyst(cur, calc_id: str) -> Optional[Dict[str, Any]]:
    """Opcjonalna walidacja output.\"_04b_pricing_dyst\"."""
    log.info("[VALIDATE/04b] start (opcjonalne)")
    schema, table = "output", "_04b_pricing_dyst"
    if not await _tbl_exists(cur, schema, table):
        log.info("[VALIDATE/04b] brak %s.\"%s\" — pomijam", schema, table)
        return None

    sql = f"""
    SELECT COUNT(*) AS n
    FROM {schema}."{table}"
    WHERE calc_id = %(calc_id)s;
    """
    row = await _one_row(cur, sql, {"calc_id": calc_id})
    log.info("[VALIDATE/04b] result: %s", row)
    return row


async def _check_rowcount_consistency(cur, calc_id: str) -> Dict[str, Any]:
    """Spójność liczby wierszy między 01/02/03/04 (dla tego samego calc_id)."""
    log.info("[VALIDATE/ROWCOUNT] start")

    sql = """
    WITH c01 AS (SELECT COUNT(*) AS n FROM output."_01_ingest"   WHERE calc_id = %(calc_id)s),
         c02 AS (SELECT COUNT(*) AS n FROM output."_02_proposer" WHERE calc_id = %(calc_id)s),
         c03 AS (SELECT COUNT(*) AS n FROM output."_03_commit"   WHERE calc_id = %(calc_id)s),
         c04 AS (SELECT COUNT(*) AS n FROM output."_04_pricing"  WHERE calc_id = %(calc_id)s)
    SELECT (SELECT n FROM c01) AS n01,
           (SELECT n FROM c02) AS n02,
           (SELECT n FROM c03) AS n03,
           (SELECT n FROM c04) AS n04;
    """

    row = await _one_row(cur, sql, {"calc_id": calc_id})
    log.info("[VALIDATE/ROWCOUNT] result: %s", row)

    n01, n02, n03, n04 = row["n01"], row["n02"], row["n03"], row["n04"]
    if not (n01 == n02 == n03 == n04):
        raise ValueError(f"[VALIDATE/ROWCOUNT] N nierówne: 01={n01}, 02={n02}, 03={n03}, 04={n04}")

    return row


# ── ENTRYPOINT ────────────────────────────────────────────────────────────────

async def run(con: AsyncConnection, params_ts, calc_id: str) -> None:
    """
    Wejście STAGE 06 (VALIDATE).
    Rzuca ValueError, jeśli dowolna sekcja nie przejdzie walidacji.
    """
    log.info("[VALIDATE] start (params_ts=%s, calc_id=%s)", params_ts, calc_id)

    async with con.cursor(row_factory=dict_row) as cur:
        await _check_02_proposer(cur, calc_id)
        await _check_03_commit(cur, calc_id)
        await _check_04_pricing(cur, calc_id)
        await _check_04b_pricing_dyst(cur, calc_id)
        await _check_rowcount_consistency(cur, calc_id)

    log.info("[VALIDATE] all checks passed")
