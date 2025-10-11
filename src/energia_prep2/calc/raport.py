# -*- coding: utf-8 -*-
"""
raport.py — persist dla output."00/01/02/03_*" (pricing pod 03)
Tworzy tabele, jeśli nie istnieją, i robi UPSERT po (calc_id, ts_utc).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Sequence

import psycopg
from psycopg.rows import dict_row

log = logging.getLogger("energia-prep-2.calc.raport")

# ──────────────────────────────────────────────────────────────────────────────
# Nazwy tabel — cytujemy nazwy zaczynające się od cyfry
# ──────────────────────────────────────────────────────────────────────────────
TABLE_STAGE_00 = 'output."00_stage_ingest"'
TABLE_STAGE_01 = 'output."01_stage_proposer"'
TABLE_STAGE_02 = 'output."02_stage_commit"'
TABLE_STAGE_03 = 'output."03_stage_pricing"'   # dawniej: stage04_pricing_detail

# Nazwy indeksów (bez cudzysłowów)
IDX_00_TS = "output_00_stage_ingest_ts_utc_idx"
IDX_01_TS = "output_01_stage_proposer_ts_utc_idx"
IDX_02_TS = "output_02_stage_commit_ts_utc_idx"
IDX_03_TS = "output_03_stage_pricing_ts_utc_idx"

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _as_list(x: Any) -> List[Any]:
    if isinstance(x, (list, tuple)):
        return list(x)
    return [x]

def _num(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)

def _bool(x: Any, default: bool = False) -> bool:
    try:
        return bool(x)
    except Exception:
        return bool(default)

def _get_vec(H: Dict[str, Any], key: str, N: int, default: float = 0.0) -> List[Any]:
    if key in H:
        v = _as_list(H[key])
        if len(v) == N:
            return v
    return [default] * N

def _get_vec_opt(H: Dict[str, Any], key: str, N: int) -> List[Any]:
    if key in H:
        v = _as_list(H[key])
        if len(v) == N:
            return v
    return [None] * N

def _get_i(H: Dict[str, Any], key: str, i: int, default: Any = None) -> Any:
    """
    Zwraca i-ty element z H[key] jeżeli to sekwencja; jeśli skalar → zwraca skalar,
    jeśli lista za krótka → default.
    """
    v = H.get(key, default)
    try:
        return v[i]
    except TypeError:
        # skalar / brak indeksowania
        return v
    except IndexError:
        return default

# ──────────────────────────────────────────────────────────────────────────────
# Ensure DDL
# ──────────────────────────────────────────────────────────────────────────────
async def ensure_tables(con: psycopg.AsyncConnection) -> None:
    async with con.cursor() as cur:
        # STAGE 00 — INGEST (params_ts jako timestamptz)
        await cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_STAGE_00} (
            calc_id        uuid          NOT NULL,
            params_ts      timestamptz     NULL,

            ts_utc         timestamptz   NOT NULL,
            ts_local       timestamp       NULL,
            ts_hour        integer         NULL,
            year           integer         NULL,
            month          integer         NULL,
            day            integer         NULL,
            dow            integer         NULL,
            hour           integer         NULL,
            is_workday     boolean         NULL,
            is_holiday     boolean         NULL,
            dt_h           double precision NULL,

            -- bazowe serie
            pv_pp_1mwp     double precision NULL,
            pv_wz_1mwp     double precision NULL,
            wind_1mwp      double precision NULL,
            fixing_ii_price double precision NULL,

            -- przeliczenia (MWh/MW)
            cons_mwh       double precision NULL,
            prod_mwh       double precision NULL,
            surplus_mwh    double precision NULL,
            deficit_mwh    double precision NULL,

            -- capy i sprawności
            cap_import_ac_mwh double precision NULL,
            cap_export_ac_mwh double precision NULL,
            cap_charge_net_mwh double precision NULL,
            cap_discharge_net_mwh double precision NULL,
            eta_ch         double precision NULL,
            eta_dis        double precision NULL,
            lambda_oze     double precision NULL,
            lambda_arbi    double precision NULL,
            bonus_take_frac double precision NULL,
            bonus_give_frac double precision NULL,

            -- strefy/maski
            is_weekend     boolean NULL,
            dyst_zone      text    NULL,
            mocowa_mask    boolean NULL,

            -- stany i pojemności (skalary)
            soc0_oze_mwh   double precision NULL,
            soc0_arbi_mwh  double precision NULL,
            emax_oze_mwh   double precision NULL,
            emax_arbi_mwh  double precision NULL,
            moc_umowna_mw  double precision NULL,

            PRIMARY KEY (calc_id, ts_utc)
        );
        """)
        await cur.execute(f'CREATE INDEX IF NOT EXISTS {IDX_00_TS} ON {TABLE_STAGE_00}(ts_utc);')

        # STAGE 01 — PROPOSER
        await cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_STAGE_01} (
            calc_id        uuid        NOT NULL,
            ts_utc         timestamptz NOT NULL,

            -- decyzje i progi
            prop_arbi_ch_from_grid_ac_mwh  double precision NULL,
            prop_arbi_dis_to_grid_ac_mwh   double precision NULL,
            dec_ch          boolean NULL,
            dec_dis         boolean NULL,
            dec_ch_base     boolean NULL,
            dec_dis_base    boolean NULL,
            thr_low         double precision NULL,
            thr_high        double precision NULL,
            delta_k         double precision NULL,
            margin_ch       double precision NULL,
            margin_dis      double precision NULL,

            PRIMARY KEY (calc_id, ts_utc)
        );
        """)
        await cur.execute(f'CREATE INDEX IF NOT EXISTS {IDX_01_TS} ON {TABLE_STAGE_01}(ts_utc);')

        # STAGE 02 — COMMIT
        await cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_STAGE_02} (
            calc_id        uuid        NOT NULL,
            ts_utc         timestamptz NOT NULL,

            -- energetyka główna (AC/NET)
            e_import_mwh            double precision NULL,
            e_export_mwh            double precision NULL,
            charge_from_surplus_mwh double precision NULL,  -- NET
            charge_from_grid_mwh    double precision NULL,  -- AC
            discharge_to_load_mwh   double precision NULL,  -- NET
            discharge_to_grid_mwh   double precision NULL,  -- AC

            -- rozbicia energetyczne (AC)
            export_from_surplus_ac_mwh double precision NULL,
            export_from_arbi_ac_mwh    double precision NULL,
            import_for_load_ac_mwh     double precision NULL,
            import_for_arbi_ac_mwh     double precision NULL,

            -- snapshoty SOC + procenty
            soc_oze_before_idle_mwh  double precision NULL,
            soc_oze_after_idle_mwh   double precision NULL,
            soc_arbi_before_idle_mwh double precision NULL,
            soc_arbi_after_idle_mwh  double precision NULL,
            soc_arbi_pct_of_arbi     double precision NULL,
            soc_arbi_pct_of_total    double precision NULL,

            -- straty energetyczne (MWh)
            loss_idle_oze_mwh         double precision NULL,
            loss_idle_arbi_mwh        double precision NULL,
            loss_conv_ch_mwh          double precision NULL,
            loss_conv_dis_to_grid_mwh double precision NULL,
            loss_conv_dis_to_load_mwh double precision NULL,
            wasted_surplus_due_to_export_cap_mwh double precision NULL,

            -- cap diagnostics
            cap_blocked_dis_ac_mwh double precision NULL,
            cap_blocked_ch_ac_mwh  double precision NULL,
            unserved_load_after_cap_ac_mwh double precision NULL,

            PRIMARY KEY (calc_id, ts_utc)
        );
        """)
        await cur.execute(f'CREATE INDEX IF NOT EXISTS {IDX_02_TS} ON {TABLE_STAGE_02}(ts_utc);')

        # STAGE 03 — PRICING
        await cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_STAGE_03} (
            calc_id        uuid        NOT NULL,
            params_ts      timestamptz   NULL,
            ts_utc         timestamptz NOT NULL,
            ts_local       timestamp   NULL,

            -- ceny
            price_import_pln_mwh double precision NULL,
            price_export_pln_mwh double precision NULL,

            -- bazowe przepływy finansowe
            rev_arbi_to_grid_pln     double precision NULL,
            rev_surplus_export_pln   double precision NULL,
            cost_grid_to_arbi_pln    double precision NULL,
            cost_import_for_load_pln double precision NULL,
            cashflow_net_pln         double precision NULL,

            -- straty energetyczne (MWh)
            loss_idle_oze_mwh         double precision NULL,
            loss_idle_arbi_mwh        double precision NULL,
            loss_conv_ch_mwh          double precision NULL,
            loss_conv_dis_to_grid_mwh double precision NULL,
            loss_conv_dis_to_load_mwh double precision NULL,
            wasted_surplus_due_to_export_cap_mwh double precision NULL,

            -- straty w PLN (diag)
            pln_loss_conv_ch              double precision NULL,
            pln_loss_conv_dis_to_grid     double precision NULL,
            pln_loss_conv_dis_to_load     double precision NULL,
            pln_loss_idle_arbi            double precision NULL,
            pln_loss_idle_oze             double precision NULL,
            pln_wasted_surplus_cap        double precision NULL,
            losses_diag_total_pln         double precision NULL,

            -- cap diagnostics (AC) + PLN
            cap_blocked_dis_ac_mwh  double precision NULL,
            cap_blocked_ch_ac_mwh   double precision NULL,
            unserved_load_after_cap_ac_mwh double precision NULL,
            pln_cap_blocked_dis_ac  double precision NULL,
            pln_cap_blocked_ch_ac   double precision NULL,
            pln_unserved_load_after_cap double precision NULL,
            cap_opportunity_total_pln double precision NULL,

            PRIMARY KEY (calc_id, ts_utc)
        );
        """)
        await cur.execute(f'CREATE INDEX IF NOT EXISTS {IDX_03_TS} ON {TABLE_STAGE_03}(ts_utc);')
    await con.commit()

# ──────────────────────────────────────────────────────────────────────────────
# PERSIST: Stage 00
# ──────────────────────────────────────────────────────────────────────────────
async def persist_stage00(con: psycopg.AsyncConnection, calc_id, params_ts, H: Dict[str, Any], N: int) -> None:
    await ensure_tables(con)
    cols = """
        calc_id, params_ts, ts_utc, ts_local, ts_hour, year, month, day, dow, hour,
        is_workday, is_holiday, dt_h,
        pv_pp_1mwp, pv_wz_1mwp, wind_1mwp, fixing_ii_price,
        cons_mwh, prod_mwh, surplus_mwh, deficit_mwh,
        cap_import_ac_mwh, cap_export_ac_mwh, cap_charge_net_mwh, cap_discharge_net_mwh,
        eta_ch, eta_dis, lambda_oze, lambda_arbi, bonus_take_frac, bonus_give_frac,
        is_weekend, dyst_zone, mocowa_mask,
        soc0_oze_mwh, soc0_arbi_mwh, emax_oze_mwh, emax_arbi_mwh, moc_umowna_mw
    """
    ins = f"""
    INSERT INTO {TABLE_STAGE_00} ({cols})
    VALUES (
        %(calc_id)s, %(params_ts)s, %(ts_utc)s, %(ts_local)s, %(ts_hour)s, %(year)s, %(month)s, %(day)s, %(dow)s, %(hour)s,
        %(is_workday)s, %(is_holiday)s, %(dt_h)s,
        %(pv_pp_1mwp)s, %(pv_wz_1mwp)s, %(wind_1mwp)s, %(fixing_ii_price)s,
        %(cons_mwh)s, %(prod_mwh)s, %(surplus_mwh)s, %(deficit_mwh)s,
        %(cap_import_ac_mwh)s, %(cap_export_ac_mwh)s, %(cap_charge_net_mwh)s, %(cap_discharge_net_mwh)s,
        %(eta_ch)s, %(eta_dis)s, %(lambda_oze)s, %(lambda_arbi)s, %(bonus_take_frac)s, %(bonus_give_frac)s,
        %(is_weekend)s, %(dyst_zone)s, %(mocowa_mask)s,
        %(soc0_oze_mwh)s, %(soc0_arbi_mwh)s, %(emax_oze_mwh)s, %(emax_arbi_mwh)s, %(moc_umowna_mw)s
    )
    ON CONFLICT (calc_id, ts_utc) DO UPDATE SET
        params_ts = EXCLUDED.params_ts,
        ts_local  = EXCLUDED.ts_local,
        ts_hour   = EXCLUDED.ts_hour,
        year      = EXCLUDED.year,
        month     = EXCLUDED.month,
        day       = EXCLUDED.day,
        dow       = EXCLUDED.dow,
        hour      = EXCLUDED.hour,
        is_workday= EXCLUDED.is_workday,
        is_holiday= EXCLUDED.is_holiday,
        dt_h      = EXCLUDED.dt_h,
        pv_pp_1mwp= EXCLUDED.pv_pp_1mwp,
        pv_wz_1mwp= EXCLUDED.pv_wz_1mwp,
        wind_1mwp = EXCLUDED.wind_1mwp,
        fixing_ii_price = EXCLUDED.fixing_ii_price,
        cons_mwh  = EXCLUDED.cons_mwh,
        prod_mwh  = EXCLUDED.prod_mwh,
        surplus_mwh = EXCLUDED.surplus_mwh,
        deficit_mwh = EXCLUDED.deficit_mwh,
        cap_import_ac_mwh = EXCLUDED.cap_import_ac_mwh,
        cap_export_ac_mwh = EXCLUDED.cap_export_ac_mwh,
        cap_charge_net_mwh = EXCLUDED.cap_charge_net_mwh,
        cap_discharge_net_mwh = EXCLUDED.cap_discharge_net_mwh,
        eta_ch    = EXCLUDED.eta_ch,
        eta_dis   = EXCLUDED.eta_dis,
        lambda_oze= EXCLUDED.lambda_oze,
        lambda_arbi= EXCLUDED.lambda_arbi,
        bonus_take_frac = EXCLUDED.bonus_take_frac,
        bonus_give_frac = EXCLUDED.bonus_give_frac,
        is_weekend = EXCLUDED.is_weekend,
        dyst_zone  = EXCLUDED.dyst_zone,
        mocowa_mask= EXCLUDED.mocowa_mask,
        soc0_oze_mwh  = EXCLUDED.soc0_oze_mwh,
        soc0_arbi_mwh = EXCLUDED.soc0_arbi_mwh,
        emax_oze_mwh  = EXCLUDED.emax_oze_mwh,
        emax_arbi_mwh = EXCLUDED.emax_arbi_mwh,
        moc_umowna_mw = EXCLUDED.moc_umowna_mw
    ;
    """
    data: List[Dict[str, Any]] = []
    for i in range(N):
        dt = H["ts_local"][i]
        data.append(dict(
            calc_id=calc_id,
            params_ts=params_ts,
            ts_utc=H["ts_utc"][i],
            ts_local=H["ts_local"][i],
            ts_hour=getattr(dt, "hour", None),
            year=getattr(dt, "year", None),
            month=getattr(dt, "month", None),
            day=getattr(dt, "day", None),
            dow=getattr(dt, "weekday", lambda: None)(),
            hour=getattr(dt, "hour", None),
            is_workday=(getattr(dt, "weekday", lambda: 0)() < 5) if dt else None,
            is_holiday=None,
            dt_h=_num(_get_i(H, "dt_h", i), None),

            pv_pp_1mwp=_num(_get_i(H, "pv_pp_1mwp", i), None),
            pv_wz_1mwp=_num(_get_i(H, "pv_wz_1mwp", i), None),
            wind_1mwp=_num(_get_i(H, "wind_1mwp", i), None),
            fixing_ii_price=_num(_get_i(H, "fixing_ii_price", i), None),

            cons_mwh=_num(_get_i(H, "cons_mwh", i), None),
            prod_mwh=_num(_get_i(H, "prod_mwh", i), None),
            surplus_mwh=_num(_get_i(H, "surplus_mwh", i), None),
            deficit_mwh=_num(_get_i(H, "deficit_mwh", i), None),

            cap_import_ac_mwh=_num(_get_i(H, "cap_import_ac_mwh", i), None),
            cap_export_ac_mwh=_num(_get_i(H, "cap_export_ac_mwh", i), None),
            cap_charge_net_mwh=_num(_get_i(H, "cap_charge_net_mwh", i), None),
            cap_discharge_net_mwh=_num(_get_i(H, "cap_discharge_net_mwh", i), None),

            eta_ch=_num(_get_i(H, "eta_ch", i), None),
            eta_dis=_num(_get_i(H, "eta_dis", i), None),
            lambda_oze=_num(_get_i(H, "lambda_oze", i), None),
            lambda_arbi=_num(_get_i(H, "lambda_arbi", i), None),
            bonus_take_frac=_num(_get_i(H, "bonus_take_frac", i), None),
            bonus_give_frac=_num(_get_i(H, "bonus_give_frac", i), None),

            is_weekend=_bool(_get_i(H, "is_weekend", i), None),
            dyst_zone=_get_i(H, "dyst_zone", i),
            mocowa_mask=_bool(_get_i(H, "mocowa_mask", i), None),

            soc0_oze_mwh=_num(H.get("soc0_oze_mwh"), None),
            soc0_arbi_mwh=_num(H.get("soc0_arbi_mwh"), None),
            emax_oze_mwh=_num(H.get("emax_oze_mwh"), None),
            emax_arbi_mwh=_num(H.get("emax_arbi_mwh"), None),
            moc_umowna_mw=_num(H.get("moc_umowna_mw"), None),
        ))
    async with con.cursor() as cur:
        await cur.executemany(ins, data)
        await cur.execute(f"ANALYZE {TABLE_STAGE_00};")
    await con.commit()
    log.info("[PERSIST-00] zapisano (n=%d)", len(data))

# ──────────────────────────────────────────────────────────────────────────────
# PERSIST: Stage 01
# ──────────────────────────────────────────────────────────────────────────────
async def persist_stage01(
    con: psycopg.AsyncConnection,
    calc_id,
    P: Dict[str, Any],
    ts_utc_seq: Sequence[Any],
    N: int
) -> None:
    """Zapisuje proposer. Oś czasu brana z H_buf.ts_utc (przekazana jako ts_utc_seq)."""
    await ensure_tables(con)
    cols = """
        calc_id, ts_utc,
        prop_arbi_ch_from_grid_ac_mwh, prop_arbi_dis_to_grid_ac_mwh,
        dec_ch, dec_dis, dec_ch_base, dec_dis_base,
        thr_low, thr_high, delta_k, margin_ch, margin_dis
    """
    ins = f"""
    INSERT INTO {TABLE_STAGE_01} ({cols})
    VALUES (
        %(calc_id)s, %(ts_utc)s,
        %(prop_arbi_ch_from_grid_ac_mwh)s, %(prop_arbi_dis_to_grid_ac_mwh)s,
        %(dec_ch)s, %(dec_dis)s, %(dec_ch_base)s, %(dec_dis_base)s,
        %(thr_low)s, %(thr_high)s, %(delta_k)s, %(margin_ch)s, %(margin_dis)s
    )
    ON CONFLICT (calc_id, ts_utc) DO UPDATE SET
        prop_arbi_ch_from_grid_ac_mwh = EXCLUDED.prop_arbi_ch_from_grid_ac_mwh,
        prop_arbi_dis_to_grid_ac_mwh  = EXCLUDED.prop_arbi_dis_to_grid_ac_mwh,
        dec_ch = EXCLUDED.dec_ch,
        dec_dis= EXCLUDED.dec_dis,
        dec_ch_base = EXCLUDED.dec_ch_base,
        dec_dis_base= EXCLUDED.dec_dis_base,
        thr_low = EXCLUDED.thr_low,
        thr_high= EXCLUDED.thr_high,
        delta_k = EXCLUDED.delta_k,
        margin_ch = EXCLUDED.margin_ch,
        margin_dis= EXCLUDED.margin_dis
    ;
    """
    data: List[Dict[str, Any]] = []
    for i in range(N):
        data.append(dict(
            calc_id=calc_id,
            ts_utc=ts_utc_seq[i],
            prop_arbi_ch_from_grid_ac_mwh=_num(P.get("prop_arbi_ch_from_grid_ac_mwh", [None]*N)[i], None),
            prop_arbi_dis_to_grid_ac_mwh=_num(P.get("prop_arbi_dis_to_grid_ac_mwh", [None]*N)[i], None),
            dec_ch=bool(P.get("dec_ch", [None]*N)[i]),
            dec_dis=bool(P.get("dec_dis", [None]*N)[i]),
            dec_ch_base=bool(P.get("dec_ch_base", [None]*N)[i]),
            dec_dis_base=bool(P.get("dec_dis_base", [None]*N)[i]),
            thr_low=_num(P.get("thr_low", [None]*N)[i], None),
            thr_high=_num(P.get("thr_high", [None]*N)[i], None),
            delta_k=_num(P.get("delta_k", [None]*N)[i], None),
            margin_ch=_num(P.get("margin_ch", [None]*N)[i], None),
            margin_dis=_num(P.get("margin_dis", [None]*N)[i], None),
        ))
    async with con.cursor() as cur:
        await cur.executemany(ins, data)
        await cur.execute(f"ANALYZE {TABLE_STAGE_01};")
    await con.commit()
    log.info("[PERSIST-01] zapisano (n=%d)", len(data))

# ──────────────────────────────────────────────────────────────────────────────
# PERSIST: Stage 02
# ──────────────────────────────────────────────────────────────────────────────
async def persist_stage02(
    con: psycopg.AsyncConnection,
    calc_id,
    R: Dict[str, Any],
    ts_utc_seq: Sequence[Any],
    N: int
) -> None:
    """Zapisuje commit. Oś czasu: R['ts_utc'] jeśli jest, inaczej fallback do ts_utc_seq."""
    await ensure_tables(con)
    cols = """
        calc_id, ts_utc,
        e_import_mwh, e_export_mwh,
        charge_from_surplus_mwh, charge_from_grid_mwh, discharge_to_load_mwh, discharge_to_grid_mwh,
        export_from_surplus_ac_mwh, export_from_arbi_ac_mwh, import_for_load_ac_mwh, import_for_arbi_ac_mwh,
        soc_oze_before_idle_mwh, soc_oze_after_idle_mwh, soc_arbi_before_idle_mwh, soc_arbi_after_idle_mwh,
        soc_arbi_pct_of_arbi, soc_arbi_pct_of_total,
        loss_idle_oze_mwh, loss_idle_arbi_mwh, loss_conv_ch_mwh, loss_conv_dis_to_grid_mwh, loss_conv_dis_to_load_mwh,
        wasted_surplus_due_to_export_cap_mwh,
        cap_blocked_dis_ac_mwh, cap_blocked_ch_ac_mwh, unserved_load_after_cap_ac_mwh
    """
    ins = f"""
    INSERT INTO {TABLE_STAGE_02} ({cols})
    VALUES (
        %(calc_id)s, %(ts_utc)s,
        %(e_import_mwh)s, %(e_export_mwh)s,
        %(charge_from_surplus_mwh)s, %(charge_from_grid_mwh)s, %(discharge_to_load_mwh)s, %(discharge_to_grid_mwh)s,
        %(export_from_surplus_ac_mwh)s, %(export_from_arbi_ac_mwh)s, %(import_for_load_ac_mwh)s, %(import_for_arbi_ac_mwh)s,
        %(soc_oze_before_idle_mwh)s, %(soc_oze_after_idle_mwh)s, %(soc_arbi_before_idle_mwh)s, %(soc_arbi_after_idle_mwh)s,
        %(soc_arbi_pct_of_arbi)s, %(soc_arbi_pct_of_total)s,
        %(loss_idle_oze_mwh)s, %(loss_idle_arbi_mwh)s, %(loss_conv_ch_mwh)s, %(loss_conv_dis_to_grid_mwh)s, %(loss_conv_dis_to_load_mwh)s,
        %(wasted_surplus_due_to_export_cap_mwh)s,
        %(cap_blocked_dis_ac_mwh)s, %(cap_blocked_ch_ac_mwh)s, %(unserved_load_after_cap_ac_mwh)s
    )
    ON CONFLICT (calc_id, ts_utc) DO UPDATE SET
        e_import_mwh = EXCLUDED.e_import_mwh,
        e_export_mwh = EXCLUDED.e_export_mwh,
        charge_from_surplus_mwh = EXCLUDED.charge_from_surplus_mwh,
        charge_from_grid_mwh = EXCLUDED.charge_from_grid_mwh,
        discharge_to_load_mwh = EXCLUDED.discharge_to_load_mwh,
        discharge_to_grid_mwh = EXCLUDED.discharge_to_grid_mwh,
        export_from_surplus_ac_mwh = EXCLUDED.export_from_surplus_ac_mwh,
        export_from_arbi_ac_mwh = EXCLUDED.export_from_arbi_ac_mwh,
        import_for_load_ac_mwh = EXCLUDED.import_for_load_ac_mwh,
        import_for_arbi_ac_mwh = EXCLUDED.import_for_arbi_ac_mwh,
        soc_oze_before_idle_mwh = EXCLUDED.soc_oze_before_idle_mwh,
        soc_oze_after_idle_mwh  = EXCLUDED.soc_oze_after_idle_mwh,
        soc_arbi_before_idle_mwh = EXCLUDED.soc_arbi_before_idle_mwh,
        soc_arbi_after_idle_mwh  = EXCLUDED.soc_arbi_after_idle_mwh,
        soc_arbi_pct_of_arbi = EXCLUDED.soc_arbi_pct_of_arbi,
        soc_arbi_pct_of_total = EXCLUDED.soc_arbi_pct_of_total,
        loss_idle_oze_mwh = EXCLUDED.loss_idle_oze_mwh,
        loss_idle_arbi_mwh = EXCLUDED.loss_idle_arbi_mwh,
        loss_conv_ch_mwh = EXCLUDED.loss_conv_ch_mwh,
        loss_conv_dis_to_grid_mwh = EXCLUDED.loss_conv_dis_to_grid_mwh,
        loss_conv_dis_to_load_mwh = EXCLUDED.loss_conv_dis_to_load_mwh,
        wasted_surplus_due_to_export_cap_mwh = EXCLUDED.wasted_surplus_due_to_export_cap_mwh,
        cap_blocked_dis_ac_mwh = EXCLUDED.cap_blocked_dis_ac_mwh,
        cap_blocked_ch_ac_mwh  = EXCLUDED.cap_blocked_ch_ac_mwh,
        unserved_load_after_cap_ac_mwh = EXCLUDED.unserved_load_after_cap_ac_mwh
    ;
    """
    r_ts = R.get("ts_utc", ts_utc_seq)
    data: List[Dict[str, Any]] = []
    for i in range(N):
        data.append(dict(
            calc_id=calc_id,
            ts_utc=r_ts[i],
            e_import_mwh=_num(R.get("e_import_mwh", [None]*N)[i], None),
            e_export_mwh=_num(R.get("e_export_mwh", [None]*N)[i], None),
            charge_from_surplus_mwh=_num(R.get("charge_from_surplus_mwh", [None]*N)[i], None),
            charge_from_grid_mwh=_num(R.get("charge_from_grid_mwh", [None]*N)[i], None),
            discharge_to_load_mwh=_num(R.get("discharge_to_load_mwh", [None]*N)[i], None),
            discharge_to_grid_mwh=_num(R.get("discharge_to_grid_mwh", [None]*N)[i], None),
            export_from_surplus_ac_mwh=_num(R.get("export_from_surplus_ac_mwh", [None]*N)[i], None),
            export_from_arbi_ac_mwh=_num(R.get("export_from_arbi_ac_mwh", [None]*N)[i], None),
            import_for_load_ac_mwh=_num(R.get("import_for_load_ac_mwh", [None]*N)[i], None),
            import_for_arbi_ac_mwh=_num(R.get("import_for_arbi_ac_mwh", [None]*N)[i], None),
            soc_oze_before_idle_mwh=_num(R.get("soc_oze_before_idle_mwh", [None]*N)[i], None),
            soc_oze_after_idle_mwh=_num(R.get("soc_oze_after_idle_mwh", [None]*N)[i], None),
            soc_arbi_before_idle_mwh=_num(R.get("soc_arbi_before_idle_mwh", [None]*N)[i], None),
            soc_arbi_after_idle_mwh=_num(R.get("soc_arbi_after_idle_mwh", [None]*N)[i], None),
            soc_arbi_pct_of_arbi=_num(R.get("soc_arbi_pct_of_arbi", [None]*N)[i], None),
            soc_arbi_pct_of_total=_num(R.get("soc_arbi_pct_of_total", [None]*N)[i], None),
            loss_idle_oze_mwh=_num(R.get("loss_idle_oze_mwh", [None]*N)[i], None),
            loss_idle_arbi_mwh=_num(R.get("loss_idle_arbi_mwh", [None]*N)[i], None),
            loss_conv_ch_mwh=_num(R.get("loss_conv_ch_mwh", [None]*N)[i], None),
            loss_conv_dis_to_grid_mwh=_num(R.get("loss_conv_dis_to_grid_mwh", [None]*N)[i], None),
            loss_conv_dis_to_load_mwh=_num(R.get("loss_conv_dis_to_load_mwh", [None]*N)[i], None),
            wasted_surplus_due_to_export_cap_mwh=_num(R.get("wasted_surplus_due_to_export_cap_mwh", [None]*N)[i], None),
            cap_blocked_dis_ac_mwh=_num(R.get("cap_blocked_dis_ac_mwh", [None]*N)[i], None),
            cap_blocked_ch_ac_mwh=_num(R.get("cap_blocked_ch_ac_mwh", [None]*N)[i], None),
            unserved_load_after_cap_ac_mwh=_num(R.get("unserved_load_after_cap_ac_mwh", [None]*N)[i], None),
        ))
    async with con.cursor() as cur:
        await cur.executemany(ins, data)
        await cur.execute(f"ANALYZE {TABLE_STAGE_02};")
    await con.commit()
    log.info("[PERSIST-02] zapisano (n=%d)", len(data))

# ──────────────────────────────────────────────────────────────────────────────
# PERSIST: Stage 03 — PRICING
# ──────────────────────────────────────────────────────────────────────────────
async def persist_pricing(con: psycopg.AsyncConnection, calc_id, params_ts, pricing_rows: Sequence[Dict[str, Any]]) -> None:
    """Zapis do output.\"03_stage_pricing\"."""
    await ensure_tables(con)
    cols = """
        calc_id, params_ts, ts_utc, ts_local,
        price_import_pln_mwh, price_export_pln_mwh,
        rev_arbi_to_grid_pln, rev_surplus_export_pln,
        cost_grid_to_arbi_pln, cost_import_for_load_pln, cashflow_net_pln,
        loss_idle_oze_mwh, loss_idle_arbi_mwh, loss_conv_ch_mwh, loss_conv_dis_to_grid_mwh, loss_conv_dis_to_load_mwh,
        wasted_surplus_due_to_export_cap_mwh,
        pln_loss_conv_ch, pln_loss_conv_dis_to_grid, pln_loss_conv_dis_to_load, pln_loss_idle_arbi, pln_loss_idle_oze,
        pln_wasted_surplus_cap, losses_diag_total_pln,
        cap_blocked_dis_ac_mwh, cap_blocked_ch_ac_mwh, unserved_load_after_cap_ac_mwh,
        pln_cap_blocked_dis_ac, pln_cap_blocked_ch_ac, pln_unserved_load_after_cap, cap_opportunity_total_pln
    """
    ins = f"""
    INSERT INTO {TABLE_STAGE_03} ({cols})
    VALUES (
        %(calc_id)s, %(params_ts)s, %(ts_utc)s, %(ts_local)s,
        %(price_import_pln_mwh)s, %(price_export_pln_mwh)s,
        %(rev_arbi_to_grid_pln)s, %(rev_surplus_export_pln)s,
        %(cost_grid_to_arbi_pln)s, %(cost_import_for_load_pln)s, %(cashflow_net_pln)s,
        %(loss_idle_oze_mwh)s, %(loss_idle_arbi_mwh)s, %(loss_conv_ch_mwh)s, %(loss_conv_dis_to_grid_mwh)s, %(loss_conv_dis_to_load_mwh)s,
        %(wasted_surplus_due_to_export_cap_mwh)s,
        %(pln_loss_conv_ch)s, %(pln_loss_conv_dis_to_grid)s, %(pln_loss_conv_dis_to_load)s, %(pln_loss_idle_arbi)s, %(pln_loss_idle_oze)s,
        %(pln_wasted_surplus_cap)s, %(losses_diag_total_pln)s,
        %(cap_blocked_dis_ac_mwh)s, %(cap_blocked_ch_ac_mwh)s, %(unserved_load_after_cap_ac_mwh)s,
        %(pln_cap_blocked_dis_ac)s, %(pln_cap_blocked_ch_ac)s, %(pln_unserved_load_after_cap)s, %(cap_opportunity_total_pln)s
    )
    ON CONFLICT (calc_id, ts_utc) DO UPDATE SET
        params_ts = EXCLUDED.params_ts,
        ts_local  = EXCLUDED.ts_local,
        price_import_pln_mwh = EXCLUDED.price_import_pln_mwh,
        price_export_pln_mwh = EXCLUDED.price_export_pln_mwh,
        rev_arbi_to_grid_pln = EXCLUDED.rev_arbi_to_grid_pln,
        rev_surplus_export_pln = EXCLUDED.rev_surplus_export_pln,
        cost_grid_to_arbi_pln = EXCLUDED.cost_grid_to_arbi_pln,
        cost_import_for_load_pln = EXCLUDED.cost_import_for_load_pln,
        cashflow_net_pln = EXCLUDED.cashflow_net_pln,
        loss_idle_oze_mwh = EXCLUDED.loss_idle_oze_mwh,
        loss_idle_arbi_mwh = EXCLUDED.loss_idle_arbi_mwh,
        loss_conv_ch_mwh = EXCLUDED.loss_conv_ch_mwh,
        loss_conv_dis_to_grid_mwh = EXCLUDED.loss_conv_dis_to_grid_mwh,
        loss_conv_dis_to_load_mwh = EXCLUDED.loss_conv_dis_to_load_mwh,
        wasted_surplus_due_to_export_cap_mwh = EXCLUDED.wasted_surplus_due_to_export_cap_mwh,
        pln_loss_conv_ch = EXCLUDED.pln_loss_conv_ch,
        pln_loss_conv_dis_to_grid = EXCLUDED.pln_loss_conv_dis_to_grid,
        pln_loss_conv_dis_to_load = EXCLUDED.pln_loss_conv_dis_to_load,
        pln_loss_idle_arbi = EXCLUDED.pln_loss_idle_arbi,
        pln_loss_idle_oze  = EXCLUDED.pln_loss_idle_oze,
        pln_wasted_surplus_cap = EXCLUDED.pln_wasted_surplus_cap,
        losses_diag_total_pln = EXCLUDED.losses_diag_total_pln,
        cap_blocked_dis_ac_mwh = EXCLUDED.cap_blocked_dis_ac_mwh,
        cap_blocked_ch_ac_mwh  = EXCLUDED.cap_blocked_ch_ac_mwh,
        unserved_load_after_cap_ac_mwh = EXCLUDED.unserved_load_after_cap_ac_mwh,
        pln_cap_blocked_dis_ac = EXCLUDED.pln_cap_blocked_dis_ac,
        pln_cap_blocked_ch_ac  = EXCLUDED.pln_cap_blocked_ch_ac,
        pln_unserved_load_after_cap = EXCLUDED.pln_unserved_load_after_cap,
        cap_opportunity_total_pln = EXCLUDED.cap_opportunity_total_pln
    ;
    """
    data: List[Dict[str, Any]] = []
    for r in pricing_rows:
        data.append(dict(
            calc_id=calc_id,
            params_ts=params_ts,
            ts_utc=r["ts_utc"],
            ts_local=r.get("ts_local"),
            price_import_pln_mwh=r.get("price_import_pln_mwh"),
            price_export_pln_mwh=r.get("price_export_pln_mwh"),
            rev_arbi_to_grid_pln=r.get("rev_arbi_to_grid_pln"),
            rev_surplus_export_pln=r.get("rev_surplus_export_pln"),
            cost_grid_to_arbi_pln=r.get("cost_grid_to_arbi_pln"),
            cost_import_for_load_pln=r.get("cost_import_for_load_pln"),
            cashflow_net_pln=r.get("cashflow_net_pln"),
            loss_idle_oze_mwh=r.get("loss_idle_oze_mwh"),
            loss_idle_arbi_mwh=r.get("loss_idle_arbi_mwh"),
            loss_conv_ch_mwh=r.get("loss_conv_ch_mwh"),
            loss_conv_dis_to_grid_mwh=r.get("loss_conv_dis_to_grid_mwh"),
            loss_conv_dis_to_load_mwh=r.get("loss_conv_dis_to_load_mwh"),
            wasted_surplus_due_to_export_cap_mwh=r.get("wasted_surplus_due_to_export_cap_mwh"),
            pln_loss_conv_ch=r.get("pln_loss_conv_ch"),
            pln_loss_conv_dis_to_grid=r.get("pln_loss_conv_dis_to_grid"),
            pln_loss_conv_dis_to_load=r.get("pln_loss_conv_dis_to_load"),
            pln_loss_idle_arbi=r.get("pln_loss_idle_arbi"),
            pln_loss_idle_oze=r.get("pln_loss_idle_oze"),
            pln_wasted_surplus_cap=r.get("pln_wasted_surplus_cap"),
            losses_diag_total_pln=r.get("losses_diag_total_pln"),
            cap_blocked_dis_ac_mwh=r.get("cap_blocked_dis_ac_mwh"),
            cap_blocked_ch_ac_mwh=r.get("cap_blocked_ch_ac_mwh"),
            unserved_load_after_cap_ac_mwh=r.get("unserved_load_after_cap_ac_mwh"),
            pln_cap_blocked_dis_ac=r.get("pln_cap_blocked_dis_ac"),
            pln_cap_blocked_ch_ac=r.get("pln_cap_blocked_ch_ac"),
            pln_unserved_load_after_cap=r.get("pln_unserved_load_after_cap"),
            cap_opportunity_total_pln=r.get("cap_opportunity_total_pln"),
        ))
    async with con.cursor() as cur:
        await cur.executemany(ins, data)
        await cur.execute(f"ANALYZE {TABLE_STAGE_03};")
    await con.commit()
    log.info("[PERSIST-03] zapisano pricing (n=%d)", len(pricing_rows))

# ──────────────────────────────────────────────────────────────────────────────
# WRAPPERY dla runnera
# ──────────────────────────────────────────────────────────────────────────────
async def persist(con: psycopg.AsyncConnection, H_buf: Dict[str, Any], P_buf: Dict[str, Any], R_buf: Dict[str, Any]) -> None:
    """
    Wrapper zgodny z wywołaniem w runnerze:
        await raport.persist(con, H_buf, P_buf, C_buf)
    Pobiera calc_id/params_ts/N z H_buf i zapisuje Stage 00, 01, 02.
    """
    await ensure_tables(con)
    calc_id = H_buf.get("calc_id")
    params_ts = H_buf.get("params_ts")
    ts_utc = H_buf.get("ts_utc", [])  # <— bez 'or []'
    N = len(ts_utc)
    if not calc_id or N == 0:
        raise RuntimeError("[RAPORT.persist] Brak calc_id albo pusta oś czasu (ts_utc).")

    await persist_stage00(con, calc_id, params_ts, H_buf, N)
    await persist_stage01(con, calc_id, P_buf, ts_utc, N)
    await persist_stage02(con, calc_id, R_buf, ts_utc, N)
    log.info("[PERSIST] 00/01/02 zapisane (calc_id=%s, N=%d)", calc_id, N)

async def persist_stage04_pricing_detail(
    con: psycopg.AsyncConnection,
    *,
    calc_id,
    params_ts,
    pricing_rows: Sequence[Dict[str, Any]],
    truncate: bool = False,
) -> None:
    """
    Wrapper zgodny z runnerem (stare wywołanie):
        await raport.persist_stage04_pricing_detail(con, params_ts=..., calc_id=..., pricing_rows=..., truncate=True)
    Implementacja zapisuje do nowej tabeli output."03_stage_pricing".
    """
    await ensure_tables(con)
    if truncate:
        async with con.cursor() as cur:
            await cur.execute(f"DELETE FROM {TABLE_STAGE_03} WHERE calc_id = %s", (str(calc_id),))
        await con.commit()
        log.info("[PERSIST-03] wyczyszczono poprzednie wiersze (calc_id=%s)", calc_id)

    await persist_pricing(con, calc_id, params_ts, pricing_rows)
