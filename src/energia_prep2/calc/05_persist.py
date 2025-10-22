# 05_persist.py — ETAP 05 (PERSIST)
# ===============================================================
from __future__ import annotations

import os
import logging
from typing import Any, Dict, List, Sequence, Iterable, Iterator, Tuple
from time import perf_counter
import io, csv
import psycopg
import numpy as np

log = logging.getLogger("energia-prep-2.calc.persist_05")

# ── ENV ───────────────────────────────────────────────────────────────────────
CLEAR_MODE = os.getenv("CALC_CLEAR_MODE", "delete").strip().lower()
try:
    COPY_THRESHOLD = int(os.getenv("CALC_COPY_THRESHOLD", "5000"))
except ValueError:
    COPY_THRESHOLD = 5000
PERSIST_REINDEX = os.getenv("CALC_PERSIST_REINDEX", "1").strip().lower() not in ("0", "false", "no")

COPY_CHUNK_ROWS = int(os.getenv("CALC_COPY_CHUNK_ROWS", "20000"))   # ile wierszy na porcję
COPY_CHUNK_BYTES = int(os.getenv("CALC_COPY_CHUNK_BYTES", "4194304"))  # 4 MiB; 0 = wyłącz limit bajtów
COPY_USE_FREEZE = os.getenv("CALC_COPY_FREEZE", "1").strip().lower() not in ("0","false","no")

# ── Tabele i indeksy ─────────────────────────────────────────────────────────
TABLE_STAGE_01_INGEST   = 'output."_01_ingest"'
TABLE_STAGE_02_PROPOSER = 'output."_02_proposer"'
TABLE_STAGE_03_COMMIT   = 'output."_03_commit"'
TABLE_STAGE_04_PRICING  = 'output."_04_pricing"'

# Nazwy indeksów
IDX_01_TS  = "output_01_ingest_ts_utc_idx"
IDX_02_TS  = "output_02_proposer_ts_utc_idx"
IDX_03_TS  = "output_03_commit_ts_utc_idx"
IDX_04_TS  = "output_04_pricing_ts_utc_idx"

# Nazwy kwalifikowane schematem – używane tylko dla DROP INDEX
QIDX_01_TS = f"output.{IDX_01_TS}"
QIDX_02_TS = f"output.{IDX_02_TS}"
QIDX_03_TS = f"output.{IDX_03_TS}"
QIDX_04_TS = f"output.{IDX_04_TS}"

# ── Helpers: sanityzacja numpy → py ──────────────────────────────────────────
def _to_py_scalar(v: Any) -> Any:
    try:
        if isinstance(v, np.generic):
            return v.item()
    except Exception:
        pass
    return v

def _as_list(x: Any) -> List[Any]:
    if isinstance(x, (list, tuple)):
        return list(x)
    if isinstance(x, np.ndarray):
        return x.tolist()
    return [x]

def _num(x: Any, default: float | None = 0.0) -> float | None:
    try:
        x = _to_py_scalar(x)
        return float(x)
    except Exception:
        return default

def _bool(x: Any, default: bool | None = False) -> bool | None:
    try:
        x = _to_py_scalar(x)
        if isinstance(x, (int, float)) and x in (0, 1):
            return bool(int(x))
        return bool(x)
    except Exception:
        return default

def _get_vec(H: Dict[str, Any], key: str, N: int, default: Any = None) -> List[Any]:
    if key in H:
        v = _as_list(H[key])
        if len(v) == N:
            return [_to_py_scalar(t) for t in v]
    return [default] * N

# ── COPY helpers ─────────────────────────────────────────────────────────────
def _sanitize_value(v: Any) -> Any:
    if v is None:
        return None
    v = _to_py_scalar(v)
    if isinstance(v, str):
        s = v.strip()
        if s.startswith('"') and s.endswith('"') and len(s) >= 2:
            s = s[1:-1].strip()
        if s in (r"\N", "", "NULL", "None"):
            return None
    return v

def _sanitize_rows(rows_seq: Iterable[Sequence[Any]]) -> Iterator[List[Any]]:
    for row in rows_seq:
        yield [_sanitize_value(v) for v in row]

async def _copy_rows(con, table, columns, rows, freeze: bool = False) -> Tuple[float, float, int, int]:
    """
    COPY jako CSV w dużych chunkach.
    DLA NULL: wysyłamy puste pole (domyślne zachowanie COPY CSV → NULL).

    Zwraca: (csv_ms, write_ms, wrote_rows, bytes_total)
    """
    cols = ", ".join(columns)

    # Uwaga: nie ustawiamy "NULL '\N'". Zostawiamy domyślne mapowanie: pusty field => NULL.
    opts = ["FORMAT csv"]
    if freeze:
        opts.append("FREEZE true")

    sql = f"COPY {table} ({cols}) FROM STDIN WITH ({', '.join(opts)})"

    async with con.cursor() as cur:
        async with cur.copy(sql) as cp:
            buf = io.StringIO()
            writer = csv.writer(buf, lineterminator="\n")
            wrote = 0
            write_ms_total = 0.0
            bytes_total = 0
            t_total = perf_counter()

            for r in _sanitize_rows(rows):
                writer.writerow(["" if v is None else v for v in r])
                wrote += 1

                if (COPY_CHUNK_ROWS and (wrote % COPY_CHUNK_ROWS == 0)) or \
                   (COPY_CHUNK_BYTES and buf.tell() >= COPY_CHUNK_BYTES):
                    _val = buf.getvalue()
                    _t = perf_counter()
                    await cp.write(_val)
                    write_ms_total += (perf_counter() - _t) * 1000.0
                    bytes_total += len(_val)
                    buf.seek(0); buf.truncate(0)

            if buf.tell():
                _val = buf.getvalue()
                _t = perf_counter()
                await cp.write(_val)
                write_ms_total += (perf_counter() - _t) * 1000.0
                bytes_total += len(_val)

            total_ms = (perf_counter() - t_total) * 1000.0
            csv_ms = max(0.0, total_ms - write_ms_total)
            return (csv_ms, write_ms_total, wrote, bytes_total)

# ── DDL ──────────────────────────────────────────────────────────────────────
async def ensure_tables(con: psycopg.AsyncConnection) -> None:
    async with con.cursor() as cur:
        await cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_STAGE_01_INGEST} (
            calc_id uuid NOT NULL,
            params_ts timestamptz NULL,
            ts_utc timestamptz NOT NULL,
            y integer NULL, m integer NULL, d integer NULL, h integer NULL, dow integer NULL,
            is_work boolean NULL, is_free boolean NULL,
            zone_off boolean NULL, zone_peak_am boolean NULL, zone_peak_pm boolean NULL, moc_peak boolean NULL,
            price_import_pln_mwh double precision NULL,
            price_export_pln_mwh double precision NULL,
            price_dyst_pln_mwh   double precision NULL,
            price_sys_pln_mwh    double precision NULL,
            bess_losses_rel      double precision NULL,
            base_min_profit_pln_mwh double precision NULL,
            cycles_per_day double precision NULL,
            allow_carry_over boolean NULL, force_order boolean NULL,
            bonus_ch_window double precision NULL, bonus_dis_window double precision NULL,
            bonus_low_soc_ch double precision NULL, bonus_high_soc_dis double precision NULL,
            soc_low_threshold double precision NULL, soc_high_threshold double precision NULL,
            emax_total_mwh double precision NULL, emax_oze_mwh double precision NULL, emax_arbi_mwh double precision NULL,
            frac_oze double precision NULL,
            p_ch_max_mw double precision NULL, p_dis_max_mw double precision NULL,
            eta_charger double precision NULL, eta_discharger double precision NULL,
            self_discharge_per_h double precision NULL,
            PRIMARY KEY (calc_id, ts_utc)
        );""")
        await cur.execute(f'CREATE INDEX IF NOT EXISTS {IDX_01_TS} ON {TABLE_STAGE_01_INGEST}(ts_utc);')

        await cur.execute(f'ALTER TABLE {TABLE_STAGE_01_INGEST} ADD COLUMN IF NOT EXISTS prod_pv_pp_mwh double precision NULL')
        await cur.execute(f'ALTER TABLE {TABLE_STAGE_01_INGEST} ADD COLUMN IF NOT EXISTS prod_pv_wz_mwh double precision NULL')
        await cur.execute(f'ALTER TABLE {TABLE_STAGE_01_INGEST} ADD COLUMN IF NOT EXISTS prod_wiatr_mwh double precision NULL')
        await cur.execute(f'ALTER TABLE {TABLE_STAGE_01_INGEST} ADD COLUMN IF NOT EXISTS prod_total_mwh double precision NULL')
        await cur.execute(f'ALTER TABLE {TABLE_STAGE_01_INGEST} ADD COLUMN IF NOT EXISTS load_total_mwh double precision NULL')
        await cur.execute(f'ALTER TABLE {TABLE_STAGE_01_INGEST} ADD COLUMN IF NOT EXISTS surplus_net_mwh double precision NULL')
        await cur.execute(f'ALTER TABLE {TABLE_STAGE_01_INGEST} ADD COLUMN IF NOT EXISTS deficit_net_mwh double precision NULL')
        await cur.execute(f'ALTER TABLE {TABLE_STAGE_01_INGEST} ADD COLUMN IF NOT EXISTS net_flow_mwh double precision NULL')

        await cur.execute(f'ALTER TABLE {TABLE_STAGE_01_INGEST} ADD COLUMN IF NOT EXISTS cap_bess_charge_net_mwh double precision NULL')
        await cur.execute(f'ALTER TABLE {TABLE_STAGE_01_INGEST} ADD COLUMN IF NOT EXISTS cap_bess_discharge_net_mwh double precision NULL')
        await cur.execute(f'ALTER TABLE {TABLE_STAGE_01_INGEST} ADD COLUMN IF NOT EXISTS cap_grid_import_ac_mwh double precision NULL')
        await cur.execute(f'ALTER TABLE {TABLE_STAGE_01_INGEST} ADD COLUMN IF NOT EXISTS cap_grid_export_ac_mwh double precision NULL')

        await cur.execute(f'ALTER TABLE {TABLE_STAGE_01_INGEST} ADD COLUMN IF NOT EXISTS bonus_hrs_ch_list jsonb NULL')
        await cur.execute(f'ALTER TABLE {TABLE_STAGE_01_INGEST} ADD COLUMN IF NOT EXISTS bonus_hrs_dis_list jsonb NULL')
        await cur.execute(f'ALTER TABLE {TABLE_STAGE_01_INGEST} ADD COLUMN IF NOT EXISTS bonus_hrs_ch_free_list jsonb NULL')
        await cur.execute(f'ALTER TABLE {TABLE_STAGE_01_INGEST} ADD COLUMN IF NOT EXISTS bonus_hrs_dis_free_list jsonb NULL')

        # ── 02: rozszerzona definicja z czasówkami, ceną i diagnostyką
        await cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_STAGE_02_PROPOSER} (
            calc_id uuid NOT NULL,
            ts_utc  timestamptz NOT NULL,
            y integer NULL, m integer NULL, d integer NULL, h integer NULL, dow integer NULL,
            is_work boolean NULL, is_free boolean NULL,
            price_energy_pln_mwh double precision NULL,

            prop_arbi_ch_from_grid_ac_mwh double precision NULL,
            prop_arbi_dis_to_grid_ac_mwh  double precision NULL,

            dec_ch boolean NULL, dec_dis boolean NULL,
            dec_ch_base boolean NULL, dec_dis_base boolean NULL,

            thr_low double precision NULL, thr_high double precision NULL,
            delta_k double precision NULL,
            margin_ch double precision NULL, margin_dis double precision NULL,

            -- DIAGNOSTYKA metodyki arbitrażu:
            pending_arbi_net_mwh double precision NULL,
            soc_arbi_sim_mwh double precision NULL,
            in_ch_bonus boolean NULL,
            in_dis_bonus boolean NULL,
            low_soc_bonus_hit boolean NULL,
            high_soc_bonus_hit boolean NULL,
            cycles_used_today double precision NULL,
            dk_minus_thr_low double precision NULL,
            dk_minus_thr_high double precision NULL,
            pair_low_hour double precision NULL,
            pair_high_hour double precision NULL,

            PRIMARY KEY (calc_id, ts_utc)
        );""")
        await cur.execute(f'CREATE INDEX IF NOT EXISTS {IDX_02_TS} ON {TABLE_STAGE_02_PROPOSER}(ts_utc);')

        await cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_STAGE_03_COMMIT} (
            calc_id uuid NOT NULL,
            ts_utc  timestamptz NOT NULL,
            y integer NULL, m integer NULL, d integer NULL, h integer NULL, dow integer NULL,
            is_work boolean NULL, is_free boolean NULL,
            e_import_mwh double precision NULL, e_export_mwh double precision NULL,
            charge_from_surplus_mwh double precision NULL,
            charge_from_grid_mwh double precision NULL,
            discharge_to_load_mwh double precision NULL,
            discharge_to_grid_mwh double precision NULL,
            export_from_surplus_ac_mwh double precision NULL,
            export_from_arbi_ac_mwh double precision NULL,
            import_for_load_ac_mwh double precision NULL,
            import_for_arbi_ac_mwh double precision NULL,
            soc_oze_before_idle_mwh double precision NULL, soc_oze_after_idle_mwh double precision NULL,
            soc_arbi_before_idle_mwh double precision NULL, soc_arbi_after_idle_mwh double precision NULL,
            soc_arbi_pct_of_arbi double precision NULL, soc_arbi_pct_of_total double precision NULL,
            -- DODANE: procenty dla OZE
            soc_oze_pct_of_oze double precision NULL, soc_oze_pct_of_total double precision NULL,
            loss_idle_oze_mwh double precision NULL, loss_idle_arbi_mwh double precision NULL,
            loss_conv_ch_mwh double precision NULL, loss_conv_dis_to_grid_mwh double precision NULL, loss_conv_dis_to_load_mwh double precision NULL,
            wasted_surplus_due_to_export_cap_mwh double precision NULL,
            cap_blocked_dis_ac_mwh double precision NULL, cap_blocked_ch_ac_mwh double precision NULL, unserved_load_after_cap_ac_mwh double precision NULL,
            -- NOWE 3 KOLUMNY FINANSOWE (tylko ARBI):
            rev_arbi_to_grid_pln double precision NULL,
            cost_grid_to_arbi_pln double precision NULL,
            cashflow_arbi_pln double precision NULL,
            PRIMARY KEY (calc_id, ts_utc)
        );""")
        await cur.execute(f'CREATE INDEX IF NOT EXISTS {IDX_03_TS} ON {TABLE_STAGE_03_COMMIT}(ts_utc);')


        await cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_STAGE_04_PRICING} (
            calc_id uuid NOT NULL,
            params_ts timestamptz NULL,
            ts_utc timestamptz NOT NULL,
            ts_local timestamp NULL,
            y integer NULL, m integer NULL, d integer NULL, h integer NULL, dow integer NULL,
            is_work boolean NULL, is_free boolean NULL,
            price_import_pln_mwh double precision NULL, price_export_pln_mwh double precision NULL,
            rev_arbi_to_grid_pln double precision NULL, rev_surplus_export_pln double precision NULL,
            cost_grid_to_arbi_pln double precision NULL, cost_import_for_load_pln double precision NULL, cashflow_net_pln double precision NULL,
            loss_idle_oze_mwh double precision NULL, loss_idle_arbi_mwh double precision NULL,
            loss_conv_ch_mwh double precision NULL, loss_conv_dis_to_grid_mwh double precision NULL, loss_conv_dis_to_load_mwh double precision NULL,
            wasted_surplus_due_to_export_cap_mwh double precision NULL,
            pln_loss_conv_ch double precision NULL, pln_loss_conv_dis_to_grid double precision NULL, pln_loss_conv_dis_to_load double precision NULL,
            pln_loss_idle_arbi double precision NULL, pln_loss_idle_oze double precision NULL,
            pln_wasted_surplus_cap double precision NULL, losses_diag_total_pln double precision NULL,
            cap_blocked_dis_ac_mwh double precision NULL, cap_blocked_ch_ac_mwh double precision NULL, unserved_load_after_cap_ac_mwh double precision NULL,
            pln_cap_blocked_dis_ac double precision NULL, pln_cap_blocked_ch_ac double precision NULL, pln_unserved_load_after_cap double precision NULL,
            cap_opportunity_total_pln double precision NULL,
            PRIMARY KEY (calc_id, ts_utc)
        );""")
        await cur.execute(f'CREATE INDEX IF NOT EXISTS {IDX_04_TS} ON {TABLE_STAGE_04_PRICING}(ts_utc);')

# ── CLEAR_MODE ────────────────────────────────────────────────────────────────
async def _clear_stage_tables(con: psycopg.AsyncConnection, *, mode: str, calc_id: str, in_txn: bool = False) -> None:
    mode = (mode or "").strip().lower()
    tables = [TABLE_STAGE_01_INGEST, TABLE_STAGE_02_PROPOSER, TABLE_STAGE_03_COMMIT, TABLE_STAGE_04_PRICING]

    async with con.cursor() as cur:
        if mode == "delete":
            for t in tables:
                await cur.execute(f'DELETE FROM {t} WHERE calc_id = %s', (str(calc_id),))
            log.info('[05/PERSIST] CLEAR_MODE=delete — usunięto poprzednie wiersze dla calc_id=%s', calc_id)
        elif mode == "truncate":
            await cur.execute(f'TRUNCATE {", ".join(tables)}')
            log.info('[05/PERSIST] CLEAR_MODE=truncate — wyczyszczono wszystkie tabele stage.')
        elif mode == "drop":
            for t in tables:
                await cur.execute(f'DROP TABLE IF EXISTS {t} CASCADE')
            log.info('[05/PERSIST] CLEAR_MODE=drop — zdropowano tabele stage.')
            if not in_txn:
                await con.commit()
            await ensure_tables(con)
            return
        else:
            log.info('[05/PERSIST] CLEAR_MODE=%s — brak czyszczenia.', mode or "(empty)")

    if not in_txn:
        await con.commit()

# ── PERSIST: 01_INGEST ───────────────────────────────────────────────────────
COLS_01 = [
    "calc_id","params_ts","ts_utc","y","m","d","h","dow","is_work","is_free",
    "zone_off","zone_peak_am","zone_peak_pm","moc_peak",
    "price_import_pln_mwh","price_export_pln_mwh","price_dyst_pln_mwh","price_sys_pln_mwh","bess_losses_rel",
    "base_min_profit_pln_mwh","cycles_per_day","allow_carry_over","force_order",
    "bonus_ch_window","bonus_dis_window","bonus_low_soc_ch","bonus_high_soc_dis",
    "soc_low_threshold","soc_high_threshold",
    "emax_total_mwh","emax_oze_mwh","emax_arbi_mwh","frac_oze",
    "p_ch_max_mw","p_dis_max_mw","eta_charger","eta_discharger","self_discharge_per_h",
    "prod_pv_pp_mwh","prod_pv_wz_mwh","prod_wiatr_mwh","prod_total_mwh","load_total_mwh",
    "surplus_net_mwh","deficit_net_mwh","net_flow_mwh",
    "cap_bess_charge_net_mwh","cap_bess_discharge_net_mwh","cap_grid_import_ac_mwh","cap_grid_export_ac_mwh",
    "bonus_hrs_ch_list","bonus_hrs_dis_list","bonus_hrs_ch_free_list","bonus_hrs_dis_free_list"
]

async def persist_stage01_ingest(con: psycopg.AsyncConnection, calc_id, params_ts, H: Dict[str, Any], N: int, *, in_txn: bool = False) -> None:
    ts_utc_seq = _get_vec(H, "ts_utc", N)
    y_seq      = _get_vec(H, "y", N); m_seq = _get_vec(H, "m", N); d_seq = _get_vec(H, "d", N)
    h_seq      = _get_vec(H, "h", N); dow_seq = _get_vec(H, "dow", N)
    is_work    = [_bool(v, None) for v in _get_vec(H, "is_work", N)]
    is_free    = [_bool(v, None) for v in _get_vec(H, "is_free", N)]
    zone_off   = [_bool(v, None) for v in _get_vec(H, "zone_off", N)]
    zone_peak_am = [_bool(v, None) for v in _get_vec(H, "zone_peak_am", N)]
    zone_peak_pm = [_bool(v, None) for v in _get_vec(H, "zone_peak_pm", N)]
    moc_peak   = [_bool(v, None) for v in _get_vec(H, "moc_peak", N)]
    price_import = _get_vec(H, "price_import_pln_mwh", N, None)
    price_export = _get_vec(H, "price_export_pln_mwh", N, None)
    price_dyst   = _get_vec(H, "price_dyst_pln_mwh", N, None)
    price_sys    = _get_vec(H, "price_sys_pln_mwh", N, None)
    bess_losses  = _get_vec(H, "bess_losses_rel", N, None)

    base_min_profit_pln_mwh = _num(H.get("base_min_profit_pln_mwh"), None)
    cycles_per_day          = _num(H.get("cycles_per_day"), None)
    allow_carry_over        = _bool(H.get("allow_carry_over"), None)
    force_order             = _bool(H.get("force_order"), None)
    bonus_ch_window         = _num(H.get("bonus_ch_window"), None)
    bonus_dis_window        = _num(H.get("bonus_dis_window"), None)
    bonus_low_soc_ch        = _num(H.get("bonus_low_soc_ch"), None)
    bonus_high_soc_dis      = _num(H.get("bonus_high_soc_dis"), None)
    soc_low_threshold       = _num(H.get("soc_low_threshold"), None)
    soc_high_threshold      = _num(H.get("soc_high_threshold"), None)
    emax_total_mwh   = _num(H.get("emax_total_mwh"), None)
    emax_oze_mwh     = _num(H.get("emax_oze_mwh"), None)
    emax_arbi_mwh    = _num(H.get("emax_arbi_mwh"), None)
    frac_oze         = _num(H.get("frac_oze"), None)
    p_ch_max_mw      = _num(H.get("p_ch_max_mw"), None)
    p_dis_max_mw     = _num(H.get("p_dis_max_mw"), None)
    # dopasowanie do 01_ingest:
    eta_charger      = _num(H.get("eta_ch_frac"), None)
    eta_discharger   = _num(H.get("eta_dis_frac"), None)
    self_discharge_h = _num(H.get("bess_lambda_h_frac"), None)

    pv_pp   = _get_vec(H, "prod_pv_pp_mwh", N, None)
    pv_wz   = _get_vec(H, "prod_pv_wz_mwh", N, None)
    wiatr   = _get_vec(H, "prod_wiatr_mwh", N, None)
    prod_tot_declared = _get_vec(H, "prod_total_mwh", N, None)
    load_tot = _get_vec(H, "load_total_mwh", N, None)
    surplus_net = _get_vec(H, "surplus_net_mwh", N, None)
    deficit_net = _get_vec(H, "deficit_net_mwh", N, None)
    net_flow = _get_vec(H, "net_flow_mwh", N, None)
    if all(v is None for v in load_tot):
        load_tot = _get_vec(H, "load_net_mwh", N, None)

    # NOWE: CAPY (z 01_ingest/H)
    cap_bess_charge = _get_vec(H, "cap_bess_charge_net_mwh",    N, None)
    cap_bess_dis    = _get_vec(H, "cap_bess_discharge_net_mwh", N, None)
    cap_grid_imp    = _get_vec(H, "cap_grid_import_ac_mwh",     N, None)
    cap_grid_exp    = _get_vec(H, "cap_grid_export_ac_mwh",     N, None)

    prod_tot = []
    for i in range(N):
        a, b, c, t = pv_pp[i], pv_wz[i], wiatr[i], prod_tot_declared[i]
        if t is not None:
            prod_tot.append(_num(t, None))
        else:
            if a is None or b is None or c is None:
                prod_tot.append(None)
            else:
                try:
                    prod_tot.append(float(a) + float(b) + float(c))
                except Exception:
                    prod_tot.append(None)

    rows_dict: List[Dict[str, Any]] = []
    for i in range(N):
        rows_dict.append(dict(
            calc_id=calc_id, params_ts=params_ts, ts_utc=ts_utc_seq[i],
            y=y_seq[i], m=m_seq[i], d=d_seq[i], h=h_seq[i], dow=dow_seq[i],
            is_work=is_work[i], is_free=is_free[i],
            zone_off=zone_off[i], zone_peak_am=zone_peak_am[i], zone_peak_pm=zone_peak_pm[i], moc_peak=moc_peak[i],
            price_import_pln_mwh=price_import[i],
            price_export_pln_mwh=price_export[i],
            price_dyst_pln_mwh=price_dyst[i], price_sys_pln_mwh=price_sys[i],
            bess_losses_rel=bess_losses[i],
            base_min_profit_pln_mwh=base_min_profit_pln_mwh, cycles_per_day=cycles_per_day,
            allow_carry_over=allow_carry_over, force_order=force_order,
            bonus_ch_window=bonus_ch_window, bonus_dis_window=bonus_dis_window,
            bonus_low_soc_ch=bonus_low_soc_ch, bonus_high_soc_dis=bonus_high_soc_dis,
            soc_low_threshold=soc_low_threshold, soc_high_threshold=soc_high_threshold,
            emax_total_mwh=emax_total_mwh, emax_oze_mwh=emax_oze_mwh, emax_arbi_mwh=emax_arbi_mwh,
            frac_oze=frac_oze, p_ch_max_mw=p_ch_max_mw, p_dis_max_mw=p_dis_max_mw,
            eta_charger=eta_charger, eta_discharger=eta_discharger, self_discharge_per_h=self_discharge_h,
            prod_pv_pp_mwh=_num(pv_pp[i], None),
            prod_pv_wz_mwh=_num(pv_wz[i], None),
            prod_wiatr_mwh=_num(wiatr[i], None),
            prod_total_mwh=_num(prod_tot[i], None),
            load_total_mwh=_num(load_tot[i], None),
            surplus_net_mwh=_num(surplus_net[i], None),
            deficit_net_mwh=_num(deficit_net[i], None),
            net_flow_mwh=_num(net_flow[i], None),
            cap_bess_charge_net_mwh=_num(cap_bess_charge[i], None),
            cap_bess_discharge_net_mwh=_num(cap_bess_dis[i], None),
            cap_grid_import_ac_mwh=_num(cap_grid_imp[i], None),
            cap_grid_export_ac_mwh=_num(cap_grid_exp[i], None),
            bonus_hrs_ch_list=H.get("bonus_hrs_ch_list", []),
            bonus_hrs_dis_list=H.get("bonus_hrs_dis_list", []),
            bonus_hrs_ch_free_list=H.get("bonus_hrs_ch_free_list", []),
            bonus_hrs_dis_free_list=H.get("bonus_hrs_dis_free_list", []),
        ))

    if N >= COPY_THRESHOLD:
        rows_seq = [[r[c] for c in COLS_01] for r in rows_dict]
        await _copy_rows(con, TABLE_STAGE_01_INGEST, COLS_01, rows_seq, freeze=COPY_USE_FREEZE)
    else:
        cols = ", ".join(COLS_01)
        placeholders = ", ".join([f"%({c})s" for c in COLS_01])
        ins = f"""
        INSERT INTO {TABLE_STAGE_01_INGEST} ({cols})
        VALUES ({placeholders})
        ON CONFLICT (calc_id, ts_utc) DO UPDATE SET
            params_ts = EXCLUDED.params_ts,
            y = EXCLUDED.y, m = EXCLUDED.m, d = EXCLUDED.d, h = EXCLUDED.h, dow = EXCLUDED.dow,
            is_work = EXCLUDED.is_work, is_free = EXCLUDED.is_free,
            zone_off = EXCLUDED.zone_off, zone_peak_am = EXCLUDED.zone_peak_am, zone_peak_pm = EXCLUDED.zone_peak_pm, moc_peak = EXCLUDED.moc_peak,
            price_import_pln_mwh = EXCLUDED.price_import_pln_mwh,
            price_export_pln_mwh = EXCLUDED.price_export_pln_mwh,
            price_dyst_pln_mwh   = EXCLUDED.price_dyst_pln_mwh,
            price_sys_pln_mwh    = EXCLUDED.price_sys_pln_mwh,
            bess_losses_rel      = EXCLUDED.bess_losses_rel,
            base_min_profit_pln_mwh = EXCLUDED.base_min_profit_pln_mwh,
            cycles_per_day          = EXCLUDED.cycles_per_day,
            allow_carry_over        = EXCLUDED.allow_carry_over,
            force_order             = EXCLUDED.force_order,
            bonus_ch_window         = EXCLUDED.bonus_ch_window,
            bonus_dis_window        = EXCLUDED.bonus_dis_window,
            bonus_low_soc_ch        = EXCLUDED.bonus_low_soc_ch,
            bonus_high_soc_dis      = EXCLUDED.bonus_high_soc_dis,
            soc_low_threshold       = EXCLUDED.soc_low_threshold,
            soc_high_threshold      = EXCLUDED.soc_high_threshold,
            emax_total_mwh          = EXCLUDED.emax_total_mwh,
            emax_oze_mwh            = EXCLUDED.emax_oze_mwh,
            emax_arbi_mwh           = EXCLUDED.emax_arbi_mwh,
            frac_oze                = EXCLUDED.frac_oze,
            p_ch_max_mw             = EXCLUDED.p_ch_max_mw,
            p_dis_max_mw            = EXCLUDED.p_dis_max_mw,
            eta_charger             = EXCLUDED.eta_charger,
            eta_discharger          = EXCLUDED.eta_discharger,
            self_discharge_per_h    = EXCLUDED.self_discharge_per_h,
            prod_pv_pp_mwh          = EXCLUDED.prod_pv_pp_mwh,
            prod_pv_wz_mwh          = EXCLUDED.prod_pv_wz_mwh,
            prod_wiatr_mwh          = EXCLUDED.prod_wiatr_mwh,
            prod_total_mwh          = EXCLUDED.prod_total_mwh,
            load_total_mwh          = EXCLUDED.load_total_mwh,
            surplus_net_mwh         = EXCLUDED.surplus_net_mwh,
            deficit_net_mwh         = EXCLUDED.deficit_net_mwh,
            net_flow_mwh            = EXCLUDED.net_flow_mwh,
            cap_bess_charge_net_mwh    = EXCLUDED.cap_bess_charge_net_mwh,
            cap_bess_discharge_net_mwh = EXCLUDED.cap_bess_discharge_net_mwh,
            cap_grid_import_ac_mwh     = EXCLUDED.cap_grid_import_ac_mwh,
            cap_grid_export_ac_mwh     = EXCLUDED.cap_grid_export_ac_mwh,
            bonus_hrs_ch_list          = EXCLUDED.bonus_hrs_ch_list,
            bonus_hrs_dis_list         = EXCLUDED.bonus_hrs_dis_list,
            bonus_hrs_ch_free_list     = EXCLUDED.bonus_hrs_ch_free_list,
            bonus_hrs_dis_free_list    = EXCLUDED.bonus_hrs_dis_free_list
        ;
        """
        async with con.cursor() as cur:
            await cur.executemany(ins, rows_dict)

    if not in_txn:
        async with con.cursor() as cur:
            await cur.execute(f"ANALYZE {TABLE_STAGE_01_INGEST};")
        await con.commit()
    log.info("[05/PERSIST] zapisano 01_ingest (n=%d, mode=%s)", N, "COPY" if N >= COPY_THRESHOLD else "executemany")

# ── PERSIST: 02_PROPOSER (rozszerzone raportowanie) ──────────────────────────
COLS_02 = [
    "calc_id","ts_utc",
    "y","m","d","h","dow","is_work","is_free",
    "price_energy_pln_mwh",

    "prop_arbi_ch_from_grid_ac_mwh","prop_arbi_dis_to_grid_ac_mwh",
    "dec_ch","dec_dis","dec_ch_base","dec_dis_base",
    "thr_low","thr_high","delta_k","margin_ch","margin_dis",

    "pending_arbi_net_mwh","soc_arbi_sim_mwh",
    "in_ch_bonus","in_dis_bonus","low_soc_bonus_hit","high_soc_bonus_hit",
    "cycles_used_today","dk_minus_thr_low","dk_minus_thr_high",
    "pair_low_hour","pair_high_hour"
]

async def persist_stage02_proposer(
    con: psycopg.AsyncConnection,
    calc_id,
    H: Dict[str, Any],
    P: Dict[str, Any],
    ts_utc_seq: Sequence[Any],
    N: int,
    *,
    in_txn: bool = False
) -> None:
    # Czasówki i cena (z H_buf)
    y_seq      = _get_vec(H, "y", N); m_seq = _get_vec(H, "m", N); d_seq = _get_vec(H, "d", N)
    h_seq      = _get_vec(H, "h", N); dow_seq = _get_vec(H, "dow", N)
    is_work    = [_bool(v, None) for v in _get_vec(H, "is_work", N)]
    is_free    = [_bool(v, None) for v in _get_vec(H, "is_free", N)]
    # dopasowanie do 01_ingest: brak "price_energy_pln_mwh" → użyj importu (Fixing I)
    price_energy = _get_vec(H, "price_import_pln_mwh", N)

    # Dane decyzji i progów
    prop_ch   = _as_list(P.get("prop_arbi_ch_from_grid_ac_mwh", [None]*N))
    prop_dis  = _as_list(P.get("prop_arbi_dis_to_grid_ac_mwh", [None]*N))
    dec_ch    = _as_list(P.get("dec_ch", [None]*N))
    dec_dis   = _as_list(P.get("dec_dis", [None]*N))
    dec_ch_b  = _as_list(P.get("dec_ch_base", [None]*N))
    dec_dis_b = _as_list(P.get("dec_dis_base", [None]*N))
    thr_low   = _as_list(P.get("thr_low", [None]*N))
    thr_high  = _as_list(P.get("thr_high", [None]*N))
    delta_k   = _as_list(P.get("delta_k", [None]*N))
    margin_ch = _as_list(P.get("margin_ch", [None]*N))
    margin_dis= _as_list(P.get("margin_dis", [None]*N))

    # DIAG z 02
    pending_arbi_net_mwh = _as_list(P.get("pending_arbi_net_mwh", [None]*N))
    soc_arbi_sim_mwh     = _as_list(P.get("soc_arbi_sim_mwh", [None]*N))
    in_ch_bonus          = _as_list(P.get("in_ch_bonus", [None]*N))
    in_dis_bonus         = _as_list(P.get("in_dis_bonus", [None]*N))
    low_soc_bonus_hit    = _as_list(P.get("low_soc_bonus_hit", [None]*N))
    high_soc_bonus_hit   = _as_list(P.get("high_soc_bonus_hit", [None]*N))
    cycles_used_today    = _as_list(P.get("cycles_used_today", [None]*N))
    dk_minus_thr_low     = _as_list(P.get("dk_minus_thr_low", [None]*N))
    dk_minus_thr_high    = _as_list(P.get("dk_minus_thr_high", [None]*N))
    pair_low_hour        = _as_list(P.get("pair_low_hour", [None]*N))
    pair_high_hour       = _as_list(P.get("pair_high_hour", [None]*N))

    if N >= COPY_THRESHOLD:
        rows_seq = [
            [
                calc_id, ts_utc_seq[i],
                y_seq[i], m_seq[i], d_seq[i], h_seq[i], dow_seq[i], is_work[i], is_free[i],
                _num(price_energy[i], None),

                _num(prop_ch[i], None), _num(prop_dis[i], None),
                _bool(dec_ch[i], None), _bool(dec_dis[i], None),
                _bool(dec_ch_b[i], None), _bool(dec_dis_b[i], None),
                _num(thr_low[i], None), _num(thr_high[i], None),
                _num(delta_k[i], None), _num(margin_ch[i], None), _num(margin_dis[i], None),

                _num(pending_arbi_net_mwh[i], None), _num(soc_arbi_sim_mwh[i], None),
                _bool(in_ch_bonus[i], None), _bool(in_dis_bonus[i], None),
                _bool(low_soc_bonus_hit[i], None), _bool(high_soc_bonus_hit[i], None),
                _num(cycles_used_today[i], None), _num(dk_minus_thr_low[i], None), _num(dk_minus_thr_high[i], None),
                _num(pair_low_hour[i], None), _num(pair_high_hour[i], None)
            ]
            for i in range(N)
        ]
        csv_ms, write_ms, wrote, b_total = await _copy_rows(con, TABLE_STAGE_02_PROPOSER, COLS_02, rows_seq, freeze=COPY_USE_FREEZE)
        log.info("[05/PERSIST][02] COPY rows=%d bytes=%d csv=%.1fms write=%.1fms", wrote, b_total, csv_ms, write_ms)
    else:
        cols = ", ".join(COLS_02)
        placeholders = ", ".join([f"%({c})s" for c in COLS_02])
        ins = f"""
        INSERT INTO {TABLE_STAGE_02_PROPOSER} ({cols})
        VALUES ({placeholders})
        ON CONFLICT (calc_id, ts_utc) DO UPDATE SET
            y = EXCLUDED.y, m = EXCLUDED.m, d = EXCLUDED.d, h = EXCLUDED.h, dow = EXCLUDED.dow,
            is_work = EXCLUDED.is_work, is_free = EXCLUDED.is_free,
            price_energy_pln_mwh = EXCLUDED.price_energy_pln_mwh,

            prop_arbi_ch_from_grid_ac_mwh = EXCLUDED.prop_arbi_ch_from_grid_ac_mwh,
            prop_arbi_dis_to_grid_ac_mwh  = EXCLUDED.prop_arbi_dis_to_grid_ac_mwh,
            dec_ch = EXCLUDED.dec_ch, dec_dis= EXCLUDED.dec_dis,
            dec_ch_base = EXCLUDED.dec_ch_base, dec_dis_base= EXCLUDED.dec_dis_base,
            thr_low = EXCLUDED.thr_low, thr_high= EXCLUDED.thr_high,
            delta_k = EXCLUDED.delta_k, margin_ch = EXCLUDED.margin_ch, margin_dis= EXCLUDED.margin_dis,

            pending_arbi_net_mwh = EXCLUDED.pending_arbi_net_mwh,
            soc_arbi_sim_mwh = EXCLUDED.soc_arbi_sim_mwh,
            in_ch_bonus = EXCLUDED.in_ch_bonus, in_dis_bonus = EXCLUDED.in_dis_bonus,
            low_soc_bonus_hit = EXCLUDED.low_soc_bonus_hit, high_soc_bonus_hit = EXCLUDED.high_soc_bonus_hit,
            cycles_used_today = EXCLUDED.cycles_used_today,
            dk_minus_thr_low = EXCLUDED.dk_minus_thr_low, dk_minus_thr_high = EXCLUDED.dk_minus_thr_high,
            pair_low_hour = EXCLUDED.pair_low_hour, pair_high_hour = EXCLUDED.pair_high_hour
        ;"""
        rows_dict: List[Dict[str, Any]] = []
        for i in range(N):
            rows_dict.append(dict(
                calc_id=calc_id, ts_utc=ts_utc_seq[i],
                y=y_seq[i], m=m_seq[i], d=d_seq[i], h=h_seq[i], dow=dow_seq[i],
                is_work=is_work[i], is_free=is_free[i],
                price_energy_pln_mwh=_num(price_energy[i], None),

                prop_arbi_ch_from_grid_ac_mwh=_num(prop_ch[i], None),
                prop_arbi_dis_to_grid_ac_mwh=_num(prop_dis[i], None),
                dec_ch=_bool(dec_ch[i], None), dec_dis=_bool(dec_dis[i], None),
                dec_ch_base=_bool(dec_ch_b[i], None), dec_dis_base=_bool(dec_dis_b[i], None),
                thr_low=_num(thr_low[i], None), thr_high=_num(thr_high[i], None),
                delta_k=_num(delta_k[i], None), margin_ch=_num(margin_ch[i], None), margin_dis=_num(margin_dis[i], None),

                pending_arbi_net_mwh=_num(pending_arbi_net_mwh[i], None),
                soc_arbi_sim_mwh=_num(soc_arbi_sim_mwh[i], None),
                in_ch_bonus=_bool(in_ch_bonus[i], None),
                in_dis_bonus=_bool(in_dis_bonus[i], None),
                low_soc_bonus_hit=_bool(low_soc_bonus_hit[i], None),
                high_soc_bonus_hit=_bool(high_soc_bonus_hit[i], None),
                cycles_used_today=_num(cycles_used_today[i], None),
                dk_minus_thr_low=_num(dk_minus_thr_low[i], None),
                dk_minus_thr_high=_num(dk_minus_thr_high[i], None),
                pair_low_hour=_num(pair_low_hour[i], None),
                pair_high_hour=_num(pair_high_hour[i], None),
            ))
        async with con.cursor() as cur:
            await cur.executemany(ins, rows_dict)

    if not in_txn:
        async with con.cursor() as cur:
            await cur.execute(f"ANALYZE {TABLE_STAGE_02_PROPOSER};")
        await con.commit()
    log.info("[05/PERSIST] zapisano 02_proposer (n=%d, mode=%s)", N, "COPY" if N >= COPY_THRESHOLD else "executemany")

# ── PERSIST: 03_COMMIT ───────────────────────────────────────────────────────
COLS_03 = [
    "calc_id","ts_utc",
    "y","m","d","h","dow","is_work","is_free",
    "e_import_mwh","e_export_mwh",
    "charge_from_surplus_mwh","charge_from_grid_mwh","discharge_to_load_mwh","discharge_to_grid_mwh",
    "export_from_surplus_ac_mwh","export_from_arbi_ac_mwh","import_for_load_ac_mwh","import_for_arbi_ac_mwh",
    "soc_oze_before_idle_mwh","soc_oze_after_idle_mwh","soc_arbi_before_idle_mwh","soc_arbi_after_idle_mwh",
    "soc_arbi_pct_of_arbi","soc_arbi_pct_of_total",
    "soc_oze_pct_of_oze","soc_oze_pct_of_total",
    "loss_idle_oze_mwh","loss_idle_arbi_mwh","loss_conv_ch_mwh","loss_conv_dis_to_grid_mwh","loss_conv_dis_to_load_mwh",
    "wasted_surplus_due_to_export_cap_mwh",
    "cap_blocked_dis_ac_mwh","cap_blocked_ch_ac_mwh","unserved_load_after_cap_ac_mwh",
    "rev_arbi_to_grid_pln","cost_grid_to_arbi_pln","cashflow_arbi_pln"
]

async def persist_stage03_commit(con: psycopg.AsyncConnection, calc_id, H: Dict[str, Any], R: Dict[str, Any], ts_utc_seq: Sequence[Any], N: int, *, in_txn: bool = False) -> None:
    t_vec0 = perf_counter()
    # czasówki z H (raportowanie)
    y_seq      = _get_vec(H, "y", N); m_seq = _get_vec(H, "m", N); d_seq = _get_vec(H, "d", N)
    h_seq      = _get_vec(H, "h", N); dow_seq = _get_vec(H, "dow", N)
    is_work    = [_bool(v, None) for v in _get_vec(H, "is_work", N)]
    is_free    = [_bool(v, None) for v in _get_vec(H, "is_free", N)]

    r_ts    = _as_list(R.get("ts_utc", ts_utc_seq))
    e_imp   = _as_list(R.get("e_import_mwh", [None]*N))
    e_exp   = _as_list(R.get("e_export_mwh", [None]*N))
    ch_sur  = _as_list(R.get("charge_from_surplus_mwh", [None]*N))
    ch_grid = _as_list(R.get("charge_from_grid_mwh", [None]*N))
    dis_load= _as_list(R.get("discharge_to_load_mwh", [None]*N))
    dis_grid= _as_list(R.get("discharge_to_grid_mwh", [None]*N))
    ex_sur  = _as_list(R.get("export_from_surplus_ac_mwh", [None]*N))
    ex_arbi = _as_list(R.get("export_from_arbi_ac_mwh", [None]*N))
    imp_load= _as_list(R.get("import_for_load_ac_mwh", [None]*N))
    imp_arbi= _as_list(R.get("import_for_arbi_ac_mwh", [None]*N))
    soze_b  = _as_list(R.get("soc_oze_before_idle_mwh", [None]*N))
    soze_a  = _as_list(R.get("soc_oze_after_idle_mwh", [None]*N))
    sarb_b  = _as_list(R.get("soc_arbi_before_idle_mwh", [None]*N))
    sarb_a  = _as_list(R.get("soc_arbi_after_idle_mwh", [None]*N))
    sarb_pct= _as_list(R.get("soc_arbi_pct_of_arbi", [None]*N))
    stot_pct= _as_list(R.get("soc_arbi_pct_of_total", [None]*N))
    # procenty OZE (jeśli 03_commit je zwraca; inaczej NULL)
    soze_pct_oze   = _as_list(R.get("soc_oze_pct_of_oze", [None]*N))
    soze_pct_total = _as_list(R.get("soc_oze_pct_of_total", [None]*N))

    lid_oze = _as_list(R.get("loss_idle_oze_mwh", [None]*N))
    lid_arbi= _as_list(R.get("loss_idle_arbi_mwh", [None]*N))
    lch     = _as_list(R.get("loss_conv_ch_mwh", [None]*N))
    ldg     = _as_list(R.get("loss_conv_dis_to_grid_mwh", [None]*N))
    ldl     = _as_list(R.get("loss_conv_dis_to_load_mwh", [None]*N))
    wasted  = _as_list(R.get("wasted_surplus_due_to_export_cap_mwh", [None]*N))
    cap_dis = _as_list(R.get("cap_blocked_dis_ac_mwh", [None]*N))
    cap_ch  = _as_list(R.get("cap_blocked_ch_ac_mwh", [None]*N))
    unserved= _as_list(R.get("unserved_load_after_cap_ac_mwh", [None]*N))

    # NOWE (finanse ARBI):
    r_rev_arbi = _as_list(R.get("rev_arbi_to_grid_pln", [None] * N))
    r_cost_arbi = _as_list(R.get("cost_grid_to_arbi_pln", [None] * N))
    r_cf_arbi = _as_list(R.get("cashflow_arbi_pln", [None] * N))
    t_vec_ms = (perf_counter() - t_vec0) * 1000.0

    if N >= COPY_THRESHOLD:
        t_seq0 = perf_counter()
        rows_seq = [
            [
                calc_id, r_ts[i],
                y_seq[i], m_seq[i], d_seq[i], h_seq[i], dow_seq[i], is_work[i], is_free[i],
                _num(e_imp[i], None), _num(e_exp[i], None),
                _num(ch_sur[i], None), _num(ch_grid[i], None),
                _num(dis_load[i], None), _num(dis_grid[i], None),
                _num(ex_sur[i], None), _num(ex_arbi[i], None),
                _num(imp_load[i], None), _num(imp_arbi[i], None),
                _num(soze_b[i], None), _num(soze_a[i], None),
                _num(sarb_b[i], None), _num(sarb_a[i], None),
                _num(sarb_pct[i], None), _num(stot_pct[i], None),
                _num(soze_pct_oze[i], None), _num(soze_pct_total[i], None),
                _num(lid_oze[i], None), _num(lid_arbi[i], None),
                _num(lch[i], None), _num(ldg[i], None), _num(ldl[i], None),
                _num(wasted[i], None),
                _num(cap_dis[i], None), _num(cap_ch[i], None), _num(unserved[i], None),
                _num(r_rev_arbi[i], None), _num(r_cost_arbi[i], None), _num(r_cf_arbi[i], None)

            ]
            for i in range(N)
        ]
        t_seq_ms = (perf_counter() - t_seq0) * 1000.0
        csv_ms, write_ms, wrote, b_total = await _copy_rows(con, TABLE_STAGE_03_COMMIT, COLS_03, rows_seq, freeze=COPY_USE_FREEZE)
        log.info("[05/PERSIST][03/TIMER] build_vec=%.1f ms | build_seq=%.1f ms | copy.csv=%.1f ms | copy.write=%.1f ms | rows=%d bytes=%d",
                 t_vec_ms, t_seq_ms, csv_ms, write_ms, wrote, b_total)
    else:
        cols = ", ".join(COLS_03)
        placeholders = ", ".join([f"%({c})s" for c in COLS_03])
        ins = f"""
        INSERT INTO {TABLE_STAGE_03_COMMIT} ({cols})
        VALUES ({placeholders})
        ON CONFLICT (calc_id, ts_utc) DO UPDATE SET
            y = EXCLUDED.y, m = EXCLUDED.m, d = EXCLUDED.d, h = EXCLUDED.h, dow = EXCLUDED.dow,
            is_work = EXCLUDED.is_work, is_free = EXCLUDED.is_free,
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
            soc_oze_pct_of_oze = EXCLUDED.soc_oze_pct_of_oze,
            soc_oze_pct_of_total = EXCLUDED.soc_oze_pct_of_total,
            loss_idle_oze_mwh = EXCLUDED.loss_idle_oze_mwh,
            loss_idle_arbi_mwh = EXCLUDED.loss_idle_arbi_mwh,
            loss_conv_ch_mwh = EXCLUDED.loss_conv_ch_mwh,
            loss_conv_dis_to_grid_mwh = EXCLUDED.loss_conv_dis_to_grid_mwh,
            loss_conv_dis_to_load_mwh = EXCLUDED.loss_conv_dis_to_load_mwh,
            wasted_surplus_due_to_export_cap_mwh = EXCLUDED.wasted_surplus_due_to_export_cap_mwh,
            cap_blocked_dis_ac_mwh = EXCLUDED.cap_blocked_dis_ac_mwh,
            cap_blocked_ch_ac_mwh  = EXCLUDED.cap_blocked_ch_ac_mwh,
            unserved_load_after_cap_ac_mwh = EXCLUDED.unserved_load_after_cap_ac_mwh,
            rev_arbi_to_grid_pln  = EXCLUDED.rev_arbi_to_grid_pln,
            cost_grid_to_arbi_pln = EXCLUDED.cost_grid_to_arbi_pln,
            cashflow_arbi_pln     = EXCLUDED.cashflow_arbi_pln
        ;"""
        rows_dict: List[Dict[str, Any]] = []
        t_dict0 = perf_counter()
        for i in range(N):
            rows_dict.append(dict(
                calc_id=calc_id, ts_utc=r_ts[i],
                y=y_seq[i], m=m_seq[i], d=d_seq[i], h=h_seq[i], dow=dow_seq[i],
                is_work=is_work[i], is_free=is_free[i],
                e_import_mwh=_num(e_imp[i], None),
                e_export_mwh=_num(e_exp[i], None),
                charge_from_surplus_mwh=_num(ch_sur[i], None),
                charge_from_grid_mwh=_num(ch_grid[i], None),
                discharge_to_load_mwh=_num(dis_load[i], None),
                discharge_to_grid_mwh=_num(dis_grid[i], None),
                export_from_surplus_ac_mwh=_num(ex_sur[i], None),
                export_from_arbi_ac_mwh=_num(ex_arbi[i], None),
                import_for_load_ac_mwh=_num(imp_load[i], None),
                import_for_arbi_ac_mwh=_num(imp_arbi[i], None),
                soc_oze_before_idle_mwh=_num(soze_b[i], None),
                soc_oze_after_idle_mwh=_num(soze_a[i], None),
                soc_arbi_before_idle_mwh=_num(sarb_b[i], None),
                soc_arbi_after_idle_mwh=_num(sarb_a[i], None),
                soc_arbi_pct_of_arbi=_num(sarb_pct[i], None),
                soc_arbi_pct_of_total=_num(stot_pct[i], None),
                soc_oze_pct_of_oze=_num(soze_pct_oze[i], None),
                soc_oze_pct_of_total=_num(soze_pct_total[i], None),
                loss_idle_oze_mwh=_num(lid_oze[i], None),
                loss_idle_arbi_mwh=_num(lid_arbi[i], None),
                loss_conv_ch_mwh=_num(lch[i], None),
                loss_conv_dis_to_grid_mwh=_num(ldg[i], None),
                loss_conv_dis_to_load_mwh=_num(ldl[i], None),
                wasted_surplus_due_to_export_cap_mwh=_num(wasted[i], None),
                cap_blocked_dis_ac_mwh=_num(cap_dis[i], None),
                cap_blocked_ch_ac_mwh=_num(cap_ch[i], None),
                unserved_load_after_cap_ac_mwh=_num(unserved[i], None),
                rev_arbi_to_grid_pln=_num(r_rev_arbi[i], None),
                cost_grid_to_arbi_pln=_num(r_cost_arbi[i], None),
                cashflow_arbi_pln=_num(r_cf_arbi[i], None),

            ))
        t_dict_ms = (perf_counter() - t_dict0) * 1000.0
        t_exec0 = perf_counter()
        async with con.cursor() as cur:
            await cur.executemany(ins, rows_dict)
        t_exec_ms = (perf_counter() - t_exec0) * 1000.0
        log.info("[05/PERSIST][03/TIMER] build_vec=%.1f ms | dict=%.1f ms | executemany=%.1f ms | rows=%d",
                 t_vec_ms, t_dict_ms, t_exec_ms, N)

    if not in_txn:
        async with con.cursor() as cur:
            await cur.execute(f"ANALYZE {TABLE_STAGE_03_COMMIT};")
        await con.commit()
    log.info("[05/PERSIST] zapisano 03_commit (n=%d, mode=%s)", N, "COPY" if N >= COPY_THRESHOLD else "executemany")

# ── PERSIST: 04_PRICING ──────────────────────────────────────────────────────
COLS_04 = [
    "calc_id","params_ts","ts_utc","ts_local",
    "y","m","d","h","dow","is_work","is_free",
    "price_import_pln_mwh","price_export_pln_mwh",
    "rev_arbi_to_grid_pln","rev_surplus_export_pln",
    "cost_grid_to_arbi_pln","cost_import_for_load_pln","cashflow_net_pln",
    "loss_idle_oze_mwh","loss_idle_arbi_mwh","loss_conv_ch_mwh","loss_conv_dis_to_grid_mwh","loss_conv_dis_to_load_mwh",
    "wasted_surplus_due_to_export_cap_mwh",
    "pln_loss_conv_ch","pln_loss_conv_dis_to_grid","pln_loss_conv_dis_to_load","pln_loss_idle_arbi","pln_loss_idle_oze",
    "pln_wasted_surplus_cap","losses_diag_total_pln",
    "cap_blocked_dis_ac_mwh","cap_blocked_ch_ac_mwh","unserved_load_after_cap_ac_mwh",
    "pln_cap_blocked_dis_ac","pln_cap_blocked_ch_ac","pln_unserved_load_after_cap","cap_opportunity_total_pln"
]

async def persist_stage04_pricing(con: psycopg.AsyncConnection, *, calc_id, params_ts, H: Dict[str, Any], pricing_rows: Sequence[Dict[str, Any]], in_txn: bool = False) -> None:
    # czasówki z H (raportowanie)
    N = len(_as_list(H.get("ts_utc", [])))
    y_seq      = _get_vec(H, "y", N); m_seq = _get_vec(H, "m", N); d_seq = _get_vec(H, "d", N)
    h_seq      = _get_vec(H, "h", N); dow_seq = _get_vec(H, "dow", N)
    is_work    = [_bool(v, None) for v in _get_vec(H, "is_work", N)]
    is_free    = [_bool(v, None) for v in _get_vec(H, "is_free", N)]

    rows_dict: List[Dict[str, Any]] = []
    for i, r in enumerate(pricing_rows):
        rows_dict.append({k: r.get(k) if k not in ("calc_id","params_ts") else None for k in COLS_04})
        rows_dict[-1].update(
            calc_id=calc_id, params_ts=params_ts,
            ts_utc=r["ts_utc"], ts_local=r.get("ts_local"),
            y=y_seq[i], m=m_seq[i], d=d_seq[i], h=h_seq[i], dow=dow_seq[i],
            is_work=is_work[i], is_free=is_free[i],
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
        )

    N = len(rows_dict)
    if N >= COPY_THRESHOLD:
        rows_seq = [[r[c] for c in COLS_04] for r in rows_dict]
        await _copy_rows(con, TABLE_STAGE_04_PRICING, COLS_04, rows_seq, freeze=COPY_USE_FREEZE)
    else:
        cols = ", ".join(COLS_04)
        placeholders = ", ".join([f"%({c})s" for c in COLS_04])
        ins = f"""
        INSERT INTO {TABLE_STAGE_04_PRICING} ({cols})
        VALUES ({placeholders})
        ON CONFLICT (calc_id, ts_utc) DO UPDATE SET
            params_ts = EXCLUDED.params_ts, ts_local = EXCLUDED.ts_local,
            y = EXCLUDED.y, m = EXCLUDED.m, d = EXCLUDED.d, h = EXCLUDED.h, dow = EXCLUDED.dow,
            is_work = EXCLUDED.is_work, is_free = EXCLUDED.is_free,
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
        ;"""
        async with con.cursor() as cur:
            await cur.executemany(ins, rows_dict)

    if not in_txn:
        async with con.cursor() as cur:
            await cur.execute(f"ANALYZE {TABLE_STAGE_04_PRICING};")
        await con.commit()
    log.info("[05/PERSIST] zapisano 04_pricing (n=%d, mode=%s)", N, "COPY" if N >= COPY_THRESHOLD else "executemany")

# ── persist_all ───────────────────────────────────────────────────────────────
async def persist_all(
    con: psycopg.AsyncConnection,
    H_buf: Dict[str, Any],
    P_buf: Dict[str, Any],
    R_buf: Dict[str, Any],
    pricing_rows: Sequence[Dict[str, Any]],
    *,
    params_ts=None,
    truncate: bool | None = None,
) -> None:
    await ensure_tables(con)

    calc_id = H_buf.get("calc_id")
    if params_ts is None:
        params_ts = H_buf.get("params_ts")

    ts_utc = _as_list(H_buf.get("ts_utc", []))
    N = len(ts_utc)
    if not calc_id or N == 0:
        raise RuntimeError("[05/PERSIST] Brak calc_id albo pusta oś czasu (ts_utc).")

    t_total_start = perf_counter()
    t_clear = t_01 = t_02 = t_03 = t_04 = t_analyze = 0.0
    t_drop_idx = t_create_idx_02 = t_create_idx_03 = 0.0
    dropped_02 = dropped_03 = False

    async with con.transaction():
        async with con.cursor() as cur:
            await cur.execute("SET LOCAL synchronous_commit = OFF;")
            await cur.execute("SET LOCAL lock_timeout = '2s';")
            await cur.execute("SET LOCAL statement_timeout = '0';")

        _t = perf_counter()
        await _clear_stage_tables(con, mode=CLEAR_MODE, calc_id=str(calc_id), in_txn=True)
        t_clear = (perf_counter() - _t) * 1000.0

        # zdejmij indeksy 02/03 (kwalifikowane)
        if PERSIST_REINDEX and N >= COPY_THRESHOLD:
            _t = perf_counter()
            async with con.cursor() as cur:
                await cur.execute(f"DROP INDEX IF EXISTS {QIDX_02_TS};")
                await cur.execute(f"DROP INDEX IF EXISTS {QIDX_03_TS};")
            t_drop_idx = (perf_counter() - _t) * 1000.0
            dropped_02 = dropped_03 = True

        _t = perf_counter()
        await persist_stage01_ingest(con, calc_id, params_ts, H_buf, N, in_txn=True)
        t_01 = (perf_counter() - _t) * 1000.0

        _t = perf_counter()
        await persist_stage02_proposer(con, calc_id, H_buf, P_buf, ts_utc, N, in_txn=True)
        t_02 = (perf_counter() - _t) * 1000.0

        _t = perf_counter()
        # → przekazujemy H_buf (czasówki) także do 03
        await persist_stage03_commit(con, calc_id, H_buf, R_buf, ts_utc, N, in_txn=True)
        t_03 = (perf_counter() - _t) * 1000.0

        # odbuduj indeksy (bez kwalifikacji nazwy w CREATE)
        if dropped_02:
            _t = perf_counter()
            async with con.cursor() as cur:
                await cur.execute(f"CREATE INDEX IF NOT EXISTS {IDX_02_TS} ON {TABLE_STAGE_02_PROPOSER}(ts_utc);")
            t_create_idx_02 = (perf_counter() - _t) * 1000.0
        if dropped_03:
            _t = perf_counter()
            async with con.cursor() as cur:
                await cur.execute(f"CREATE INDEX IF NOT EXISTS {IDX_03_TS} ON {TABLE_STAGE_03_COMMIT}(ts_utc);")
            t_create_idx_03 = (perf_counter() - _t) * 1000.0

        _t = perf_counter()
        # → przekazujemy H_buf (czasówki) do 04
        await persist_stage04_pricing(con, calc_id=calc_id, params_ts=params_ts, H=H_buf, pricing_rows=pricing_rows, in_txn=True)
        t_04 = (perf_counter() - _t) * 1000.0

        _t = perf_counter()
        async with con.cursor() as cur:
            await cur.execute(f"ANALYZE {TABLE_STAGE_01_INGEST};")
            await cur.execute(f"ANALYZE {TABLE_STAGE_02_PROPOSER};")
            await cur.execute(f"ANALYZE {TABLE_STAGE_03_COMMIT};")
            await cur.execute(f"ANALYZE {TABLE_STAGE_04_PRICING};")
        t_analyze = (perf_counter() - _t) * 1000.0

    t_total = (perf_counter() - t_total_start) * 1000.0

    log.info(
        "[05/PERSIST] zapisane 01/02/03/04 (calc_id=%s, N=%d) [CLEAR_MODE=%s, COPY_THRESHOLD=%d, REINDEX=%s]",
        calc_id, N, CLEAR_MODE, COPY_THRESHOLD, "on" if PERSIST_REINDEX else "off"
    )
    log.info(
        "[05/PERSIST][TIMER] clear=%.0f ms | drop_idx=%.0f ms | 01_ingest=%.0f ms | 02_proposer=%.0f ms | "
        "03_commit=%.0f ms | 04_pricing=%.0f ms | create_idx_02=%.0f ms | create_idx_03=%.0f ms | analyze=%.0f ms | total=%.0f ms",
        t_clear, t_drop_idx, t_01, t_02, t_03, t_04, t_create_idx_02, t_create_idx_03, t_analyze, t_total
    )

    log.info("[05/PERSIST] COPY mode=csv chunks rows=%d bytes=%d freeze=%s",
             COPY_CHUNK_ROWS, COPY_CHUNK_BYTES, "on" if COPY_USE_FREEZE else "off")
