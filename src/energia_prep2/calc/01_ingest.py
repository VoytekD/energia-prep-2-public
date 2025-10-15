# 01_ingest.py — Stage 01 (INGEST)
# --------------------------------------------------------------------------------------
# Cel: na podstawie `norm` z ETAPU 00 oraz osi czasu z `input.date_dim` przygotować
#      strukturę H wymaganą przez dalsze etapy (02_proposer, 03_commit, 04_pricing).
#
# Co robi skrypt:
#  1) Pobiera skonsolidowane parametry `norm` (ETAP 00 = source of truth).
#  2) Buduje oś czasu N godzin z `input.date_dim` (ts_utc, ts_local, y, m, d, h, dow, date_key).
#  3) Wylicza maski godzinowe stref dystrybucyjnych:
#       • zone_peak_am  — szczyt poranny (z okna "morn"),
#       • zone_peak_pm  — szczyt popołudniowo-wieczorny (z okna "aft"),
#       • zone_off      — pozostałe godziny (poza AM/PM).
#     (Na wewnętrzne potrzeby bonusów używany jest union: zone_peak_union = AM ∪ PM.)
#  4) Wektoryzuje bonusy okienne (bonus_hrs_ch / bonus_hrs_dis).
#  5) CENY: tylko Fixing I → price_import = price_export = Fixing I (brak Fixing II i spread’u).
#  6) Przenosi skalary BESS i polityk (w tym bonus_*_soc) wymagane w 02.
#  7) Wyprowadza capy (03 wymagane na twardo) na bazie `norm`:
#       • cap_import_ac_mwh     = min(moc_umowna_mw, p_ch_max_mw)
#       • cap_export_ac_mwh     = min(moc_umowna_mw, p_dis_max_mw)
#       • cap_charge_net_mwh    = min(moc_umowna_mw, p_ch_max_mw)  * eta_ch_frac
#       • cap_discharge_net_mwh = min(moc_umowna_mw, p_dis_max_mw) / eta_dis_frac
#  8) Dodaje stany początkowe SOC (z norm): soc0_arbi_mwh, soc0_oze_mwh.
#  9) Liczy godzinową produkcję/konsumpcję po mnożnikach z input.*, dopasowaną do osi UTC z date_dim
#     (duplikaty → średnia, braki → interpolacja liniowa, krawędzie → kopia sąsiada) i bilans NET.
#
# Kontrakty:
#  • Wszystkie parametry pochodzą z `norm` (00), 01 tylko przelicza do postaci godzinowej.
#  • `date_key` — (N,) string "YYYY-MM-DD" (wymagany w 02).
#  • `bonus_low_soc_ch`, `bonus_high_soc_dis` — skalar (wymagany w 02).
#
# API: async def run(con, calc_id, params_ts) -> Dict[str, Any]
# --------------------------------------------------------------------------------------

from __future__ import annotations

import logging
from typing import Any, Dict, Tuple
from collections import OrderedDict

import numpy as np
import psycopg
from psycopg.rows import dict_row

from . import get_consolidated_norm

log = logging.getLogger("energia-prep-2.calc.ingest_01")

# ─────────────────────────────────────────────────────────────────────────────
# Stałe pomocnicze
# ─────────────────────────────────────────────────────────────────────────────

MON_EN = {
    1: "jan", 2: "feb", 3: "mar", 4: "apr",
    5: "may", 6: "jun", 7: "jul", 8: "aug",
    9: "sep", 10: "oct", 11: "nov", 12: "dec",
}

WF_MAP = {"wd": "work", "we": "free"}


def _require_tariff(dyst_sched: Dict[str, Any]) -> str:
    if not isinstance(dyst_sched, dict) or not dyst_sched:
        raise RuntimeError("[01] dyst_sched jest pusty — wymagane wpisy.")
    tariff = str(dyst_sched.get("wybrana_taryfa") or "").strip().lower()
    if not tariff:
        raise RuntimeError("[01] Brak wybranej taryfy (wybrana_taryfa).")
    return tariff


def _build_peak_masks_24h_am_pm(
    dyst_sched: Dict[str, Any], *, tariff: str, mon_en: str, wf: str
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Zwraca dwie 24-elementowe maski (0/1):
      • mask_am — okno 'morn' (szczyt poranny),
      • mask_pm — okno 'aft'  (szczyt popołudniowo-wieczorny).
    Okna mogą zachodzić na siebie i mogą być nocne (s>e).
    """
    wf_norm = WF_MAP.get(wf, "work")

    def _window(part: str) -> Tuple[int, int]:
        s = dyst_sched.get(f"dyst_sched_{tariff}_{mon_en}_{wf_norm}_{part}_start")
        e = dyst_sched.get(f"dyst_sched_{tariff}_{mon_en}_{wf_norm}_{part}_end")
        s = 0 if s is None or str(s).strip() == "" else int(float(s))
        e = 0 if e is None or str(e).strip() == "" else int(float(e))
        return s, e

    def _mask_from_se(s: int, e: int) -> np.ndarray:
        m = np.zeros(24, dtype=int)
        if s == 0 and e == 0:
            return m
        if s < e:
            m[s:e] = 1
        else:
            m[s:24] = 1
            m[0:e] = 1
        return m

    s_am, e_am = _window("morn")
    s_pm, e_pm = _window("aft")
    return _mask_from_se(s_am, e_am), _mask_from_se(s_pm, e_pm)


def _build_moc_mask_24h(moc_sched: Dict[str, Any], wf: str, month_num: int) -> np.ndarray:
    mon_en = MON_EN.get(int(month_num), "jan")
    wf_norm = WF_MAP.get(wf, "work")
    s_key = f"sys_sched_{mon_en}_{wf_norm}_peak_start"
    e_key = f"sys_sched_{mon_en}_{wf_norm}_peak_end"
    start = int(moc_sched[s_key])
    end = int(moc_sched[e_key])
    hours = np.zeros(24, dtype=int)
    if start == end == 0:
        return hours
    if start < end:
        hours[start:end] = 1
    else:
        hours[start:24] = 1
        hours[0:end] = 1
    return hours


async def _table_exists(con: psycopg.AsyncConnection, fq_table: str) -> bool:
    async with con.cursor() as cur:
        await cur.execute("SELECT to_regclass(%s) AS reg", (fq_table,))
        row = await cur.fetchone()
        return bool(row and row["reg"])


async def run(con: psycopg.AsyncConnection, *, calc_id: str, params_ts) -> Dict[str, Any]:
    con.row_factory = dict_row
    log.info("[01] start (calc_id=%s, params_ts=%s)", calc_id, params_ts)

    norm = await get_consolidated_norm(con, calc_id=calc_id)
    dyst_sched: Dict[str, Any] = norm.get("dyst_sched") or {}
    moc_sched: Dict[str, Any] = norm.get("moc_sched") or {}
    tariff = _require_tariff({**dyst_sched, "wybrana_taryfa": norm.get("wybrana_taryfa")})

    # Oś czasu
    async with con.cursor() as cur:
        await cur.execute("""
            SELECT
              ts_utc, ts_local,
              extract(year from ts_utc)::int as y,
              extract(month from ts_utc)::int as m,
              extract(day from ts_utc)::int as d,
              extract(hour from ts_utc)::int as h,
              extract(isodow from ts_utc)::int as dow
            FROM input.date_dim
            ORDER BY ts_utc
        """)
        rows = await cur.fetchall()

    if not rows:
        raise RuntimeError("[01] input.date_dim puste — brak osi czasu.")

    N = len(rows)
    ts_utc = np.array([r["ts_utc"] for r in rows])
    ts_local = np.array([r["ts_local"] for r in rows])
    y = np.array([r["y"] for r in rows], dtype=int)
    m = np.array([r["m"] for r in rows], dtype=int)
    d = np.array([r["d"] for r in rows], dtype=int)
    h = np.array([r["h"] for r in rows], dtype=int)
    dow = np.array([r["dow"] for r in rows], dtype=int)

    date_key = np.array([f"{int(Y):04d}-{int(M):02d}-{int(D):02d}" for Y, M, D in zip(y, m, d)], dtype=object)
    is_work = (dow >= 1) & (dow <= 5)
    is_free = ~is_work

    # ── Maski stref: AM / PM / OFF ────────────────────────────────────────────
    zone_peak_am = np.zeros(N, dtype=int)
    zone_peak_pm = np.zeros(N, dtype=int)
    zone_off     = np.zeros(N, dtype=int)

    for i in range(N):
        mon_en = MON_EN.get(int(m[i]), "jan")
        wf = "wd" if is_work[i] else "we"
        hours24_am, hours24_pm = _build_peak_masks_24h_am_pm(dyst_sched, tariff=tariff, mon_en=mon_en, wf=wf)
        am = int(hours24_am[h[i]])  # 0/1
        pm = int(hours24_pm[h[i]])  # 0/1
        zone_peak_am[i] = am
        zone_peak_pm[i] = pm
        zone_off[i] = 1 - int((am | pm) == 1)

    # Zbiorcza maska szczytu (tylko do logiki wewnętrznej bonusów)
    zone_peak_union = (zone_peak_am | zone_peak_pm).astype(int)

    # Opłata mocowa (maski)
    moc_peak = np.zeros(N, dtype=int)
    for i in range(N):
        wf = "wd" if is_work[i] else "we"
        mon = int(m[i])
        hours24 = _build_moc_mask_24h(moc_sched, wf=wf, month_num=mon)
        moc_peak[i] = hours24[h[i]]

    # CENY — tylko Fixing I; import/export równe (spread dodamy później z formularza)
    fixing_i = np.zeros(N, dtype=float)
    if await _table_exists(con, "input.ceny_godzinowe"):
        async with con.cursor() as cur:
            await cur.execute("""
                SELECT ts_utc, fixing_i_price
                FROM input.ceny_godzinowe
                ORDER BY ts_utc
            """)
            rows_px = await cur.fetchall()
        if rows_px:
            px_map = {r["ts_utc"]: float(r["fixing_i_price"] or 0.0) for r in rows_px}
            fixing_i = np.array([px_map.get(ts, np.nan) for ts in ts_utc], dtype=float)

    price_import = fixing_i.copy()
    price_export = fixing_i.copy()
    if np.isnan(price_import).all():
        log.warning("[01] Wszystkie wartości Fixing I są NaN – sprawdź input.ceny_godzinowe.fixing_i_price.")

    # dalsza część bez zmian (BESS, polityki, produkcja itd.)
    emax_total_mwh = float(norm.get("emax_total_mwh", 0.0))
    emax_oze_mwh = float(norm.get("emax_oze_mwh", 0.0))
    emax_arbi_mwh = float(norm.get("emax_arbi_mwh", 0.0))
    frac_oze = float(norm.get("frac_oze", 0.0))
    p_ch_max_mw = float(norm.get("p_ch_max_mw", 0.0))
    p_dis_max_mw = float(norm.get("p_dis_max_mw", 0.0))
    eta_ch_frac = float(norm.get("eta_ch_frac", 0.0))
    eta_dis_frac = float(norm.get("eta_dis_frac", 0.0))
    bess_lambda_h_frac = float(norm.get("bess_lambda_h_frac", 0.0))
    moc_umowna_mw = float(norm.get("moc_umowna_mw", 0.0))

    base_min_profit = float(norm.get("base_min_profit_pln_mwh", 0.0))
    cycles_per_day = float(norm.get("cycles_per_day", 1.0))
    allow_carry = bool(norm.get("allow_carry_over", False))
    force_order = bool(norm.get("force_order", False))

    # 8) Wektoryzacja bonusów okiennych — wynikowe maski godzinowe
    bonus_ch_window  = float(norm.get("bonus_ch_window", 0.0))   # h/dzień
    bonus_dis_window = float(norm.get("bonus_dis_window", 0.0))  # h/dzień

    bonus_hrs_ch  = np.zeros(N, dtype=int)
    bonus_hrs_dis = np.zeros(N, dtype=int)

    if bonus_ch_window > 0 or bonus_dis_window > 0:
        ymd = np.stack([y, m, d], axis=1)
        days_unique = np.unique(ymd, axis=0)

        if bonus_ch_window > 0:
            w = int(bonus_ch_window)
            for Y, M, D in days_unique:
                day_mask = (y == Y) & (m == M) & (d == D)
                cand_off = np.where(day_mask & (zone_off == 1))[0]
                if cand_off.size >= w:
                    bonus_hrs_ch[cand_off[:w]] = 1
                else:
                    need = w - cand_off.size
                    cand_peak = np.where(day_mask & (zone_peak_union == 1))[0]
                    sel = np.concatenate([cand_off, cand_peak[:max(0, need)]])
                    if sel.size > 0:
                        bonus_hrs_ch[sel] = 1

        if bonus_dis_window > 0:
            w = int(bonus_dis_window)
            for Y, M, D in days_unique:
                day_mask = (y == Y) & (m == M) & (d == D)
                cand_peak = np.where(day_mask & (zone_peak_union == 1))[0]
                if cand_peak.size >= w:
                    bonus_hrs_dis[cand_peak[:w]] = 1
                else:
                    need = w - cand_peak.size
                    cand_off = np.where(day_mask & (zone_off == 1))[0]
                    sel = np.concatenate([cand_peak, cand_off[:max(0, need)]])
                    if sel.size > 0:
                        bonus_hrs_dis[sel] = 1

    # 9) CAPY (skalary z 00 → broadcast 01) — 4 nowe nazwy kanoniczne:
    #   • BESS (NET, z parametru c w godzinach): cap_bess_charge_net_mwh, cap_bess_discharge_net_mwh
    #   • GRID (AC, z mocy umownej MW):         cap_grid_import_ac_mwh,   cap_grid_export_ac_mwh
    cap_bess_charge_net_s = float(norm.get("cap_bess_charge_net_mwh", 0.0))
    cap_bess_discharge_net_s = float(norm.get("cap_bess_discharge_net_mwh", 0.0))
    cap_grid_import_ac_s = float(norm.get("cap_grid_import_ac_mwh", 0.0))
    cap_grid_export_ac_s = float(norm.get("cap_grid_export_ac_mwh", 0.0))

    cap_bess_charge_net_mwh = np.full(N, cap_bess_charge_net_s, dtype=float)
    cap_bess_discharge_net_mwh = np.full(N, cap_bess_discharge_net_s, dtype=float)
    cap_grid_import_ac_mwh = np.full(N, cap_grid_import_ac_s, dtype=float)
    cap_grid_export_ac_mwh = np.full(N, cap_grid_export_ac_s, dtype=float)

    # 11) Godzinowa produkcja i konsumpcja → dopasowanie DO OSI UTC (date_dim) + mnożniki + bilans NET
    try:
        m_pv_pp = float(norm["moc_pv_pp"])
        m_pv_wz = float(norm["moc_pv_wz"])
        m_wiatr = float(norm["moc_wiatr"])
    except KeyError as ex:
        raise RuntimeError(f"[01] Brak mnożnika w norm: {ex}. Wymagane: moc_pv_pp, moc_pv_wz, moc_wiatr.")
    m_load = float(norm.get("zmiany_konsumpcji", 1.0))

    async def _fetch(sql: str):
        async with con.cursor() as cur:
            await cur.execute(sql)
            return await cur.fetchall() or []

    def _avg_by_ymdh(rows, cols, ts_key="ts_local"):
        """Zbiera wartości po (Y,M,D,H) wyznaczonym z TS (local), uśrednia duplikaty."""
        from collections import defaultdict
        acc = defaultdict(lambda: {c: [] for c in cols})
        for r in rows:
            ts = r[ts_key]
            key = (ts.year, ts.month, ts.day, ts.hour)
            for c in cols:
                v = r.get(c)
                if v is not None:
                    try:
                        acc[key][c].append(float(v))
                    except Exception:
                        pass
        out = {}
        for key, d in acc.items():
            out[key] = {c: (sum(vs)/len(vs) if vs else None) for c, vs in d.items()}
        return out

    def _align_interp_ymdh(y_vec, m_vec, d_vec, h_vec, by_key_map, col):
        """Dopasowanie mapy (Y,M,D,H)->val do osi UTC: (y_vec,m_vec,d_vec,h_vec) + interpolacja i edge-fill."""
        Nloc = y_vec.shape[0]
        arr = np.full(Nloc, np.nan, dtype=float)
        pos = {(int(y_vec[i]), int(m_vec[i]), int(d_vec[i]), int(h_vec[i])): i for i in range(Nloc)}
        for key, payload in by_key_map.items():
            i = pos.get(key)
            if i is not None:
                v = payload.get(col)
                if v is not None:
                    try:
                        arr[i] = float(v)
                    except Exception:
                        pass
        valid = np.flatnonzero(~np.isnan(arr))
        if valid.size == 0:
            return np.zeros_like(arr)
        x = valid.astype(float); yv = arr[valid]
        xi = np.arange(arr.shape[0], dtype=float)
        return np.interp(xi, x, yv, left=yv[0], right=yv[-1])

    # Źródła danych: input.produkcja / input.konsumpcja
    prod_cols = ["pv_pp_1mwp", "pv_wz_1mwp", "wind_1mwp"]
    load_cols = ["zuzycie_mw"]

    prod_map = {}
    load_map = {}

    if await _table_exists(con, "input.produkcja"):
        rows_p = await _fetch("""
            SELECT ts_local, pv_pp_1mwp, pv_wz_1mwp, wind_1mwp
            FROM input.produkcja
            ORDER BY ts_local
        """)
        prod_map = _avg_by_ymdh(rows_p, prod_cols)

    if await _table_exists(con, "input.konsumpcja"):
        rows_l = await _fetch("""
            SELECT ts_local, zuzycie_mw
            FROM input.konsumpcja
            ORDER BY ts_local
        """)
        load_map = _avg_by_ymdh(rows_l, load_cols)

    keys = set(prod_map.keys()) | set(load_map.keys())
    by_ymdh = {k: {**prod_map.get(k, {}), **load_map.get(k, {})} for k in keys}

    # Dopasowanie do osi UTC (date_dim → y/m/d/h pochodzą z ts_utc)
    raw_pv_pp_1mwp = _align_interp_ymdh(y, m, d, h, by_ymdh, "pv_pp_1mwp")
    raw_pv_wz_1mwp = _align_interp_ymdh(y, m, d, h, by_ymdh, "pv_wz_1mwp")
    raw_wind_1mwp  = _align_interp_ymdh(y, m, d, h, by_ymdh, "wind_1mwp")
    raw_load_mw    = _align_interp_ymdh(y, m, d, h, by_ymdh, "zuzycie_mw")

    # Produkcja po mnożnikach (z 1 MWp → MWh dla mocy z norm)
    prod_pv_pp_mwh = raw_pv_pp_1mwp * m_pv_pp
    prod_pv_wz_mwh = raw_pv_wz_1mwp * m_pv_wz
    prod_wiatr_mwh = raw_wind_1mwp  * m_wiatr
    prod_total_mwh = prod_pv_pp_mwh + prod_pv_wz_mwh + prod_wiatr_mwh

    # Konsumpcja: MW → MWh na godzinę, po mnożniku zmian
    load_total_mwh = raw_load_mw * m_load

    surplus_net_mwh = np.maximum(prod_total_mwh - load_total_mwh, 0.0)
    deficit_net_mwh = np.maximum(load_total_mwh - prod_total_mwh, 0.0)

    # Bilans netto godzinowy: dodatni = nadwyżka, ujemny = deficyt
    net_flow_mwh = surplus_net_mwh - deficit_net_mwh

    # 12) Wyjściowy obiekt H
    H: Dict[str, Any] = OrderedDict()
    H["calc_id"]   = calc_id
    H["params_ts"] = params_ts
    H["N"]         = int(N)

    # Oś czasu i klucze kalendarzowe
    H["ts_utc"]   = ts_utc
    H["ts_local"] = ts_local
    H["y"] = y; H["m"] = m; H["d"] = d; H["h"] = h; H["dow"] = dow
    H["date_key"] = date_key
    H["is_work"] = is_work.astype(int)
    H["is_free"] = is_free.astype(int)
    H["is_workday"] = is_work.astype(int)
    H["ts_hour"] = np.arange(N, dtype=int)

    # Maski stref (NOWE KLUCZE)
    H["zone_off"]     = zone_off
    H["zone_peak_am"] = zone_peak_am
    H["zone_peak_pm"] = zone_peak_pm

    # Opłata mocowa (maski)
    H["moc_peak"] = moc_peak

    # Ceny (Fixing I)
    H["price_import_pln_mwh"] = price_import
    H["price_export_pln_mwh"] = price_export

    # Bonusy godzinowe
    H["bonus_hrs_ch"]  = bonus_hrs_ch
    H["bonus_hrs_dis"] = bonus_hrs_dis

    # Skalary polityki/sterowania — nazwy jak w norm
    H["base_min_profit_pln_mwh"] = base_min_profit
    H["cycles_per_day"]          = cycles_per_day
    H["allow_carry_over"]        = allow_carry
    H["force_order"]             = force_order
    H["soc_low_threshold"]       = float(norm.get("soc_low_threshold", 0.0))
    H["soc_high_threshold"]      = float(norm.get("soc_high_threshold", 100.0))
    H["arbi_dis_to_load"] = bool(norm.get("arbi_dis_to_load", False))

    # Skalary wymagane przez 02_proposer
    H["bonus_low_soc_ch"]   = float(norm.get("bonus_low_soc_ch", 0.0))
    H["bonus_high_soc_dis"] = float(norm.get("bonus_high_soc_dis", 0.0))

    # Skalary BESS — nazwy jak w norm
    H["emax_total_mwh"]     = emax_total_mwh
    H["emax_oze_mwh"]       = emax_oze_mwh
    H["emax_arbi_mwh"]      = emax_arbi_mwh
    H["frac_oze"]           = frac_oze
    H["p_ch_max_mw"]        = p_ch_max_mw
    H["p_dis_max_mw"]       = p_dis_max_mw
    H["eta_ch_frac"]        = eta_ch_frac
    H["eta_dis_frac"]       = eta_dis_frac
    H["bess_lambda_h_frac"] = bess_lambda_h_frac

    # CAPY dla 03 (broadcast z 00; nowe nazwy)
    H["cap_grid_import_ac_mwh"] = cap_grid_import_ac_mwh
    H["cap_grid_export_ac_mwh"] = cap_grid_export_ac_mwh
    H["cap_bess_charge_net_mwh"] = cap_bess_charge_net_mwh
    H["cap_bess_discharge_net_mwh"] = cap_bess_discharge_net_mwh

    # SOC początkowe (dla 03) – clamp do [0, emax_*]
    soc0_arbi_mwh = float(norm.get("soc_arbi_start_mwh", 0.0))
    soc0_oze_mwh = float(norm.get("soc_oze_start_mwh", 0.0))

    emax_arbi = float(norm.get("emax_arbi_mwh", 0.0))
    emax_oze = float(norm.get("emax_oze_mwh", 0.0))

    H["soc0_arbi_mwh"] = max(0.0, min(soc0_arbi_mwh, emax_arbi))
    H["soc0_oze_mwh"] = max(0.0, min(soc0_oze_mwh, emax_oze))

    # Produkcja/konsumpcja po mnożnikach + bilans NET
    H["prod_pv_pp_mwh"] = prod_pv_pp_mwh
    H["prod_pv_wz_mwh"] = prod_pv_wz_mwh
    H["prod_wiatr_mwh"] = prod_wiatr_mwh
    H["prod_total_mwh"] = prod_total_mwh
    H["load_total_mwh"] = load_total_mwh
    H["surplus_net_mwh"] = surplus_net_mwh
    H["deficit_net_mwh"] = deficit_net_mwh
    H["net_flow_mwh"] = net_flow_mwh


    log.info("[01] end — OK (N=%d)", N)
    return H
