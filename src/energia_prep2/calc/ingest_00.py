# ingest_00.py — Stage 00 (INGEST) — zgodne z kontraktem RUNNERA (Opcja A)
# API: async def run(con, calc_id, params_ts) -> Dict[str, Any]
#  • pobiera norm przez io.get_consolidated_norm(con, calc_id)
#  • buduje oś czasu z input.date_dim + dołącza wejścia
#  • zwraca H z polami wymaganymi przez preflighty runnera/proposer/commit

from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple
from collections import OrderedDict

import numpy as np
import psycopg
from psycopg.rows import dict_row

# lokalne moduły
from . import io as calc_io

log = logging.getLogger("energia-prep-2.calc.ingest_00")

# ──────────────────────────────────────────────────────────────────────────────
# Pomocnicze
# ──────────────────────────────────────────────────────────────────────────────

# używamy angielskich skrótów miesięcy jako jedynego standardu w harmonogramach
_MONTH_ABBR_EN = {
    1: "jan", 2: "feb", 3: "mar", 4: "apr", 5: "may", 6: "jun",
    7: "jul", 8: "aug", 9: "sep", 10: "oct", 11: "nov", 12: "dec"
}

def _interp_nan(arr: np.ndarray) -> np.ndarray:
    y = arr.astype(np.float64)
    n = len(y)
    if n == 0:
        return y
    idx = np.arange(n, dtype=np.float64)
    mask = ~np.isnan(y)
    if mask.all():
        return y
    if not mask.any():
        return np.zeros_like(y)
    ret = y.copy()
    # brzegi
    first = int(np.argmax(mask))
    last  = n - 1 - int(np.argmax(mask[::-1]))
    if first > 0:
        ret[:first] = y[first]
    if last < n - 1:
        ret[last+1:] = y[last]
    # środek
    good_x = idx[mask]
    good_y = y[mask]
    bad = ~mask
    ret[bad] = np.interp(idx[bad], good_x, good_y)
    return ret

def _zone_from_flat_sched(
    dyst_sched: Dict[str, Any],
    tariff: str,
    wf: str,             # 'wd' | 'we' (wejście z logiki czasu)
    month_num: int,
    hour: int
) -> str:
    """
    Wyznacza strefę dystrybucyjną z 'płaskiego' słownika norm (BEZ wstecznej zgodności).
    WYMAGANY format kluczy:
      dyst_sched_<tariff>_<mon_en>_<work|free>_<zone>_end
    - <mon_en> ∈ {jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec}
    - wf: 'wd' → 'work', 'we' → 'free'
    Brak fallbacków: jeśli brakuje danych → RuntimeError.
    """
    if not dyst_sched:
        raise RuntimeError("[00] dyst_sched jest pusty — wymagane wpisy.")
    t = (tariff or "").lower().strip()
    if not t:
        raise RuntimeError("[00] Brak wybranej taryfy (wybrana_taryfa).")
    mon = _MONTH_ABBR_EN[int(month_num)]
    wf_key = "work" if wf == "wd" else "free"

    prefix = f"dyst_sched_{t}_{mon}_{wf_key}_"
    bounds: List[Tuple[int, str]] = []
    for k, v in dyst_sched.items():
        if isinstance(k, str) and k.startswith(prefix) and k.endswith("_end"):
            zone = k[len(prefix):-4]  # bez "_end"
            try:
                eh = int(v)
            except Exception as ex:
                raise RuntimeError(f"[00] '{k}' nie jest int.") from ex
            if not (0 <= eh <= 23):
                raise RuntimeError(f"[00] '{k}' poza zakresem 0..23: {v}")
            bounds.append((eh, zone))

    if not bounds:
        # brak jakiegokolwiek <zone>_end dla danego miesiąca i trybu work/free
        raise RuntimeError(f"[00] Brak wpisów '{prefix}*_end' w dyst_sched.")

    bounds.sort(key=lambda x: x[0])
    for end_h, zone in bounds:
        if hour <= end_h:
            return zone
    return bounds[-1][1]

def _build_moc_mask_24h(moc_sched: Dict[str, Any], wf: str, month_num: int) -> np.ndarray:
    """
    Buduje maskę 24h dla opłaty mocowej wg NOWEGO schematu ID:
      sys_sched_<mon_en>_<work|free>_peak_<start|end>
    - <mon_en> ∈ {jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec}
    - wf: 'wd' → 'work', 'we' → 'free'
    Brak wstecznej zgodności — stare klucze nie są obsługiwane.
    Wymagane są oba klucze start i end; w przeciwnym razie zgłaszany jest błąd.
    Semantyka: start==0 i end==0 → brak godzin szczytu (cała doba off).
    """
    if not isinstance(moc_sched, dict):
        raise RuntimeError("[00] moc_sched nie jest dict.")
    mon_en = _MONTH_ABBR_EN[int(month_num)]
    wf_key = "work" if wf == "wd" else "free"
    s_key = f"sys_sched_{mon_en}_{wf_key}_peak_start"
    e_key = f"sys_sched_{mon_en}_{wf_key}_peak_end"
    if s_key not in moc_sched or e_key not in moc_sched:
        raise RuntimeError(f"[00] Brak kluczy '{s_key}' lub '{e_key}' w moc_sched.")
    try:
        s = int(moc_sched[s_key])
        e = int(moc_sched[e_key])
    except Exception as ex:
        raise RuntimeError(f"[00] sys_sched wartości muszą być int: {s_key}/{e_key}") from ex
    # clamp
    s = max(0, min(23, s))
    e = max(0, min(23, e))
    mask = np.zeros(24, dtype=np.bool_)
    if not (s == 0 and e == 0):
        if e >= s:
            mask[s:e+1] = True
        else:
            # zakres "zawijający się" (np. 22→6) — traktujemy jako dwa odcinki
            mask[s:24] = True
            mask[0:e+1] = True
    return mask

def _as_bool_vec_for_hours(hour_list, N, hours_vec):
    """Z listy godzin (np. [8,9,10]) buduje maskę bool[N] względem wektora godzin `hours_vec`."""
    if hour_list is None:
        return np.zeros(N, dtype=np.bool_)
    # dopuszczamy zarówno listę intów, jak i listę bool[24] — w obu wypadkach zmapujemy po godzinie
    try:
        if all(isinstance(h, (int, np.integer)) for h in hour_list):
            hs = set(int(h) for h in hour_list)
            return np.array([h in hs for h in hours_vec], dtype=np.bool_)
        else:
            # traktuj jako bool[24]
            arr = np.asarray(hour_list, dtype=np.bool_)
            if arr.shape[0] == 24:
                return np.array([bool(arr[h]) for h in hours_vec], dtype=np.bool_)
    except Exception:
        pass
    # fallback: pusto
    return np.zeros(N, dtype=np.bool_)

# ──────────────────────────────────────────────────────────────────────────────
# Główne wejście Stage 00
# ──────────────────────────────────────────────────────────────────────────────

async def run(con: psycopg.AsyncConnection, *, calc_id, params_ts) -> Dict[str, Any]:
    """Zwraca dict H zgodny z oczekiwaniami runnera/proposera/commitu (Opcja A)."""
    log.info("[00] start (calc_id=%s, params_ts=%s)", calc_id, params_ts)

    # 0) norm (skalary + listy) z IO
    norm = await calc_io.get_consolidated_norm(con, calc_id)
    # Podstawowe scalary
    eta_ch_frac  = float(norm.get("eta_ch_frac", 0.0))
    eta_dis_frac = float(norm.get("eta_dis_frac", 0.0))
    bess_lambda  = float(norm.get("bess_lambda_h_frac", 0.0))
    emax_arbi    = float(norm.get("emax_arbi_mwh", 0.0))
    emax_oze     = float(norm.get("emax_oze_mwh", 0.0))
    soc0_arbi    = float(norm.get("soc_arbi_start_mwh", norm.get("soc0_arbi_mwh", 0.0)))
    soc0_oze     = float(norm.get("soc_oze_start_mwh",  norm.get("soc0_oze_mwh",  0.0)))
    cap_imp_ac   = float(norm.get("p_import_cap_mwh", norm.get("cap_import_ac_mwh", np.inf)))
    cap_exp_ac   = float(norm.get("p_export_cap_mwh", norm.get("cap_export_ac_mwh", np.inf)))
    cap_ch_net   = float(norm.get("e_ch_cap_net_arbi_mwh", norm.get("cap_charge_net_mwh", np.inf)))
    cap_dis_net  = float(norm.get("e_dis_cap_net_arbi_mwh", norm.get("cap_discharge_net_mwh", np.inf)))
    moc_umowna_mw = float(norm.get("moc_umowna_mw", 0.0))

    # polityki i progi do 01
    base_min_profit = float(norm.get("base_min_profit_pln_mwh", 0.0))
    cycles_per_day  = float(norm.get("cycles_per_day", 1.0))
    allow_carry     = bool(norm.get("allow_carry_over", False))
    force_order     = bool(norm.get("force_order", False))
    bonus_ch_window = float(norm.get("bonus_ch_window", 0.0))
    bonus_dis_window= float(norm.get("bonus_dis_window", 0.0))
    bonus_low_soc_ch= float(norm.get("bonus_low_soc_ch", 0.0))
    bonus_high_soc_dis = float(norm.get("bonus_high_soc_dis", 0.0))
    soc_low_thr     = float(norm.get("soc_low_threshold", 0.0))
    soc_high_thr    = float(norm.get("soc_high_threshold", 100.0))

    # ── bez aliasów / bez wstecznej zgodności — nazwy jak w norm ──────────────
    # ── bez aliasów / bez wstecznej zgodności — nazwy jak w norm ──────────────
    dyst_sched = norm.get("dyst_sched") or {}
    moc_sched = norm.get("moc_sched") or {}

    _raw_tariff = str(
        norm.get("wybrana_taryfa")
        or (norm.get("par_kontraktu") or {}).get("wybrana_taryfa")
        or ""
    )
    t = _raw_tariff.strip().replace('"', '').lower()
    if t.startswith("wybrana_"):
        t = t[len("wybrana_"):]
    tariff = t

    # TZ: bierzemy z norm, fallback na PL
    tz = str(norm.get("local_tz"))

    log.debug("[00] tariff=%s | dyst_sched_keys=%d | moc_sched_keys=%d",
              tariff, len(dyst_sched or {}), len(moc_sched or {}))

    # 1) dane godzinowe
    con.row_factory = dict_row
    async with con.cursor() as cur:
        await cur.execute(
            """
            SELECT
                d.ts_utc,
                (d.ts_utc AT TIME ZONE %s) AS ts_local,
                k.zuzycie_mw,
                p.pv_pp_1mwp,
                p.pv_wz_1mwp,
                p.wind_1mwp,
                c.fixing_ii_price
            FROM input.date_dim AS d
            LEFT JOIN input.konsumpcja     AS k ON k.ts_local = (d.ts_utc AT TIME ZONE %s)
            LEFT JOIN input.produkcja      AS p ON p.ts_local = (d.ts_utc AT TIME ZONE %s)
            LEFT JOIN input.ceny_godzinowe AS c ON c.ts_utc   = d.ts_utc
            ORDER BY d.ts_utc
            """,
            (tz, tz, tz)
        )
        rows = await cur.fetchall()

    if not rows:
        raise RuntimeError("[00] input.date_dim puste — brak osi czasu.")

    ts_utc   = np.array([r["ts_utc"] for r in rows], dtype=object)
    ts_local = np.array([r["ts_local"] for r in rows], dtype=object)
    N        = len(ts_utc)
    ts_hour  = np.arange(N, dtype=np.int64)
    hours    = np.array([dt.hour for dt in ts_local], dtype=np.int16)
    date_key = np.array([dt.date().isoformat() for dt in ts_local], dtype=object)
    is_weekend = np.array([(dt.weekday() >= 5) for dt in ts_local], dtype=np.bool_)
    is_workday = ~is_weekend

    # długość godziny (23/24/25h)
    dt_h = np.empty(N, dtype=np.float64)
    for i in range(N):
        if i+1 < N:
            dt_h[i] = (ts_utc[i+1] - ts_utc[i]).total_seconds()/3600.0
        else:
            dt_h[i] = dt_h[i-1] if i>0 else 1.0

    # serie wejściowe
    zuzycie_mw = _interp_nan(np.array([r["zuzycie_mw"] for r in rows], dtype=np.float64))
    pv_pp_1mwp = _interp_nan(np.array([r["pv_pp_1mwp"] for r in rows], dtype=np.float64))
    pv_wz_1mwp = _interp_nan(np.array([r["pv_wz_1mwp"] for r in rows], dtype=np.float64))
    wind_1mwp  = _interp_nan(np.array([r["wind_1mwp"]  for r in rows], dtype=np.float64))
    fixing_ii  = _interp_nan(np.array([r["fixing_ii_price"] for r in rows], dtype=np.float64))

    # przeliczenia bazowe
    prod_mwh    = (pv_pp_1mwp + pv_wz_1mwp + wind_1mwp) * dt_h
    cons_mwh    = zuzycie_mw * dt_h
    surplus_net = np.maximum(0.0, prod_mwh - cons_mwh)
    deficit_net = np.maximum(0.0, cons_mwh - prod_mwh)

    # strefy i maski
    strefy = np.array([
        _zone_from_flat_sched(
            dyst_sched,
            tariff,
            "we" if is_weekend[i] else "wd",
            ts_local[i].month,
            hours[i]
        )
        for i in range(N)
    ], dtype=object)

    mask_moc = np.zeros(N, dtype=np.bool_)
    for i in range(N):
        wf = "we" if is_weekend[i] else "wd"
        mask24 = _build_moc_mask_24h(moc_sched, wf, ts_local[i].month)
        mask_moc[i] = bool(mask24[hours[i]])

    # bonusy — maski N na bazie list godzin
    bonus_hrs_ch       = _as_bool_vec_for_hours(norm.get("bonus_hrs_ch"), N, hours)
    bonus_hrs_dis      = _as_bool_vec_for_hours(norm.get("bonus_hrs_dis"), N, hours)
    bonus_hrs_ch_free  = _as_bool_vec_for_hours(norm.get("bonus_hrs_ch_free"), N, hours)
    bonus_hrs_dis_free = _as_bool_vec_for_hours(norm.get("bonus_hrs_dis_free"), N, hours)

    # capy (NET) jako wektory N — dla 01
    e_ch_cap_net_arbi_mwh  = np.full(N, float(norm.get("e_ch_cap_net_arbi_mwh", cap_ch_net)), dtype=np.float64)
    e_dis_cap_net_arbi_mwh = np.full(N, float(norm.get("e_dis_cap_net_arbi_mwh", cap_dis_net)), dtype=np.float64)

    # ceny (PLN/MWh) — aliasy dla 01/02
    price_import = fixing_ii.copy()
    price_export = fixing_ii.copy()

    # ──────────────────────────────────────────────────────────────────────
    # Wyjście H
    # ──────────────────────────────────────────────────────────────────────
    H: Dict[str, Any] = OrderedDict()
    # identyfikacja i oś
    H["calc_id"]   = str(calc_id)
    H["params_ts"] = params_ts
    H["N"]         = int(N)
    H["ts_utc"]    = ts_utc
    H["ts_local"]  = ts_local
    H["ts_hour"]   = ts_hour
    H["date_key"]  = date_key
    H["hour"]      = hours
    H["is_weekend"] = is_weekend
    H["is_workday"] = is_workday

    # podstawy energetyczne
    H["cons_mwh"]         = cons_mwh
    H["prod_mwh"]         = prod_mwh
    H["surplus_mwh"]      = surplus_net
    H["deficit_mwh"]      = deficit_net
    H["e_surplus_mwh"]    = surplus_net    # aliasy dla 02
    H["e_deficit_mwh"]    = deficit_net
    H["surplus_net_mwh"]  = surplus_net
    H["p_load_net_mwh"]   = deficit_net

    # serie źródłowe i dt_h
    H["pv_pp_1mwp"]       = pv_pp_1mwp
    H["pv_wz_1mwp"]       = pv_wz_1mwp
    H["wind_1mwp"]        = wind_1mwp
    H["fixing_ii_price"]  = fixing_ii
    H["dt_h"]             = dt_h

    # ceny
    H["price_import_pln_mwh"] = price_import
    H["price_export_pln_mwh"] = price_export

    # strefy i maski
    H["dyst_zone"]   = strefy
    H["mocowa_mask"] = mask_moc

    # capy AC (mogą zostać skalarami)
    H["cap_import_ac_mwh"] = cap_imp_ac
    H["cap_export_ac_mwh"] = cap_exp_ac

    # capy NET (jako wektory N) + aliasy dla starych nazw
    H["e_ch_cap_net_arbi_mwh"]  = e_ch_cap_net_arbi_mwh
    H["e_dis_cap_net_arbi_mwh"] = e_dis_cap_net_arbi_mwh
    H["cap_charge_net_mwh"]     = e_ch_cap_net_arbi_mwh
    H["cap_discharge_net_mwh"]  = e_dis_cap_net_arbi_mwh

    # sprawności i λ — skalarne (dla preflightów 01/02) + wektorowe dla persist-00
    H["eta_ch_frac"]        = eta_ch_frac
    H["eta_dis_frac"]       = eta_dis_frac
    H["bess_lambda_h_frac"] = bess_lambda
    H["eta_ch"]             = np.full(N, eta_ch_frac, dtype=np.float64)
    H["eta_dis"]            = np.full(N, eta_dis_frac, dtype=np.float64)
    H["lambda_oze"]         = np.full(N, bess_lambda, dtype=np.float64)
    H["lambda_arbi"]        = np.full(N, bess_lambda, dtype=np.float64)

    # pojemności i stany
    H["emax_arbi_mwh"]      = emax_arbi
    H["emax_oze_mwh"]       = emax_oze
    H["soc0_arbi_mwh"]      = soc0_arbi
    H["soc0_oze_mwh"]       = soc0_oze
    H["soc_arbi_start_mwh"] = soc0_arbi
    H["soc_oze_start_mwh"]  = soc0_oze
    H["moc_umowna_mw"]      = moc_umowna_mw

    # parametry do 01
    H["base_min_profit_pln_mwh"] = base_min_profit
    H["cycles_per_day"]          = cycles_per_day
    H["allow_carry_over"]        = allow_carry
    H["force_order"]             = force_order
    H["bonus_ch_window"]         = bonus_ch_window
    H["bonus_dis_window"]        = bonus_dis_window
    H["bonus_low_soc_ch"]        = bonus_low_soc_ch
    H["bonus_high_soc_dis"]      = bonus_high_soc_dis
    H["soc_low_threshold"]       = soc_low_thr
    H["soc_high_threshold"]      = soc_high_thr

    H["bonus_hrs_ch"]        = bonus_hrs_ch
    H["bonus_hrs_dis"]       = bonus_hrs_dis
    H["bonus_hrs_ch_free"]   = bonus_hrs_ch_free
    H["bonus_hrs_dis_free"]  = bonus_hrs_dis_free

    # uzupełnienia dla persist-00 (jeśli nie używane, zostają zerami)
    H["bonus_take_frac"]     = np.zeros(N, dtype=np.float64)
    H["bonus_give_frac"]     = np.zeros(N, dtype=np.float64)

    log.info("[00] end — OK (N=%d)", N)
    return H
