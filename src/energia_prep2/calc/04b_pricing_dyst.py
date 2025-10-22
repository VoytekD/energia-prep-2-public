# 04b_pricing_dyst.py — ETAP 04b (PRICING_DYST): energia + opłaty (dystrybucyjne, systemowe, fiskalne)
# ==============================================================================================
# Wejścia:
#   H_buf : Dict[str, Any]  ← wynik 01_ingest (oś czasu + maski stref + ceny Fixing I)
#   C_buf : Dict[str, Any]  ← wynik 03_commit (potrzebny dla scenariusza z BESS: import_for_load_ac_mwh)
#   norm  : Dict[str, Any]  ← wynik 00_snapshot (parametry kontraktu, dystr., systemowe, fiskalne, LCOE)
#
# Wyjście:
#   List[Dict[str, Any]]  — wiersze godzinowe (WIDE), jednocześnie dla trzech scenariuszy:
#       grid_only, oze_plus_grid, oze_grid_bess
#   Każdy komponent kosztu rozpisany per taryfa: _b21, _b22, _b23 i _wybrana.
#
# Najważniejsze założenia (uzgodnione):
# - Commodity = czysta cena energii (stała/dynamiczna) × MWh (BEZ marż i opłaty handlowej).
# - Opłaty handlowe (marża zmienna, marża stała [PLN/MWh], opłata handlowa [mies]→na godzinę) liczymy OSOBNO.
# - Dystrybucja:
#     • zmienna wg WORK/FREE × stref (B23: AM/PM/OFF; B22: DZIEŃ/NOC; B21: SINGLE),
#     • stała miesięczna dzielona przez faktyczne godziny w miesiącu (DST).
#     • komponent jakościowy (dyst_qual) to stałe PLN/MWh bez harmonogramu — dodajemy do stawki godzinowej.
# - Systemowe: OZE, KOG, mocowa (tylko w godzinach kwalifikowanych `cap_is_peak`), A(K) wyznaczane per dzień z ΔS.
# - Fiskalne: akcyza [PLN/MWh], VAT po wszystkich innych składnikach.
# - Rounding: kwoty do 2 miejsc (pomocnicze do 6).
# - LLCOE OZE: średnia ważona (PV/Wiatr) — KPI raportowe (nie wchodzi do VAT).
#
# Norm — wymagane klucze:
#   norm["contract"] = {
#       "model_ceny": "stala" | "dynamiczna",
#       "tariff_selected": "B21"|"B22"|"B23",
#       "proc_zmiana_ceny_pct": float,
#       "Cena_stala_pln_mwh": float,
#       "opl_handlowa_mies_pln": float,
#       "marza_stala_pln_mwh": float,
#       "marza_zmienna_pln_mwh": float,
#   }
#   norm["tax"] = {"vat_rate": float, "akcyza_pln_mwh": float}
#   norm["sys"] = {"oze_pln_mwh": float, "kog_pln_mwh": float, "mocowa_pln_mwh": float}
#   norm["kparam"] = {"k_thresholds_pct":[...], "k_A_values":{"K1":...,"K2":...,"K3":...,"K4":...}}
#   norm["dyst_var"] =
#       {"B21":{"work":{"single":x,"qual_pln_mwh":q},"free":{"single":x2,"qual_pln_mwh":q}},
#        "B22":{"work":{"day":x,"night":y,"qual_pln_mwh":q},"free":{"day":x2,"night":y2,"qual_pln_mwh":q}},
#        "B23":{"work":{"am":a,"pm":b,"off":c,"qual_pln_mwh":q},"free":{"am":a2,"pm":b2,"off":c2,"qual_pln_mwh":q}}}
#   norm["dyst_fixed"] =
#       {"B21":{"fixed_per_kw_month_pln":..,"trans_per_kw_month_pln":..,"abon_month_pln":..,"derived_month_total_pln":..},
#        "B22":{...}, "B23":{...}}
#   norm["lcoe_pv_pp"], norm["lcoe_pv_wz"], norm["lcoe_wiatr"] — skalary PLN/MWh.
#
# Maski i dane godzinowe z H_buf:
#   ts_utc[N], y/m/d/h, is_free, mask_b2x_am/pm/off (dystrybucja), cap_is_peak (mocowa),
#   price_import_pln_mwh[N], prod_pv_pp_mwh, prod_pv_wz_mwh, prod_wiatr_mwh,
#   load_total_mwh, deficit_net_mwh
# Z C_buf:
#   import_for_load_ac_mwh[N]
# ----------------------------------------------------------------------------------------------

from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple
from collections import defaultdict
import logging
import numpy as np

log = logging.getLogger("energia-prep-2.calc.pricing_dyst_04b")


# ───────────────────────── helpers ─────────────────────────

def _as_np(x, dtype=float) -> np.ndarray:
    return np.asarray(x if x is not None else [], dtype=dtype)

def _ensure_vec(name: str, v: Sequence[Any], N: int) -> np.ndarray:
    arr = _as_np(v)
    if arr.size != N:
        raise RuntimeError(f"[04b] '{name}' length={arr.size} != N={N}")
    return arr

def _round2(x: np.ndarray) -> np.ndarray:
    return np.round(x.astype(float), 2)

def _hours_in_month(y: np.ndarray, m: np.ndarray) -> np.ndarray:
    """Zwraca wektor H_in_month dla każdej godziny (liczba godzin w miesiącu tej godziny)."""
    ym = y.astype(int) * 100 + m.astype(int)
    out = np.zeros_like(ym, dtype=int)
    for val in np.unique(ym):
        out[ym == val] = int(np.sum(ym == val))
    return out

def _daily_ids(y: np.ndarray, m: np.ndarray, d: np.ndarray) -> np.ndarray:
    return (y.astype(int)*10000 + m.astype(int)*100 + d.astype(int)).astype(int)

def _compute_llcoe_hourly_from_scalars(H: Dict[str, Any],
                                       lcoe_pv_pp: float,
                                       lcoe_pv_wz: float,
                                       lcoe_wiatr: float) -> np.ndarray:
    pv_pp = _as_np(H["prod_pv_pp_mwh"])
    pv_wz = _as_np(H["prod_pv_wz_mwh"])
    wiatr = _as_np(H["prod_wiatr_mwh"])
    total = pv_pp + pv_wz + wiatr
    num = pv_pp * float(lcoe_pv_pp) + pv_wz * float(lcoe_pv_wz) + wiatr * float(lcoe_wiatr)
    with np.errstate(divide="ignore", invalid="ignore"):
        w = np.where(total > 0.0, num / total, 0.0)
    return np.round(w, 6)

def _compute_K_A_per_day(pobor: np.ndarray,
                         is_cap_peak: np.ndarray,
                         day_id: np.ndarray,
                         k_thresholds_pct: Sequence[float],
                         k_A_values: Dict[str, float]) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Liczy ΔS[%], wybiera grupę K i współczynnik A per dzień.
    Zwraca wektory ΔS_day_pct, A_day, K_group_day (as str codes K1..K4) — rozpłaszczone po godzinach.
    """
    thr = sorted([float(x) for x in k_thresholds_pct])

    def to_K(delta_s: float) -> str:
        if delta_s < thr[0]:      return "K1"
        elif delta_s < thr[1]:    return "K2"
        elif delta_s < thr[2]:    return "K3"
        else:                     return "K4"

    K_group_by_day: Dict[int, str] = {}
    A_by_day: Dict[int, float] = {}
    dS_by_day: Dict[int, float] = {}

    for did in np.unique(day_id):
        idx = (day_id == did)
        peak = pobor[idx & (is_cap_peak == 1)]
        off  = pobor[idx & (is_cap_peak == 0)]
        Np = max(int(np.sum(is_cap_peak[idx] == 1)), 0)
        Nf = max(int(np.sum(is_cap_peak[idx] == 0)), 0)

        E_peak = float(np.sum(peak))
        E_off  = float(np.sum(off))

        if Nf == 0:
            delta_s = 1e9
        else:
            avg_peak = E_peak / max(Np, 1)
            avg_off  = E_off  / max(Nf, 1)
            if avg_off <= 0.0 and avg_peak > 0.0:
                delta_s = 1e9
            elif avg_off <= 0.0 and avg_peak <= 0.0:
                delta_s = 0.0
            else:
                delta_s = ((avg_peak / avg_off) - 1.0) * 100.0

        Kg = to_K(delta_s)
        A  = float(k_A_values.get(Kg, 1.0))

        K_group_by_day[did] = Kg
        A_by_day[did]       = A
        dS_by_day[did]      = delta_s

    # rozpłaszczenie na godziny
    K_group_vec = np.array([K_group_by_day[int(d)] for d in day_id], dtype=object)
    A_vec       = np.array([A_by_day[int(d)]       for d in day_id], dtype=float)
    dS_vec      = np.array([dS_by_day[int(d)]      for d in day_id], dtype=float)
    return dS_vec, A_vec, K_group_vec


# ───────────────────────── główna logika ─────────────────────────

def run(H_buf: Dict[str, Any],
        C_buf: Dict[str, Any],
        norm: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Zwraca listę wierszy godzinowych z kosztami dla trzech scenariuszy, WIDE (B21/B22/B23/_wybrana).
    """
    # N i oś czasu
    ts_utc = H_buf["ts_utc"]
    N = len(ts_utc)
    y = _ensure_vec("y", H_buf["y"], N)
    m = _ensure_vec("m", H_buf["m"], N)
    d = _ensure_vec("d", H_buf["d"], N)
    is_free = _ensure_vec("is_free", H_buf["is_free"], N).astype(int)

    # Maski stref — wyłącznie nowe pola (wspólne dla wszystkich taryf)
    z_am    = _ensure_vec("mask_b2x_am",  H_buf["mask_b2x_am"],  N).astype(int)
    z_pm    = _ensure_vec("mask_b2x_pm",  H_buf["mask_b2x_pm"],  N).astype(int)
    z_off   = _ensure_vec("mask_b2x_off", H_buf["mask_b2x_off"], N).astype(int)

    # Opłata mocowa — tylko nowe pole
    cap_peak = _ensure_vec("cap_is_peak", H_buf["cap_is_peak"], N).astype(int)

    hours_month = _hours_in_month(y, m)
    day_id = _daily_ids(y, m, d)

    # --- kontrakt / ceny
    contract = norm["contract"]
    model_ceny  = str(contract["model_ceny"]).lower()
    sel_tariff  = str(contract["tariff_selected"]).upper()
    cena_stala  = float(contract.get("Cena_stala_pln_mwh", 0.0))
    proc_delta  = float(contract.get("proc_zmiana_ceny_pct", 0.0)) / 100.0
    marza_zm    = float(contract.get("marza_zmienna_pln_mwh", 0.0))
    marza_st    = float(contract.get("marza_stala_pln_mwh", 0.0))
    opl_mies    = float(contract.get("opl_handlowa_mies_pln", 0.0))

    price_import = _ensure_vec("price_import_pln_mwh", H_buf["price_import_pln_mwh"], N)
    if model_ceny == "dynamiczna" and proc_delta != 0.0:
        price_import = price_import * (1.0 + proc_delta)
    if model_ceny == "stala":
        price_import = np.full(N, cena_stala, dtype=float)

    # --- fiskalne/systemowe
    tax = norm["tax"]
    vat_rate   = float(tax["vat_rate"])
    akcyza     = float(tax["akcyza_pln_mwh"])

    sys_rates = norm["sys"]
    sys_oze   = float(sys_rates["oze_pln_mwh"])
    sys_kog   = float(sys_rates["kog_pln_mwh"])
    sys_moc   = float(sys_rates["mocowa_pln_mwh"])

    kparam = norm["kparam"]
    k_thr  = kparam["k_thresholds_pct"]
    k_A    = kparam["k_A_values"]

    # --- dystrybucyjne
    dyst_var  = norm["dyst_var"]
    dyst_fix  = norm["dyst_fixed"]  # zawiera: fixed_per_kw_month_pln, trans_per_kw_month_pln, abon_month_pln, derived_month_total_pln

    # --- LLCOE (skalare → wektor godzinowy)
    llcoe_hourly = _compute_llcoe_hourly_from_scalars(
        H_buf,
        float(norm.get("lcoe_pv_pp", 0.0)),
        float(norm.get("lcoe_pv_wz", 0.0)),
        float(norm.get("lcoe_wiatr", 0.0)),
    )

    # ── SCENARIUSZE poboru
    pobor = {
        "grid":       _ensure_vec("load_total_mwh",     H_buf["load_total_mwh"], N),
        "oze_grid":   np.maximum(_ensure_vec("deficit_net_mwh", H_buf["deficit_net_mwh"], N), 0.0),
        "mag_oze":    _ensure_vec("import_for_load_ac_mwh", C_buf["import_for_load_ac_mwh"], N),
    }

    # ── K/A per dzień (dla każdego scenariusza osobno)
    K_info = {}
    for scen, e in pobor.items():
        dS_day, A_day, K_group = _compute_K_A_per_day(e, cap_peak, day_id, k_thr, k_A)
        K_info[scen] = {"delta_s_day_pct": dS_day, "A_day": A_day, "K_group": K_group}

    # ── Funkcje naliczające per taryfa
    def _dyst_var_per_tariff(tariff: str, is_free_vec, am, pm, off, e_mwh):
        """Zmienna część dystrybucji (PLN/h): stawka(tryb,strefa) * e_mwh + komponent jakościowy."""
        t = tariff.upper()
        conf = dyst_var[t]
        mode = np.where(is_free_vec == 1, "free", "work")

        if t == "B23":
            r = conf
            rate_base = (
                am  * np.where(mode == "work", r["work"]["am"],  r["free"]["am"]) +
                pm  * np.where(mode == "work", r["work"]["pm"],  r["free"]["pm"]) +
                off * np.where(mode == "work", r["work"]["off"], r["free"]["off"])
            )
            qual = np.where(mode == "work", r["work"]["qual_pln_mwh"], r["free"]["qual_pln_mwh"])
            rate = rate_base + qual

        elif t == "B22":
            r = conf
            day_mask   = (am + pm).clip(0, 1)
            night_mask = off
            rate_base = (
                day_mask   * np.where(mode == "work", r["work"]["day"],   r["free"]["day"]) +
                night_mask * np.where(mode == "work", r["work"]["night"], r["free"]["night"])
            )
            qual = np.where(mode == "work", r["work"]["qual_pln_mwh"], r["free"]["qual_pln_mwh"])
            rate = rate_base + qual

        else:  # B21
            r = conf
            rate_base = np.where(mode == "work", r["work"]["single"], r["free"]["single"])
            qual = np.where(mode == "work", r["work"]["qual_pln_mwh"], r["free"]["qual_pln_mwh"])
            rate = rate_base + qual

        return _round2(e_mwh * rate)

    def _dyst_fixed_per_tariff(tariff: str, hours_in_month_vec: np.ndarray) -> np.ndarray:
        """Stała część dystrybucji (PLN/h): derived_month_total_pln / godziny_w_miesiącu."""
        monthly_total = float(dyst_fix[tariff]["derived_month_total_pln"])
        with np.errstate(divide="ignore", invalid="ignore"):
            per_h = np.where(hours_in_month_vec > 0, monthly_total / hours_in_month_vec, 0.0)
        return _round2(per_h)

    # ── budowa rekordów
    rows: List[Dict[str, Any]] = []
    tariffs = ["B21", "B22", "B23"]

    for i in range(N):
        base = {
            "ts_utc": H_buf["ts_utc"][i],
            "y": int(y[i]), "m": int(m[i]), "d": int(d[i]),
            "is_work": bool(H_buf["is_work"][i]), "is_free": bool(is_free[i]),
            "tariff_selected": sel_tariff,
            "model_ceny": model_ceny,
            "llcoe_oze_pln_mwh_h": float(llcoe_hourly[i]),
            # strefy dystrybucyjne (diagnostyka)
            "is_dyst_am": bool(z_am[i] == 1), "is_dyst_pm": bool(z_pm[i] == 1), "is_dyst_off": bool(z_off[i] == 1),
            # mocowa — okno kwalifikowane
            "cap_is_peak": bool(cap_peak[i] == 1),
        }

        # pętle po scenariuszach (WIDE ×3)
        for scen_key, e in pobor.items():
            e_i = float(e[i])
            # Energy (commodity)
            energia_netto = e_i * float(price_import[i])
            # Handels
            marza_zmienna = e_i * marza_zm
            marza_stala   = e_i * marza_st
            opl_handlowa  = (opl_mies / max(int(hours_month[i]), 1))

            # Systemowe niezależne od taryfy (OZE/KOG) + mocowa (z A_dnia)
            A_day_i  = float(K_info[scen_key]["A_day"][i])
            sys_oze_i = e_i * sys_oze
            sys_kog_i = e_i * sys_kog
            sys_moc_i = e_i * sys_moc * A_day_i * (1 if cap_peak[i] == 1 else 0)

            akcyza_i = e_i * akcyza

            # per taryfa
            for t in tariffs + ["_SEL_"]:
                tariff = sel_tariff if t == "_SEL_" else t
                suffix = {"B21": "_b21", "B22": "_b22", "B23": "_b23"}[tariff] if t != "_SEL_" else "_wybrana"

                # Dystrybucja: var + fixed
                dv = float(_dyst_var_per_tariff(tariff, is_free, z_am, z_pm, z_off, pobor[scen_key])[i])
                df = float(_dyst_fixed_per_tariff(tariff, hours_month)[i])

                # Podstawa VAT = suma netto
                podstawa = (
                    energia_netto + marza_zmienna + marza_stala + opl_handlowa +
                    dv + df + sys_oze_i + sys_kog_i + sys_moc_i + akcyza_i
                )
                vat_i = round(podstawa * vat_rate, 2)
                sum_netto = round(podstawa, 2)
                sum_brutto = sum_netto + vat_i

                base.update({
                    f"pobor_{scen_key}_mwh": e_i,

                    f"energia_netto_{scen_key}{suffix}_pln_h": round(energia_netto, 2),

                    f"marza_zmienna_{scen_key}{suffix}_pln_h": round(marza_zmienna, 2),
                    f"marza_stala_{scen_key}{suffix}_pln_h":   round(marza_stala, 2),
                    f"opl_handlowa_{scen_key}{suffix}_pln_h":  round(opl_handlowa, 2),

                    f"dyst_var_{scen_key}{suffix}_pln_h":  dv,
                    f"dyst_stala_{scen_key}{suffix}_pln_h": df,
                    f"dyst_netto_{scen_key}{suffix}_pln_h": round(dv + df, 2),

                    f"sys_oze_{scen_key}{suffix}_pln_h":    round(sys_oze_i, 2),
                    f"sys_kog_{scen_key}{suffix}_pln_h":    round(sys_kog_i, 2),
                    f"sys_mocowa_{scen_key}{suffix}_pln_h": round(sys_moc_i, 2),
                    f"sys_netto_{scen_key}{suffix}_pln_h":  round(sys_oze_i + sys_kog_i + sys_moc_i, 2),

                    f"akcyza_{scen_key}{suffix}_pln_h": round(akcyza_i, 2),
                    f"vat_{scen_key}{suffix}_pln_h":    vat_i,

                    f"sum_netto_{scen_key}{suffix}_pln_h":  sum_netto,
                    f"sum_brutto_{scen_key}{suffix}_pln_h": sum_brutto,
                })

            # diagnostyka K/ΔS — na poziomie godziny (rozpłaszczone)
            base.update({
                f"cap_k_group_day_{scen_key}":       str(K_info[scen_key]["K_group"][i]),
                f"cap_A_day_{scen_key}":             float(K_info[scen_key]["A_day"][i]),
                f"cap_delta_s_day_{scen_key}_pct":   float(K_info[scen_key]["delta_s_day_pct"][i]),
            })

        rows.append(base)

    log.info("[04b/PRICING_DYST] OK — rows=%d", len(rows))
    return rows


# Lista kolumn (orientacyjnie) dla 05_persist
COLS_04B = [
    "calc_id","params_ts","ts_utc","y","m","d","is_work","is_free",
    "tariff_selected","model_ceny","is_dyst_am","is_dyst_pm","is_dyst_off",
    "cap_is_peak","llcoe_oze_pln_mwh_h",
    # pobór (3 scenariusze)
    "pobor_grid_mwh","pobor_oze_grid_mwh","pobor_mag_oze_mwh",
    # … komponenty kosztowe jak w base.update w run()
]
