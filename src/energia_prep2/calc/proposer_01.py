# proposer_01.py — P_buf: propozycje arbitrażu (AC) + DIAG wpływu bonusów
from __future__ import annotations

import os
import logging
from typing import Any, Dict, DefaultDict, List, Optional, Tuple
from collections import defaultdict

import numpy as np

log = logging.getLogger("proposer_01")
log.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

EPS = 1e-12

def _hours_set(v, name: str) -> set[int]:
    out: set[int] = set()
    if v is None:
        return out
    if not isinstance(v, (list, tuple)):
        raise RuntimeError(f"[01] {name} musi być listą godzin 0..23 albo None.")
    for x in v:
        h = int(x)
        if not (0 <= h <= 23):
            raise RuntimeError(f"[01] {name}: godzina poza zakresem: {h}")
        out.add(h)
    return out


def _build_daily_pairs(
    day_prices: Dict[Tuple[int,int,int], List[Tuple[int, float]]],
    cycles_per_day: int
) -> Tuple[
    Dict[Tuple[int,int,int], Dict[int,int]],
    Dict[Tuple[int,int,int], Dict[int,int]],
    Dict[Tuple[int,int,int], List[Tuple[int,int,float,float,float]]]
]:
    low_idx_by_hour: Dict[Tuple[int,int,int], Dict[int,int]] = {}
    high_idx_by_hour: Dict[Tuple[int,int,int], Dict[int,int]] = {}
    pair_info: Dict[Tuple[int,int,int], List[Tuple[int,int,float,float,float]]] = {}

    for dkey, hp in day_prices.items():
        if not hp:
            low_idx_by_hour[dkey] = {}
            high_idx_by_hour[dkey] = {}
            pair_info[dkey] = []
            continue

        lows_sorted  = sorted(hp, key=lambda t: (t[1], t[0]))
        highs_sorted = sorted(hp, key=lambda t: (-t[1], t[0]))
        N = max(1, int(cycles_per_day))

        low_candidates: List[Tuple[int, float]] = []
        used_low_hours: set[int] = set()
        for h, p in lows_sorted:
            if h in used_low_hours:
                continue
            low_candidates.append((h, p))
            used_low_hours.add(h)
            if len(low_candidates) >= N:
                break

        used_high_hours: set[int] = set()
        pairs: List[Tuple[int,int,float,float,float]] = []
        for h_low, p_low in low_candidates:
            chosen: Optional[Tuple[int,float]] = None
            for h_high, p_high in highs_sorted:
                if h_high in used_high_hours or h_high == h_low:
                    continue
                if h_high > h_low:
                    chosen = (h_high, p_high)
                    break
            if chosen is None:
                continue
            h_high, p_high = chosen
            used_high_hours.add(h_high)
            pairs.append((h_low, h_high, p_low, p_high, p_high - p_low))
            if len(pairs) >= N:
                break

        low_map: Dict[int,int] = {}
        high_map: Dict[int,int] = {}
        info: List[Tuple[int,int,float,float,float]] = []
        for k, (hl, hh, pl, ph, dd) in enumerate(pairs):
            low_map[hl] = k
            high_map[hh] = k
            info.append((hl, hh, pl, ph, dd))

        low_idx_by_hour[dkey]  = low_map
        high_idx_by_hour[dkey] = high_map
        pair_info[dkey]        = info

    return low_idx_by_hour, high_idx_by_hour, pair_info


def run(H: Dict[str, Any]) -> Dict[str, Any]:
    if "ts_utc" not in H:
        raise RuntimeError("[01] Brak H['ts_utc']")
    N = int(len(H["ts_utc"]))
    if N <= 0:
        raise RuntimeError("[01] Pusta oś czasu (N=0).")

    req = [
        "year","month","day","hour","is_workday","dt_h",
        "price_tge_pln_mwh",
        "e_ch_cap_net_arbi_mwh","e_dis_cap_net_arbi_mwh",
        "emax_arbi_mwh","eta_ch_frac","eta_dis_frac",
        "base_min_profit_pln_mwh","cycles_per_day",
        "bonus_ch_window","bonus_dis_window",
        "bonus_hrs_ch","bonus_hrs_dis","bonus_hrs_ch_free","bonus_hrs_dis_free",
        "bonus_low_soc_ch","bonus_high_soc_dis",
        "soc_low_threshold","soc_high_threshold",
        "bess_lambda_h_frac",
        "soc_arbi_start_mwh",
        "force_order","allow_carry_over",
    ]
    miss = [k for k in req if k not in H]
    if miss:
        raise KeyError(f"[01] Brak w H: {miss}")

    def _arr(name, dtype):
        v = H[name]
        return v.astype(dtype) if isinstance(v, np.ndarray) else np.array(v, dtype=dtype)

    year  = _arr("year", np.int32); month = _arr("month", np.int32); day = _arr("day", np.int32)
    hour  = _arr("hour", np.int32); is_workday = _arr("is_workday", np.bool_)
    price = _arr("price_tge_pln_mwh", np.float64)
    e_ch_cap_net  = _arr("e_ch_cap_net_arbi_mwh", np.float64)
    e_dis_cap_net = _arr("e_dis_cap_net_arbi_mwh", np.float64)

    emax_arbi     = float(H["emax_arbi_mwh"][0] if isinstance(H["emax_arbi_mwh"], np.ndarray) else H["emax_arbi_mwh"])
    eta_ch        = float(H["eta_ch_frac"][0] if isinstance(H["eta_ch_frac"], np.ndarray) else H["eta_ch_frac"])
    eta_dis       = float(H["eta_dis_frac"][0] if isinstance(H["eta_dis_frac"], np.ndarray) else H["eta_dis_frac"])

    base_min_profit = float(H["base_min_profit_pln_mwh"][0] if isinstance(H["base_min_profit_pln_mwh"], np.ndarray) else H["base_min_profit_pln_mwh"])
    cycles_per_day  = int(H["cycles_per_day"][0] if isinstance(H["cycles_per_day"], np.ndarray) else H["cycles_per_day"])
    force_order     = bool(H["force_order"][0] if isinstance(H["force_order"], np.ndarray) else H["force_order"])
    allow_carry_over= bool(H["allow_carry_over"][0] if isinstance(H["allow_carry_over"], np.ndarray) else H["allow_carry_over"])

    bonus_ch_window   = float(H["bonus_ch_window"][0] if isinstance(H["bonus_ch_window"], np.ndarray) else H["bonus_ch_window"])
    bonus_dis_window  = float(H["bonus_dis_window"][0] if isinstance(H["bonus_dis_window"], np.ndarray) else H["bonus_dis_window"])
    bonus_low_soc_ch  = float(H["bonus_low_soc_ch"][0] if isinstance(H["bonus_low_soc_ch"], np.ndarray) else H["bonus_low_soc_ch"])
    bonus_high_soc_dis= float(H["bonus_high_soc_dis"][0] if isinstance(H["bonus_high_soc_dis"], np.ndarray) else H["bonus_high_soc_dis"])

    soc_low_threshold = float(H["soc_low_threshold"][0] if isinstance(H["soc_low_threshold"], np.ndarray) else H["soc_low_threshold"])
    soc_high_threshold= float(H["soc_high_threshold"][0] if isinstance(H["soc_high_threshold"], np.ndarray) else H["soc_high_threshold"])

    hrs_ch_work   = _hours_set(H.get("bonus_hrs_ch"),       "bonus_hrs_ch")
    hrs_dis_work  = _hours_set(H.get("bonus_hrs_dis"),      "bonus_hrs_dis")
    hrs_ch_free   = _hours_set(H.get("bonus_hrs_ch_free"),  "bonus_hrs_ch_free")
    hrs_dis_free  = _hours_set(H.get("bonus_hrs_dis_free"), "bonus_hrs_dis_free")

    soc_curr = float(H["soc_arbi_start_mwh"][0] if isinstance(H["soc_arbi_start_mwh"], np.ndarray) else H["soc_arbi_start_mwh"])
    soc_curr = max(0.0, min(soc_curr, emax_arbi))

    prop_ch_ac  = np.zeros(N, dtype=np.float64)
    prop_dis_ac = np.zeros(N, dtype=np.float64)

    nan = float("nan")
    is_pair_low_a  = np.zeros(N, dtype=np.int8)
    is_pair_high_a = np.zeros(N, dtype=np.int8)
    delta_k_a   = np.full(N, nan)

    # <-- KLUCZOWA ZMIANA: thr_* wyliczane dla KAŻDEJ godziny, nie NaN
    thr_low_a       = np.zeros(N, dtype=np.float64)
    thr_high_a      = np.zeros(N, dtype=np.float64)
    thr_low_delta_a  = np.zeros(N, dtype=np.float64)
    thr_high_delta_a = np.zeros(N, dtype=np.float64)

    dec_ch_a        = np.zeros(N, dtype=np.int8)
    dec_dis_a       = np.zeros(N, dtype=np.int8)
    dec_ch_base_a   = np.zeros(N, dtype=np.int8)
    dec_dis_base_a  = np.zeros(N, dtype=np.int8)

    day_prices: DefaultDict[Tuple[int,int,int], List[Tuple[int, float]]] = defaultdict(list)
    for i in range(N):
        dkey = (int(year[i]), int(month[i]), int(day[i]))
        day_prices[dkey].append((int(hour[i]), float(price[i])))

    low_idx_by_hour, high_idx_by_hour, pair_info = _build_daily_pairs(day_prices, cycles_per_day)

    pending_arbi_net = 0.0
    cycles_used: Dict[Tuple[int,int,int], int] = defaultdict(int)
    prev_dkey: Optional[Tuple[int,int,int]] = None

    for i in range(N):
        dkey = (int(year[i]), int(month[i]), int(day[i]))
        hour_i = int(hour[i])
        is_work = bool(is_workday[i])

        if prev_dkey is None or dkey != prev_dkey:
            if not allow_carry_over:
                pending_arbi_net = 0.0
            if dkey not in cycles_used:
                cycles_used[dkey] = 0
        prev_dkey = dkey

        k_low  = low_idx_by_hour.get(dkey, {}).get(hour_i)
        k_high = high_idx_by_hour.get(dkey, {}).get(hour_i)

        is_pair_low_a[i]  = 1 if k_low  is not None else 0
        is_pair_high_a[i] = 1 if k_high is not None else 0

        # BONUSY / PROGI — LICZONE DLA KAŻDEJ GODZINY
        ch_bonus_hour  = hour_i in (hrs_ch_work  if is_work else hrs_ch_free)
        dis_bonus_hour = hour_i in (hrs_dis_work if is_work else hrs_dis_free)
        low_soc_bonus_hit  = (soc_curr <= soc_low_threshold)
        high_soc_bonus_hit = (soc_curr >= soc_high_threshold)

        thr_low  = base_min_profit
        if ch_bonus_hour:     thr_low += bonus_ch_window
        if low_soc_bonus_hit: thr_low += bonus_low_soc_ch

        thr_high = base_min_profit
        if dis_bonus_hour:     thr_high += bonus_dis_window
        if high_soc_bonus_hit: thr_high += bonus_high_soc_dis

        thr_low_a[i]  = thr_low
        thr_high_a[i] = thr_high
        thr_low_delta_a[i]  = thr_low  - base_min_profit
        thr_high_delta_a[i] = thr_high - base_min_profit

        # DELTA PAR (może nie istnieć dla tej godziny)
        delta_k = None
        if k_low is not None or k_high is not None:
            k = k_low if k_low is not None else k_high
            try:
                hl, hh, pl, ph, dd = pair_info[dkey][k]
                delta_k = float(dd)
            except Exception:
                delta_k = None

        if delta_k is None:
            # brak pary dla tej godziny → brak decyzji, ale progi już mamy
            continue

        delta_k_a[i] = delta_k

        cap_ch_net  = float(e_ch_cap_net[i])
        cap_dis_net = float(e_dis_cap_net[i])
        headroom_soc = max(0.0, emax_arbi - soc_curr)

        score_low_base  = delta_k - base_min_profit
        score_high_base = delta_k - base_min_profit
        dec_low_base = (k_low is not None) and (score_low_base  >= 0.0) and (cycles_used[dkey] < cycles_per_day)
        dec_high_base= (k_high is not None) and (score_high_base >= 0.0)

        dec_ch_base_a[i]  = 1 if dec_low_base  else 0
        dec_dis_base_a[i] = 1 if dec_high_base else 0

        # Decyzje z uwzględnieniem bonusów/progów
        dec_low = False
        if k_low is not None and (delta_k - thr_low) >= 0.0 and cycles_used[dkey] < cycles_per_day:
            charge_net = min(cap_ch_net, headroom_soc)
            if charge_net > EPS:
                prop_ch_ac[i] = charge_net / max(eta_ch, EPS)
                soc_curr = min(emax_arbi, soc_curr + charge_net)
                pending_arbi_net += charge_net
                dec_low = True
        dec_ch_a[i] = 1 if dec_low else 0

        dec_high = False
        if k_high is not None and (delta_k - thr_high) >= 0.0:
            can_dis_net = min(cap_dis_net, soc_curr)
            if force_order:
                can_dis_net = min(can_dis_net, pending_arbi_net)
            if can_dis_net > EPS:
                prop_dis_ac[i] = can_dis_net * max(eta_dis, EPS)
                soc_curr = max(0.0, soc_curr - can_dis_net)
                if pending_arbi_net > 0.0:
                    prev_pending = pending_arbi_net
                    pending_arbi_net = max(0.0, pending_arbi_net - can_dis_net)
                    if prev_pending > 0.0 and pending_arbi_net <= EPS:
                        cycles_used[dkey] = cycles_used[dkey] + 1
                dec_high = True
        dec_dis_a[i] = 1 if dec_high else 0

    P_buf: Dict[str, Any] = {
        "N": N,
        "prop_arbi_dis_to_grid_ac_mwh": prop_dis_ac,
        "prop_arbi_ch_from_grid_ac_mwh": prop_ch_ac,
        "diag": {
            "is_pair_low":  is_pair_low_a,
            "is_pair_high": is_pair_high_a,
            "delta_k": delta_k_a,
            "thr_low": thr_low_a, "thr_high": thr_high_a,
            "thr_low_delta": thr_low_delta_a, "thr_high_delta": thr_high_delta_a,
            "dec_ch": dec_ch_a, "dec_dis": dec_dis_a,
            "dec_ch_base": dec_ch_base_a, "dec_dis_base": dec_dis_base_a,
        }
    }
    if "calc_id" in H:   P_buf["calc_id"] = H["calc_id"]
    if "params_ts" in H: P_buf["params_ts"] = H["params_ts"]

    # ===== warstwa kompatybilności z raport_03.stage01 (TOP-LEVEL klucze) =====
    def _as_bool_arr(v, n):
        if isinstance(v, np.ndarray): return v.astype(np.bool_)
        if isinstance(v, (list, tuple)): return np.array(v, dtype=np.bool_)
        return np.zeros(n, dtype=np.bool_)

    # decyzje bazowe i final
    P_buf["dec_ch"]       = _as_bool_arr(P_buf["diag"]["dec_ch"], N)
    P_buf["dec_dis"]      = _as_bool_arr(P_buf["diag"]["dec_dis"], N)
    P_buf["dec_dis_base"] = _as_bool_arr(P_buf["diag"]["dec_dis_base"], N)
    P_buf["dec_ch_base"]  = _as_bool_arr(P_buf["diag"]["dec_ch_base"],  N)

    # sanity (dwustronne progi)
    P_buf["sanity_soc_low"]    = _as_bool_arr(P_buf["diag"]["thr_low_delta"]  >= 0, N)
    P_buf["sanity_soc_high"]   = _as_bool_arr(P_buf["diag"]["thr_high_delta"] >= 0, N)
    P_buf["sanity_delta_low"]  = _as_bool_arr(~np.isnan(P_buf["diag"]["thr_low"]),  N)
    P_buf["sanity_delta_high"] = _as_bool_arr(~np.isnan(P_buf["diag"]["thr_high"]), N)

    # selection/decision — domyślne „puste”
    P_buf["selected_for_delta"] = np.zeros(N, dtype=np.bool_)
    P_buf["decision"]           = np.array([""] * N, dtype=object)

    log.info("[01] P_buf OK (N=%d, sum_ch=%.3f MWh, sum_dis=%.3f MWh)",
             N, float(np.sum(prop_ch_ac)), float(np.sum(prop_dis_ac)))
    return P_buf
