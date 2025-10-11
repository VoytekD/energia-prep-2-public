# proposer_01.py — generator propozycji arbitrażu (AC) + bogaty DIAG
# -----------------------------------------------------------------------------
# STANDARD WE/WY po zmianach w IO/00:
#  • emax_arbi_mwh  → SKALAR
#  • bonus_hrs_*    → MASKI BOOL (wektory N)
#  • Pary low/high i Δk:
#      - jeśli H ma: k_low_by_hour, k_high_by_hour, delta_k_by_idx → używamy,
#      - w przeciwnym wypadku fallback: Δk[i] = price_export[i] - price_import[i],
#        a mapowania są tożsamościowe (i → i).
# -----------------------------------------------------------------------------

from __future__ import annotations

import os
import logging
from typing import Any, Dict, DefaultDict, Optional
from collections import defaultdict

import numpy as np

log = logging.getLogger("energia-prep-2.calc.proposer_01")


EPS = 1e-12


def _req_scalar(H: Dict[str, Any], key: str) -> float:
    """Twardy skalar: liczba, żadnych list/ndarray/Series."""
    from numbers import Number
    if key not in H:
        raise RuntimeError(f"[01] Brak klucza: {key}")
    v = H[key]
    if isinstance(v, (list, tuple)) or getattr(v, "shape", None) not in (None, ()):
        raise RuntimeError(f"[01] {key} ma być SKALAREM; otrzymano kontener/ndarray.")
    if not isinstance(v, Number):
        raise RuntimeError(f"[01] {key} ma być liczbą; typ={type(v)}")
    return float(v)


def _req_vec_len(H: Dict[str, Any], key: str, N: int) -> np.ndarray:
    """Twardy wektor długości N (np.ndarray/list/Series) rzutowany na np.bool_ lub float64 przez caller."""
    if key not in H:
        raise RuntimeError(f"[01] Brak klucza wektora: {key}")
    v = H[key]
    try:
        _ = len(v)
    except Exception:
        raise RuntimeError(f"[01] {key} musi być wektorem długości N={N}, a nie skalarem.")
    arr = np.asarray(v)
    if arr.shape[0] != N:
        raise RuntimeError(f"[01] {key} ma długość {arr.shape[0]}, oczekiwano N={N}.")
    return arr


def run(H: Dict[str, Any], P: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parametry wejściowe (H) — spodziewane klucze:
    ----------------------------------------------------------------------------
    ts_hour:                int[N]              # indeksy 0..N-1 (tożsamościowe do fallbacku)
    date_key:               str[N]              # "YYYY-MM-DD"
    is_workday:             bool[N]
    hour:                   int[N]              # 0..23 (nieużywane w parowaniu; mamy ts_hour)
    emax_arbi_mwh:          float               # SKALAR
    e_ch_cap_net_arbi_mwh:  float[N]            # limit ładowania NET (po ηch)
    e_dis_cap_net_arbi_mwh: float[N]            # limit rozładowania NET (po ηdis)
    eta_ch_frac:            float               # SKALAR
    eta_dis_frac:           float               # SKALAR
    price_import_pln_mwh:   float[N]            # fallback Δk
    price_export_pln_mwh:   float[N]            # fallback Δk

    # PROGI, BONUSY, POLITYKI (skalary)
    base_min_profit_pln_mwh, cycles_per_day, allow_carry_over, force_order,
    bonus_ch_window, bonus_dis_window, bonus_low_soc_ch, bonus_high_soc_dis,
    soc_low_threshold [%], soc_high_threshold [%]

    # Okna — MASKI BOOL (N)
    bonus_hrs_ch, bonus_hrs_dis, bonus_hrs_ch_free, bonus_hrs_dis_free

    # (opcjonalnie) Pary i Δk
    k_low_by_hour:  Dict[int, Optional[int]]
    k_high_by_hour: Dict[int, Optional[int]]
    delta_k_by_idx: Dict[int, float]

    # Stan początkowy
    soc_arbi_start_mwh: float
    """

    # --- [1] Rozmiar i podstawowe wektory ---------------------------------------------------------
    ts_hour = np.asarray(H["ts_hour"], dtype=np.int64)
    N = len(ts_hour)

    # twarde typy po standaryzacji
    emax_arbi = _req_scalar(H, "emax_arbi_mwh")          # SKALAR
    eta_ch    = _req_scalar(H, "eta_ch_frac")
    eta_dis   = _req_scalar(H, "eta_dis_frac")

    cap_ch_net  = np.asarray(_req_vec_len(H, "e_ch_cap_net_arbi_mwh",  N), dtype=np.float64)
    cap_dis_net = np.asarray(_req_vec_len(H, "e_dis_cap_net_arbi_mwh", N), dtype=np.float64)

    is_workday = np.asarray(H["is_workday"], dtype=np.bool_)
    if len(is_workday) != N:
        raise RuntimeError(f"[01] is_workday ma długość {len(is_workday)}, oczekiwano N={N}.")

    # --- [2] Parametry ekonomiczne i polityki sterowania ------------------------------------------
    base_min_profit      = _req_scalar(H, "base_min_profit_pln_mwh")
    cycles_per_day       = int(_req_scalar(H, "cycles_per_day"))
    allow_carry_over     = bool(H["allow_carry_over"])
    force_order          = bool(H["force_order"])

    bonus_ch_window      = _req_scalar(H, "bonus_ch_window")
    bonus_dis_window     = _req_scalar(H, "bonus_dis_window")
    bonus_low_soc_ch     = _req_scalar(H, "bonus_low_soc_ch")
    bonus_high_soc_dis   = _req_scalar(H, "bonus_high_soc_dis")

    soc_low_threshold_pct  = _req_scalar(H, "soc_low_threshold")
    soc_high_threshold_pct = _req_scalar(H, "soc_high_threshold")

    # Okna godzinowe — MASKI BOOL (N); wybór maski zależnie od workday/free
    bonus_ch_mask       = np.asarray(_req_vec_len(H, "bonus_hrs_ch",       N), dtype=np.bool_)
    bonus_dis_mask      = np.asarray(_req_vec_len(H, "bonus_hrs_dis",      N), dtype=np.bool_)
    bonus_ch_free_mask  = np.asarray(_req_vec_len(H, "bonus_hrs_ch_free",  N), dtype=np.bool_)
    bonus_dis_free_mask = np.asarray(_req_vec_len(H, "bonus_hrs_dis_free", N), dtype=np.bool_)

    # --- [3] Pary low/high i Δk — opcjonalne; jeśli brak → fallback na cenach ---------------------
    k_low_by_hour  = H.get("k_low_by_hour")
    k_high_by_hour = H.get("k_high_by_hour")
    delta_k_by_idx = H.get("delta_k_by_idx")

    if (k_low_by_hour is None) or (k_high_by_hour is None) or (delta_k_by_idx is None):
        # Fallback: Δk[i] = price_export[i] - price_import[i], a mapowania i→i
        price_import = np.asarray(H.get("price_import_pln_mwh", H.get("price_tge_pln_mwh", np.zeros(N))), dtype=np.float64)
        price_export = np.asarray(H.get("price_export_pln_mwh", H.get("price_tge_pln_mwh", np.zeros(N))), dtype=np.float64)
        delta_vec = price_export - price_import

        k_low_by_hour  = {int(ts_hour[i]): int(ts_hour[i]) for i in range(N)}
        k_high_by_hour = {int(ts_hour[i]): int(ts_hour[i]) for i in range(N)}
        delta_k_by_idx = {int(ts_hour[i]): float(delta_vec[i]) for i in range(N)}

        log.debug("[01] Fallback parowania Δk uruchomiony (brak k_*_by_hour w H).")

    # --- [4] Stan początkowy ARBI i inicjalizacja pending -----------------------------------------
    soc_arbi_start = float(H.get("soc_arbi_start_mwh", 0.0))
    soc_curr = float(soc_arbi_start)
    pending_arbi_net = float(soc_curr)  # patrz komentarz dot. force_order

    # --- [5] Bufory wyjściowe + DIAG --------------------------------------------------------------
    prop_ch_ac  = np.zeros(N, dtype=np.float64)   # [MWh AC]
    prop_dis_ac = np.zeros(N, dtype=np.float64)   # [MWh AC]

    dec_ch_base  = np.zeros(N, dtype=bool)
    dec_dis_base = np.zeros(N, dtype=bool)
    dec_ch       = np.zeros(N, dtype=bool)
    dec_dis      = np.zeros(N, dtype=bool)

    thr_low  = np.zeros(N, dtype=np.float64)
    thr_high = np.zeros(N, dtype=np.float64)
    delta_k  = np.full(N, np.nan, dtype=np.float64)

    cycles_used: DefaultDict[str, int] = defaultdict(int)
    prev_dkey: Optional[str] = None

    date_key = np.asarray(H["date_key"], dtype=object)
    if len(date_key) != N:
        raise RuntimeError(f"[01] date_key ma długość {len(date_key)}, oczekiwano N={N}.")

    # --- [6] Pętla główna po godzinach ------------------------------------------------------------
    for i in range(N):
        idx  = int(ts_hour[i])          # używamy indeksu (tożsamość w fallbacku)
        dkey = str(date_key[i])         # "YYYY-MM-DD"
        work = bool(is_workday[i])

        # Reset dobowy
        if (prev_dkey is None) or (dkey != prev_dkey):
            if not allow_carry_over:
                pending_arbi_net = 0.0
            if dkey not in cycles_used:
                cycles_used[dkey] = 0
            prev_dkey = dkey

        # Pojemności i headroom bieżącej godziny
        emax          = float(emax_arbi)           # SKALAR – stała pojemność ARBI
        cap_ch_net_i  = float(cap_ch_net[i])       # MWh NET (po ηch)
        cap_dis_net_i = float(cap_dis_net[i])      # MWh NET (po ηdis)
        headroom_soc  = max(0.0, emax - soc_curr)

        # Progi thr_low/thr_high (PLN/MWh)
        in_ch_bonus  = bool(bonus_ch_mask[i]  if work else bonus_ch_free_mask[i])
        in_dis_bonus = bool(bonus_dis_mask[i] if work else bonus_dis_free_mask[i])

        low_thr_mwh  = emax * soc_low_threshold_pct  / 100.0
        high_thr_mwh = emax * soc_high_threshold_pct / 100.0

        low_soc_bonus_hit  = (soc_curr <= low_thr_mwh)
        high_soc_bonus_hit = (soc_curr >= high_thr_mwh)

        thr_l = base_min_profit \
                + (bonus_ch_window if in_ch_bonus else 0.0) \
                + (bonus_low_soc_ch if low_soc_bonus_hit else 0.0)

        thr_h = base_min_profit \
                + (bonus_dis_window if in_dis_bonus else 0.0) \
                + (bonus_high_soc_dis if high_soc_bonus_hit else 0.0)

        thr_low[i]  = thr_l
        thr_high[i] = thr_h

        # Pary godzin (low/high) i Δk
        k_low  = k_low_by_hour.get(idx, None)
        k_high = k_high_by_hour.get(idx, None)
        if (k_low is None) or (k_high is None):
            continue

        dk = float(delta_k_by_idx.get(k_high, np.nan))
        delta_k[i] = dk

        # Decyzje bazowe (Δk vs base_min_profit) — tylko diag
        dec_low_base  = (dk - base_min_profit >= 0.0) and (cycles_used[dkey] < cycles_per_day) and (headroom_soc > EPS)
        dec_high_base = (dk - base_min_profit >= 0.0) and (cycles_used[dkey] < cycles_per_day) and (soc_curr > EPS)
        dec_ch_base[i]  = dec_low_base
        dec_dis_base[i] = dec_high_base

        # CHARGE
        if (dk - thr_l) >= 0.0 and (cycles_used[dkey] < cycles_per_day) and headroom_soc > EPS:
            charge_net = min(cap_ch_net_i, headroom_soc)
            if charge_net > EPS:
                prop_ch_ac[i] = charge_net / max(eta_ch, EPS)  # [MWh AC]
                soc_curr = min(emax, soc_curr + charge_net)    # NET
                pending_arbi_net += charge_net
                dec_ch[i] = True

        # DISCHARGE
        if (dk - thr_h) >= 0.0 and (cycles_used[dkey] < cycles_per_day) and soc_curr > EPS:
            can_dis_net = min(cap_dis_net_i, soc_curr)
            if force_order:
                can_dis_net = min(can_dis_net, pending_arbi_net)

            if can_dis_net > EPS:
                prop_dis_ac[i] = can_dis_net * max(eta_dis, EPS)  # [MWh AC]
                soc_curr = max(0.0, soc_curr - can_dis_net)

                if pending_arbi_net > 0.0:
                    prev_pending = pending_arbi_net
                    pending_arbi_net = max(0.0, pending_arbi_net - can_dis_net)
                    if prev_pending > 0.0 and pending_arbi_net <= EPS:
                        cycles_used[dkey] = cycles_used[dkey] + 1

                dec_dis[i] = True

    # --- [7] DIAG końcowy -------------------------------------------------------------------------
    margin_ch  = delta_k - thr_low
    margin_dis = delta_k - thr_high

    # --- [8] Wyjście ------------------------------------------------------------------------------
    out = dict(P)
    out["prop_arbi_ch_from_grid_ac_mwh"] = prop_ch_ac
    out["prop_arbi_dis_to_grid_ac_mwh"]  = prop_dis_ac

    out["dec_ch"]       = dec_ch
    out["dec_dis"]      = dec_dis
    out["dec_ch_base"]  = dec_ch_base
    out["dec_dis_base"] = dec_dis_base

    out["thr_low"]   = thr_low
    out["thr_high"]  = thr_high
    out["delta_k"]   = delta_k
    out["margin_ch"] = margin_ch
    out["margin_dis"]= margin_dis

    return out
