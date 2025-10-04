# calc/commit_02.py
from __future__ import annotations

from typing import Any, Dict
import numpy as np

EPS = 1e-12

def _as_arr(H: Dict[str, Any], key: str, dtype=float) -> np.ndarray:
    v = H[key]
    return v.astype(dtype) if isinstance(v, np.ndarray) else np.array([v], dtype=dtype)

def _scalar_at(H: Dict[str, Any], key: str, i: int) -> float:
    v = H[key]
    if isinstance(v, np.ndarray):
        return float(v[i])
    return float(v)

def commit(H: Dict[str, Any], P: Dict[str, Any]) -> Dict[str, Any]:
    """
    02_commit — egzekucja limitów, SOC i mocy umownej (RAM-first).

    Kontrakt C (wektory N) — DOPASOWANY do raport_03.stage02:
      - charge_from_surplus_mwh        (NET do SOC_OZE z nadwyżki autokonsumpcji)
      - charge_from_grid_mwh           (AC zakup do ładowania ARBI)
      - discharge_to_load_mwh          (NET do LOAD: OZE + ARBI)
      - discharge_to_grid_mwh          (AC eksport do sieci — po autokonsumpcji + arbitraż)
      - loss_discharge_to_load_mwh
      - loss_idle_oze_mwh, loss_idle_arbi_mwh
      - soc_oze_before_mwh, soc_arbi_before_mwh
      - soc_oze_after_idle_mwh, soc_arbi_after_idle_mwh

    Dodatkowo liczymy:
      - e_import_mwh / e_curtailment_mwh
    """
    if "N" not in H:
        raise KeyError("[02] Wymagam H['N'] (RAM-first, zero fallbacków).")
    N = int(H["N"])

    # Propozycje w AC (arbitraż)
    prop_dis_ac = _as_arr(P, "prop_arbi_dis_to_grid_ac_mwh")
    prop_ch_ac  = _as_arr(P, "prop_arbi_ch_from_grid_ac_mwh")
    if len(prop_dis_ac) != N or len(prop_ch_ac) != N:
        raise RuntimeError("[02] Wektory P mają inny N niż H.")

    # Wyjścia (C) — nazewnictwo zgodne ze stage_02
    charge_from_surplus_mwh = np.zeros(N, dtype=np.float64)  # == e_oze_ch_from_surplus_net_mwh
    charge_from_grid_mwh    = np.zeros(N, dtype=np.float64)  # AC zakup do ładowania ARBI (apply_ch_ac)
    discharge_to_load_mwh   = np.zeros(N, dtype=np.float64)  # NET: OZE+ARBI do LOAD
    discharge_to_grid_mwh   = np.zeros(N, dtype=np.float64)  # AC eksport do sieci (po autokonsumpcji + arbitraż)

    # Pozostałe śledzone wielkości
    e_import_mwh      = np.zeros(N, dtype=np.float64)  # całkowity import AC (LOAD niedobór + charge_from_grid)
    e_export_mwh      = np.zeros(N, dtype=np.float64)  # całkowity eksport AC (surplus po OZE/ARBI + arbitraż)
    e_curtailment_mwh = np.zeros(N, dtype=np.float64)

    soc_oze_before_mwh  = np.zeros(N, dtype=np.float64)
    soc_arbi_before_mwh = np.zeros(N, dtype=np.float64)
    soc_oze_after_idle_mwh  = np.zeros(N, dtype=np.float64)
    soc_arbi_after_idle_mwh = np.zeros(N, dtype=np.float64)

    loss_charge_from_grid_mwh    = np.zeros(N, dtype=np.float64)
    loss_charge_from_surplus_mwh = np.zeros(N, dtype=np.float64)
    loss_discharge_to_load_mwh   = np.zeros(N, dtype=np.float64)
    loss_idle_oze_mwh            = np.zeros(N, dtype=np.float64)
    loss_idle_arbi_mwh           = np.zeros(N, dtype=np.float64)

    # Stan początkowy SOC (tylko pierwszy rekord w serii)
    soc_oze = float(_scalar_at(H, "soc_oze_start_mwh", 0))
    soc_arbi= float(_scalar_at(H, "soc_arbi_start_mwh", 0))

    emax_oze_arr  = _as_arr(H, "emax_oze_mwh")
    emax_arbi_arr = _as_arr(H, "emax_arbi_mwh")

    # Cappy (NET-na-godzinę)
    cap_oze_ch_net  = _as_arr(H, "e_ch_cap_net_oze_mwh")
    cap_oze_dis_net = _as_arr(H, "e_dis_cap_net_oze_mwh")
    cap_arbi_ch_net = _as_arr(H, "e_ch_cap_net_arbi_mwh")
    cap_arbi_dis_net= _as_arr(H, "e_dis_cap_net_arbi_mwh")

    # Inne wektory
    e_surplus = _as_arr(H, "e_surplus_mwh")
    e_deficit = _as_arr(H, "e_deficit_mwh")
    h_len_h   = _as_arr(H, "h_len_h", float)
    moc_umowna_mw = _as_arr(H, "moc_umowna_mw")

    eta_ch_arr  = _as_arr(H, "eta_ch_frac")
    eta_dis_arr = _as_arr(H, "eta_dis_frac")
    lam_h_arr   = _as_arr(H, "bess_lambda_h_frac")

    arbi_dis_to_load_arr = _as_arr(H, "arbi_dis_to_load", dtype=np.bool_)

    for i in range(N):
        eta_ch  = float(eta_ch_arr[i])
        eta_dis = float(eta_dis_arr[i])
        lam_h   = float(lam_h_arr[i])
        dt_h    = float(h_len_h[i])  # długość tej godziny w [h]

        emax_oze  = float(emax_oze_arr[i])
        emax_arbi = float(emax_arbi_arr[i])

        cap_oze_ch  = float(cap_oze_ch_net[i])
        cap_oze_dis = float(cap_oze_dis_net[i])
        cap_arbi_ch = float(cap_arbi_ch_net[i])
        cap_arbi_dis= float(cap_arbi_dis_net[i])

        surplus = float(e_surplus[i])
        deficit = float(e_deficit[i])

        # Limit eksportu AC w tej godzinie (moc * czas[h])
        export_cap_ac = max(0.0, float(moc_umowna_mw[i])) * dt_h
        export_used_ac = 0.0

        # ====== AUTOKONSUMPCJA: NADWYŻKA ======
        if surplus > EPS:
            # OZE ładowanie (AC→NET), wynik NET idzie do SOC_OZE
            headroom_oze = max(0.0, emax_oze - soc_oze)
            max_by_surplus_net = surplus * max(eta_ch, EPS)  # X_net ≤ surplus * eta_ch
            x_net = min(cap_oze_ch, headroom_oze, max_by_surplus_net)
            if x_net > EPS:
                ac_used = x_net / max(eta_ch, EPS)
                charge_from_surplus_mwh[i] = x_net
                soc_oze = min(emax_oze, soc_oze + x_net)
                surplus = max(0.0, surplus - ac_used)
                loss_charge_from_surplus_mwh[i] += max(0.0, ac_used - x_net)

            # Spill do ARBI (AC→NET)
            headroom_arbi = max(0.0, emax_arbi - soc_arbi)
            max_by_surplus_net = surplus * max(eta_ch, EPS)
            x_net = min(cap_arbi_ch, headroom_arbi, max_by_surplus_net)
            if x_net > EPS:
                ac_used = x_net / max(eta_ch, EPS)
                soc_arbi = min(emax_arbi, soc_arbi + x_net)
                surplus = max(0.0, surplus - ac_used)
                loss_charge_from_surplus_mwh[i] += max(0.0, ac_used - x_net)

            # Reszta → eksport (AC)
            export_room = max(0.0, export_cap_ac - export_used_ac)
            export_oze_ac = min(surplus, export_room)
            if export_oze_ac > EPS:
                e_export_mwh[i] += export_oze_ac
                discharge_to_grid_mwh[i] += export_oze_ac
                export_used_ac += export_oze_ac
                surplus -= export_oze_ac

            # Curtailment jeżeli jeszcze coś zostało po limicie
            if surplus > EPS:
                e_curtailment_mwh[i] = surplus
                # „surplus” przepada

        # ====== AUTOKONSUMPCJA: DEFICYT ======
        elif deficit > EPS:
            # OZE → LOAD (NET→AC)
            max_net = min(cap_oze_dis, soc_oze)
            need_net = deficit / max(eta_dis, EPS)
            x_net = min(max_net, need_net)
            if x_net > EPS:
                x_ac = x_net * max(eta_dis, EPS)
                discharge_to_load_mwh[i] += x_net
                soc_oze = max(0.0, soc_oze - x_net)
                deficit = max(0.0, deficit - x_ac)
                loss_discharge_to_load_mwh[i] += max(0.0, x_net - x_ac)

            # ARBI → LOAD jeżeli dozwolone
            if arbi_dis_to_load_arr[i]:
                max_net = min(cap_arbi_dis, soc_arbi)
                need_net = deficit / max(eta_dis, EPS)
                x_net = min(max_net, need_net)
                if x_net > EPS:
                    x_ac = x_net * max(eta_dis, EPS)
                    discharge_to_load_mwh[i] += x_net
                    soc_arbi = max(0.0, soc_arbi - x_net)
                    deficit = max(0.0, deficit - x_ac)
                    loss_discharge_to_load_mwh[i] += max(0.0, x_net - x_ac)

            # Reszta → import (AC; bez limitu)
            if deficit > EPS:
                e_import_mwh[i] += deficit
                deficit = 0.0

        # ====== ARBITRAŻ (po autokonsumpcji) ======

        # Sprzedaż (ARBI→GRID, AC)
        res_dis_net = max(0.0, min(cap_arbi_dis, soc_arbi))
        export_room = max(0.0, export_cap_ac - export_used_ac)
        apply_dis_ac = min(float(prop_dis_ac[i]), res_dis_net * max(eta_dis, EPS), export_room)
        if apply_dis_ac > EPS:
            used_net = apply_dis_ac / max(eta_dis, EPS)
            soc_arbi = max(0.0, soc_arbi - used_net)
            e_export_mwh[i] += apply_dis_ac
            discharge_to_grid_mwh[i] += apply_dis_ac
            export_used_ac += apply_dis_ac
            # straty rozładowania księgujemy razem z „to_load” metryką
            loss_discharge_to_load_mwh[i] += max(0.0, used_net - apply_dis_ac)

        # Zakup (GRID→ARBI, AC)
        res_ch_net = max(0.0, min(cap_arbi_ch, max(0.0, emax_arbi - soc_arbi)))
        max_ch_ac_by_soccap = res_ch_net / max(eta_ch, EPS)
        apply_ch_ac = min(float(prop_ch_ac[i]), max_ch_ac_by_soccap)
        if apply_ch_ac > EPS:
            gain_net = apply_ch_ac * max(eta_ch, EPS)
            soc_arbi = min(emax_arbi, soc_arbi + gain_net)
            e_import_mwh[i] += apply_ch_ac
            charge_from_grid_mwh[i] += apply_ch_ac
            loss_charge_from_grid_mwh[i] += max(0.0, apply_ch_ac - gain_net)

        # ====== Idle (po wszystkim) — skalowane długością godziny ======
        pre_oze  = soc_oze
        pre_arbi = soc_arbi
        soc_oze_before_mwh[i]  = pre_oze
        soc_arbi_before_mwh[i] = pre_arbi

        idle_loss_oze  = max(0.0, min(pre_oze,  pre_oze  * lam_h * dt_h))
        idle_loss_arbi = max(0.0, min(pre_arbi, pre_arbi * lam_h * dt_h))

        soc_oze  = max(0.0, pre_oze  - idle_loss_oze)
        soc_arbi = max(0.0, pre_arbi - idle_loss_arbi)

        loss_idle_oze_mwh[i]  = idle_loss_oze
        loss_idle_arbi_mwh[i] = idle_loss_arbi

        # Snap po idle
        soc_oze_after_idle_mwh[i]  = soc_oze
        soc_arbi_after_idle_mwh[i] = soc_arbi

    C: Dict[str, Any] = {
        "N": N,

        # KONTRAKT stage_02 (raport_03)
        "charge_from_surplus_mwh":        charge_from_surplus_mwh,
        "charge_from_grid_mwh":           charge_from_grid_mwh,
        "discharge_to_load_mwh":          discharge_to_load_mwh,
        "discharge_to_grid_mwh":          discharge_to_grid_mwh,
        "loss_discharge_to_load_mwh":     loss_discharge_to_load_mwh,
        "loss_idle_oze_mwh":              loss_idle_oze_mwh,
        "loss_idle_arbi_mwh":             loss_idle_arbi_mwh,
        "soc_oze_before_mwh":             soc_oze_before_mwh,
        "soc_arbi_before_mwh":            soc_arbi_before_mwh,
        "soc_oze_after_idle_mwh":         soc_oze_after_idle_mwh,
        "soc_arbi_after_idle_mwh":        soc_arbi_after_idle_mwh,

        # Dodatkowo: przydaje się do analizy
        "e_import_mwh":                   e_import_mwh,
        "e_export_mwh":                   e_export_mwh,
        "e_curtailment_mwh":              e_curtailment_mwh,
        "loss_charge_from_grid_mwh":      loss_charge_from_grid_mwh,
        "loss_charge_from_surplus_mwh":   loss_charge_from_surplus_mwh,
    }
    if "calc_id" in H:   C["calc_id"] = H["calc_id"]
    if "params_ts" in H: C["params_ts"] = H["params_ts"]
    return C
