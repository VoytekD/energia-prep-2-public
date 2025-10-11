# commit_02.py — Commit przepływów + TELEMETRIA + KPI finansowe (TGE)
# -----------------------------------------------------------------------------
# KOLEJNOŚĆ W GODZINIE:
#  1) SURPLUS (NET) → ARBI (NET), dopiero reszta → eksport (AC).
#    Eksport z SURPLUS trafia wyłącznie do export_from_surplus_ac_mwh; nie zwiększa discharge_to_grid_mwh.
#  2) DEFICYT LOAD (NET) → najpierw ARBI→LOAD (NET), reszta → import (AC).
#  3) ARBI → GRID (AC) wg propozycji 01 (limity: SOC, ηdis, cap_export).
#  4) ARBI ← GRID (AC) wg propozycji 01 (limity: headroom, ηch, cap_import).
#  5) IDLE-LOSS (λ) na końcu godziny; snapshoty SOC po idle.
#
# TELEMETRIA DODANA:
#  • loss_idle_*_mwh                  — straty postojowe (λ)
#  • loss_conv_ch_mwh                 — straty sprawności przy ładowaniu z GRID
#  • loss_conv_dis_to_grid_mwh        — straty sprawności przy rozładowaniu na GRID
#  • loss_conv_dis_to_load_mwh        — straty sprawności przy rozładowaniu na LOAD (diagnostycznie)
#  • wasted_surplus_due_to_export_cap_mwh — niewyeksportowana nadwyżka z powodu cap’u
#  • cap_blocked_*_ac_mwh             — część propozycji 01 obcięta cap’em (nie „straty”, ale warto raportować)
#  • unserved_load_after_cap_ac_mwh   — niedostarczony LOAD po ograniczeniach cap (diagnostycznie)
#
# KPI FINANSOWE:
#  • rev_arbi_to_grid_pln, rev_surplus_export_pln
#  • cost_grid_to_arbi_pln, cost_import_for_load_pln
#  • cashflow_net_pln

from __future__ import annotations

import logging
from typing import Dict, Any
import numpy as np

log = logging.getLogger("energia-prep-2.calc.commit_02")

EPS = 1e-12

def _as_vec(x, N, dtype=np.float64):
    """Zwraca wektor długości N. Jeśli x jest skalarem → broadcast."""
    arr = np.asarray(x)
    if arr.ndim == 0:
        return np.full(N, float(arr), dtype=dtype)
    return arr.astype(dtype, copy=False)

def run(H: Dict[str, Any], P: Dict[str, Any]) -> Dict[str, Any]:
    ts_hour = H["ts_hour"]
    N = len(ts_hour)

    # Pojemności — SKALARY
    emax_arbi = float(H.get("emax_arbi_mwh", 0.0))
    emax_oze  = float(H.get("emax_oze_mwh",  0.0))

    # Sprawności i straty
    eta_ch  = float(H["eta_ch_frac"])
    eta_dis = float(H["eta_dis_frac"])
    lambda_h = float(H.get("bess_lambda_h_frac", 0.0))  # jedna lambda/h dla obu "komór"

    # SOC startowe
    soc_oze  = float(H["soc0_oze_mwh"])
    soc_arbi = float(H["soc0_arbi_mwh"])

    # Capy (AC/NET) — dopuszczamy skalar lub wektor
    cap_export_ac_mwh = _as_vec(H["cap_export_ac_mwh"], N)
    cap_import_ac_mwh = _as_vec(H["cap_import_ac_mwh"], N)
    cap_ch_net_mwh    = _as_vec(H["cap_charge_net_mwh"], N)
    cap_dis_net_mwh   = _as_vec(H["cap_discharge_net_mwh"], N)

    # Sygnały bazowe (kopie — nie mutujemy H)
    surplus_net_mwh = _as_vec(H["surplus_net_mwh"], N).copy()
    load_net_mwh    = _as_vec(
        H.get("load_net_mwh", H.get("p_load_net_mwh", H.get("deficit_mwh", np.zeros(N)))),
        N
    ).copy()

    # Ceny — bezpiecznie: aliasy z ingest lub fallback do fixing_ii_price
    price_import = _as_vec(
        H.get("price_import_pln_mwh", H.get("fixing_ii_price", np.zeros(N))),
        N
    )
    price_export = _as_vec(
        H.get("price_export_pln_mwh", H.get("fixing_ii_price", np.zeros(N))),
        N
    )

    # Propozycje 01 (AC)
    prop_dis_to_grid_ac_mwh  = _as_vec(P["prop_arbi_dis_to_grid_ac_mwh"], N)
    prop_ch_from_grid_ac_mwh = _as_vec(P["prop_arbi_ch_from_grid_ac_mwh"], N)

    # Przepływy (NET/AC)
    charge_from_surplus_mwh = np.zeros(N, dtype=np.float64)  # NET OZE→ARBI
    charge_from_grid_mwh    = np.zeros(N, dtype=np.float64)  # NET ARBI (ile NET po ηch wylądowało w ARBI)
    discharge_to_load_mwh   = np.zeros(N, dtype=np.float64)  # NET ARBI→LOAD
    discharge_to_grid_mwh   = np.zeros(N, dtype=np.float64)  # AC  ARBI→GRID (wykonane)
    # UWAGA: discharge_to_grid_mwh dotyczy WYŁĄCZNIE ARBI→GRID; eksport z SURPLUS jest liczony w export_from_surplus_ac_mwh
    e_import_mwh            = np.zeros(N, dtype=np.float64)  # AC  całkowity import (LOAD + ARBI charge)
    e_export_mwh            = np.zeros(N, dtype=np.float64)  # AC  całkowity eksport (SURPLUS + ARBI→GRID)

    # Rozbicia AC do finansów/telemetrii
    export_from_surplus_ac_mwh = np.zeros(N, dtype=np.float64)  # AC z SURPLUS (po tym jak część poszła do ARBI)
    export_from_arbi_ac_mwh    = np.zeros(N, dtype=np.float64)  # AC z ARBI→GRID (semantycznie == discharge_to_grid_mwh)
    import_for_load_ac_mwh     = np.zeros(N, dtype=np.float64)  # AC dla pokrycia deficytu LOAD (po ARBI→LOAD)
    import_for_arbi_ac_mwh     = np.zeros(N, dtype=np.float64)  # AC potrzebne do naładowania ARBI (== e_import na ARBI)

    # Snapshoty SOC + procenty
    soc_oze_before_idle_mwh  = np.zeros(N, dtype=np.float64)
    soc_oze_after_idle_mwh   = np.zeros(N, dtype=np.float64)
    soc_arbi_before_idle_mwh = np.zeros(N, dtype=np.float64)  # snapshot przed stratami
    soc_arbi_after_idle_mwh  = np.zeros(N, dtype=np.float64)
    soc_arbi_pct_of_arbi     = np.zeros(N, dtype=np.float64)
    soc_arbi_pct_of_total    = np.zeros(N, dtype=np.float64)
    loss_idle_oze_mwh        = np.zeros(N, dtype=np.float64)
    loss_idle_arbi_mwh       = np.zeros(N, dtype=np.float64)

    # Straty sprawności i przez cap
    loss_conv_ch_mwh              = np.zeros(N, dtype=np.float64)
    loss_conv_dis_to_grid_mwh     = np.zeros(N, dtype=np.float64)
    loss_conv_dis_to_load_mwh     = np.zeros(N, dtype=np.float64)
    wasted_surplus_due_to_export_cap_mwh = np.zeros(N, dtype=np.float64)

    # KPI finansowe per godzina (PLN)
    rev_arbi_to_grid_pln     = np.zeros(N, dtype=np.float64)
    rev_surplus_export_pln   = np.zeros(N, dtype=np.float64)
    cost_grid_to_arbi_pln    = np.zeros(N, dtype=np.float64)
    cost_import_for_load_pln = np.zeros(N, dtype=np.float64)
    cashflow_net_pln         = np.zeros(N, dtype=np.float64)

    # Diagnoza capów: binding & blokady
    bind_export_cap = np.zeros(N, dtype=bool)
    bind_import_cap = np.zeros(N, dtype=bool)
    cap_blocked_dis_ac_mwh = np.zeros(N, dtype=np.float64)  # (opcjonalnie w przyszłości)
    cap_blocked_ch_ac_mwh  = np.zeros(N, dtype=np.float64)
    unserved_load_after_cap_ac_mwh = np.zeros(N, dtype=np.float64)

    # Pętla godzinowa
    for i in range(N):
        # --------------------------
        # 1) SURPLUS → ARBI, reszta → eksport
        if surplus_net_mwh[i] > EPS:
            # ile możemy załadować do ARBI (NET) biorąc pod uwagę headroom?
            headroom_arbi = max(0.0, emax_arbi - soc_arbi)
            can_to_arbi_net = min(surplus_net_mwh[i], headroom_arbi, cap_ch_net_mwh[i])
            if can_to_arbi_net > EPS:
                charge_from_surplus_mwh[i] += can_to_arbi_net
                surplus_net_mwh[i] -= can_to_arbi_net
                soc_arbi = min(emax_arbi, soc_arbi + can_to_arbi_net)
            # export z pozostałej nadwyżki (AC), ograniczony cap_export
            if surplus_net_mwh[i] > EPS:
                export_ac = surplus_net_mwh[i]
                if np.isfinite(cap_export_ac_mwh[i]) and export_ac > cap_export_ac_mwh[i] + EPS:
                    bind_export_cap[i] = True
                    export_ac = cap_export_ac_mwh[i]
                if export_ac > EPS:
                    e_export_mwh[i] += export_ac
                    export_from_surplus_ac_mwh[i] += export_ac
                # binding eksportu?
                if np.isfinite(cap_export_ac_mwh[i]) and export_ac + EPS >= cap_export_ac_mwh[i]:
                    bind_export_cap[i] = True
            # Niewyeksportowana nadwyżka jeśli cap ograniczył:
            if bind_export_cap[i] and surplus_net_mwh[i] > EPS:
                wasted_surplus_due_to_export_cap_mwh[i] = surplus_net_mwh[i]
                surplus_net_mwh[i] = 0.0  # „stracona” (nieużyta) nadwyżka w tej godzinie

        # --------------------------
        # 2) DEFICYT LOAD → ARBI→LOAD, reszta → import
        deficit = max(0.0, load_net_mwh[i])
        if deficit > EPS:
            # użyj zasobów ARBI (NET)
            can_from_arbi = min(deficit, soc_arbi)  # <— fixed: było soc_arbi_mwh (nieistnieje)
            if can_from_arbi > EPS:
                discharge_to_load_mwh[i] += can_from_arbi       # NET
                soc_arbi = max(0.0, soc_arbi - can_from_arbi)
                deficit  -= can_from_arbi
                # straty konwersji przy zasilaniu LOAD (diagnostycznie)
                loss_conv_dis_to_load_mwh[i] += can_from_arbi * (1.0/eta_dis - 1.0) if eta_dis > EPS else 0.0
            # reszta z GRID (AC), ograniczona cap_import
            if deficit > EPS:
                import_ac = deficit
                if np.isfinite(cap_import_ac_mwh[i]) and import_ac > cap_import_ac_mwh[i] + EPS:
                    bind_import_cap[i] = True
                    import_ac = cap_import_ac_mwh[i]
                if import_ac > EPS:
                    e_import_mwh[i] += import_ac
                    import_for_load_ac_mwh[i] += import_ac
                if np.isfinite(cap_import_ac_mwh[i]) and import_ac + EPS >= cap_import_ac_mwh[i]:
                    bind_import_cap[i] = True

        # --------------------------
        # 3) ARBI → GRID (AC) wg propozycji 01
        out_ac = min(prop_dis_to_grid_ac_mwh[i],
                     cap_export_ac_mwh[i] if np.isfinite(cap_export_ac_mwh[i]) else prop_dis_to_grid_ac_mwh[i])
        out_ac = min(out_ac, soc_arbi * eta_dis)  # po uwzględnieniu sprawności (AC)
        if out_ac > EPS:
            discharge_to_grid_mwh[i] += out_ac
            export_from_arbi_ac_mwh[i] += out_ac
            e_export_mwh[i] += out_ac
            # energia NET zużyta z ARBI
            used_net = out_ac / eta_dis if eta_dis > EPS else out_ac
            soc_arbi = max(0.0, soc_arbi - used_net)
            # straty sprawności przy rozładowaniu na GRID
            loss_conv_dis_to_grid_mwh[i] += used_net - out_ac

        # --------------------------
        # 4) ARBI ← GRID (AC) wg propozycji 01
        in_ac = min(prop_ch_from_grid_ac_mwh[i],
                    cap_import_ac_mwh[i] if np.isfinite(cap_import_ac_mwh[i]) else prop_ch_from_grid_ac_mwh[i])
        # ile NET trafi do ARBI po uwzględnieniu sprawności ładowania?
        net_to_arbi = in_ac * eta_ch
        # sprawdź headroom i cap_ch_net
        headroom_arbi = max(0.0, emax_arbi - soc_arbi)
        net_to_arbi = min(net_to_arbi, headroom_arbi, cap_ch_net_mwh[i])
        if net_to_arbi > EPS:
            charge_from_grid_mwh[i] += net_to_arbi      # NET przyrost soc_arbi
            soc_arbi = min(emax_arbi, soc_arbi + net_to_arbi)
            # ile AC potrzeba było na ten net_to_arbi?
            ac_needed = net_to_arbi / eta_ch if eta_ch > EPS else net_to_arbi
            e_import_mwh[i] += ac_needed
            import_for_arbi_ac_mwh[i] += ac_needed
            # straty sprawności ładowania
            loss_conv_ch_mwh[i] += ac_needed - net_to_arbi
        else:
            # jeśli cap_ch_net ograniczył, raportuj blokadę propozycji:
            if cap_ch_net_mwh[i] + EPS < (prop_ch_from_grid_ac_mwh[i] * eta_ch):
                cap_blocked_ch_ac_mwh[i] += (prop_ch_from_grid_ac_mwh[i] * eta_ch) - cap_ch_net_mwh[i]

        # --------------------------
        # 5) IDLE-LOSS na obu komorach
        soc_oze_before_idle_mwh[i]  = soc_oze
        soc_arbi_before_idle_mwh[i] = soc_arbi
        if lambda_h > EPS:
            # proporcjonalnie do energii zakumulowanej na koniec godziny (po 1..4)
            loss_oze  = soc_oze  * lambda_h
            loss_arbi = soc_arbi * lambda_h
            soc_oze  = max(0.0, soc_oze  - loss_oze)
            soc_arbi = max(0.0, soc_arbi - loss_arbi)
            loss_idle_oze_mwh[i]  += loss_oze
            loss_idle_arbi_mwh[i] += loss_arbi
        soc_oze_after_idle_mwh[i]  = soc_oze
        soc_arbi_after_idle_mwh[i] = soc_arbi

        # procenty SOC (diagnostyczne)
        total_emax = max(0.0, emax_oze + emax_arbi)
        soc_arbi_pct_of_arbi[i]  = (100.0 * soc_arbi_after_idle_mwh[i] / emax_arbi) if emax_arbi > EPS else 0.0
        soc_arbi_pct_of_total[i] = (100.0 * (soc_oze_after_idle_mwh[i] + soc_arbi_after_idle_mwh[i]) / total_emax) if total_emax > EPS else 0.0

        # niedostarczony LOAD jeśli cap_import przyciął
        if bind_import_cap[i] and load_net_mwh[i] > EPS:
            unserved = max(0.0, load_net_mwh[i] - (discharge_to_load_mwh[i] + import_for_load_ac_mwh[i]))
            if unserved > EPS:
                unserved_load_after_cap_ac_mwh[i] += unserved

    # KPI finansowe (PLN) — WEKTOROWO
    rev_arbi_to_grid_pln     = export_from_arbi_ac_mwh    * price_export
    rev_surplus_export_pln   = export_from_surplus_ac_mwh * price_export
    cost_grid_to_arbi_pln    = import_for_arbi_ac_mwh     * price_import
    cost_import_for_load_pln = import_for_load_ac_mwh     * price_import
    cashflow_net_pln         = (rev_arbi_to_grid_pln + rev_surplus_export_pln
                                - cost_grid_to_arbi_pln - cost_import_for_load_pln)

    # Zbiórka wyników
    out: Dict[str, Any] = {}

    out["charge_from_surplus_mwh"] = charge_from_surplus_mwh  # NET
    out["charge_from_grid_mwh"]    = charge_from_grid_mwh     # NET (ile trafiło do ARBI)
    out["discharge_to_load_mwh"]   = discharge_to_load_mwh    # NET
    out["discharge_to_grid_mwh"]   = discharge_to_grid_mwh    # AC
    out["e_import_mwh"]            = e_import_mwh
    out["e_export_mwh"]            = e_export_mwh

    out["export_from_surplus_ac_mwh"] = export_from_surplus_ac_mwh
    out["export_from_arbi_ac_mwh"]    = export_from_arbi_ac_mwh
    out["import_for_load_ac_mwh"]     = import_for_load_ac_mwh
    out["import_for_arbi_ac_mwh"]     = import_for_arbi_ac_mwh

    out["soc_oze_before_idle_mwh"]  = soc_oze_before_idle_mwh
    out["soc_oze_after_idle_mwh"]   = soc_oze_after_idle_mwh
    out["soc_arbi_before_idle_mwh"] = soc_arbi_before_idle_mwh
    out["soc_arbi_after_idle_mwh"]  = soc_arbi_after_idle_mwh
    out["soc_arbi_pct_of_arbi"]     = soc_arbi_pct_of_arbi
    out["soc_arbi_pct_of_total"]    = soc_arbi_pct_of_total

    out["loss_idle_oze_mwh"]  = loss_idle_oze_mwh
    out["loss_idle_arbi_mwh"] = loss_idle_arbi_mwh
    out["loss_conv_ch_mwh"]              = loss_conv_ch_mwh
    out["loss_conv_dis_to_grid_mwh"]     = loss_conv_dis_to_grid_mwh
    out["loss_conv_dis_to_load_mwh"]     = loss_conv_dis_to_load_mwh
    out["wasted_surplus_due_to_export_cap_mwh"] = wasted_surplus_due_to_export_cap_mwh

    out["cap_blocked_dis_ac_mwh"] = cap_blocked_dis_ac_mwh
    out["cap_blocked_ch_ac_mwh"]  = cap_blocked_ch_ac_mwh
    out["unserved_load_after_cap_ac_mwh"] = unserved_load_after_cap_ac_mwh

    out["price_import_pln_mwh"]     = price_import
    out["price_export_pln_mwh"]     = price_export
    out["rev_arbi_to_grid_pln"]     = rev_arbi_to_grid_pln
    out["rev_surplus_export_pln"]   = rev_surplus_export_pln
    out["cost_grid_to_arbi_pln"]    = cost_grid_to_arbi_pln
    out["cost_import_for_load_pln"] = cost_import_for_load_pln
    out["cashflow_net_pln"]         = cashflow_net_pln

    log.info("[02] end — OK")
    return out
