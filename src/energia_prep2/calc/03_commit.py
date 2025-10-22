# 03_commit.py — ETAP 03 (COMMIT): rozliczenie przepływów (OZE + ARBI) + SOC + KPI
# =================================================================================
# KOLEJNOŚĆ W GODZINIE (ustalenia końcowe):
#   1) SURPLUS (NET) → priorytet: OZE ← SURPLUS (NET) → ARBI ← SURPLUS (NET) → eksport (AC).
#      Cap BESS (NET) dotyczy WYŁĄCZNIE ładowania ARBI; OZE NIE zużywa capu BESS.
#   2) DEFICYT (NET) → priorytet: OZE → LOAD (NET) → (jeśli arbi_dis_to_load=True) ARBI → LOAD (NET) → import (AC).
#      Cap BESS (NET) dotyczy WYŁĄCZNIE rozładowania ARBI; OZE → LOAD NIE zużywa capu BESS.
#   3) ARBI → GRID (AC) wg sygnału 02 (prop_arbi_dis_to_grid_ac_mwh), ograniczony: SOC, C_dis (BESS), η_dis.
#   4) ARBI ← GRID (AC) wg sygnału 02 (prop_arbi_ch_from_grid_ac_mwh), ograniczony: headroom, C_ch (BESS), η_ch.
#   5) EGZEKUCJA MOCY UMOWNEJ (GRID, AC) NA SUMACH:
#        • eksport_total = eksport z SURPLUS + ARBI → GRID ≤ cap_grid_export_ac_mwh,
#          tniemy z priorytetem: najpierw ARBI→GRID, potem SURPLUS (reszta = waste).
#        • import_total  = import na LOAD + ARBI ← GRID ≤ cap_grid_import_ac_mwh,
#          tniemy z priorytetem: najpierw ARBI←GRID (cofamy SOC/NET/straty), potem import na LOAD (reszta = unserved).
#   6) IDLE-LOSS (λ) na końcu godziny (osobno dla OZE i ARBI); snapshoty SOC po idle.
#
# TELEMETRIA I KPI bez zmian poza tym, że straty konwersji do LOAD liczymy także dla OZE→LOAD.
# =================================================================================

from __future__ import annotations

import logging
from typing import Dict, Any
import numpy as np

log = logging.getLogger("energia-prep-2.calc.commit_03")
EPS = 1e-12


def _as_vec(x, N: int, dtype=np.float64):
    arr = np.asarray(x)
    if arr.ndim == 0:
        return np.full(N, float(arr), dtype=dtype)
    return arr.astype(dtype, copy=False)


def run(H: Dict[str, Any], P: Dict[str, Any]) -> Dict[str, Any]:
    ts_hour = H["ts_hour"]
    N = len(ts_hour)

    # --- Skalary pojemności i parametrów konwersji ---
    emax_arbi = float(H.get("emax_arbi_mwh", 0.0))
    emax_oze = float(H.get("emax_oze_mwh", 0.0))
    eta_ch = float(H["eta_ch_frac"])
    eta_dis = float(H["eta_dis_frac"])
    lambda_h = float(H.get("bess_lambda_h_frac", 0.0))

    # Polityki
    arbi_dis_to_load = bool(H.get("arbi_dis_to_load", True))  # czy ARBI może zasilać LOAD przy deficycie

    # SOC startowe
    soc_oze = float(H["soc0_oze_mwh"])
    soc_arbi = float(H["soc0_arbi_mwh"])

    # --- Capy godzinowe ---
    # GRID (AC) — moc umowna na SUMACH
    cap_grid_export_ac_mwh = _as_vec(H["cap_grid_export_ac_mwh"], N)
    cap_grid_import_ac_mwh = _as_vec(H["cap_grid_import_ac_mwh"], N)
    # BESS (NET) — przepustowość magazynu (C)
    cap_bess_charge_net_mwh = _as_vec(H["cap_bess_charge_net_mwh"], N)
    cap_bess_discharge_net_mwh = _as_vec(H["cap_bess_discharge_net_mwh"], N)

    # --- Bilans NET (z 01) ---
    surplus_net_mwh = _as_vec(H["surplus_net_mwh"], N).copy()
    deficit_net_mwh = _as_vec(H["deficit_net_mwh"], N).copy()

    # --- Ceny ---# brak/NaN/±Inf traktujemy jako 0.0 PLN/MWh — żeby revenue/cost/cashflow nie „zarażały się” NaN
    price_import = _as_vec(H.get("price_import_pln_mwh", np.zeros(N)), N)
    price_export = _as_vec(H.get("price_export_pln_mwh", np.zeros(N)), N)
    # Uwaga: chcemy rozróżnić "cena = 0" od "brak ceny".
    # Dlatego NIE nadpisujemy samych cen; tworzymy wersje 'effective' tylko do KPI:
    price_import_eff = np.where(np.isfinite(price_import), price_import, 0.0)
    price_export_eff = np.where(np.isfinite(price_export), price_export, 0.0)
    

    # --- Propozycje z 02 (AC) ---
    prop_dis_to_grid_ac_mwh = _as_vec(P["prop_arbi_dis_to_grid_ac_mwh"], N)
    prop_ch_from_grid_ac_mwh = _as_vec(P["prop_arbi_ch_from_grid_ac_mwh"], N)

    # --- Bufory przepływów ---
    # NET/AC rozbicia
    charge_from_surplus_mwh = np.zeros(N)       # NET → ARBI (z nadwyżki)
    charge_from_grid_mwh = np.zeros(N)          # NET → ARBI (z sieci)
    discharge_to_load_mwh = np.zeros(N)         # NET → LOAD (OZE + ARBI razem)
    discharge_to_grid_mwh = np.zeros(N)         # AC  → GRID (ARBI)
    e_import_mwh = np.zeros(N)                  # AC  ← GRID (LOAD + ARBI charge)
    e_export_mwh = np.zeros(N)                  # AC  → GRID (SURPLUS + ARBI dis)

    # Rozbicia AC na źródła/cel
    export_from_surplus_ac_mwh = np.zeros(N)    # AC z nadwyżki (po OZE/ARBI)
    export_from_arbi_ac_mwh = np.zeros(N)       # AC z ARBI→GRID
    import_for_load_ac_mwh = np.zeros(N)        # AC na pokrycie deficytu LOAD
    import_for_arbi_ac_mwh = np.zeros(N)        # AC na ładowanie ARBI

    # Snapshoty SOC i procenty
    soc_oze_before_idle_mwh = np.zeros(N)
    soc_oze_after_idle_mwh = np.zeros(N)
    soc_arbi_before_idle_mwh = np.zeros(N)
    soc_arbi_after_idle_mwh = np.zeros(N)
    soc_arbi_pct_of_arbi = np.zeros(N)
    soc_arbi_pct_of_total = np.zeros(N)
    soc_oze_pct_of_oze = np.zeros(N)
    soc_oze_pct_of_total = np.zeros(N)

    # Straty i ograniczenia
    loss_idle_oze_mwh = np.zeros(N)
    loss_idle_arbi_mwh = np.zeros(N)
    loss_conv_ch_mwh = np.zeros(N)              # straty ładowania (ARBI)
    loss_conv_dis_to_grid_mwh = np.zeros(N)     # straty rozładowania do GRID (ARBI)
    loss_conv_dis_to_load_mwh = np.zeros(N)     # straty rozładowania do LOAD (OZE i ARBI)
    wasted_surplus_due_to_export_cap_mwh = np.zeros(N)

    # Diagnostyka capów (bindy/cięcia)
    bind_export_cap = np.zeros(N, dtype=bool)
    bind_import_cap = np.zeros(N, dtype=bool)
    cap_blocked_dis_ac_mwh = np.zeros(N)        # info: propozycja 02 > możliwości (BESS C_dis / SOC) → GRID
    cap_blocked_ch_ac_mwh = np.zeros(N)         # info: propozycja 02 > możliwości (BESS C_ch / headroom) ← GRID
    unserved_load_after_cap_ac_mwh = np.zeros(N)

    # --- Pętla godzinowa ---
    for i in range(N):
        # Budżety capów BESS (NET) w tej godzinie
        dis_net_budget = float(cap_bess_discharge_net_mwh[i]) if np.isfinite(cap_bess_discharge_net_mwh[i]) else np.inf
        ch_net_budget  = float(cap_bess_charge_net_mwh[i])    if np.isfinite(cap_bess_charge_net_mwh[i])    else np.inf

        # 1) SURPLUS: OZE ← SURPLUS → ARBI ← SURPLUS → eksport
        if surplus_net_mwh[i] > EPS:
            # 1a) OZE — ładowanie NIE podlega capowi BESS
            headroom_oze = max(0.0, emax_oze - soc_oze)
            to_oze_net = min(surplus_net_mwh[i], headroom_oze)
            if to_oze_net > EPS:
                soc_oze = min(emax_oze, soc_oze + to_oze_net)
                surplus_net_mwh[i] -= to_oze_net

            # 1b) ARBI — ładowanie pod cap BESS (C_ch)
            headroom_arbi = max(0.0, emax_arbi - soc_arbi)
            to_arbi_net = min(surplus_net_mwh[i], headroom_arbi, ch_net_budget)
            if to_arbi_net > EPS:
                charge_from_surplus_mwh[i] += to_arbi_net
                surplus_net_mwh[i] -= to_arbi_net
                soc_arbi = min(emax_arbi, soc_arbi + to_arbi_net)
                if np.isfinite(cap_bess_charge_net_mwh[i]):
                    ch_net_budget = max(0.0, ch_net_budget - to_arbi_net)

            # 1c) Pozostała nadwyżka → eksport (AC) — BEZ capu GRID w tym kroku (tnij na sumach poniżej)
            if surplus_net_mwh[i] > EPS:
                export_ac = surplus_net_mwh[i]
                e_export_mwh[i] += export_ac
                export_from_surplus_ac_mwh[i] += export_ac
                surplus_net_mwh[i] = 0.0  # wszystko, co zostało, zamieniamy na eksport kandydacki

        # 2) DEFICYT: OZE → LOAD → (opc.) ARBI → LOAD → import
        deficit = max(0.0, deficit_net_mwh[i])
        if deficit > EPS:
            # 2a) OZE → LOAD — NIE podlega capowi BESS
            from_oze = min(deficit, soc_oze)
            if from_oze > EPS:
                soc_oze = max(0.0, soc_oze - from_oze)
                deficit -= from_oze
                discharge_to_load_mwh[i] += from_oze
                # strata konwersji do LOAD
                loss_conv_dis_to_load_mwh[i] += from_oze * (1.0 / eta_dis - 1.0) if eta_dis > EPS else 0.0

            # 2b) (Opcjonalnie) ARBI → LOAD — pod cap BESS (C_dis)
            if deficit > EPS and arbi_dis_to_load and dis_net_budget > EPS:
                can_from_arbi = min(deficit, soc_arbi, dis_net_budget)
                if can_from_arbi > EPS:
                    soc_arbi = max(0.0, soc_arbi - can_from_arbi)
                    deficit -= can_from_arbi
                    dis_net_budget = max(0.0, dis_net_budget - can_from_arbi)
                    discharge_to_load_mwh[i] += can_from_arbi
                    loss_conv_dis_to_load_mwh[i] += can_from_arbi * (1.0 / eta_dis - 1.0) if eta_dis > EPS else 0.0

            # 2c) Reszta deficytu → import (AC) — BEZ capu GRID w tym kroku (tnij na sumach poniżej)
            if deficit > EPS:
                import_ac = deficit
                e_import_mwh[i] += import_ac
                import_for_load_ac_mwh[i] += import_ac
                # jeśli coś zostaje po cięciu później, będzie to unserved_load_after_cap_ac_mwh

        # 3) ARBI → GRID (AC) wg propozycji 02, ograniczony SOC/C_dis/η_dis (BEZ capu GRID tu)
        if dis_net_budget > EPS:
            max_net_for_grid = min(soc_arbi, dis_net_budget)
            max_ac_for_grid = max_net_for_grid * eta_dis
            out_ac = min(prop_dis_to_grid_ac_mwh[i], max_ac_for_grid)
            if out_ac > EPS:
                used_net = out_ac / eta_dis if eta_dis > EPS else out_ac
                used_net = min(used_net, max_net_for_grid)
                soc_arbi = max(0.0, soc_arbi - used_net)
                dis_net_budget = max(0.0, dis_net_budget - used_net)
                discharge_to_grid_mwh[i] += out_ac
                export_from_arbi_ac_mwh[i] += out_ac
                e_export_mwh[i] += out_ac
                loss_conv_dis_to_grid_mwh[i] += used_net - out_ac
            else:
                # diagnostyka: ograniczenie przez BESS C_dis/SOC
                want_ac = prop_dis_to_grid_ac_mwh[i]
                possible_ac = max_net_for_grid * eta_dis
                blocked = max(0.0, want_ac - possible_ac)
                if blocked > EPS:
                    cap_blocked_dis_ac_mwh[i] += blocked

        # 4) ARBI ← GRID (AC) wg propozycji 02, ograniczony headroom/C_ch/η_ch (BEZ capu GRID tu)
        in_ac = prop_ch_from_grid_ac_mwh[i]
        net_to_arbi = in_ac * eta_ch
        headroom_arbi = max(0.0, emax_arbi - soc_arbi)
        net_to_arbi = min(net_to_arbi, headroom_arbi, ch_net_budget)
        if net_to_arbi > EPS:
            soc_arbi = min(emax_arbi, soc_arbi + net_to_arbi)
            charge_from_grid_mwh[i] += net_to_arbi
            ac_needed = net_to_arbi / eta_ch if eta_ch > EPS else net_to_arbi
            e_import_mwh[i] += ac_needed
            import_for_arbi_ac_mwh[i] += ac_needed
            loss_conv_ch_mwh[i] += ac_needed - net_to_arbi
            if np.isfinite(cap_bess_charge_net_mwh[i]):
                ch_net_budget = max(0.0, ch_net_budget - net_to_arbi)
        else:
            # diagnostyka: ograniczenie przez BESS C_ch/headroom
            want_net = prop_ch_from_grid_ac_mwh[i] * eta_ch
            blocked_net = max(0.0, want_net - max(0.0, min(headroom_arbi, ch_net_budget)))
            if blocked_net > EPS:
                cap_blocked_ch_ac_mwh[i] += blocked_net

        # 5) EGZEKUCJA MOCY UMOWNEJ (AC) NA SUMACH (po wszystkich ścieżkach)
        # Eksport (SURPLUS + ARBI)
        export_total = export_from_surplus_ac_mwh[i] + export_from_arbi_ac_mwh[i]
        if np.isfinite(cap_grid_export_ac_mwh[i]) and export_total > cap_grid_export_ac_mwh[i] + EPS:
            overflow = export_total - cap_grid_export_ac_mwh[i]
            # Priorytet: tnij najpierw ARBI→GRID
            cut_arbi = min(export_from_arbi_ac_mwh[i], overflow)
            if cut_arbi > EPS:
                export_from_arbi_ac_mwh[i] -= cut_arbi
                e_export_mwh[i] -= cut_arbi
                # odwrócenie NET/SOC (η_dis)
                net_revert = cut_arbi / eta_dis if eta_dis > EPS else cut_arbi
                soc_arbi = min(emax_arbi, soc_arbi + net_revert)
                # przywracamy budżet BESS DIS w granicach capu
                if np.isfinite(cap_bess_discharge_net_mwh[i]):
                    dis_net_budget = min(dis_net_budget + net_revert, cap_bess_discharge_net_mwh[i])
            overflow -= cut_arbi
            if overflow > EPS:
                cut_surplus = min(export_from_surplus_ac_mwh[i], overflow)
                if cut_surplus > EPS:
                    export_from_surplus_ac_mwh[i] -= cut_surplus
                    e_export_mwh[i] -= cut_surplus
                    wasted_surplus_due_to_export_cap_mwh[i] += cut_surplus
                overflow -= cut_surplus
            bind_export_cap[i] = True

        # Import (LOAD + ARBI)
        import_total = import_for_load_ac_mwh[i] + import_for_arbi_ac_mwh[i]
        if np.isfinite(cap_grid_import_ac_mwh[i]) and import_total > cap_grid_import_ac_mwh[i] + EPS:
            overflow = import_total - cap_grid_import_ac_mwh[i]
            # Priorytet: tnij najpierw GRID→ARBI (cofamy SOC/NET/straty)
            cut_arbi = min(import_for_arbi_ac_mwh[i], overflow)
            if cut_arbi > EPS:
                import_for_arbi_ac_mwh[i] -= cut_arbi
                e_import_mwh[i] -= cut_arbi
                # odwrócenie NET/SOC (η_ch)
                net_revert = cut_arbi * eta_ch
                if net_revert > EPS:
                    soc_arbi = max(0.0, soc_arbi - net_revert)
                    charge_from_grid_mwh[i] = max(0.0, charge_from_grid_mwh[i] - net_revert)
                    loss_conv_ch_mwh[i] = max(0.0, loss_conv_ch_mwh[i] - (cut_arbi - net_revert))
                    # przywrócenie budżetu BESS CH w granicach capu
                    if np.isfinite(cap_bess_charge_net_mwh[i]):
                        ch_net_budget = min(ch_net_budget + net_revert, cap_bess_charge_net_mwh[i])
            overflow -= cut_arbi
            if overflow > EPS:
                cut_load = min(import_for_load_ac_mwh[i], overflow)
                if cut_load > EPS:
                    import_for_load_ac_mwh[i] -= cut_load
                    e_import_mwh[i] -= cut_load
                    unserved_load_after_cap_ac_mwh[i] += cut_load
                overflow -= cut_load
            bind_import_cap[i] = True

        # 6) IDLE-LOSS (λ) + snapshoty SOC
        soc_oze_before_idle_mwh[i] = soc_oze
        soc_arbi_before_idle_mwh[i] = soc_arbi

        if lambda_h > EPS:
            loss_oze = soc_oze * lambda_h
            loss_arbi = soc_arbi * lambda_h
            soc_oze = max(0.0, soc_oze - loss_oze)
            soc_arbi = max(0.0, soc_arbi - loss_arbi)
            loss_idle_oze_mwh[i] += loss_oze
            loss_idle_arbi_mwh[i] += loss_arbi

        soc_oze_after_idle_mwh[i] = soc_oze
        soc_arbi_after_idle_mwh[i] = soc_arbi

        # Procenty SOC (diagnostyka)
        total_emax = max(0.0, emax_oze + emax_arbi)
        soc_arbi_pct_of_arbi[i] = (100.0 * soc_arbi_after_idle_mwh[i] / emax_arbi) if emax_arbi > EPS else 0.0
        soc_arbi_pct_of_total[i] = (100.0 * soc_arbi_after_idle_mwh[i] / total_emax) if total_emax > EPS else 0.0
        soc_oze_pct_of_oze[i] = (100.0 * soc_oze_after_idle_mwh[i] / emax_oze) if emax_oze > EPS else 0.0
        soc_oze_pct_of_total[i] = (100.0 * soc_oze_after_idle_mwh[i] / total_emax) if total_emax > EPS else 0.0

    # --- KPI finansowe ---
    rev_arbi_to_grid_pln = export_from_arbi_ac_mwh * price_export_eff
    rev_surplus_export_pln = export_from_surplus_ac_mwh * price_export_eff
    cost_grid_to_arbi_pln = import_for_arbi_ac_mwh * price_import_eff
    cost_import_for_load_pln = import_for_load_ac_mwh * price_import_eff

    # NOWE: cashflow tylko dla ARBI (przychód ARBI – koszt ARBI)
    cashflow_arbi_pln = rev_arbi_to_grid_pln - cost_grid_to_arbi_pln
    # Dotychczasowe „net” obejmujące wszystko:
    cashflow_net_pln = (
        rev_arbi_to_grid_pln
        + rev_surplus_export_pln
        - cost_grid_to_arbi_pln
        - cost_import_for_load_pln
    )

    # --- Zbiórka wyników ---
    out: Dict[str, Any] = {}

    # Przepływy
    out["charge_from_surplus_mwh"] = charge_from_surplus_mwh
    out["charge_from_grid_mwh"] = charge_from_grid_mwh
    out["discharge_to_load_mwh"] = discharge_to_load_mwh
    out["discharge_to_grid_mwh"] = discharge_to_grid_mwh
    out["e_import_mwh"] = e_import_mwh
    out["e_export_mwh"] = e_export_mwh

    # Rozbicia AC
    out["export_from_surplus_ac_mwh"] = export_from_surplus_ac_mwh
    out["export_from_arbi_ac_mwh"] = export_from_arbi_ac_mwh
    out["import_for_load_ac_mwh"] = import_for_load_ac_mwh
    out["import_for_arbi_ac_mwh"] = import_for_arbi_ac_mwh

    # SOC i procenty
    out["soc_oze_before_idle_mwh"] = soc_oze_before_idle_mwh
    out["soc_oze_after_idle_mwh"] = soc_oze_after_idle_mwh
    out["soc_arbi_before_idle_mwh"] = soc_arbi_before_idle_mwh
    out["soc_arbi_after_idle_mwh"] = soc_arbi_after_idle_mwh
    out["soc_arbi_pct_of_arbi"] = soc_arbi_pct_of_arbi
    out["soc_arbi_pct_of_total"] = soc_arbi_pct_of_total
    out["soc_oze_pct_of_oze"] = soc_oze_pct_of_oze
    out["soc_oze_pct_of_total"] = soc_oze_pct_of_total

    # Straty/Capy/Diag
    out["loss_idle_oze_mwh"] = loss_idle_oze_mwh
    out["loss_idle_arbi_mwh"] = loss_idle_arbi_mwh
    out["loss_conv_ch_mwh"] = loss_conv_ch_mwh
    out["loss_conv_dis_to_grid_mwh"] = loss_conv_dis_to_grid_mwh
    out["loss_conv_dis_to_load_mwh"] = loss_conv_dis_to_load_mwh
    out["wasted_surplus_due_to_export_cap_mwh"] = wasted_surplus_due_to_export_cap_mwh
    out["cap_blocked_dis_ac_mwh"] = cap_blocked_dis_ac_mwh
    out["cap_blocked_ch_ac_mwh"] = cap_blocked_ch_ac_mwh
    out["unserved_load_after_cap_ac_mwh"] = unserved_load_after_cap_ac_mwh
    out["bind_export_cap"] = bind_export_cap
    out["bind_import_cap"] = bind_import_cap

    # Ceny i KPI
    out["price_import_pln_mwh"] = price_import
    out["price_export_pln_mwh"] = price_export
    out["rev_arbi_to_grid_pln"] = rev_arbi_to_grid_pln
    out["rev_surplus_export_pln"] = rev_surplus_export_pln
    out["cost_grid_to_arbi_pln"] = cost_grid_to_arbi_pln
    out["cost_import_for_load_pln"] = cost_import_for_load_pln
    out["cashflow_arbi_pln"] = cashflow_arbi_pln           # NOWE
    out["cashflow_net_pln"] = cashflow_net_pln

    log.info("[03/COMMIT] end — OK (OZE-first; cap BESS tylko na ARBI; cap GRID na sumach AC; SOC & KPI spójne)")
    return out
