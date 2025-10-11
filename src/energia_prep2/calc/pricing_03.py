# pricing_03.py — Raport finansowy (nowy): wycena strat i cap’ów
# -----------------------------------------------------------------------------
# Wejście:
#   H_buf: osie czasu, ceny (price_import/export)
#   P_buf: paramy z IO (nieużywane tu)
#   C_buf: wyniki commit_02 z telemetrią energii i strat
#
# Wyjście:
#   pricing_rows: list[dict] — jeden wiersz na godzinę, z polami PLN/MWh i PLN
#   (persist robi raport.persist_stage04_pricing_detail)
# -----------------------------------------------------------------------------

from __future__ import annotations
from typing import Any, Dict, List
import numpy as np

def _as_np(x, dtype=float):
    return np.asarray(x, dtype=dtype)

def run(*, H_buf: Dict[str, Any], P_buf: Dict[str, Any], C_buf: Dict[str, Any]) -> List[Dict[str, Any]]:
    N = len(H_buf["ts_hour"])

    price_import = _as_np(C_buf.get("price_import_pln_mwh", H_buf.get("price_import_pln_mwh", np.zeros(N))))
    price_export = _as_np(C_buf.get("price_export_pln_mwh", H_buf.get("price_export_pln_mwh", np.zeros(N))))

    # Przepływy bazowe (PLN już policzone w commit_02)
    rev_arbi_to_grid_pln     = _as_np(C_buf.get("rev_arbi_to_grid_pln",     np.zeros(N)))
    rev_surplus_export_pln   = _as_np(C_buf.get("rev_surplus_export_pln",   np.zeros(N)))
    cost_grid_to_arbi_pln    = _as_np(C_buf.get("cost_grid_to_arbi_pln",    np.zeros(N)))
    cost_import_for_load_pln = _as_np(C_buf.get("cost_import_for_load_pln", np.zeros(N)))
    cashflow_net_pln         = _as_np(C_buf.get("cashflow_net_pln",         np.zeros(N)))

    # Straty energetyczne (MWh)
    loss_idle_oze_mwh        = _as_np(C_buf.get("loss_idle_oze_mwh",        np.zeros(N)))
    loss_idle_arbi_mwh       = _as_np(C_buf.get("loss_idle_arbi_mwh",       np.zeros(N)))
    loss_conv_ch_mwh         = _as_np(C_buf.get("loss_conv_ch_mwh",         np.zeros(N)))
    loss_conv_dis_to_grid_mwh= _as_np(C_buf.get("loss_conv_dis_to_grid_mwh",np.zeros(N)))
    loss_conv_dis_to_load_mwh= _as_np(C_buf.get("loss_conv_dis_to_load_mwh",np.zeros(N)))
    wasted_surplus_cap_mwh   = _as_np(C_buf.get("wasted_surplus_due_to_export_cap_mwh", np.zeros(N)))

    # Blokady przez cap (AC, „zablokowana propozycja” – nie strata energii, ale koszt alternatywny)
    cap_blocked_dis_ac_mwh   = _as_np(C_buf.get("cap_blocked_dis_ac_mwh",   np.zeros(N)))
    cap_blocked_ch_ac_mwh    = _as_np(C_buf.get("cap_blocked_ch_ac_mwh",    np.zeros(N)))
    unserved_load_after_cap  = _as_np(C_buf.get("unserved_load_after_cap_ac_mwh", np.zeros(N)))

    # WYCE NA PLN — przyjęta metodologia:
    #  • straty ładowania (conv_ch) → koszt wg ceny importu
    #  • straty rozładowania na GRID → utracony przychód wg ceny eksportu
    #  • straty rozładowania na LOAD → utracona oszczędność wg ceny importu
    #  • idle ARBI → utracona oszczędność wg ceny importu
    #  • idle OZE  → utracony przychód wg ceny eksportu
    #  • wasted_surplus_cap → utracony przychód wg ceny eksportu
    #  • cap_blocked_dis_ac → utracony przychód wg ceny eksportu (potencjał 01)
    #  • cap_blocked_ch_ac  → potencjalny dodatkowy koszt wg ceny importu (niezrealizowane ładowanie)
    #  • unserved_load_after_cap → „niedostarczony” LOAD (wycena wg ceny importu)

    pln_loss_conv_ch              = loss_conv_ch_mwh          * price_import
    pln_loss_conv_dis_to_grid     = loss_conv_dis_to_grid_mwh * price_export
    pln_loss_conv_dis_to_load     = loss_conv_dis_to_load_mwh * price_import
    pln_loss_idle_arbi            = loss_idle_arbi_mwh        * price_import
    pln_loss_idle_oze             = loss_idle_oze_mwh         * price_export
    pln_wasted_surplus_cap        = wasted_surplus_cap_mwh    * price_export
    pln_cap_blocked_dis_ac        = cap_blocked_dis_ac_mwh    * price_export
    pln_cap_blocked_ch_ac         = cap_blocked_ch_ac_mwh     * price_import
    pln_unserved_load_after_cap   = unserved_load_after_cap   * price_import

    # Diagnostyczna suma strat (nie korygować nią cashflow_net_pln – część tych kosztów już w nim zawarta)
    losses_diag_total_pln = (
        pln_loss_conv_ch
        + pln_loss_conv_dis_to_grid
        + pln_loss_conv_dis_to_load
        + pln_loss_idle_arbi
        + pln_loss_idle_oze
        + pln_wasted_surplus_cap
    )

    # Dodatkowe „koszty alternatywne” (cap-blocked i unserved) — raportowane osobno
    cap_opportunity_total_pln = pln_cap_blocked_dis_ac + pln_cap_blocked_ch_ac + pln_unserved_load_after_cap

    # Złożenie per-godzina
    ts_utc   = H_buf.get("ts_utc")
    ts_local = H_buf.get("ts_local")
    rows: List[Dict[str, Any]] = []
    for i in range(N):
        rows.append({
            "ts_utc":   ts_utc[i],
            "ts_local": ts_local[i],

            # ceny
            "price_import_pln_mwh": float(price_import[i]),
            "price_export_pln_mwh": float(price_export[i]),

            # bazowe przepływy finansowe
            "rev_arbi_to_grid_pln":     float(rev_arbi_to_grid_pln[i]),
            "rev_surplus_export_pln":   float(rev_surplus_export_pln[i]),
            "cost_grid_to_arbi_pln":    float(cost_grid_to_arbi_pln[i]),
            "cost_import_for_load_pln": float(cost_import_for_load_pln[i]),
            "cashflow_net_pln":         float(cashflow_net_pln[i]),

            # straty energetyczne (MWh)
            "loss_idle_oze_mwh":               float(loss_idle_oze_mwh[i]),
            "loss_idle_arbi_mwh":              float(loss_idle_arbi_mwh[i]),
            "loss_conv_ch_mwh":                float(loss_conv_ch_mwh[i]),
            "loss_conv_dis_to_grid_mwh":       float(loss_conv_dis_to_grid_mwh[i]),
            "loss_conv_dis_to_load_mwh":       float(loss_conv_dis_to_load_mwh[i]),
            "wasted_surplus_due_to_export_cap_mwh": float(wasted_surplus_cap_mwh[i]),

            # wycena strat (PLN)
            "pln_loss_conv_ch":            float(pln_loss_conv_ch[i]),
            "pln_loss_conv_dis_to_grid":   float(pln_loss_conv_dis_to_grid[i]),
            "pln_loss_conv_dis_to_load":   float(pln_loss_conv_dis_to_load[i]),
            "pln_loss_idle_arbi":          float(pln_loss_idle_arbi[i]),
            "pln_loss_idle_oze":           float(pln_loss_idle_oze[i]),
            "pln_wasted_surplus_cap":      float(pln_wasted_surplus_cap[i]),
            "losses_diag_total_pln":       float(losses_diag_total_pln[i]),

            # efekty cap (opportunity)
            "cap_blocked_dis_ac_mwh":      float(cap_blocked_dis_ac_mwh[i]),
            "cap_blocked_ch_ac_mwh":       float(cap_blocked_ch_ac_mwh[i]),
            "unserved_load_after_cap_ac_mwh": float(unserved_load_after_cap[i]),
            "pln_cap_blocked_dis_ac":      float(pln_cap_blocked_dis_ac[i]),
            "pln_cap_blocked_ch_ac":       float(pln_cap_blocked_ch_ac[i]),
            "pln_unserved_load_after_cap": float(pln_unserved_load_after_cap[i]),
            "cap_opportunity_total_pln":   float(cap_opportunity_total_pln[i]),
        })

    return rows
