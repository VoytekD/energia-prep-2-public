# 04_pricing.py — ETAP 04 (PRICING): raport finansowy, wycena strat i efektów cap’ów
# ==============================================================================
# ROLA MODUŁU (ETAP 04 / PRICING)
# - Zbiera wyniki energetyczne i cenowe z poprzednich etapów (01..03),
# - Przelicza straty energetyczne na wartości PLN (wg przyjętej metodologii),
# - Szacuje „koszty alternatywne” wynikające z ograniczeń cap (blokady),
# - Buduje listę rekordów per godzina (pricing_rows) do persistowania (05/PERSIST).
#
# WEJŚCIA:
#   H_buf : Dict[str, Any]
#     • osie czasu: ts_utc[N], ts_local[N]
#     • (opcjonalnie) ceny: price_import_pln_mwh[N], price_export_pln_mwh[N]
#   P_buf : Dict[str, Any]
#     • nieużywane w tym module (placeholder dla spójności interfejsu)
#   C_buf : Dict[str, Any]
#     • wyniki etapu 03 (COMMIT) – m.in. przepływy, straty MWh, KPI PLN, efekty cap
#
# WYJŚCIA:
#   List[Dict[str, Any]] — „pricing_rows”: gotowe wiersze per godzina, z cenami,
#   przepływami finansowymi, wyceną strat oraz metrykami „opportunity cost”.
#
# UWAGI:
# - Ten moduł NIE zmienia logiki rozliczeń energetycznych (robi to 03/COMMIT).
# - Tu wyłącznie multiplikujemy wielkości MWh przez ceny, sumujemy i etykietujemy.
# ==============================================================================

from __future__ import annotations

from typing import Any, Dict, List
import logging

import numpy as np

# Logger etapu 04 (PRICING) — spójny z nową konwencją
log = logging.getLogger("energia-prep-2.calc.pricing_04")


# ──────────────────────────────────────────────────────────────────────────────
# Pomocnicze
# ──────────────────────────────────────────────────────────────────────────────
def _as_np(x, dtype=float) -> np.ndarray:
    """
    Rzutowanie na np.ndarray o zadanym dtype.
    - Ujednolica typy wejściowe (list/ndarray/serie) bez modyfikacji wartości.
    """
    return np.asarray(x, dtype=dtype)


# ──────────────────────────────────────────────────────────────────────────────
# Główna funkcja modułu (ETAP 04 / PRICING)
# ──────────────────────────────────────────────────────────────────────────────
def run(*, H_buf: Dict[str, Any], P_buf: Dict[str, Any], C_buf: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Buduje wiersze „pricing_rows” na podstawie:
      - cen (import/export) z C_buf lub H_buf (fallback),
      - przepływów/KPI z COMMIT,
      - strat energetycznych i efektów cap (w MWh).

    Krok po kroku:
      1) Odczyt N (długości szeregu) i cen godzinowych.
      2) Wczytanie bazowych KPI PLN z COMMIT (rev/cost/cashflow).
      3) Wczytanie strat energetycznych (MWh) oraz efektów cap (MWh).
      4) Wycena strat (PLN) wg metodologii (import→koszt, export→przychód).
      5) Wyliczenie sum diagnostycznych (losses_diag_total_pln i „opportunity”).
      6) Złożenie listy rekordów per godzina (do zapisu przez 05/PERSIST).
    """
    # [1] Rozmiar i ceny -------------------------------------------------------
    N = len(H_buf["ts_hour"])

    # Ceny — preferuj C_buf (po commit), fallback do H_buf (po ingest)
    price_import = _as_np(C_buf.get("price_import_pln_mwh",
                       H_buf.get("price_import_pln_mwh", np.zeros(N))))
    price_export = _as_np(C_buf.get("price_export_pln_mwh",
                       H_buf.get("price_export_pln_mwh", np.zeros(N))))

    # [2] Bazowe przepływy finansowe (PLN) z COMMIT ----------------------------
    rev_arbi_to_grid_pln     = _as_np(C_buf.get("rev_arbi_to_grid_pln",     np.zeros(N)))
    rev_surplus_export_pln   = _as_np(C_buf.get("rev_surplus_export_pln",   np.zeros(N)))
    cost_grid_to_arbi_pln    = _as_np(C_buf.get("cost_grid_to_arbi_pln",    np.zeros(N)))
    cost_import_for_load_pln = _as_np(C_buf.get("cost_import_for_load_pln", np.zeros(N)))
    cashflow_net_pln         = _as_np(C_buf.get("cashflow_net_pln",         np.zeros(N)))

    # [3] Straty energetyczne i efekty cap (MWh) -------------------------------
    loss_idle_oze_mwh         = _as_np(C_buf.get("loss_idle_oze_mwh",         np.zeros(N)))
    loss_idle_arbi_mwh        = _as_np(C_buf.get("loss_idle_arbi_mwh",        np.zeros(N)))
    loss_conv_ch_mwh          = _as_np(C_buf.get("loss_conv_ch_mwh",          np.zeros(N)))
    loss_conv_dis_to_grid_mwh = _as_np(C_buf.get("loss_conv_dis_to_grid_mwh", np.zeros(N)))
    loss_conv_dis_to_load_mwh = _as_np(C_buf.get("loss_conv_dis_to_load_mwh", np.zeros(N)))
    wasted_surplus_cap_mwh    = _as_np(C_buf.get("wasted_surplus_due_to_export_cap_mwh", np.zeros(N)))

    cap_blocked_dis_ac_mwh    = _as_np(C_buf.get("cap_blocked_dis_ac_mwh",    np.zeros(N)))
    cap_blocked_ch_ac_mwh     = _as_np(C_buf.get("cap_blocked_ch_ac_mwh",     np.zeros(N)))
    unserved_load_after_cap   = _as_np(C_buf.get("unserved_load_after_cap_ac_mwh", np.zeros(N)))

    # [4] Wycena strat (PLN) — metodologia ------------------------------------
    #  • straty ładowania (conv_ch)             → koszt wg ceny importu
    #  • straty rozładowania na GRID            → utracony przychód wg ceny eksportu
    #  • straty rozładowania na LOAD            → utracona oszczędność wg ceny importu
    #  • idle ARBI                              → utracona oszczędność wg ceny importu
    #  • idle OZE                               → utracony przychód wg ceny eksportu
    #  • wasted_surplus_cap                     → utracony przychód wg ceny eksportu
    #  • cap_blocked_dis_ac (zablokowany DIS)   → utracony przychód wg ceny eksportu
    #  • cap_blocked_ch_ac  (zablokowany CH)    → potencjalny dodatkowy koszt wg ceny importu
    #  • unserved_load_after_cap                → „niedostarczony” LOAD (wycena wg ceny importu)
    pln_loss_conv_ch            = loss_conv_ch_mwh          * price_import
    pln_loss_conv_dis_to_grid   = loss_conv_dis_to_grid_mwh * price_export
    pln_loss_conv_dis_to_load   = loss_conv_dis_to_load_mwh * price_import
    pln_loss_idle_arbi          = loss_idle_arbi_mwh        * price_import
    pln_loss_idle_oze           = loss_idle_oze_mwh         * price_export
    pln_wasted_surplus_cap      = wasted_surplus_cap_mwh    * price_export
    pln_cap_blocked_dis_ac      = cap_blocked_dis_ac_mwh    * price_export
    pln_cap_blocked_ch_ac       = cap_blocked_ch_ac_mwh     * price_import
    pln_unserved_load_after_cap = unserved_load_after_cap   * price_import

    # [5] Sumy diagnostyczne (nie korygują cashflow) ---------------------------
    losses_diag_total_pln = (
        pln_loss_conv_ch
        + pln_loss_conv_dis_to_grid
        + pln_loss_conv_dis_to_load
        + pln_loss_idle_arbi
        + pln_loss_idle_oze
        + pln_wasted_surplus_cap
    )
    cap_opportunity_total_pln = (
        pln_cap_blocked_dis_ac + pln_cap_blocked_ch_ac + pln_unserved_load_after_cap
    )

    # [6] Składanie rekordów per godzina --------------------------------------
    ts_utc   = H_buf.get("ts_utc")
    ts_local = H_buf.get("ts_local")
    rows: List[Dict[str, Any]] = []

    log.debug("[04/PRICING] build rows: N=%d", N)

    for i in range(N):
        rows.append({
            "ts_utc":   ts_utc[i],
            "ts_local": ts_local[i],

            # ceny (PLN/MWh)
            "price_import_pln_mwh": float(price_import[i]),
            "price_export_pln_mwh": float(price_export[i]),

            # bazowe przepływy finansowe (PLN)
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

            # efekty cap (opportunity, PLN/MWh → PLN)
            "cap_blocked_dis_ac_mwh":      float(cap_blocked_dis_ac_mwh[i]),
            "cap_blocked_ch_ac_mwh":       float(cap_blocked_ch_ac_mwh[i]),
            "unserved_load_after_cap_ac_mwh": float(unserved_load_after_cap[i]),
            "pln_cap_blocked_dis_ac":      float(pln_cap_blocked_dis_ac[i]),
            "pln_cap_blocked_ch_ac":       float(pln_cap_blocked_ch_ac[i]),
            "pln_unserved_load_after_cap": float(pln_unserved_load_after_cap[i]),
            "cap_opportunity_total_pln":   float(cap_opportunity_total_pln[i]),
        })

    log.info("[04/PRICING] end — OK (n=%d)", len(rows))
    return rows
