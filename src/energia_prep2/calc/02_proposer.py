# 02_proposer.py — ETAP 02 (PROPOSER): GENERATOR SYGNAŁÓW I PROPOZYCJI ARBITRAŻU (AC)
# ====================================================================================
# CEL MODUŁU
# -----------
# Ten etap WYŁĄCZNIE planuje arbitraż ARBI (handel z siecią) i wystawia:
#   • prop_arbi_ch_from_grid_ac_mwh[i]  — propozycję energii AC do POBRANIA z sieci (ładowanie ARBI),
#   • prop_arbi_dis_to_grid_ac_mwh[i]   — propozycję energii AC do ODDAWANIA do sieci (rozładowanie ARBI),
#   • oraz sygnały decyzyjne (dec_ch/dec_dis) i bogatą diagnostykę (progi, marginesy, pary, liczniki cykli).
#
# W TYM ETAPIE NIE MA REALIZACJI PRZEPŁYWÓW — to robi 03_commit (uwzględniając bilans OZE, capy AC/NET,
# sprawności, limity eksportu/importu, idle loss, politykę arbi_dis_to_load itp.).
#
# METODYKA (UZGODNIONA)
# ---------------------
# 1) DETERMINISTYCZNE PARY (LOW→HIGH) Z CEN PER-DZIEŃ (bez fallbacków):
#    • Na każdy dzień (date_key) wybieramy do cycles_per_day najtańsze godziny importu (kandydaci CH)
#      i do cycles_per_day najdroższe godziny eksportu (kandydaci DIS).
#    • Parujemy greedy: najtańszy import ↔ najdroższy dostępny eksport, z warunkiem czasowym (low_index < high_index).
#    • Godziny spoza par nie biorą udziału w decyzjach (dla nich Δk jest NaN).
#
# 2) EKONOMIA W GODZINIE (I): SPREAD Δk
#    • Dla każdej godziny należącej do pary low/high liczymy:
#         Δk = price_export[k_high] − price_import[k_low]
#
# 3) EKONOMIA W GODZINIE (II): PROGI OPŁACALNOŚCI
#    • Progi liczymy przez niezależne składowe, a potem sumujemy:
#         thr_low  = base_min_profit + hour_bonus_ch + soc_bonus_ch
#         thr_high = base_min_profit + hour_bonus_dis + soc_bonus_dis
#      gdzie:
#        - hour_bonus_ch  działa TYLKO jeśli godzina jest w masce bonus_hrs_ch (1),
#        - hour_bonus_dis działa TYLKO jeśli godzina jest w masce bonus_hrs_dis (1),
#        - soc_bonus_ch   działa TYLKO jeśli SOC ≤ low_threshold,
#        - soc_bonus_dis  działa TYLKO jeśli SOC ≥ high_threshold.
#      Dzięki temu "bonus godzinowy" i "bonus SOC" są **osobno** i dopiero potem sumują się do progu.
#
# 4) SYGNAŁY BAZOWE (bez capów AC/NET, tylko logika):
#       dec_ch_base[i]  = (Δk − thr_low  ≥ 0)  AND (headroom > 0)  AND (cykle_dnia < cycles_per_day)
#       dec_dis_base[i] = (Δk − thr_high ≥ 0)  AND (SOC > 0)       AND (cykle_dnia < cycles_per_day)
#    • headroom = emax_arbi − SOC (stan symulowany wewnątrz 02; 02 trzyma lokalny SOC i „pending”).
#
# 5) PROPONOWANE ILOŚCI (AC), OGRANICZONE CAPAMI **AC** (po stronie sieci) I SPRAWNOŚCIAMI:
#    • Jeśli dec_ch_base[i] = True:
#         cap_ch_ac   = cap_import_ac_mwh[i]                       # limit AC importu (urządzenie/umowa)
#         charge_net  = min(cap_ch_ac * eta_ch_frac, headroom)     # maksymalny NET przy tym limicie AC
#         prop_arbi_ch_from_grid_ac_mwh[i] = charge_net / eta_ch_frac
#         SOC_sym += charge_net;   pending_arbi_net += charge_net
#    • Jeśli dec_dis_base[i] = True:
#         cap_dis_ac  = cap_export_ac_mwh[i]                        # limit AC eksportu (urządzenie/umowa)
#         can_dis_net = min(cap_dis_ac / eta_dis_frac, SOC_sym)     # maksymalny NET przy tym limicie AC
#         jeśli force_order=True: can_dis_net = min(can_dis_net, pending_arbi_net)
#         prop_arbi_dis_to_grid_ac_mwh[i] = can_dis_net * eta_dis_frac
#         SOC_sym -= can_dis_net; pending_arbi_net -= can_dis_net
#         jeśli pending_arbi_net→0: cycles_used_today += 1  (zamyka się CYKL)
#
# 6) DEFINICJA „CYKLU” (Twoja):
#    • Jeden cykl = „pełen proces” CH→…→DIS, nawet jeśli ładowanie i rozładowanie trwają po kilka godzin,
#      przedzielone przerwami (godzinami bez działania). W praktyce:
#        - Liczymy cykl w momencie, gdy po rozładowaniach „pending_arbi_net” spadnie do ~0 (wykorzystano to, co
#          wcześniej w tym cyklu naładowano).
#        - Per-dzień nie przekraczamy cycles_per_day cykli.
#        - allow_carry_over=False → reset pending_arbi_net na starcie nowego dnia; True → przenosimy.
#
# WEJŚCIE (H):
#  • ts_hour [int]                       — indeksy godzin (0..N-1 albo epoch-hour; służy tylko jako oś czasu),
#  • price_import_pln_mwh[i], price_export_pln_mwh[i],
#  • emax_arbi_mwh, eta_ch_frac, eta_dis_frac,
#  • e_ch_cap_net_arbi_mwh[i], e_dis_cap_net_arbi_mwh[i],   # (zostawione dla kompat., nieużywane do grid↔ARBI)
#  • cap_import_ac_mwh[i], cap_export_ac_mwh[i],            # NOWE: limity AC import/eksport (z 01_ingest)
#  • date_key[i] (YYYY-MM-DD), cycles_per_day, allow_carry_over, force_order,
#  • soc_low_threshold (%), soc_high_threshold (%),
#  • bonus_hrs_ch[i] ∈ {0,1}, bonus_hrs_dis[i] ∈ {0,1},
#  • bonus_ch_window (PLN/MWh), bonus_dis_window (PLN/MWh),
#  • bonus_low_soc_ch (PLN/MWh), bonus_high_soc_dis (PLN/MWh),
#  • base_min_profit_pln_mwh,
#  • (opcjonalnie) soc0_arbi_mwh — stan początkowy ARBI w tej serii (domyślnie 0).
#
# WYJŚCIE:
#  • prop_arbi_ch_from_grid_ac_mwh[i], prop_arbi_dis_to_grid_ac_mwh[i],
#  • dec_ch[i], dec_dis[i], dec_ch_base[i], dec_dis_base[i],
#  • thr_low[i], thr_high[i], delta_k[i], margin_ch[i], margin_dis[i],
#  • pending_arbi_net_mwh[i], soc_arbi_sim_mwh[i],
#  • in_ch_bonus[i], in_dis_bonus[i], low_soc_bonus_hit[i], high_soc_bonus_hit[i],
#  • cycles_used_today[i], dk_minus_thr_low[i], dk_minus_thr_high[i],
#  • pair_low_hour[i], pair_high_hour[i] (indeksy godzin low/high w parze, −1 jeśli brak).
#
# Uwaga: „prop_*” to wartości maksymalne możliwe wg 02 (AC po η i capach AC).
#        03 może je obciąć dodatkowymi capami po stronie NET (wspólne z OZE), emax, bilansem, itd.
# ====================================================================================

from __future__ import annotations

import logging
from typing import Any, Dict, DefaultDict, Optional, List, Tuple
from collections import defaultdict

import numpy as np

log = logging.getLogger("energia-prep-2.calc.proposer_02")
EPS = 1e-12


# ──────────────────────────────────────────────────────────────────────────────
# Pomocnicze: walidacja pól (scalar / vector)
# ──────────────────────────────────────────────────────────────────────────────
def _req_scalar(H: Dict[str, Any], key: str) -> float:
    """Wymaga skalara w H[key]. Zwraca float. Rzuca RuntimeError jeśli brak/nie-scalar."""
    from numbers import Number
    if key not in H:
        raise RuntimeError(f"[02] Brak klucza: {key}")
    v = H[key]
    if isinstance(v, (list, tuple)) or getattr(v, "shape", None) not in (None, ()):
        raise RuntimeError(f"[02] {key} ma być SKALAREM; otrzymano kontener/ndarray.")
    if not isinstance(v, Number):
        raise RuntimeError(f"[02] {key} ma być liczbą; typ={type(v)}")
    return float(v)


def _req_vec_len(H: Dict[str, Any], key: str, N: int) -> np.ndarray:
    """Wymaga wektora długości N w H[key]. Zwraca ndarray (bez kopiowania, gdy możliwe)."""
    if key not in H:
        raise RuntimeError(f"[02] Brak klucza wektora: {key}")
    arr = np.asarray(H[key])
    if arr.shape[0] != N:
        raise RuntimeError(f"[02] {key} ma długość {arr.shape[0]}, oczekiwano N={N}.")
    return arr


# ──────────────────────────────────────────────────────────────────────────────
# Deterministyczne parowanie per-dzień (EKSTREMA-ONLY, BEZ FALLBACKÓW)
# ──────────────────────────────────────────────────────────────────────────────
def _build_daily_pairs(
    date_key: np.ndarray,
    price_import: np.ndarray,
    price_export: np.ndarray,
    cycles_per_day: int,
) -> Tuple[Dict[int, int], Dict[int, int]]:
    """
    Budujemy mapy par:
      • k_low_by_hour[hour_low]   = hour_high
      • k_high_by_hour[hour_high] = hour_low
    ZASADA:
      1) Per dzień wybierz do 'cycles_per_day' najtańsze godziny importu (kandydaci do CH),
         i do 'cycles_per_day' najdroższe godziny eksportu (kandydaci do DIS).
      2) Paruj WYŁĄCZNIE te ekstrema, greedy, z warunkiem low < high. Brak pary → pomiń low.
         (Brak fallbacków — nie szukamy poza zestawem ekstremów ani inną heurystyką.)
    """
    k_low_by_hour: Dict[int, int] = {}
    k_high_by_hour: Dict[int, int] = {}

    N = len(date_key)
    hours = np.arange(N, dtype=int)

    for day in np.unique(date_key):
        mask = (date_key == day)
        idx = hours[mask]
        if idx.size == 0:
            continue

        # K najtańszych importów (rosnąco po cenie; przy remisie rosnąco po czasie dzięki idx)
        cand_low = list(idx[np.argsort(price_import[idx])][:cycles_per_day])
        # K najdroższych eksportów (malejąco po cenie; przy remisie rosnąco po czasie dzięki idx)
        cand_high = list(idx[np.argsort(-price_export[idx])][:cycles_per_day])

        # Greedy: dla każdego low (w porządku czasowym) bierz NAJDROŻSZY jeszcze wolny high > low
        cand_low.sort()  # rosnąco po czasie
        used_high: set[int] = set()
        for i_low in cand_low:
            # „Najdroższy jeszcze wolny high > i_low” → przechodzimy po cand_high w kolejności jak powyżej
            i_high = next((h for h in cand_high if h not in used_high and h > i_low), None)
            if i_high is None:
                # Brak dopuszczalnej pary (nie łamiemy reguły low<high ani nie rozszerzamy zbioru)
                continue
            used_high.add(i_high)
            k_low_by_hour[i_low] = i_high
            k_high_by_hour[i_high] = i_low

    return k_low_by_hour, k_high_by_hour


# ──────────────────────────────────────────────────────────────────────────────
# Główna funkcja etapu 02 — generowanie SYGNAŁÓW/PROPOZYCJI arbitrażu
# ──────────────────────────────────────────────────────────────────────────────
def run(H: Dict[str, Any], P: Dict[str, Any]) -> Dict[str, Any]:
    # --- Oś czasu / rozmiar serii
    ts_hour = np.asarray(H["ts_hour"], dtype=np.int64)  # indeks godzin (0..N-1 albo epoch-hour)
    N = len(ts_hour)

    # --- Ceny (wektory)
    price_import = np.asarray(H["price_import_pln_mwh"], dtype=float)
    price_export = np.asarray(H["price_export_pln_mwh"], dtype=float)

    # --- Parametry magazynu i sprawności (skalary)
    emax_arbi = _req_scalar(H, "emax_arbi_mwh")
    eta_ch = _req_scalar(H, "eta_ch_frac")
    eta_dis = _req_scalar(H, "eta_dis_frac")

    # --- NOWE: Limity AC (wektory) — to one ograniczają planowanie grid↔ARBI
    cap_grid_import_ac = _req_vec_len(H, "cap_grid_import_ac_mwh", N)
    cap_grid_export_ac = _req_vec_len(H, "cap_grid_export_ac_mwh", N)

    # BESS (NET) – opcjonalnie; jeśli brak w H, przyjmujemy brak ograniczenia (∞)
    if "cap_bess_charge_net_mwh" in H:
        cap_bess_charge_net = _req_vec_len(H, "cap_bess_charge_net_mwh", N)
    else:
        cap_bess_charge_net = np.full(N, np.inf)
    if "cap_bess_discharge_net_mwh" in H:
        cap_bess_discharge_net = _req_vec_len(H, "cap_bess_discharge_net_mwh", N)
    else:
        cap_bess_discharge_net = np.full(N, np.inf)

    # efektywne capy AC na godzinę (fizycznie wykonalne: GRID ∧ BESS∧η)
    cap_eff_ch_ac = np.minimum(cap_grid_import_ac, cap_bess_charge_net / max(eta_ch, 1e-12))  # AC ← GRID
    cap_eff_dis_ac = np.minimum(cap_grid_export_ac, cap_bess_discharge_net * max(eta_dis, 1e-12))  # AC → GRID

    # --- Kalendarz
    date_key = np.asarray(H["date_key"], dtype=object)

    # --- Polityki / ekonomia
    base_min_profit = _req_scalar(H, "base_min_profit_pln_mwh")
    cycles_per_day = int(_req_scalar(H, "cycles_per_day"))
    allow_carry_over = bool(H["allow_carry_over"])
    force_order = bool(H["force_order"])

    # --- Progi SOC (%)
    soc_low_threshold_pct = _req_scalar(H, "soc_low_threshold")
    soc_high_threshold_pct = _req_scalar(H, "soc_high_threshold")

    # --- Bonusy: MASKI godzinowe + bonusy godzinowe + bonusy SOC (liczone OSOBNO)
    bonus_ch_mask = _req_vec_len(H, "bonus_hrs_ch", N).astype(bool)
    bonus_dis_mask = _req_vec_len(H, "bonus_hrs_dis", N).astype(bool)

    # Bonus GODZINOWY (może być 0; działa tylko gdy godzina w masce)
    hour_bonus_ch  = float(H.get("bonus_ch_window", 0.0))
    hour_bonus_dis = float(H.get("bonus_dis_window", 0.0))

    # Bonus SOC (skalary) — działa tylko gdy SOC poniżej/powyżej progu
    soc_bonus_ch  = _req_scalar(H, "bonus_low_soc_ch")
    soc_bonus_dis = _req_scalar(H, "bonus_high_soc_dis")

    # --- Stany symulacyjne wewnątrz 02 (nie są to realne przepływy; służą do liczenia sygnałów)
    soc_arbi_start = float(H.get("soc0_arbi_mwh", 0.0))  # stan początkowy ARBI dla symulacji 02
    soc_curr = soc_arbi_start
    pending_arbi_net = soc_curr  # „ile NET w ARBI czeka do rozładowania” z aktualnego cyklu

    # --- PARY per-dzień (deterministyczne z cen; EKSTREMA ONLY)
    k_low_by_hour, k_high_by_hour = _build_daily_pairs(date_key, price_import, price_export, cycles_per_day)

    # --- Bufory wynikowe (propozycje AC + sygnały + diagnostyka)
    prop_ch_ac = np.zeros(N)   # propozycja AC do pobrania z sieci (ładowanie ARBI)
    prop_dis_ac = np.zeros(N)  # propozycja AC do oddania do sieci (rozładowanie ARBI)

    dec_ch = np.zeros(N, dtype=bool)
    dec_dis = np.zeros(N, dtype=bool)
    dec_ch_base = np.zeros(N, dtype=bool)
    dec_dis_base = np.zeros(N, dtype=bool)

    thr_low = np.zeros(N)      # próg opłacalności dla ładowania (po zsumowaniu bonusów)
    thr_high = np.zeros(N)     # próg opłacalności dla rozładowania (po zsumowaniu bonusów)
    delta_k = np.full(N, np.nan)  # Δk (tylko dla godzin należących do par; inaczej NaN)

    # Diagnostyka stanu i progów
    pending_arr = np.zeros(N)     # wewnętrzny „pending” po każdej godzinie (NET)
    soc_sim_arr = np.zeros(N)     # wewnętrzny SOC po każdej godzinie (NET)
    in_ch_bonus_arr = np.zeros(N, dtype=bool)
    in_dis_bonus_arr = np.zeros(N, dtype=bool)
    low_soc_bonus_hit_arr = np.zeros(N, dtype=bool)
    high_soc_bonus_hit_arr = np.zeros(N, dtype=bool)
    cycles_used_today_arr = np.zeros(N)
    dk_minus_thr_low = np.full(N, np.nan)
    dk_minus_thr_high = np.full(N, np.nan)
    pair_low_hour = np.full(N, -1.0)
    pair_high_hour = np.full(N, -1.0)

    cycles_used: DefaultDict[object, int] = defaultdict(int)  # licznik cykli per-dzień (wg Twojej definicji)
    prev_day: Optional[object] = None

    # ──────────────────────────────────────────────────────────────────────────
    # GŁÓWNA PĘTLA GODZINOWA — generujemy wyłącznie SYGNAŁY/PROPOZYCJE
    # ──────────────────────────────────────────────────────────────────────────
    for i in range(N):
        # 0) Obsługa granicy dnia (carry-over)
        day = date_key[i]
        if (prev_day is None) or (day != prev_day):
            if not allow_carry_over:
                # Jeżeli NIE przenosimy cyklu na następny dzień, to nowy dzień zaczyna z pending=0
                pending_arbi_net = 0.0
            prev_day = day

        # 1) Ustal, czy godzina i jest low lub high w wyznaczonych parach (jeśli nie — brak decyzji)
        is_low = i in k_low_by_hour
        is_high = i in k_high_by_hour

        # 2) Bonusy: flaga godzinowa (tylko wtedy zadziała bonus godzinowy)
        in_ch_bonus = bool(bonus_ch_mask[i])
        in_dis_bonus = bool(bonus_dis_mask[i])
        in_ch_bonus_arr[i] = in_ch_bonus
        in_dis_bonus_arr[i] = in_dis_bonus

        # 3) Stan magazynu i progi SOC dla bonusu SOC
        emax = emax_arbi
        headroom = max(0.0, emax - soc_curr)  # ile net możemy jeszcze włożyć
        low_thr_soc = emax * (soc_low_threshold_pct / 100.0)    # próg „niski SOC”
        high_thr_soc = emax * (soc_high_threshold_pct / 100.0)  # próg „wysoki SOC”
        low_soc_bonus_hit = soc_curr <= low_thr_soc
        high_soc_bonus_hit = soc_curr >= high_thr_soc
        low_soc_bonus_hit_arr[i] = low_soc_bonus_hit
        high_soc_bonus_hit_arr[i] = high_soc_bonus_hit

        # 4) Progi opłacalności (sumujemy niezależne bonusy)
        #    • godzina w masce? → dodajemy hour_bonus_* (może być 0)
        #    • SOC poza progiem? → dodajemy soc_bonus_*
        thr_l = base_min_profit + (hour_bonus_ch if in_ch_bonus else 0.0) + (soc_bonus_ch if low_soc_bonus_hit else 0.0)
        thr_h = base_min_profit + (hour_bonus_dis if in_dis_bonus else 0.0) + (soc_bonus_dis if high_soc_bonus_hit else 0.0)
        thr_low[i] = thr_l
        thr_high[i] = thr_h

        # 5) Jeśli godzina i nie jest low/high — nie liczymy Δk i nie podejmujemy decyzji
        if not (is_low or is_high):
            pending_arr[i] = pending_arbi_net
            soc_sim_arr[i] = soc_curr
            cycles_used_today_arr[i] = cycles_used[day]
            continue

        # 6) Ustal parę dla tej godziny i policz Δk
        if is_low:
            il = i
            ih = k_low_by_hour[i]   # odpowiadający high
        else:  # is_high
            ih = i
            il = k_high_by_hour[i]  # odpowiadający low

        pair_low_hour[i] = float(il)
        pair_high_hour[i] = float(ih)

        dk = float(price_export[ih] - price_import[il])  # spread ceny: sprzedaż(high) - zakup(low)
        delta_k[i] = dk
        dk_minus_thr_low[i] = dk - thr_l
        dk_minus_thr_high[i] = dk - thr_h

        # 7) SYGNAŁ „CH” (ładowanie ARBI z sieci) — tylko gdy godzina jest „low”
        if is_low:
            # Warunek ekonomiczny + miejsce w magazynie + limit cykli dnia
            dec_ch_base[i] = (dk - thr_l) >= 0.0 and headroom > EPS and (cycles_used[day] < cycles_per_day)
            if dec_ch_base[i]:
                # OGRANICZENIE PRZEZ CAP **AC** (import) → przelicz na NET przez η_ch
                cap_ch_ac = cap_eff_ch_ac[i]  # [MWh AC] = min(GRID, BESS/η_ch)
                cap_ch_net_from_ac = cap_ch_ac * max(eta_ch, EPS)  # [MWh NET]
                # Ile NET możemy włożyć do ARBI w tej godzinie (limit AC→NET i headroom)
                charge_net = min(cap_ch_net_from_ac, headroom)
                if charge_net > EPS:
                    # Propozycja AC (ile TRZEBA pobrać z sieci przy danej sprawności)
                    prop_ch_ac[i] = charge_net / max(eta_ch, EPS)
                    # Aktualizujemy stan lokalny 02: to symulacja do liczenia cyklu/pending
                    soc_curr = min(emax, soc_curr + charge_net)
                    pending_arbi_net += charge_net
                    dec_ch[i] = True

        # 8) SYGNAŁ „DIS” (rozładowanie ARBI do sieci) — tylko gdy godzina jest „high”
        if is_high:
            # OGRANICZENIE PRZEZ CAP **AC** (eksport) → przelicz maksymalny NET przez η_dis
            cap_dis_ac = cap_eff_dis_ac[i]  # [MWh AC] = min(GRID, BESS*η_dis)
            cap_dis_net_from_ac = cap_dis_ac / max(eta_dis, EPS)  # [MWh NET]
            # Ile NET możemy wyjąć — ograniczone limitem AC→NET i SOC
            can_dis = min(cap_dis_net_from_ac, soc_curr)
            # force_order: nie rozładowuj więcej niż „pending” z bieżącego cyklu (CH→…→DIS)
            if force_order:
                can_dis = min(can_dis, pending_arbi_net)
            # Warunek ekonomiczny + dostępny zasób + limit cykli dnia
            dec_dis_base[i] = (dk - thr_h) >= 0.0 and can_dis > EPS and (cycles_used[day] < cycles_per_day)
            if dec_dis_base[i]:
                # Propozycja AC (ile MOŻEMY oddać do sieci przy η_dis)
                prop_dis_ac[i] = can_dis * max(eta_dis, EPS)
                # Aktualizujemy stan lokalny 02 (symulacja do logiki cyklu):
                soc_curr = max(0.0, soc_curr - can_dis)
                pending_arbi_net = max(0.0, pending_arbi_net - can_dis)
                dec_dis[i] = True
                # Zamknięcie cyklu: jeśli wykorzystaliśmy „pending” → nalicz cykl
                if pending_arbi_net <= EPS:
                    cycles_used[day] += 1

        # 9) Zapisz diagnostykę po godzinie
        pending_arr[i] = pending_arbi_net
        soc_sim_arr[i] = soc_curr
        cycles_used_today_arr[i] = cycles_used[day]

    # ──────────────────────────────────────────────────────────────────────────
    # ZBIÓRKA WYNIKU — tylko propozycje/sygnały/diagnostyka (BEZ realnych przepływów)
    # ──────────────────────────────────────────────────────────────────────────
    out = dict(P)  # przenosimy ewentualne pola z P bez zmian

    # Propozycje AC (do decyzji 03)
    out["prop_arbi_ch_from_grid_ac_mwh"] = prop_ch_ac
    out["prop_arbi_dis_to_grid_ac_mwh"] = prop_dis_ac

    # Sygnały i progi/marginesy
    out["dec_ch"] = dec_ch
    out["dec_dis"] = dec_dis
    out["dec_ch_base"] = dec_ch_base
    out["dec_dis_base"] = dec_dis_base
    out["thr_low"] = thr_low
    out["thr_high"] = thr_high
    out["delta_k"] = delta_k
    out["margin_ch"] = delta_k - thr_low
    out["margin_dis"] = delta_k - thr_high

    # Diagnostyka (stan symulowany 02 + bonusy + pary)
    out["pending_arbi_net_mwh"] = pending_arr
    out["soc_arbi_sim_mwh"] = soc_sim_arr
    out["in_ch_bonus"] = in_ch_bonus_arr
    out["in_dis_bonus"] = in_dis_bonus_arr
    out["low_soc_bonus_hit"] = low_soc_bonus_hit_arr
    out["high_soc_bonus_hit"] = high_soc_bonus_hit_arr
    out["cycles_used_today"] = cycles_used_today_arr
    out["dk_minus_thr_low"] = dk_minus_thr_low
    out["dk_minus_thr_high"] = dk_minus_thr_high
    out["pair_low_hour"] = pair_low_hour
    out["pair_high_hour"] = pair_high_hour

    log.info("[02] OK — deterministyczne pary (ekstrema-only, bez fallbacków) wyznaczone, sygnały i propozycje wygenerowane (N=%d).", N)
    return out
