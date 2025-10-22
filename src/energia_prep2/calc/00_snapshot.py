"""
ETAP 00 — SNAPSHOT & KONSOLIDACJA (single source of truth)

Kontrakt:
- output.snapshot.raw  : ostatnie rekordy params.* (1:1, JSON)
- output.snapshot.norm : spójny słownik bez aliasów i fallbacków:
  • BESS (sprawności, c-rate, czasy do pełna, moce jeśli policzalne),
  • Polityki/economia (progi, bonusy, listy godzin),
  • VAT i AKCYZA,
  • LCOE,
  • CAPY (skalary):
      - cap_bess_charge_net_mwh      [MWh/h] (NET; z c[h])
      - cap_bess_discharge_net_mwh   [MWh/h] (NET; z c[h])
      - cap_grid_import_ac_mwh       [MWh/h] (AC; z MW)
      - cap_grid_export_ac_mwh       [MWh/h] (AC; z MW)
  • Schedulery opłat systemowych i dystrybucyjnych (dla B21/B22/B23),
  • Stawki DYSTRYBUCYJNE (zmienne PLN/MWh, jakościowa PLN/MWh, stałe per kW/mies., abonament m-c),
  • Stawki SYSTEMOWE (OZE/Kogeneracyjna/Mocowa) [PLN/MWh],
  • Parametr K (progi i współczynniki A),
  • Kontrakt energii + opłaty handlowe (model, cena stała, marże, opłata handlowa),
  • Kopia całych params.* pod norm["__raw"] dla audytu.
"""

import json
import logging
from typing import Any, Dict, List, Mapping, Optional, Tuple

import psycopg
from psycopg.rows import dict_row

from datetime import date, datetime
from decimal import Decimal
import uuid
import numpy as np

log = logging.getLogger("energia-prep-2.calc.snapshot_00")

# ─────────────────────────────────────────────────────────────────────────────
# PARAMS źródła
# ─────────────────────────────────────────────────────────────────────────────

PARAM_TABLES: List[str] = [
    "params.form_oplaty_sys_sched",
    "params.form_oplaty_sys_kparam",
    "params.form_par_kontraktu",
    "params.form_oplaty_fiskalne",
    "params.form_lcoe",
    "params.form_par_arbitrazu",
    "params.form_oplaty_dyst_sched",
    "params.form_oplaty_dystrybucyjne",
    "params.form_oplaty_systemowe",
    "params.form_bess_param",
    "params.form_parametry_klienta",
    "params.form_zmienne",
]

# Wymagane skalarne wartości w NORM (zostawione bez zmian)
REQ_SCALARS: List[str] = [
    "emax_total_mwh",
    "emax_oze_mwh",
    "emax_arbi_mwh",
    "frac_oze",

    "eta_ch_frac",
    "eta_dis_frac",

    "bess_c_rate_charge",
    "bess_c_rate_discharge",
    "bess_time_to_full_charge_h",
    "bess_time_to_full_discharge_h",

    "bess_lambda_h_frac",

    "moc_umowna_mw",
    "vat_udzial",

    "lcoe_pv_pp",
    "lcoe_pv_wz",
    "lcoe_wiatr",

    "base_min_profit_pln_mwh",
    "cycles_per_day",
    "soc_low_threshold",
    "soc_high_threshold",

    "moc_pv_pp",
    "moc_pv_wz",
    "moc_wiatr",
    "zmiany_konsumpcji",

    # NOWE CAPY (skalary)
    "cap_bess_charge_net_mwh",
    "cap_bess_discharge_net_mwh",
    "cap_grid_import_ac_mwh",
    "cap_grid_export_ac_mwh",
]

OPT_SCALARS: List[str] = [
    "p_ch_max_mw",
    "p_dis_max_mw",
    "soc_oze_start_mwh",
    "soc_arbi_start_mwh",
    "allow_carry_over",
    "force_order",
    "wybrana_taryfa",
]

OPT_BONUS_SCALARS: List[str] = [
    "bonus_ch_window",
    "bonus_dis_window",
    "bonus_low_soc_ch",
    "bonus_high_soc_dis",
]

BONUS_LISTS: List[str] = [
    "bonus_hrs_ch",
    "bonus_hrs_dis",
    "bonus_hrs_ch_free",
    "bonus_hrs_dis_free",
]

MON_EN = {
    1: "jan", 2: "feb", 3: "mar", 4: "apr",
    5: "may", 6: "jun", 7: "jul", 8: "aug",
    9: "sep", 10: "oct", 11: "nov", 12: "dec",
}
WF_MAP = {"wd": "work", "we": "free"}
TARIFFS = ("b21", "b22", "b23")  # zawsze mapujemy na uppercase klucze w norm

# ─────────────────────────────────────────────────────────────────────────────
# HELPERY
# ─────────────────────────────────────────────────────────────────────────────

def _as_num(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None

def _req_num(d: Mapping[str, Any], key: str) -> float:
    if key not in d:
        raise RuntimeError(f"[00] Brak klucza: {key}")
    v = _as_num(d.get(key))
    if v is None:
        raise RuntimeError(f"[00] Klucz {key} nie jest liczbą.")
    return float(v)

def _opt_num(d: Mapping[str, Any], key: str, default: Optional[float] = None) -> Optional[float]:
    v = _as_num(d.get(key))
    return v if v is not None else default

def _req_str(d: Mapping[str, Any], key: str) -> str:
    if key not in d:
        raise RuntimeError(f"[00] Brak klucza: {key}")
    s = str(d.get(key) or "").strip()
    if not s:
        raise RuntimeError(f"[00] Klucz {key} jest pusty.")
    return s

def _normalize_tariff_value(t: str) -> str:
    t = str(t or "").strip().lower()
    if t.startswith("wybrana_"):
        t = t.replace("wybrana_", "", 1)
    if t not in ("b21", "b22", "b23"):
        raise RuntimeError(f"[00] wybrana_taryfa poza {('b21','b22','b23')}: {t!r}")
    return t

def _json_safe(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, uuid.UUID):
        return str(o)
    return str(o)

def _payload_or_self(x: Any) -> Dict[str, Any]:
    if isinstance(x, dict) and isinstance(x.get("payload"), dict):
        return x["payload"]
    return x or {}

def _normalize_vat_frac(x: Any) -> float:
    v = _as_num(x)
    if v is None:
        raise RuntimeError("[00] Brak VAT w form_oplaty_fiskalne.vat")
    return float(v / 100.0) if v > 1.0 else float(v)

def _lambda_h_from_bess(b: Mapping[str, Any]) -> float:
    h = _as_num(b.get("bess_lambda_h_frac"))
    if h is not None:
        return float(h)
    d_pct = _as_num(b.get("bess_lambda_day"))
    if d_pct is not None:
        return float((d_pct / 100.0) / 24.0)
    m_pct = _as_num(b.get("bess_lambda_month"))
    if m_pct is not None:
        return float((m_pct / 100.0) / (24.0 * 30.4375))
    return 0.0

def _crate_per_h_from_hours(b: Mapping[str, Any], c_hours_key: str, t_key: str) -> Tuple[float, float]:
    """
    Zamienia parametr 'c' (godziny do pełna) na c-rate [1/h] oraz zwraca też czas [h].
    Preferencja: jeżeli podano t_full bezpośrednio — używamy go.
    """
    t_full = _opt_num(b, t_key, None)
    if t_full and t_full > 0:
        return (1.0 / float(t_full), float(t_full))
    c_hours = _opt_num(b, c_hours_key, None)  # np. c=2h → 0.5 1/h
    if c_hours and c_hours > 0:
        return (1.0 / float(c_hours), float(c_hours))
    return (0.0, 0.0)

def _complete_power_time_from_capacity(norm: Dict[str, Any]) -> None:
    """
    Uzupełnia p i t na bazie emax i c_rate (bez zgadywania ponad: p = e*c_rate, t = e/p).
    """
    emax = _opt_num(norm, "emax_total_mwh", 0.0) or 0.0

    # charge
    p_ch  = _opt_num(norm, "p_ch_max_mw", None)
    c_ch  = _opt_num(norm, "bess_c_rate_charge", None)
    t_ch  = _opt_num(norm, "bess_time_to_full_charge_h", None)

    if (p_ch is None or p_ch <= 0) and emax > 0 and c_ch and c_ch > 0:
        norm["p_ch_max_mw"] = emax * c_ch
        p_ch = norm["p_ch_max_mw"]

    if (t_ch is None or t_ch <= 0) and emax > 0 and p_ch and p_ch > 0:
        norm["bess_time_to_full_charge_h"] = emax / p_ch

    # discharge
    p_dis = _opt_num(norm, "p_dis_max_mw", None)
    c_dis = _opt_num(norm, "bess_c_rate_discharge", None)
    t_dis = _opt_num(norm, "bess_time_to_full_discharge_h", None)

    if (p_dis is None or p_dis <= 0) and emax > 0 and c_dis and c_dis > 0:
        norm["p_dis_max_mw"] = emax * c_dis
        p_dis = norm["p_dis_max_mw"]

    if (t_dis is None or t_dis <= 0) and emax > 0 and p_dis and p_dis > 0:
        norm["bess_time_to_full_discharge_h"] = emax / p_dis

def _require_keys(prefix: str, data: Mapping[str, Any], keys: List[str]) -> None:
    missing = [k for k in keys if k not in data or data.get(k) is None]
    if missing:
        raise RuntimeError(f"[00] Brak kluczy w {prefix}: {', '.join(missing)}")

def _get_first(d: Mapping[str, Any], *keys: str, default=None):
    """Pobiera pierwszą niepustą wartość z listy kandydatów kluczy."""
    for k in keys:
        if k in d and d.get(k) not in (None, ""):
            return d.get(k)
    return default

# ─────────────────────────────────────────────────────────────────────────────
# RAW → output.snapshot.raw
# ─────────────────────────────────────────────────────────────────────────────

async def _table_exists(con: psycopg.AsyncConnection, fq_table: str) -> bool:
    async with con.cursor() as cur:
        await cur.execute("SELECT to_regclass(%s) AS reg", (fq_table,))
        row = await cur.fetchone()
        return bool(row and row["reg"])

async def dump_params_snapshot(con: psycopg.AsyncConnection, *, calc_id, params_ts) -> int:
    con.row_factory = dict_row
    total_saved = 0
    raw: Dict[str, Any] = {}

    for tbl in PARAM_TABLES:
        if not await _table_exists(con, tbl):
            log.error("[00/SNAPSHOT] Brak tabeli: %s", tbl)
            continue
        async with con.cursor() as cur:
            await cur.execute(f"SELECT * FROM {tbl} ORDER BY updated_at DESC NULLS LAST LIMIT 1")
            row = await cur.fetchone()
            if row is None:
                log.error("[00/SNAPSHOT] Pusta tabela: %s", tbl)
                continue
            raw[tbl.split(".")[-1]] = row
            total_saved += 1

    async with con.cursor() as cur:
        await cur.execute("""
            INSERT INTO output.snapshot (calc_id, params_ts, raw, norm)
            VALUES (%s, %s, %s::jsonb, '{}'::jsonb)
            ON CONFLICT (calc_id) DO UPDATE
              SET params_ts = EXCLUDED.params_ts,
                  raw       = EXCLUDED.raw,
                  created_at= GREATEST(output.snapshot.created_at, now());
        """, (str(calc_id), params_ts, json.dumps(raw, default=_json_safe)))

    log.info("[00/SNAPSHOT] RAW zapisany: %d tabel scalonych do output.snapshot.raw.", total_saved)
    return total_saved

# ─────────────────────────────────────────────────────────────────────────────
# KONSOLIDACJA RAW → NORM
# ─────────────────────────────────────────────────────────────────────────────

async def build_params_snapshot_consolidated(con: psycopg.AsyncConnection, *, calc_id, params_ts) -> Dict[str, Any]:
    con.row_factory = dict_row

    async with con.cursor() as cur:
        await cur.execute("SELECT raw FROM output.snapshot WHERE calc_id = %s", (str(calc_id),))
        row = await cur.fetchone()
        if not row or not row.get("raw"):
            raise RuntimeError("[00] Brak RAW w output.snapshot dla calc_id.")
    raw: Dict[str, Any] = row["raw"] or {}

    # Rozpakowanie payload
    f_bess    = _payload_or_self(raw.get("form_bess_param"))
    f_arbi    = _payload_or_self(raw.get("form_par_arbitrazu"))
    f_lcoe    = _payload_or_self(raw.get("form_lcoe"))
    f_kontr   = _payload_or_self(raw.get("form_par_kontraktu"))
    f_sys_k   = _payload_or_self(raw.get("form_oplaty_sys_kparam"))
    f_sys_s   = _payload_or_self(raw.get("form_oplaty_sys_sched"))
    f_dyst_k  = _payload_or_self(raw.get("form_oplaty_dystrybucyjne"))
    f_dyst_s  = _payload_or_self(raw.get("form_oplaty_dyst_sched"))
    f_klient  = _payload_or_self(raw.get("form_parametry_klienta"))
    f_zmienne = _payload_or_self(raw.get("form_zmienne"))
    f_fisk    = _payload_or_self(raw.get("form_oplaty_fiskalne"))
    f_sys_st  = _payload_or_self(raw.get("form_oplaty_systemowe"))

    norm: Dict[str, Any] = {}

    # Zachowujemy pełną kopię params.* do audytu
    norm["__raw"] = raw

    # --- Emax i podział OZE/ARBI
    emax_total = _req_num(f_zmienne, "emax") if "emax" in f_zmienne else 0.0
    procent_arbi = _req_num(f_zmienne, "procent_arbitrazu") if "procent_arbitrazu" in f_zmienne else 0.0
    emax_arbi = emax_total * (procent_arbi / 100.0) if emax_total > 0 else 0.0
    emax_oze  = max(emax_total - emax_arbi, 0.0)
    norm["emax_total_mwh"] = emax_total
    norm["emax_arbi_mwh"]  = emax_arbi
    norm["emax_oze_mwh"]   = emax_oze
    norm["frac_oze"]       = (emax_oze / emax_total) if emax_total > 0 else 0.0

    # --- Pola bazowe czasu (01–05)
    norm["_time_fields"] = ["y", "m", "d", "h", "dow", "is_work", "is_free"]

    # --- Mnożniki produkcji/konsumpcji
    norm["moc_pv_pp"] = _req_num(f_zmienne, "moc_pv_pp")
    norm["moc_pv_wz"] = _req_num(f_zmienne, "moc_pv_wz")
    norm["moc_wiatr"] = _req_num(f_zmienne, "moc_wiatr")
    _zmiany_konsumpcji_pct = _req_num(f_zmienne, "zmiany_konsumpcji")
    norm["zmiany_konsumpcji"] = 1.0 + (float(_zmiany_konsumpcji_pct) / 100.0)
    norm["zmiany_konsumpcji_frac"] = norm["zmiany_konsumpcji"]

    # --- BESS
    # Sprawności: % → frakcja
    norm["eta_ch_frac"]  = _normalize_vat_frac(f_bess.get("bess_charge_eff"))
    norm["eta_dis_frac"] = _normalize_vat_frac(f_bess.get("bess_discharge_eff"))

    # c (godziny) → c-rate [1/h] i czasy do pełna [h]
    c_ch, t_ch = _crate_per_h_from_hours(f_bess, "bess_c_rate_charge", "bess_time_to_full_charge_h")
    c_ds, t_ds = _crate_per_h_from_hours(f_bess, "bess_c_rate_discharge", "bess_time_to_full_discharge_h")
    norm["bess_c_rate_charge"]            = c_ch
    norm["bess_time_to_full_charge_h"]    = t_ch or 0.0
    norm["bess_c_rate_discharge"]         = c_ds
    norm["bess_time_to_full_discharge_h"] = t_ds or 0.0

    # moce p_ch/p_dis jeśli podane (opcjonalne)
    if "p_ch_max_mw" in f_bess:  norm["p_ch_max_mw"]  = _req_num(f_bess, "p_ch_max_mw")
    if "p_dis_max_mw" in f_bess: norm["p_dis_max_mw"] = _req_num(f_bess, "p_dis_max_mw")

    # --- LCOE (z form_lcoe)
    norm["lcoe_pv_pp"] = _req_num(f_lcoe, "lcoe_pv_pp")
    norm["lcoe_pv_wz"] = _req_num(f_lcoe, "lcoe_pv_wz")
    norm["lcoe_wiatr"] = _req_num(f_lcoe, "lcoe_wiatr")

    # wyprowadzenie mocy/czasów z emax i c-rate
    _complete_power_time_from_capacity(norm)

    # SOC start (opcjonalne)
    for k in ("soc_oze_start_mwh","soc_arbi_start_mwh"):
        if k in f_bess:
            norm[k] = _req_num(f_bess, k)

    # samorozładowanie λ
    norm["bess_lambda_h_frac"] = _lambda_h_from_bess(f_bess)

    # --- Taryfa/VAT/AKCYZA
    if "wybrana_taryfa" in f_kontr:
        norm["wybrana_taryfa"] = _normalize_tariff_value(f_kontr["wybrana_taryfa"])
    else:
        raise RuntimeError("[00] Brak klucza: wybrana_taryfa (form_par_kontraktu)")

    norm["vat_udzial"] = _normalize_vat_frac(_get_first(f_fisk, "vat", "VAT", "vat_udzial"))
    # Akcyza (PLN/MWh)
    akcyza_val = _get_first(f_fisk, "akcyza", "Akcyza", "akcyza_pln_mwh")
    if akcyza_val is None:
        raise RuntimeError("[00] Brak klucza akcyza w form_oplaty_fiskalne")
    norm.setdefault("tax", {})["vat_rate"] = norm["vat_udzial"]
    norm["tax"]["akcyza_pln_mwh"] = float(_req_num({"akcyza": akcyza_val}, "akcyza"))

    norm["price_field_energy"] = "price_import_pln_mwh"

    # --- Kontrakt & opłaty handlowe (z form_par_kontraktu)
    contract: Dict[str, Any] = {}
    contract["model_ceny"] = str(_get_first(f_kontr, "typ_kontraktu", "model_ceny", default="stala")).lower()
    # mapuj wybraną taryfę także w contract (uppercase)
    contract["tariff_selected"] = _normalize_tariff_value(f_kontr.get("wybrana_taryfa")).upper()

    # Cena stała (w JSON bywa z diakrytykiem i bez)
    cena_stala = _get_first(f_kontr, "cena_energii_stała", "cena_energii_stala", "Cena_stala", "cena_stala")
    if cena_stala is not None:
        contract["Cena_stala_pln_mwh"] = float(_req_num({"c": cena_stala}, "c"))
    # Korekta % (trzymamy jako procent, mnożymy dopiero w 04b)
    proc_delta = _get_first(f_kontr, "proc_zmiana_ceny", "proc_zmiana_ceny_pct", "proc_delta_pct")
    contract["proc_zmiana_ceny_pct"] = float(_req_num({"p": proc_delta or 0.0}, "p"))

    # Opłaty handlowe
    contract["opl_handlowa_mies_pln"]   = float(_req_num({"v": _get_first(f_kontr, "opłata_handlowa_stala", "oplata_handlowa_stala", "oplata_handlowa_mies_pln", "oplata_handlowa_mies") or 0.0}, "v"))
    contract["marza_stala_pln_mwh"]     = float(_req_num({"v": _get_first(f_kontr, "opłata_handlowa_marza_stala", "oplata_handlowa_marza_stala", "marza_stala_pln_mwh") or 0.0}, "v"))
    contract["marza_zmienna_pln_mwh"]   = float(_req_num({"v": _get_first(f_kontr, "oplata_handlowa_marza_zmienna", "opłata_handlowa_marza_zmienna", "marza_zmienna_pln_mwh") or 0.0}, "v"))
    norm["contract"] = contract

    # Moc umowna (globalna + opcjonalne rozdzielenie import/eksport)
    if "klient_moc_umowna" in f_klient:
        norm["moc_umowna_mw"] = _req_num(f_klient, "klient_moc_umowna")
    else:
        raise RuntimeError("[00] Brak klucza: klient_moc_umowna (form_parametry_klienta)")

    moc_imp_mw = _opt_num(_payload_or_self(raw.get("form_par_arbitrazu")), "moc_umowna_import_mw", None)
    moc_exp_mw = _opt_num(_payload_or_self(raw.get("form_par_arbitrazu")), "moc_umowna_export_mw", None)
    moc_global_mw = _opt_num(norm, "moc_umowna_mw", 0.0) or 0.0
    imp_mw = float(moc_imp_mw if moc_imp_mw is not None else moc_global_mw)
    exp_mw = float(moc_exp_mw if moc_exp_mw is not None else moc_global_mw)

    # --- CAPY (skalary)
    emax_total = _opt_num(norm, "emax_total_mwh", 0.0) or 0.0
    t_charge = _opt_num(norm, "bess_time_to_full_charge_h", 0.0) or 0.0
    t_dis    = _opt_num(norm, "bess_time_to_full_discharge_h", 0.0) or 0.0

    # Magazyn (NET, MWh/h) — z parametru c (godziny do pełna)
    norm["cap_bess_charge_net_mwh"]    = float(emax_total / t_charge) if t_charge > 0 else 0.0
    norm["cap_bess_discharge_net_mwh"] = float(emax_total / t_dis)    if t_dis    > 0 else 0.0

    # Sieć (AC, MWh/h) — z mocy umownej (MW ⇒ MWh/h)
    norm["cap_grid_import_ac_mwh"] = max(0.0, imp_mw) * 1.0
    norm["cap_grid_export_ac_mwh"] = max(0.0, exp_mw) * 1.0

    # --- Polityki arbitrażu
    for k in ("base_min_profit_pln_mwh","cycles_per_day","soc_low_threshold","soc_high_threshold"):
        if k not in f_arbi:
            raise RuntimeError(f"[00] Brak klucza: {k} (form_par_arbitrazu)")
        norm[k] = _req_num(f_arbi, k)

    for k in ("allow_carry_over", "force_order", "arbi_dis_to_load"):
        if k not in f_arbi:
            raise RuntimeError(f"[00] Brak klucza: {k} (form_par_arbitrazu)")
        norm[k] = bool(f_arbi.get(k))

    # bonusy: skalary (wymagamy kluczy)
    for k in OPT_BONUS_SCALARS:
        if k not in f_arbi:
            raise RuntimeError(f"[00] Brak klucza: {k} (form_par_arbitrazu)")
        norm[k] = _req_num(f_arbi, k)

    # bonusy: listy (klucze muszą istnieć)
    for k in BONUS_LISTS:
        if k not in f_arbi:
            raise RuntimeError(f"[00] Brak klucz: {k} (form_par_arbitrazu)")
        v = f_arbi.get(k)
        if not isinstance(v, list):
            raise RuntimeError(f"[00] Klucz {k} nie jest listą.")
        norm[k] = list(v)

    # --- SCHEDULERY SYSTEMOWE (mocowa)
    MON = {1:"jan",2:"feb",3:"mar",4:"apr",5:"may",6:"jun",7:"jul",8:"aug",9:"sep",10:"oct",11:"nov",12:"dec"}

    need_moc_keys: List[str] = []
    for mon_num in range(1, 13):
        mon = MON[mon_num]
        for wf_short, wf in WF_MAP.items():
            for edge in ("start","end"):
                need_moc_keys.append(f"sys_sched_{mon}_{wf}_peak_{edge}")
    if not f_sys_s:
        raise RuntimeError("[00] Brak payloadu: form_oplaty_sys_sched")
    missing_moc = [k for k in need_moc_keys if f_sys_s.get(k) is None]
    if missing_moc:
        raise RuntimeError(f"[00] Brakujące klucze w form_oplaty_sys_sched: {', '.join(missing_moc)}")
    moc_sched: Dict[str, int] = {}
    for k in need_moc_keys:
        moc_sched[k] = int(_req_num(f_sys_s, k))
    norm["moc_sched"] = moc_sched

    # --- PARAMETR K (progi i A)
    k_thresholds: List[float] = []
    for key in ("k_threshold_1_pct","k_threshold_2_pct","k_threshold_3_pct","prog_1","prog_2","prog_3"):
        if key in f_sys_k:
            v = _as_num(f_sys_k.get(key))
            if v is not None:
                k_thresholds.append(float(v))
    # fallback: jeżeli nie było nazwanych progów, spróbuj 'k_thresholds_pct' jako listy
    if not k_thresholds and isinstance(f_sys_k.get("k_thresholds_pct"), list):
        k_thresholds = [float(x) for x in f_sys_k.get("k_thresholds_pct")]
    # sort i unikalność
    k_thresholds = sorted(list({float(x) for x in k_thresholds})) or [5.0, 10.0, 15.0]

    k_A_values = {}
    for name in ("K1","K2","K3","K4"):
        v = _get_first(f_sys_k, f"A_{name}", f"a_{name}", name, f"A{name}")
        if v is not None:
            k_A_values[name] = float(_as_num(v))
    # fallback domyślne jeśli nie ma nic
    if not k_A_values:
        k_A_values = {"K1":0.17,"K2":0.50,"K3":0.83,"K4":1.00}

    norm["kparam"] = {"k_thresholds_pct": k_thresholds, "k_A_values": k_A_values}

    # --- DYSTRYBUCYJNE — harmonogramy dla B21/B22/B23 (WORK/FREE, MORN/AFT)
    if not f_dyst_s:
        raise RuntimeError("[00] Brak payloadu: form_oplaty_dyst_sched")
    dyst_sched_all: Dict[str, Dict[str, int]] = {}
    for t in TARIFFS:
        need_dyst_keys: List[str] = []
        for mon_num in range(1, 13):
            mon = MON[mon_num]
            for wf_short, wf in WF_MAP.items():
                for part in ("morn","aft"):
                    for edge in ("start","end"):
                        need_dyst_keys.append(f"dyst_sched_{t}_{mon}_{wf}_{part}_{edge}")
        missing_dyst = [k for k in need_dyst_keys if f_dyst_s.get(k) is None]
        if missing_dyst:
            raise RuntimeError(f"[00] Brakujące klucze w form_oplaty_dyst_sched dla {t.upper()}: {', '.join(missing_dyst)}")
        dmap: Dict[str, int] = {}
        for k in need_dyst_keys:
            dmap[k] = int(_req_num(f_dyst_s, k))
        dyst_sched_all[t.upper()] = dmap
    norm["dyst_sched"] = dyst_sched_all

    # --- DYSTRYBUCYJNE — stawki (zmienne: PLN/MWh; jakościowa: PLN/MWh; stałe per kW/mies.; abonament m-c)
    dyst_var: Dict[str, Dict[str, Dict[str, float]]] = {}   # {"B23":{"work":{"am":..,"pm":..,"off":..},"free":{...}}, ...}
    dyst_fixed: Dict[str, Dict[str, float]] = {}            # {"B23":{"fixed_per_kw_month_pln":..,"trans_per_kw_month_pln":..,"abon_month_pln":..,"derived_month_total_pln":..}, ...}

    # Pobierz moc umowną (do pochodnej kwoty m-c)
    moc_umowna_mw = float(norm.get("moc_umowna_mw", 0.0))
    moc_kW = moc_umowna_mw * 1000.0

    def _val_f(d: Mapping[str, Any], key: str) -> float:
        return float(_as_num(d.get(key)) or 0.0)

    for t in TARIFFS:
        TU = t.upper()

        # Zmienne — stawki AM/PM/OFF lub DAY/NIGHT albo SINGLE; WORK i FREE: jeśli brak rozdzielenia, kopiujemy z WORK do FREE
        work: Dict[str, float] = {}
        free: Dict[str, float] = {}

        # B23
        if t == "b23":
            # aliasy: morn→am, aft→pm, other→off
            am  = _val_f(f_dyst_k, f"dyst_var_{t}_morn") or _val_f(f_dyst_k, f"dyst_var_{t}_am")
            pm  = _val_f(f_dyst_k, f"dyst_var_{t}_aft")  or _val_f(f_dyst_k, f"dyst_var_{t}_pm")
            off = _val_f(f_dyst_k, f"dyst_var_{t}_other") or _val_f(f_dyst_k, f"dyst_var_{t}_off")

            # FREE warianty (jeśli są)
            am_f  = _val_f(f_dyst_k, f"dyst_var_{t}_morn_free") or _val_f(f_dyst_k, f"dyst_var_{t}_am_free")
            pm_f  = _val_f(f_dyst_k, f"dyst_var_{t}_aft_free")  or _val_f(f_dyst_k, f"dyst_var_{t}_pm_free")
            off_f = _val_f(f_dyst_k, f"dyst_var_{t}_other_free") or _val_f(f_dyst_k, f"dyst_var_{t}_off_free")

            work.update({"am": am, "pm": pm, "off": off})
            free.update({"am": (am_f or am), "pm": (pm_f or pm), "off": (off_f or off)})

        # B22
        elif t == "b22":
            day   = _val_f(f_dyst_k, f"dyst_var_{t}_day")   or (_val_f(f_dyst_k, f"dyst_var_{t}_morn") + _val_f(f_dyst_k, f"dyst_var_{t}_aft"))/2.0
            night = _val_f(f_dyst_k, f"dyst_var_{t}_night") or _val_f(f_dyst_k, f"dyst_var_{t}_other")
            day_f   = _val_f(f_dyst_k, f"dyst_var_{t}_day_free")
            night_f = _val_f(f_dyst_k, f"dyst_var_{t}_night_free")
            work.update({"day": day, "night": night})
            free.update({"day": (day_f or day), "night": (night_f or night)})

        # B21
        else:
            single   = _val_f(f_dyst_k, f"dyst_var_{t}_single") or _val_f(f_dyst_k, f"dyst_var_{t}_other") or _val_f(f_dyst_k, f"dyst_var_{t}_am")
            single_f = _val_f(f_dyst_k, f"dyst_var_{t}_single_free")
            work.update({"single": single})
            free.update({"single": (single_f or single)})

        # Jakościowa (PLN/MWh) — stała, bez harmonogramu: trzymamy w obu (work/free) dla prostoty
        qual = _val_f(f_dyst_k, f"dyst_qual_{t}")
        if "am" in work:   work["qual_pln_mwh"] = qual;   free["qual_pln_mwh"] = qual
        if "day" in work:  work["qual_pln_mwh"] = qual;   free["qual_pln_mwh"] = qual
        if "single" in work: work["qual_pln_mwh"] = qual; free["qual_pln_mwh"] = qual

        dyst_var[TU] = {"work": work, "free": free}

        # Stałe miesięczne: per kW/m-c (fixed, trans) + abonament m-c
        fixed_per_kw = _val_f(f_dyst_k, f"dyst_fixed_{t}")
        trans_per_kw = _val_f(f_dyst_k, f"dyst_trans_{t}")
        abon_month   = _val_f(f_dyst_k, f"dyst_abon_{t}")

        derived = (fixed_per_kw + trans_per_kw) * moc_kW + abon_month
        dyst_fixed[TU] = {
            "fixed_per_kw_month_pln": fixed_per_kw,
            "trans_per_kw_month_pln": trans_per_kw,
            "abon_month_pln": abon_month,
            "derived_month_total_pln": derived,
        }

    norm["dyst_var"] = dyst_var
    norm["dyst_fixed"] = dyst_fixed

    # --- SYSTEMOWE — stawki (PLN/MWh): OZE/Kogeneracyjna/Mocowa
    sys_oze = _get_first(f_sys_st, "oze_pln_mwh", "oze", "stawka_oze_pln_mwh", "stawka_oze")
    sys_kog = _get_first(f_sys_st, "kog_pln_mwh", "kogeneracyjna", "stawka_kog_pln_mwh", "stawka_kogeneracyjna")
    sys_moc = _get_first(f_sys_st, "mocowa_pln_mwh", "mocowa", "stawka_mocowa_pln_mwh", "stawka_mocowa")
    if sys_oze is None or sys_kog is None or sys_moc is None:
        raise RuntimeError("[00] Brak stawek systemowych (OZE/KOG/MOC) w form_oplaty_systemowe")
    norm["sys"] = {
        "oze_pln_mwh": float(_req_num({"v": sys_oze}, "v")),
        "kog_pln_mwh": float(_req_num({"v": sys_kog}, "v")),
        "mocowa_pln_mwh": float(_req_num({"v": sys_moc}, "v")),
    }

    # ── Walidacja końcowa wymagalnych skalarnych kluczy (bez zmian listy)
    for k in REQ_SCALARS:
        if k not in norm:
            raise RuntimeError(f"[00] Brak wymaganej wartości w NORM: {k}")
        _ = _req_num(norm, k)

    # UPSERT NORM
    async with con.cursor() as cur:
        await cur.execute("""
            UPDATE output.snapshot
               SET norm = %s::jsonb,
                   params_ts = %s,
                   created_at = GREATEST(output.snapshot.created_at, now())
             WHERE calc_id = %s
        """, (json.dumps(norm, default=_json_safe), params_ts, str(calc_id)))

    return norm

# ─────────────────────────────────────────────────────────────────────────────
# API / RUN
# ─────────────────────────────────────────────────────────────────────────────

async def get_consolidated_norm(con: psycopg.AsyncConnection, *, calc_id) -> Dict[str, Any]:
    con.row_factory = dict_row
    async with con.cursor() as cur:
        await cur.execute("SELECT norm FROM output.snapshot WHERE calc_id = %s", (str(calc_id),))
        row = await cur.fetchone()
    if not row or "norm" not in row or row["norm"] is None:
        raise RuntimeError("[00] Brak `norm` dla calc_id (najpierw odpal etap 00).")
    return row["norm"]

async def run(con: psycopg.AsyncConnection, *, calc_id, params_ts):
    saved = await dump_params_snapshot(con, calc_id=calc_id, params_ts=params_ts)
    log.info("[00/SNAPSHOT] RAW ok: %d tabel", saved)

    norm = await build_params_snapshot_consolidated(con, calc_id=calc_id, params_ts=params_ts)
    log.info("[00/SNAPSHOT] NORM ok (keys=%d).", len(norm))

    return True
