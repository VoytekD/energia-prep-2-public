"""
ETAP 00 — SNAPSHOT & KONSOLIDACJA (single source of truth)

Kontrakt:
- output.snapshot.raw  : ostatnie rekordy params.* (1:1, JSON)
- output.snapshot.norm : spójny słownik bez aliasów i fallbacków:
  • BESS (sprawności, c-rate, czasy do pełna, moce jeśli policzalne),
  • Polityki/economia (progi, bonusy, listy godzin),
  • VAT,
  • LCOE,
  • CAPY (skalary):
      - cap_bess_charge_net_mwh      [MWh/h] (NET; z c[h])
      - cap_bess_discharge_net_mwh   [MWh/h] (NET; z c[h])
      - cap_grid_import_ac_mwh       [MWh/h] (AC; z MW)
      - cap_grid_export_ac_mwh       [MWh/h] (AC; z MW)
  • Schedulery opłat systemowych i dystrybucyjnych.
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

# Wymagane skalarne wartości w NORM
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

    norm: Dict[str, Any] = {}

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

    # wyprowadzenie mocy/czasów z emax i c-rate (bez agresywnych fallbacków)
    _complete_power_time_from_capacity(norm)

    # SOC start (opcjonalne)
    for k in ("soc_oze_start_mwh","soc_arbi_start_mwh"):
        if k in f_bess:
            norm[k] = _req_num(f_bess, k)

    # samorozładowanie λ
    norm["bess_lambda_h_frac"] = _lambda_h_from_bess(f_bess)

    # --- Taryfa/VAT/LCOE
    if "wybrana_taryfa" in f_kontr:
        norm["wybrana_taryfa"] = _normalize_tariff_value(f_kontr["wybrana_taryfa"])
    else:
        raise RuntimeError("[00] Brak klucza: wybrana_taryfa (form_par_kontraktu)")

    norm["vat_udzial"] = _normalize_vat_frac(f_fisk.get("vat"))
    norm["price_field_energy"] = "price_import_pln_mwh"

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

    # --- SCHEDULERY
    MON = {1:"jan",2:"feb",3:"mar",4:"apr",5:"may",6:"jun",7:"jul",8:"aug",9:"sep",10:"oct",11:"nov",12:"dec"}

    # Mocowa (systemowa)
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

    # Dystrybucyjna (zależna od taryfy)
    t = norm["wybrana_taryfa"]
    need_dyst_keys: List[str] = []
    for mon_num in range(1, 13):
        mon = MON[mon_num]
        for wf_short, wf in WF_MAP.items():
            for part in ("morn","aft"):
                for edge in ("start","end"):
                    need_dyst_keys.append(f"dyst_sched_{t}_{mon}_{wf}_{part}_{edge}")
    if not f_dyst_s:
        raise RuntimeError("[00] Brak payloadu: form_oplaty_dyst_sched")
    missing_dyst = [k for k in need_dyst_keys if f_dyst_s.get(k) is None]
    if missing_dyst:
        raise RuntimeError(f"[00] Brakujące klucze w form_oplaty_dyst_sched: {', '.join(missing_dyst)}")
    dyst_sched: Dict[str, int] = {}
    for k in need_dyst_keys:
        dyst_sched[k] = int(_req_num(f_dyst_s, k))
    norm["dyst_sched"] = dyst_sched

    # ── Walidacja końcowa
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
        """, (json.dumps(norm), params_ts, str(calc_id)))

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
