# ingest_00.py — H_buf: szybkie bufory NumPy (oś czasu + capy/parametry + SOC start)
# (poprawione: dt_h → lokalny dzień (date); długość godziny → h_len_h float)

from __future__ import annotations

import uuid
import logging
from typing import Any, Dict, List
from collections import OrderedDict

import numpy as np
import psycopg
from psycopg import sql
from psycopg.rows import dict_row

log = logging.getLogger("ingest_00")

SNAPSHOT_TABLE = "output.params_snapshot_calc"
_SNAP_SCHEMA, _SNAP_TABLE = SNAPSHOT_TABLE.split(".", 1)
EPS = 1e-12

# ──────────────────────────────────────────────────────────────────────────────
# Helpers: snapshot norm
# ──────────────────────────────────────────────────────────────────────────────
async def _load_norm(con: psycopg.AsyncConnection, calc_id: uuid.UUID) -> Dict[str, Any]:
    con.row_factory = dict_row

    # BEZPIECZNE cytowanie identyfikatora tabeli snapshotu
    qualified = sql.Identifier(_SNAP_SCHEMA, _SNAP_TABLE).as_string(con)
    q = f"""
      SELECT row
      FROM {qualified}
      WHERE calc_id = %s AND source_table = 'params.__consolidated__'
      ORDER BY created_at DESC
      LIMIT 1
    """

    async with con.cursor() as cur:
        await cur.execute(q, (str(calc_id),))
        rec = await cur.fetchone()

    if not rec or not isinstance(rec["row"], dict) or "norm" not in rec["row"]:
        raise RuntimeError("[00] Brak payload.norm w snapshot.")
    return rec["row"]["norm"]

# ──────────────────────────────────────────────────────────────────────────────
# Oś czasu: ts_utc z input.date_dim + ts_local = (ts_utc AT TIME ZONE :local_tz)
# ──────────────────────────────────────────────────────────────────────────────
async def _axis(con: psycopg.AsyncConnection, local_tz: str) -> Dict[str, Any]:
    if not local_tz:
        raise RuntimeError("[00] local_tz jest wymagane (np. 'Europe/Warsaw').")

    con.row_factory = dict_row
    q = """
        SELECT ts_utc,
               (ts_utc AT TIME ZONE %s)                          AS "ts_local",
               -- ↓↓↓ klucze „dzienne” i „godzinowe” po CZASIE LOKALNYM
               EXTRACT(YEAR FROM (ts_utc AT TIME ZONE %s))::int  AS year,
               EXTRACT(MONTH FROM (ts_utc AT TIME ZONE %s))::int AS month,
               EXTRACT(DAY FROM (ts_utc AT TIME ZONE %s))::int   AS day,
               EXTRACT(HOUR FROM (ts_utc AT TIME ZONE %s))::int  AS hour,
               EXTRACT(DOW FROM (ts_utc AT TIME ZONE %s))::int   AS dow,
               COALESCE(is_workday, true)                        AS is_workday,
               COALESCE(is_holiday, false)                       AS is_holiday,
               holiday_name
        FROM input.date_dim
        ORDER BY ts_utc
        """
    async with con.cursor() as cur:
        await cur.execute(q, (local_tz, local_tz, local_tz, local_tz, local_tz, local_tz))
        rows = await cur.fetchall()
    if not rows:
        raise RuntimeError("[00] input.date_dim jest puste")

    # Oryginalne obiekty datetime (PY) — potrzebne do INSERTów
    ts_utc_py   = [r["ts_utc"]   for r in rows]  # tz-aware datetime
    ts_local_py = [r["ts_local"] for r in rows]  # naive datetime (lokalny zegar)

    # Dodatkowo wersje NumPy (używane tylko wewnątrz 00 do obliczeń)
    ts_utc_np = np.array(ts_utc_py, dtype="datetime64[ns]")

    # mapy pomocnicze do alignowania po ts_local (lokalny)
    local_to_idx: Dict[object, List[int]] = {}
    for i, tl in enumerate(ts_local_py):
        local_to_idx.setdefault(tl, []).append(i)

    # Długość godziny (w h) po osi UTC
    ts_sec = ts_utc_np.astype("datetime64[s]").astype(np.int64)
    dt_sec = np.empty_like(ts_sec, dtype=np.float64)
    if len(ts_sec) > 1:
        dt_sec[:-1] = np.diff(ts_sec)
        dt_sec[-1] = dt_sec[-2]
    else:
        dt_sec[0] = 3600.0
    h_len_h = np.clip(dt_sec / 3600.0, 0.0, 24.0)

    # Lokalny dzień (DATE) — zgodny z wymaganiem 03
    dt_local_date = [tl.date() for tl in ts_local_py]

    year  = np.fromiter((int(r["year"]) for r in rows),  dtype=np.int32, count=len(rows))
    month = np.fromiter((int(r["month"]) for r in rows), dtype=np.int32, count=len(rows))
    day   = np.fromiter((int(r["day"]) for r in rows),   dtype=np.int32, count=len(rows))
    hour  = np.fromiter((int(r["hour"]) for r in rows),  dtype=np.int32, count=len(rows))
    dow   = np.fromiter((int(r["dow"]) for r in rows),   dtype=np.int32, count=len(rows))
    is_workday  = np.fromiter((bool(r["is_workday"]) for r in rows),  dtype=np.bool_, count=len(rows))
    is_holiday  = np.fromiter((bool(r["is_holiday"]) for r in rows),  dtype=np.bool_, count=len(rows))
    holiday_name = [r["holiday_name"] for r in rows]

    return {
        # PY datetimes (do persistu)
        "ts_utc_py": ts_utc_py,
        "ts_local_py": ts_local_py,

        # NumPy datetimes (wewnętrznie do obliczeń w 00)
        "ts_utc": ts_utc_np,

        "local_to_idx": local_to_idx,
        "year": year, "month": month, "day": day,
        "hour": hour, "dow": dow, "is_workday": is_workday, "is_holiday": is_holiday,
        "holiday_name": holiday_name,

        # NOWE KLUCZE:
        "h_len_h": h_len_h,              # ← długość godziny [h]
        "dt_local_date": dt_local_date,  # ← lokalny dzień (date)
    }

# ──────────────────────────────────────────────────────────────────────────────
# Pobranie serii wejściowych i mapowanie (ts_local/ts_utc) + interpolacja NaN
# ──────────────────────────────────────────────────────────────────────────────
def _interp_nan_inplace(arr: np.ndarray) -> None:
    mask = np.isnan(arr)
    if not mask.any():
        return
    n = arr.shape[0]
    x = np.arange(n, dtype=np.int64)
    known = ~mask
    if known.sum() == 0:
        arr[:] = 0.0
        return
    arr[mask] = np.interp(x[mask], x[known], arr[known])
    first = np.argmax(known)
    last = n - 1 - np.argmax(known[::-1])
    arr[:first] = arr[first]
    arr[last+1:] = arr[last]

def _safe_div(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    out = np.full_like(a, np.nan, dtype=float)
    nz = b != 0
    out[nz] = a[nz] / b[nz]
    return out

async def _series(con: psycopg.AsyncConnection, axis: Dict[str, Any]) -> Dict[str, np.ndarray]:
    """
    Wejścia twarde:
      - input.konsumpcja(ts_local, zuzycie_mw) → mapujemy po ts_local,
      - input.produkcja(ts_local, pv_pp_1mwp, pv_wz_1mwp, wind_1mwp) → mapujemy po ts_local,
      - input.ceny_godzinowe(ts_utc, fixing_i_price) → mapujemy po ts_utc.
    Braki → NaN → interpolacja.
    """
    con.row_factory = dict_row
    local_to_idx: Dict[object, List[int]] = axis["local_to_idx"]
    ts_axis_utc_s = axis["ts_utc"].astype("datetime64[s]").astype(np.int64)
    n_hours = len(ts_axis_utc_s)

    def _nan() -> np.ndarray:
        a = np.empty(n_hours, dtype=np.float64)
        a[:] = np.nan
        return a

    # KONSUMPCJA (po ts_local)
    p_load = _nan()
    async with con.cursor() as cur:
        await cur.execute("SELECT ts_local, zuzycie_mw AS v FROM input.konsumpcja ORDER BY 1")
        for r in await cur.fetchall():
            tl = r["ts_local"]; v = None if r["v"] is None else float(r["v"])
            idxs = local_to_idx.get(tl)
            if idxs:
                for j in idxs:
                    p_load[j] = v if v is not None else np.nan
    _interp_nan_inplace(p_load)

    # PRODUKCJA (po ts_local)
    pv_pp  = _nan(); pv_wz = _nan(); wind = _nan()
    async with con.cursor() as cur:
        await cur.execute("""
            SELECT ts_local, pv_pp_1mwp AS pv_pp, pv_wz_1mwp AS pv_wz, wind_1mwp AS wind
            FROM input.produkcja ORDER BY 1
        """)
        for r in await cur.fetchall():
            tl = r["ts_local"]; idxs = local_to_idx.get(tl)
            if not idxs:
                continue
            v1 = None if r["pv_pp"] is None else float(r["pv_pp"])
            v2 = None if r["pv_wz"] is None else float(r["pv_wz"])
            v3 = None if r["wind"]  is None else float(r["wind"])
            for j in idxs:
                pv_pp[j] = v1 if v1 is not None else np.nan
                pv_wz[j] = v2 if v2 is not None else np.nan
                wind[j]  = v3 if v3 is not None else np.nan
    _interp_nan_inplace(pv_pp); _interp_nan_inplace(pv_wz); _interp_nan_inplace(wind)

    # CENY (po ts_utc)
    price = _nan()
    async with con.cursor() as cur:
        await cur.execute("SELECT ts_utc, fixing_i_price AS v FROM input.ceny_godzinowe ORDER BY 1")
        rows = await cur.fetchall()
    for r in rows:
        t_s = int(r["ts_utc"].timestamp())  # epoch seconds UTC (aware → seconds)
        i = np.searchsorted(ts_axis_utc_s, t_s)
        if i < n_hours and ts_axis_utc_s[i] == t_s:
            price[i] = None if r["v"] is None else float(r["v"])
    _interp_nan_inplace(price)

    return {
        "p_load_mw_base": p_load,
        "p_pv_pp_mw_base": pv_pp,
        "p_pv_wz_mw_base": pv_wz,
        "p_wind_mw_base":  wind,
        "price_tge_pln_mwh_base": price,
    }

# ──────────────────────────────────────────────────────────────────────────────
# Główny entrypoint 00
# ──────────────────────────────────────────────────────────────────────────────
async def run(con: psycopg.AsyncConnection, calc_id: uuid.UUID, params_ts) -> Dict[str, Any]:
    """
    Zwraca H_buf (OrderedDict nazw→kolumny), 1 wiersz = 1 godzina (UTC).
    """
    norm = await _load_norm(con, calc_id)
    local_tz = str(norm.get("local_tz") or "").strip()
    if not local_tz:
        raise RuntimeError("[00] 'local_tz' nieobecne w norm — uzupełnij parametry (np. Europe/Warsaw).")

    axis = await _axis(con, local_tz)
    series = await _series(con, axis)

    n_hours = len(axis["ts_utc"])  # NumPy UTC (wewn.), PY listy mają ten sam n
    h_len_h = axis["h_len_h"]      # ← długość godziny (float, [h])

    # Mnożniki
    fac_load  = float(norm["fac_load"])
    fac_pv_pp = float(norm["fac_pv_pp"])
    fac_pv_wz = float(norm["fac_pv_wz"])
    fac_wind  = float(norm["fac_wind"])

    p_load_mw   = series["p_load_mw_base"]   * fac_load
    p_pv_pp_mw  = series["p_pv_pp_mw_base"]  * fac_pv_pp
    p_pv_wz_mw  = series["p_pv_wz_mw_base"]  * fac_pv_wz
    p_wind_mw   = series["p_wind_mw_base"]   * fac_wind
    p_gen_total_mw = p_pv_pp_mw + p_pv_wz_mw + p_wind_mw

    e_surplus_mwh = np.maximum(0.0, (p_gen_total_mw - p_load_mw) * h_len_h)
    e_deficit_mwh = np.maximum(0.0, (p_load_mw - p_gen_total_mw) * h_len_h)

    # Parametry BESS i capy (NET-na-godzinę)
    eta_ch  = float(norm["eta_ch_frac"])
    eta_dis = float(norm["eta_dis_frac"])
    lam_h   = float(norm["bess_lambda_h_frac"])
    p_ch_max  = float(norm["p_ch_max_mw"])
    p_dis_max = float(norm["p_dis_max_mw"])

    e_ch_cap_net  = p_ch_max * h_len_h * eta_ch
    e_dis_cap_net = (p_dis_max * h_len_h) / max(eta_dis, EPS)

    e_ch_cap_net_oze_mwh   = e_ch_cap_net.copy()
    e_dis_cap_net_oze_mwh  = e_dis_cap_net.copy()
    e_ch_cap_net_arbi_mwh  = e_ch_cap_net.copy()
    e_dis_cap_net_arbi_mwh = e_dis_cap_net.copy()

    emax_oze_mwh  = float(norm["emax_oze_mwh"])
    emax_arbi_mwh = float(norm["emax_arbi_mwh"])

    soc_oze_start_mwh  = float(norm["soc_oze_start_mwh"])
    soc_arbi_start_mwh = float(norm["soc_arbi_start_mwh"])

    # ──────────────────────────────────────────────────────────────────────────
    # Bufor H — TYLKO dane z 00 (bez fallbacków)
    # ──────────────────────────────────────────────────────────────────────────
    H: "OrderedDict[str, object]" = OrderedDict()

    # TIMESTAMPY MUSZĄ BYĆ PY datetime (object), nie numpy.datetime64:
    H["ts_utc"]   = np.array(axis["ts_utc_py"],   dtype=object)  # tz-aware
    H["ts_local"] = np.array(axis["ts_local_py"], dtype=object)  # naive (lokalny zegar)
    H["ts_hour"]  = np.arange(0, n_hours, dtype=np.int64)

    H["year"] = axis["year"]
    H["month"] = axis["month"]
    H["day"] = axis["day"]
    H["hour"] = axis["hour"]
    H["dow"] = axis["dow"]
    H["is_workday"] = axis["is_workday"]
    H["is_holiday"] = axis["is_holiday"]
    H["dt_h"] = np.array(axis["dt_local_date"], dtype=object)  # ← DATE (lokalny dzień)
    H["h_len_h"] = h_len_h                                    # ← DŁUGOŚĆ GODZINY [h]

    # OZE po mnożnikach (przed sumą)
    H["p_pv_pp_mw"] = p_pv_pp_mw
    H["p_pv_wz_mw"] = p_pv_wz_mw
    H["p_wind_mw"]  = p_wind_mw

    H["p_gen_total_mw"] = p_gen_total_mw
    H["p_load_mw"] = p_load_mw
    H["e_surplus_mwh"] = e_surplus_mwh
    H["e_deficit_mwh"] = e_deficit_mwh

    # EMAX i capy
    H["emax_oze_mwh"]  = np.full(n_hours, emax_oze_mwh, dtype=np.float64)
    H["emax_arbi_mwh"] = np.full(n_hours, emax_arbi_mwh, dtype=np.float64)

    H["e_ch_cap_net_oze_mwh"]   = e_ch_cap_net_oze_mwh
    H["e_dis_cap_net_oze_mwh"]  = e_dis_cap_net_oze_mwh
    H["e_ch_cap_net_arbi_mwh"]  = e_ch_cap_net_arbi_mwh
    H["e_dis_cap_net_arbi_mwh"] = e_dis_cap_net_arbi_mwh

    H["eta_ch_frac"]        = np.full(n_hours, eta_ch, dtype=np.float64)
    H["eta_dis_frac"]       = np.full(n_hours, eta_dis, dtype=np.float64)
    H["bess_lambda_h_frac"] = np.full(n_hours, lam_h, dtype=np.float64)

    arbi_dis_to_load = bool(norm["arbi_dis_to_load"])
    moc_umowna_mw    = float(norm["moc_umowna_mw"])

    H["arbi_dis_to_load"] = np.full(n_hours, arbi_dis_to_load, dtype=np.bool_)
    H["moc_umowna_mw"]    = np.full(n_hours, moc_umowna_mw,   dtype=np.float64)
    H["p_dis_max_mw"]     = np.full(n_hours, p_dis_max,       dtype=np.float64)
    H["p_ch_max_mw"]      = np.full(n_hours, p_ch_max,        dtype=np.float64)

    # SOC start
    soc_oze_start  = np.zeros(n_hours, dtype=np.float64)
    soc_arbi_start = np.zeros(n_hours, dtype=np.float64)
    if n_hours > 0:
        soc_oze_start[0]  = soc_oze_start_mwh
        soc_arbi_start[0] = soc_arbi_start_mwh

    H["soc_oze_start_mwh"] = soc_oze_start
    H["soc_arbi_start_mwh"] = soc_arbi_start

    # [03] Raportowe: SOC po idle na 00 = stan startowy (idle liczysz w 02)
    soc_oze_ai  = soc_oze_start.astype(float)
    soc_arbi_ai = soc_arbi_start.astype(float)
    H["soc_oze_after_idle_mwh"]  = soc_oze_ai
    H["soc_arbi_after_idle_mwh"] = soc_arbi_ai

    # Procenty (względem części i całości emax)
    emax_oze_vec  = H["emax_oze_mwh"].astype(float)
    emax_arbi_vec = H["emax_arbi_mwh"].astype(float)
    emax_total    = (emax_oze_vec + emax_arbi_vec).astype(float)

    H["soc_oze_pct_of_oze"]    = _safe_div(soc_oze_ai,  emax_oze_vec)  * 100.0
    H["soc_arbi_pct_of_arbi"]  = _safe_div(soc_arbi_ai, emax_arbi_vec) * 100.0
    H["soc_oze_pct_of_total"]  = _safe_div(soc_oze_ai,  emax_total)    * 100.0
    H["soc_arbi_pct_of_total"] = _safe_div(soc_arbi_ai, emax_total)    * 100.0

    # Parametry arbitrażu / sanity
    base_min_profit_pln_mwh = float(norm["base_min_profit_pln_mwh"])
    cycles_per_day          = int(norm["cycles_per_day"])
    force_order             = bool(norm["force_order"])
    allow_carry_over        = bool(norm["allow_carry_over"])

    bonus_ch_window    = float(norm["bonus_ch_window"])
    bonus_dis_window   = float(norm["bonus_dis_window"])
    bonus_low_soc_ch   = float(norm["bonus_low_soc_ch"])
    bonus_high_soc_dis = float(norm["bonus_high_soc_dis"])

    bonus_hrs_ch       = norm.get("bonus_hrs_ch")
    bonus_hrs_dis      = norm.get("bonus_hrs_dis")
    bonus_hrs_ch_free  = norm.get("bonus_hrs_ch_free")
    bonus_hrs_dis_free = norm.get("bonus_hrs_dis_free")

    soc_low_threshold  = float(norm["soc_low_threshold"])
    soc_high_threshold = float(norm["soc_high_threshold"])

    price_tge_pln_mwh = series["price_tge_pln_mwh_base"]

    H["base_min_profit_pln_mwh"] = np.full(n_hours, base_min_profit_pln_mwh, dtype=np.float64)
    H["cycles_per_day"]          = np.full(n_hours, cycles_per_day,          dtype=np.int16)
    H["force_order"]             = np.full(n_hours, force_order,             dtype=np.bool_)
    H["allow_carry_over"]        = np.full(n_hours, allow_carry_over,        dtype=np.bool_)

    H["bonus_ch_window"]    = np.full(n_hours, bonus_ch_window,    dtype=np.float64)
    H["bonus_dis_window"]   = np.full(n_hours, bonus_dis_window,   dtype=np.float64)
    H["bonus_low_soc_ch"]   = np.full(n_hours, bonus_low_soc_ch,   dtype=np.float64)
    H["bonus_high_soc_dis"] = np.full(n_hours, bonus_high_soc_dis, dtype=np.float64)

    H["bonus_hrs_ch"]       = bonus_hrs_ch
    H["bonus_hrs_dis"]      = bonus_hrs_dis
    H["bonus_hrs_ch_free"]  = bonus_hrs_ch_free
    H["bonus_hrs_dis_free"] = bonus_hrs_dis_free

    H["soc_low_threshold"]  = np.full(n_hours, soc_low_threshold,  dtype=np.float64)
    H["soc_high_threshold"] = np.full(n_hours, soc_high_threshold, dtype=np.float64)

    H["price_tge_pln_mwh"] = price_tge_pln_mwh

    # META
    H["N"]        = np.int64(n_hours)
    H["calc_id"]  = str(calc_id)
    H["params_ts"] = params_ts

    return H
