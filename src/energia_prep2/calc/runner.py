# -*- coding: utf-8 -*-
"""
runner.py — orkiestracja etapów 00 → 05 + 04 (pricing detail)
z preflightami przed/po każdym etapie.

Kontrakt „do przodu”:
- proposer_01.run(H: dict, P: dict) -> dict
- commit_02.run(H: dict, P: dict) -> dict

Brak „wstecznych” fallbacków i cichego uzupełniania braków.
"""

from __future__ import annotations

import math
import logging
from time import perf_counter
from typing import Any, Dict, Iterable, Sequence

import uuid as _uuid

# lekkie importy lokalne (ciężkie moduły z projektu — tylko tutaj)
from . import io as calc_io
from . import ingest_00, proposer_01, commit_02, pricing_03, raport, validate

log = logging.getLogger("energia-prep-2.calc.runner")


# ─────────────────────────────────────────────────────────────────────────────
# Pomocnicze walidatory (preflight)
def _is_seq(x) -> bool:
    return isinstance(x, (list, tuple)) or (hasattr(x, "__len__") and hasattr(x, "__getitem__"))

def _len_of(x) -> int:
    try:
        return len(x)
    except Exception:
        return 0

def _require_keys(stage: str, obj: Dict[str, Any], keys: Sequence[str]) -> None:
    miss = [k for k in keys if k not in obj]
    if miss:
        raise RuntimeError(f"[PREFLIGHT][{stage}] Brak wymaganych kluczy: {', '.join(miss)}")

def _require_arrays(stage: str, obj: Dict[str, Any], keys: Sequence[str], n: int) -> None:
    bad = []
    for k in keys:
        v = obj.get(k)
        if not _is_seq(v) or _len_of(v) != n:
            bad.append(k)
    if bad:
        raise RuntimeError(f"[PREFLIGHT][{stage}] Pola muszą mieć długość N={n}: {', '.join(bad)}")

def _require_nonneg(stage: str, obj: Dict[str, Any], keys: Sequence[str]) -> None:
    neg = []
    for k in keys:
        v = obj.get(k)
        if _is_seq(v):
            if any((x is not None and float(x) < -1e-9) for x in v):
                neg.append(k)
        elif v is not None and float(v) < -1e-9:
            neg.append(k)
    if neg:
        raise RuntimeError(f"[PREFLIGHT][{stage}] Znaleziono wartości ujemne w: {', '.join(neg)}")

def _same_length(stage: str, obj: Dict[str, Any], keys: Sequence[str]) -> None:
    lengths = {k: _len_of(obj.get(k)) for k in keys if _is_seq(obj.get(k))}
    if not lengths:
        return
    vals = set(lengths.values())
    if len(vals) > 1:
        raise RuntimeError(f"[PREFLIGHT][{stage}] Niespójne długości: {lengths}")

def _require_prices(stage: str, H: Dict[str, Any], n: int) -> None:
    need = ("price_import_pln_mwh", "price_export_pln_mwh")
    _require_keys(stage, H, need)
    _require_arrays(stage, H, need, n)

def _require_scalars(stage: str, obj: Dict[str, Any], keys: Sequence[str]) -> None:
    """Egzekwuje twarde skalary (brak kontenerów/ndarray/Series)."""
    from numbers import Number
    bad = []
    for k in keys:
        if k not in obj:
            bad.append(k)
            continue
        v = obj.get(k)
        # list/tuple → błąd
        if isinstance(v, (list, tuple)):
            bad.append(k); continue
        # ma len() lub shape → błąd (pozwólmy na numpy scalar z shape == ())
        if getattr(v, "shape", None) not in (None, ()):
            bad.append(k); continue
        if hasattr(v, "__len__") and not isinstance(v, (str, bytes)):
            try:
                _ = len(v)
                bad.append(k); continue
            except TypeError:
                pass
        if not isinstance(v, Number):
            bad.append(k); continue
    if bad:
        raise RuntimeError(f"[PREFLIGHT][{stage}] Pola muszą być skalarami: {', '.join(bad)}")


def _preflight_after_ingest(H: Dict[str, Any]) -> None:
    _require_keys("00.after_ingest", H, ["ts_hour", "N", "calc_id", "params_ts"])
    n = int(H["N"])
    if n <= 0:
        raise RuntimeError("[PREFLIGHT][00.after_ingest] N==0 — brak godzin do obliczeń")
    # Minimalny zestaw wektorów czasu
    _require_arrays("00.after_ingest", H, ["ts_hour"], n)
    # Podstawowe parametry ARBI/OZE — zgodnie ze standardem: emax_arbi_mwh to SKALAR
    _require_scalars("00.after_ingest", H, ["emax_arbi_mwh", "eta_ch_frac", "eta_dis_frac", "bess_lambda_h_frac"])
    # Surplus/deficyt (mogą być zerowe, ale niech będą wektorami)
    maybe = [k for k in ("e_surplus_mwh", "e_deficit_mwh", "surplus_net_mwh", "p_load_net_mwh") if k in H]
    if maybe:
        _require_arrays("00.after_ingest", H, maybe, n)
    log.info("[PREFLIGHT] 00.after_ingest OK (N=%d)", n)

def _preflight_before_proposer(H: Dict[str, Any]) -> None:
    """
    Preflight przed Stage 01 (proposer):
    - Wymaga kluczowych skalarów sterujących.
    - Wymaga wektorów długości N dla osi czasu oraz limitów NET z Stage00:
      e_ch_cap_net_arbi_mwh / e_dis_cap_net_arbi_mwh.
    - Wymaga godzinowego wektora date_key (YYYY-MM-DD) do limitów cykli/resetu pending.
    - Wymaga masek bonusowych jako wektorów N (po zmianach w 00).
    """
    n = int(H["N"])

    # skalarne parametry sterujące (+ emax_arbi_mwh jako skalar)
    req_scalar = [
        "eta_ch_frac", "eta_dis_frac",
        "base_min_profit_pln_mwh",
        "soc_low_threshold", "soc_high_threshold",
        "cycles_per_day",
        "emax_arbi_mwh",
    ]
    _require_keys("01.before_proposer", H, req_scalar)
    _require_scalars("01.before_proposer", H, req_scalar)

    # wektory godzinowe (długości N)
    _require_arrays(
        "01.before_proposer",
        H,
        [
            "ts_hour",
            "e_ch_cap_net_arbi_mwh",   # NET ładowanie ARBI
            "e_dis_cap_net_arbi_mwh",  # NET rozładowanie ARBI
            "is_workday",
            "date_key",
        ],
        n,
    )

    # maski bonusowe — teraz wektory N (wcześniej sprawdzaliśmy tylko obecność kluczy)
    _require_arrays(
        "01.before_proposer",
        H,
        ["bonus_hrs_ch", "bonus_hrs_dis", "bonus_hrs_ch_free", "bonus_hrs_dis_free"],
        n,
    )

    log.info("[PREFLIGHT] 01.before_proposer OK")

def _preflight_after_proposer(H: Dict[str, Any], P: Dict[str, Any]) -> None:
    n = int(H["N"])
    keys = ["prop_arbi_ch_from_grid_ac_mwh", "prop_arbi_dis_to_grid_ac_mwh"]
    _require_keys("01.after_proposer", P, keys)
    _require_arrays("01.after_proposer", P, keys, n)
    _require_nonneg("01.after_proposer", P, keys)
    log.info("[PREFLIGHT] 01.after_proposer OK")

def _preflight_before_commit(H: Dict[str, Any], P: Dict[str, Any]) -> None:
    n = int(H["N"])
    # Propozycje 01 (muszą być wektorami N)
    _require_arrays("02.before_commit", P,
                    ["prop_arbi_ch_from_grid_ac_mwh", "prop_arbi_dis_to_grid_ac_mwh"], n)

    # Sprawności muszą istnieć (skalary)
    _require_keys("02.before_commit", H, ["eta_ch_frac", "eta_dis_frac"])

    # Limity AC mogą być:
    #  • brak (wtedy commit przyjmie +inf),
    #  • SKALAR (stały cap dla wszystkich godzin),
    #  • WEKTOR N (cap godzinowy).
    def _ok_cap(key: str) -> bool:
        if key not in H:
            return True  # brak → OK (commit użyje +inf)
        v = H.get(key)
        # skalar?
        if not hasattr(v, "__len__") or isinstance(v, (str, bytes)):
            return True
        # numpy scalar (shape == ()): OK
        if getattr(v, "shape", None) == ():
            return True
        # sekwencja: musi mieć długość N
        return _len_of(v) == n

    bad = [k for k in ("cap_export_ac_mwh", "cap_import_ac_mwh") if not _ok_cap(k)]
    if bad:
        raise RuntimeError(f"[PREFLIGHT][02.before_commit] Pola muszą być skalarem albo długości N={n}: {', '.join(bad)}")

    # Ceny TGE (bez fallbacków — commit poradzi sobie z zerami, ale tu wymagamy obecności i długości N)
    _require_prices("02.before_commit", H, n)

    log.info("[PREFLIGHT] 02.before_commit OK")

def _preflight_after_commit(H: Dict[str, Any], C: Dict[str, Any]) -> None:
    n = int(H["N"])
    req = ["e_import_mwh", "e_export_mwh",
           "charge_from_surplus_mwh", "charge_from_grid_mwh",
           "discharge_to_load_mwh", "discharge_to_grid_mwh",
           "soc_arbi_after_idle_mwh"]
    _require_arrays("02.after_commit", C, req, n)
    _require_nonneg("02.after_commit", C, req)
    # prosta kontrola spójności — eksport to suma dwóch składników, jeśli mamy rozbicia
    if "export_from_arbi_ac_mwh" in C and "export_from_surplus_ac_mwh" in C:
        _require_arrays("02.after_commit", C, ["export_from_arbi_ac_mwh", "export_from_surplus_ac_mwh"], n)
    log.info("[PREFLIGHT] 02.after_commit OK")

def _preflight_before_pricing(H: Dict[str, Any], P_params: Dict[str, Any], C: Dict[str, Any]) -> None:
    # Pricing korzysta z osi czasu i metadanych; przyjmijmy kontrolę bazową
    _require_keys("03.before_pricing", H, ["calc_id", "params_ts", "ts_hour", "N"])
    log.info("[PREFLIGHT] 03.before_pricing OK")

def _preflight_before_persist(H: Dict[str, Any], P: Dict[str, Any], C: Dict[str, Any]) -> None:
    n = int(H["N"])
    # Minimalny zestaw do zapisu 00/01/02
    _require_keys("persist.before", H, ["params_ts", "calc_id"])
    _require_arrays("persist.before", H, ["ts_utc", "ts_local", "ts_hour"], n)
    _require_arrays("persist.before", P, ["prop_arbi_ch_from_grid_ac_mwh",
                                          "prop_arbi_dis_to_grid_ac_mwh"], n)
    _require_arrays("persist.before", C, ["e_import_mwh", "e_export_mwh"], n)
    log.info("[PREFLIGHT] persist.before OK")


# ─────────────────────────────────────────────────────────────────────────────
# RUNNER
async def run_calc(con, *, job_id: str, params_ts):
    """Wykonuje pełny przebieg kalkulacji i zwraca calc_id (UUID str)."""
    calc_id = _uuid.uuid4()
    log.info("[RUNNER] start calc_id=%s", calc_id)

    t_all0 = perf_counter()

    # 00s: snapshot + konsolidacja
    t_s0 = perf_counter()
    await calc_io.dump_params_snapshot(con, calc_id=str(calc_id), params_ts=params_ts)
    await calc_io.build_params_snapshot_consolidated(con, calc_id=str(calc_id), params_ts=params_ts)
    t_s1 = perf_counter()

    # 00: ingress osi + H_buf
    t00 = perf_counter()
    H_buf = await ingest_00.run(con, calc_id=calc_id, params_ts=params_ts)
    # sanity: N musi istnieć i zgadzać się z ts_hour
    if "N" not in H_buf:
        H_buf["N"] = len(H_buf.get("ts_hour", []) or [])
    _preflight_after_ingest(H_buf)
    t01 = perf_counter()

    # 01: proposer (kontrakt run(H, P))
    t10 = perf_counter()
    P_buf: Dict[str, Any] = {}
    _preflight_before_proposer(H_buf)
    P_buf = proposer_01.run(H_buf, P_buf)
    _preflight_after_proposer(H_buf, P_buf)
    t11 = perf_counter()

    # 02: commit (kontrakt run(H, P))
    t20 = perf_counter()
    _preflight_before_commit(H_buf, P_buf)
    C_buf = commit_02.run(H_buf, P_buf)
    _preflight_after_commit(H_buf, C_buf)
    t21 = perf_counter()

    # 03: pricing (szczegóły do stage 04)
    t30p = perf_counter()
    P_params = await calc_io.get_consolidated_norm(con, calc_id)
    _preflight_before_pricing(H_buf, P_params, C_buf)
    pricing_rows = pricing_03.run(H_buf=H_buf, P_buf=P_params, C_buf=C_buf)
    t31p = perf_counter()

    # PERSIST: 00/01/02 → 03 → 05 (w naszym raporcie stage03_bess_revs jest w raport.py)
    t30 = perf_counter()
    _preflight_before_persist(H_buf, P_buf, C_buf)
    await raport.persist(con, H_buf, P_buf, C_buf)
    t31 = perf_counter()

    # PERSIST 04 (pricing detail)
    t40 = perf_counter()
    await raport.persist_stage04_pricing_detail(
        con, params_ts=params_ts, calc_id=calc_id, pricing_rows=pricing_rows, truncate=True
    )
    t41 = perf_counter()

    # VALIDATE
    t_v0 = perf_counter()
    await validate.run(con, params_ts, calc_id)
    t_v1 = perf_counter()

    # DONE marker
    try:
        async with con.cursor() as cur:
            await cur.execute(f"""
                INSERT INTO {calc_io.TABLE_JOB_STAGE} (job_id, calc_id, stage, status, started_at, finished_at)
                VALUES (%s, %s, '05', 'done', now(), now())
                ON CONFLICT (calc_id, stage)
                DO UPDATE SET status='done', finished_at=EXCLUDED.finished_at
            """, (job_id, str(calc_id)))
        await con.commit()
        log.info("[RUNNER] stage '05' oznaczony jako 'done' (job_id=%s, calc_id=%s)", job_id, calc_id)
    except Exception as e:
        log.exception("[RUNNER] Nie udało się oznaczyć stage '05' jako done: %s", e)

    t_all1 = perf_counter()
    log.info(
        "[PERF] snapshot=%.0f ms | 00=%.0f ms | 01=%.0f ms | 02=%.0f ms | pricing=%.0f ms | "
        "persist(00/01/02→05)=%.0f ms | persist04=%.0f ms | validate=%.0f ms | total=%.0f ms",
        (t_s1 - t_s0) * 1000.0,
        (t01 - t00) * 1000.0,
        (t11 - t10) * 1000.0,
        (t21 - t20) * 1000.0,
        (t31p - t30p) * 1000.0,
        (t31 - t30) * 1000.0,
        (t41 - t40) * 1000.0,
        (t_v1 - t_v0) * 1000.0,
        (t_all1 - t_all0) * 1000.0,
    )

    return str(calc_id)
