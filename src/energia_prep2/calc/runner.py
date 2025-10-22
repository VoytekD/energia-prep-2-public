# runner.py — ORKIESTRACJA ETAPÓW 00→06
# =====================================

from __future__ import annotations

import logging
from time import perf_counter
from typing import Any, Dict, Sequence
from types import ModuleType            # NEW (opcjonalnie)
import uuid as _uuid
import importlib                        # NEW (jeśli używasz importlib.import_module)
import importlib.util
import psycopg                          # NEW (dla AsyncConnection w type hint)
import sys
from pathlib import Path

# Logger główny runnnera
log = logging.getLogger("energia-prep-2.calc.runner")

# ─────────────────────────────────────────────────────────────────────────────
# Pomocniczy loader modułów etapów (TYLKO nowe pliki 00..06, bez fallbacków)
# ─────────────────────────────────────────────────────────────────────────────
def _load_stage_module(filename: str, safe_modname: str) -> Any:
    """
    Ładuje moduł etapu **z pliku** po nazwie `filename` (np. '01_ingest.py'),
    rejestrując go pod nazwą modułu `safe_modname` (np. 'energia_prep2.calc.stage01_ingest').
    Brak wstecznych zgodności: jeżeli plik nie istnieje, rzuca ImportError.
    """
    here = Path(__file__).resolve().parent
    file_path = here / filename
    if not file_path.is_file():
        raise ImportError(f"Brak pliku modułu etapu: {file_path}")

    spec = importlib.util.spec_from_file_location(safe_modname, file_path)
    if not spec or not spec.loader:
        raise ImportError(f"Nie można zbudować spec dla modułu z pliku: {file_path}")

    mod = importlib.util.module_from_spec(spec)
    sys.modules[safe_modname] = mod
    spec.loader.exec_module(mod)  # type: ignore[attr-defined]
    return mod

# ─────────────────────────────────────────────────────────────────────────────
# Preflight — pomocnicze walidatory kontraktu danych (dopasowane do 01_ingest)
# ─────────────────────────────────────────────────────────────────────────────
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
            try:
                if any((x is not None and float(x) < -1e-9) for x in v):
                    neg.append(k)
            except Exception:
                # jeżeli nie da się zrzutować, pomiń (to preflight, nie parser)
                pass
        else:
            try:
                if v is not None and float(v) < -1e-9:
                    neg.append(k)
            except Exception:
                pass
    if neg:
        raise RuntimeError(f"[PREFLIGHT][{stage}] Znaleziono wartości ujemne w: {', '.join(neg)}")

def _require_scalars(stage: str, obj: Dict[str, Any], keys: Sequence[str]) -> None:
    """Egzekwuje twarde SKALARY (brak kontenerów/ndarray/Series)."""
    from numbers import Number
    bad = []
    for k in keys:
        if k not in obj:
            bad.append(k); continue
        v = obj.get(k)
        if isinstance(v, (list, tuple)):
            bad.append(k); continue
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

def _require_prices(stage: str, H: Dict[str, Any], n: int) -> None:
    need = ("price_import_pln_mwh", "price_export_pln_mwh")
    _require_keys(stage, H, need)
    _require_arrays(stage, H, need, n)

# ─────────────────────────────────────────────────────────────────────────────
# Preflight: checki przy poszczególnych etapach (zgrane z aktualnym 01_ingest)
# ─────────────────────────────────────────────────────────────────────────────
def _preflight_after_ingest(H: Dict[str, Any]) -> None:
    _require_keys("00.after_ingest", H, ["ts_hour", "N", "calc_id", "params_ts"])
    n = int(H["N"])
    if n <= 0:
        raise RuntimeError("[PREFLIGHT][00.after_ingest] N==0 — brak godzin do obliczeń")
    _require_arrays("00.after_ingest", H, ["ts_hour"], n)
    # Skalary wg nowych nazw w 01_ingest:
    _require_scalars(
        "00.after_ingest",
        H,
        ["emax_arbi_mwh", "eta_ch_frac", "eta_dis_frac", "bess_lambda_h_frac"],
    )
    # Dopuszczalne opcjonalne szeregi (jeżeli występują)
    maybe = [k for k in ("e_surplus_mwh", "e_deficit_mwh", "surplus_net_mwh", "p_load_net_mwh") if k in H]
    if maybe:
        _require_arrays("00.after_ingest", H, maybe, n)
    log.info("[PREFLIGHT] 00.after_ingest OK (N=%d)", n)

def _preflight_before_proposer(H: Dict[str, Any]) -> None:
    n = int(H["N"])
    req_scalar = [
        "base_min_profit_pln_mwh",
        "soc_low_threshold", "soc_high_threshold",
        "cycles_per_day",
        "emax_arbi_mwh",
    ]
    _require_keys("01.before_proposer", H, req_scalar)
    _require_scalars("01.before_proposer", H, req_scalar)

    _require_arrays(
        "01.before_proposer",
        H,
        ["ts_hour", "cap_grid_import_ac_mwh", "cap_grid_export_ac_mwh", "is_work"],
        n,
    )
    # Bonusowe maski nie są obecnie wymagane w H (brak w 01_ingest) — pomijamy.
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
    _require_arrays("02.before_commit", P,
                    ["prop_arbi_ch_from_grid_ac_mwh", "prop_arbi_dis_to_grid_ac_mwh"], n)

    # Ceny (nowe nazwy z 01_ingest)
    _require_prices("02.before_commit", H, n)

    # Opcjonalne capy AC — jeśli są, to skalar lub długość N
    def _ok_cap(key: str) -> bool:
        if key not in H:
            return True
        v = H.get(key)
        if not hasattr(v, "__len__") or isinstance(v, (str, bytes)):
            return True
        if getattr(v, "shape", None) == ():
            return True
        return _len_of(v) == n

    bad = [k for k in ("cap_grid_export_ac_mwh", "cap_grid_import_ac_mwh") if not _ok_cap(k)]
    if bad:
        raise RuntimeError(
            f"[PREFLIGHT][02.before_commit] Pola muszą być skalarem albo długości N={n}: {', '.join(bad)}")

    log.info("[PREFLIGHT] 02.before_commit OK")

def _preflight_after_commit(H: Dict[str, Any], C: Dict[str, Any]) -> None:
    n = int(H["N"])
    req = ["e_import_mwh", "e_export_mwh",
           "charge_from_surplus_mwh", "charge_from_grid_mwh",
           "discharge_to_load_mwh", "discharge_to_grid_mwh",
           "soc_arbi_after_idle_mwh"]
    _require_arrays("02.after_commit", C, req, n)
    _require_nonneg("02.after_commit", C, req)
    if "export_from_arbi_ac_mwh" in C and "export_from_surplus_ac_mwh" in C:
        _require_arrays("02.after_commit", C, ["export_from_arbi_ac_mwh", "export_from_surplus_ac_mwh"], n)
    log.info("[PREFLIGHT] 02.after_commit OK")

def _preflight_before_pricing(H: Dict[str, Any], P_params: Dict[str, Any], C: Dict[str, Any]) -> None:
    _require_keys("03.before_pricing", H, ["calc_id", "params_ts", "ts_hour", "N"])
    log.info("[PREFLIGHT] 03.before_pricing OK")

def _preflight_before_persist(H: Dict[str, Any], P: Dict[str, Any], C: Dict[str, Any]) -> None:
    n = int(H["N"])
    _require_keys("persist.before", H, ["params_ts", "calc_id"])
    _require_arrays("persist.before", H, ["ts_utc", "ts_hour"], n)  # ts_local niewymagany
    _require_arrays("persist.before", P, ["prop_arbi_ch_from_grid_ac_mwh",
                                          "prop_arbi_dis_to_grid_ac_mwh"], n)
    _require_arrays("persist.before", C, ["e_import_mwh", "e_export_mwh"], n)
    log.info("[PREFLIGHT] persist.before OK")

# ─────────────────────────────────────────────────────────────────────────────
# STATUSY ETAPÓW — jedyny właściciel: runner.py
# ─────────────────────────────────────────────────────────────────────────────
TABLE_JOB_STAGE = "output.calc_job_stage"
_ALLOWED_STAGE = ("00","01","02","03","04","05","06")

async def _ensure_stage_table(con) -> None:
    async with con.cursor() as cur:
        await cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_JOB_STAGE} (
          calc_id     uuid NOT NULL,
          stage       text NOT NULL,
          status      text NOT NULL CHECK (status IN ('running','done','failed')),
          started_at  timestamptz,
          finished_at timestamptz,
          error       text,
          PRIMARY KEY (calc_id, stage)
        );
        """)
        await cur.execute(f"""
        DO $$
        BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'calc_job_stage_stage_check'
          ) THEN
            ALTER TABLE {TABLE_JOB_STAGE}
              ADD CONSTRAINT calc_job_stage_stage_check
              CHECK (stage IN ('00','01','02','03','04','05','06'));
          END IF;
        END$$;
        """)
    await con.commit()

async def _stage_start(con, calc_id: str, stage: str) -> None:
    assert stage in _ALLOWED_STAGE
    async with con.cursor() as cur:
        await cur.execute(f"""
        INSERT INTO {TABLE_JOB_STAGE} (calc_id, stage, status, started_at, finished_at, error)
        VALUES (%s, %s, 'running', now(), NULL, NULL)
        ON CONFLICT (calc_id, stage)
        DO UPDATE SET status='running',
                      started_at=COALESCE({TABLE_JOB_STAGE}.started_at, EXCLUDED.started_at),
                      finished_at=NULL,
                      error=NULL
        """, (calc_id, stage))
    await con.commit()

async def _stage_done(con, calc_id: str, stage: str) -> None:
    assert stage in _ALLOWED_STAGE
    async with con.cursor() as cur:
        await cur.execute(f"""
        UPDATE {TABLE_JOB_STAGE}
        SET status='done', finished_at=now()
        WHERE calc_id=%s AND stage=%s
        """, (calc_id, stage))
    await con.commit()

async def _stage_fail(con, calc_id: str, stage: str, error: str | None) -> None:
    assert stage in _ALLOWED_STAGE
    err = (error or "").strip()
    if len(err) > 8000:
        err = err[:8000] + "…"
    async with con.cursor() as cur:
        await cur.execute(f"""
        UPDATE {TABLE_JOB_STAGE}
        SET status='failed', finished_at=now(), error=%s
        WHERE calc_id=%s AND stage=%s
        """, (err, calc_id, stage))
    await con.commit()

# ─────────────────────────────────────────────────────────────────────────────
# RUNNER — główny przebieg obliczeń jednego joba
# ─────────────────────────────────────────────────────────────────────────────
async def run_calc(con: psycopg.AsyncConnection, *, job_id: str, params_ts) -> str:
    """
    Orkiestracja etapów 00→06 dla pojedynczej kalkulacji.
    Uwaga: statusy POSZCZEGÓLNYCH ETAPÓW są ustawiane wyłącznie tutaj.
    """
    # 1) ID kalkulacji (UUID)
    calc_id = _uuid.uuid4()
    calc_id_str = str(calc_id)
    log.info("[RUN] start calc_id=%s job_id=%s params_ts=%s", calc_id_str, job_id, params_ts)

    # 2) Załaduj moduły etapów
    snapshot_mod = _load_stage_module("00_snapshot.py", "energia_prep2.calc.stage00_snapshot")
    ingest_mod = _load_stage_module("01_ingest.py", "energia_prep2.calc.stage01_ingest")
    proposer_mod = _load_stage_module("02_proposer.py", "energia_prep2.calc.stage02_proposer")
    commit_mod = _load_stage_module("03_commit.py", "energia_prep2.calc.stage03_commit")
    pricing_mod = _load_stage_module("04_pricing.py", "energia_prep2.calc.stage04_pricing")
    persist_mod = _load_stage_module("05_persist.py", "energia_prep2.calc.stage05_persist")
    validate_mod = _load_stage_module("06_validate.py", "energia_prep2.calc.stage06_validate")

    # Pomocnicze bufory wyników pomiędzy etapami
    H_buf: Dict[str, Any] = {}
    P_buf: Dict[str, Any] = {}
    C_buf: Dict[str, Any] = {}
    pricing_rows: Sequence[Dict[str, Any]] = ()

    # PERF – całość
    t_all0 = perf_counter()

    # 3) [00] Snapshot + konsolidacja parametrów
    t_s0 = perf_counter()
    try:
        await _stage_start(con, calc_id_str, "00")
        log.info("[STAGE 00] running…")
        await snapshot_mod.dump_params_snapshot(con, calc_id=calc_id_str, params_ts=params_ts)
        await snapshot_mod.build_params_snapshot_consolidated(con, calc_id=calc_id_str, params_ts=params_ts)
        await _stage_done(con, calc_id_str, "00")
        log.info("[STAGE 00] ok (%.0f ms)", (perf_counter() - t_s0) * 1000.0)
    except Exception as e:
        log.exception("[STAGE 00] failed: %s", e)
        await _stage_fail(con, calc_id_str, "00", str(e))
        raise
    t_s1 = perf_counter()

    # 4) [01] Ingest (H)
    t00 = perf_counter()
    try:
        await _stage_start(con, calc_id_str, "01")
        log.info("[STAGE 01] running…")
        H_buf = await ingest_mod.run(con, calc_id=calc_id, params_ts=params_ts)
        if "N" not in H_buf:
            H_buf["N"] = len(H_buf.get("ts_hour", []) or [])
        _preflight_after_ingest(H_buf)
        await _stage_done(con, calc_id_str, "01")
        log.info("[STAGE 01] ok (%.0f ms)", (perf_counter() - t00) * 1000.0)
    except Exception as e:
        log.exception("[STAGE 01] failed: %s", e)
        await _stage_fail(con, calc_id_str, "01", str(e))
        raise
    t01 = perf_counter()

    # 5) [02] Proposer (P)
    t10 = perf_counter()
    try:
        await _stage_start(con, calc_id_str, "02")
        log.info("[STAGE 02] running…")
        _preflight_before_proposer(H_buf)
        P_buf = proposer_mod.run(H_buf, {})
        _preflight_after_proposer(H_buf, P_buf)
        await _stage_done(con, calc_id_str, "02")
        log.info("[STAGE 02] ok (%.0f ms)", (perf_counter() - t10) * 1000.0)
    except Exception as e:
        log.exception("[STAGE 02] failed: %s", e)
        await _stage_fail(con, calc_id_str, "02", str(e))
        raise
    t11 = perf_counter()

    # 6) [03] Commit (C)
    t20 = perf_counter()
    try:
        await _stage_start(con, calc_id_str, "03")
        log.info("[STAGE 03] running…")
        _preflight_before_commit(H_buf, P_buf)
        C_buf = commit_mod.run(H_buf, P_buf)
        _preflight_after_commit(H_buf, C_buf)
        await _stage_done(con, calc_id_str, "03")
        log.info("[STAGE 03] ok (%.0f ms)", (perf_counter() - t20) * 1000.0)
    except Exception as e:
        log.exception("[STAGE 03] failed: %s", e)
        await _stage_fail(con, calc_id_str, "03", str(e))
        raise
    t21 = perf_counter()

    # 7) [04] Pricing (wiersze)
    t30p = perf_counter()
    try:
        await _stage_start(con, calc_id_str, "04")
        log.info("[STAGE 04] running…")
        # Normy pod pricing bierzemy z 00 (konsolidat)
        P_params: Dict[str, Any] = await snapshot_mod.get_consolidated_norm(con, calc_id=calc_id_str)
        _preflight_before_pricing(H_buf, P_params, C_buf)
        pricing_rows = pricing_mod.run(H_buf=H_buf, P_buf=P_params, C_buf=C_buf)
        await _stage_done(con, calc_id_str, "04")
        log.info("[STAGE 04] ok (%.0f ms)", (perf_counter() - t30p) * 1000.0)
    except Exception as e:
        log.exception("[STAGE 04] failed: %s", e)
        await _stage_fail(con, calc_id_str, "04", str(e))
        raise
    t31p = perf_counter()

    # 8) [05] Persist (01..04) — jedno wywołanie
    t30 = perf_counter()
    try:
        await _stage_start(con, calc_id_str, "05")
        log.info("[STAGE 05] running…")
        _preflight_before_persist(H_buf, P_buf, C_buf)
        await persist_mod.persist_all(
            con, H_buf, P_buf, C_buf, pricing_rows, params_ts=params_ts, truncate=True
        )
        await _stage_done(con, calc_id_str, "05")
        log.info("[STAGE 05] ok (%.0f ms)", (perf_counter() - t30) * 1000.0)
    except Exception as e:
        log.exception("[STAGE 05] failed: %s", e)
        await _stage_fail(con, calc_id_str, "05", str(e))
        raise
    t31 = perf_counter()

    # 9) [06] Validate
    t_v0 = perf_counter()
    try:
        await _stage_start(con, calc_id_str, "06")
        log.info("[STAGE 06] running…")
        await validate_mod.run(con, params_ts, calc_id)
        await _stage_done(con, calc_id_str, "06")
        log.info("[STAGE 06] ok (%.0f ms)", (perf_counter() - t_v0) * 1000.0)
    except Exception as e:
        log.exception("[STAGE 06] failed: %s", e)
        await _stage_fail(con, calc_id_str, "06", str(e))
        raise
    t_v1 = perf_counter()

    # 10) PERF – zbiorczo
    t_all1 = perf_counter()
    log.info(
        "[PERF] 00_snapshot=%.0f ms | 01_ingest=%.0f ms | 02_proposer=%.0f ms | 03_commit=%.0f ms | "
        "04_pricing=%.0f ms | 05_persist=%.0f ms | 06_validate=%.0f ms | total=%.0f ms",
        (t_s1 - t_s0) * 1000.0,  # 00_snapshot
        (t01 - t00) * 1000.0,    # 01_ingest
        (t11 - t10) * 1000.0,    # 02_proposer
        (t21 - t20) * 1000.0,    # 03_commit
        (t31p - t30p) * 1000.0,  # 04_pricing
        (t31 - t30) * 1000.0,    # 05_persist
        (t_v1 - t_v0) * 1000.0,  # 06_validate
        (t_all1 - t_all0) * 1000.0,
    )

    log.info("[RUN] done calc_id=%s job_id=%s", calc_id_str, job_id)
    return calc_id_str
