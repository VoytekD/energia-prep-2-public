# -*- coding: utf-8 -*-
"""
calc/io.py — snapshot i konsolidacja parametrów:
- snapshot form params.* → output.params_snapshot_calc
- konsolidacja do params.__consolidated__ (payload.norm)
- struktury statusowe/queue dla listener/runner

Założenia:
- Multiselecty/wielowartościowe pola pozostają LISTAMI.
- Procenty wejściowe normalizowane do ułamków w [0,1] tylko tam, gdzie to sensowne.
- Idempotentne DDL-e (CREATE IF NOT EXISTS).

STANDARD: Skalary vs wektory czasu
- Ten moduł (IO) dostarcza WYŁĄCZNIE skalarów (i list konfiguracyjnych); nie tworzy wektorów N-godzinnych.
- W norm zapisujemy podpowiedzi typów: type_hints.scalars i type_hints.time_vectors (nazwy do użycia w 00/01…).
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Mapping

import psycopg
from psycopg.rows import dict_row

log = logging.getLogger("energia-prep-2.calc.io")

SCHEMA_PARAMS = "params"
SCHEMA_INPUT  = "input"
SCHEMA_OUTPUT = "output"

TBL_SNAPSHOT = f"{SCHEMA_OUTPUT}.params_snapshot_calc"
TBL_CONSOL   = f"{SCHEMA_PARAMS}.__consolidated__"

# ──────────────────────────────────────────────────────────────────────────────
# STANDARD (tylko deklaracje — IO nie tworzy wektorów czasu, ale nadaje kontrakt)
# ──────────────────────────────────────────────────────────────────────────────

# Skalary: MUSZĄ być liczbami (Number) — żadnych list, krotek, ndarray, Series.
IO_SCALAR_KEYS = [
    # BESS / arbitraż — decyzja „na sztywno” (skalary)
    "eta_ch_frac",
    "eta_dis_frac",
    "base_min_profit_pln_mwh",
    "soc_low_threshold",
    "soc_high_threshold",
    "cycles_per_day",

    # BESS / pojemności i moce — również skalary w norm
    "emax_total_mwh",
    "emax_oze_mwh",
    "emax_arbi_mwh",
    "frac_oze",
    "p_ch_max_mw",
    "p_dis_max_mw",
    "bess_c_rate_charge",          # informacyjne
    "bess_c_rate_discharge",       # informacyjne
    "bess_time_to_full_charge_h",
    "bess_time_to_full_discharge_h",
    "e_ch_cap_net_oze_mwh",        # cap/h jako stała (dla dt=1h)
    "e_dis_cap_net_oze_mwh",
    "e_ch_cap_net_arbi_mwh",
    "e_dis_cap_net_arbi_mwh",
    "soc_oze_start_mwh",
    "soc_arbi_start_mwh",
    "bess_lambda_h_frac",

    # Kontrakt/klient
    "moc_umowna_mw",
    "vat_udzial",
    "lcoe.lcoe_pv_pp",
    "lcoe.lcoe_pv_wz",
    "lcoe.lcoe_wiatr",
    "handlowa.oplata_handlowa_mies_pln",
    "oplaty_systemowe.stawka_oze_pln_mwh",
    "oplaty_systemowe.stawka_kog_pln_mwh",
    "oplaty_systemowe.stawka_mocowa_pln_mwh",
    "oplaty_fiskalne.akcyza_pln_mwh",

    # Zmienne (fac) — mnożniki
    "fac.load",
    "fac.pv_pp",
    "fac.pv_wz",
    "fac.wiatr",
]

# Nazwy wektorów czasu (N=8760/8784) — IO ich nie produkuje; to kontrakt dla 00/01…
IO_TIME_VECTOR_KEYS = [
    "ts_utc", "ts_local", "ts_hour", "date_key",
    "p_load_mw", "p_gen_total_mw", "p_pv_pp_mw", "p_pv_wz_mw", "p_wiatr_mw",
    "p_ch_cap_mw", "p_dis_cap_mw",
    "e_ch_cap_net_arbi_mwh_vec", "e_dis_cap_net_arbi_mwh_vec",
    "e_ch_cap_net_oze_mwh_vec",  "e_dis_cap_net_oze_mwh_vec",
]

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _as_num(v: Any, default=None):
    try:
        return float(v)
    except Exception:
        return default

def _req_num(name: str, v: Any) -> float:
    x = _as_num(v, None)
    if x is None:
        raise RuntimeError(f"[IO] Pole numeryczne '{name}' jest wymagane.")
    return float(x)

def _opt_num(name: str, v: Any, default: float) -> float:
    x = _as_num(v, None)
    if x is None:
        return float(default)
    return float(x)

def _req_str(name: str, v: Any) -> str:
    if v is None:
        raise RuntimeError(f"[IO] Pole tekstowe '{name}' jest wymagane.")
    s = str(v).strip()
    if not s:
        raise RuntimeError(f"[IO] Pole tekstowe '{name}' jest puste.")
    return s

def _normalize_tariff(v: Any) -> str:
    # BEZ aliasów/wstecznej zgodności: tylko uppercase + trim cudzysłowów.
    s = _req_str("wybrana_taryfa", v).upper().replace('"', '')
    return s

def _assert_norm_scalars(norm: Mapping[str, Any]) -> None:
    """
    Twarde sprawdzenie: wszystkie klucze zadeklarowane jako skalar
    (IO_SCALAR_KEYS) MUSZĄ być liczbami. Obsługuje kropkowane ścieżki (a.b.c).
    """
    from numbers import Number

    def get_path(d: Mapping[str, Any], path: str):
        node: Any = d
        for part in path.split("."):
            if not isinstance(node, Mapping) or part not in node:
                raise RuntimeError(f"[IO] Brak klucza skalarnego '{path}' w norm.")
            node = node[part]
        return node

    for key in IO_SCALAR_KEYS:
        v = get_path(norm, key)
        # Nie dopuszczamy list/tuple/ndarray/Series itd.
        if isinstance(v, (list, tuple)):
            raise RuntimeError(f"[IO] '{key}' ma być SKALAREM, otrzymano list/tuple.")
        if getattr(v, "shape", None) is not None:
            raise RuntimeError(f"[IO] '{key}' ma być SKALAREM, otrzymano obiekt z 'shape'.")
        if hasattr(v, "__len__") and not isinstance(v, (str, bytes)):
            # prawdziwy skalar nie ma sensownego len()
            try:
                _ = len(v)
                raise RuntimeError(f"[IO] '{key}' ma być SKALAREM, otrzymano kontener len={len(v)}.")
            except TypeError:
                pass
        if not isinstance(v, Number):
            raise RuntimeError(f"[IO] '{key}' ma być liczbą (Number), typ={type(v)}.")

# ──────────────────────────────────────────────────────────────────────────────
# Status tables / job queue required by listener & runner
# ──────────────────────────────────────────────────────────────────────────────

async def _list_param_tables(con: psycopg.AsyncConnection) -> List[str]:
    """
    Lista WSZYSTKICH tabel w schemacie 'params' (BASE TABLE),
    z wyłączeniem tabeli konsolidacyjnej '__consolidated__'.
    """
    q = """
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema=%s
        AND table_type='BASE TABLE'
        AND table_name <> '__consolidated__'
      ORDER BY table_name;
    """
    async with con.cursor() as cur:
        await cur.execute(q, (SCHEMA_PARAMS,))
        rows = await cur.fetchall()
    return [r[0] if isinstance(r, tuple) else r["table_name"] for r in rows]

TABLE_JOBS_LOCAL = f"{SCHEMA_OUTPUT}.calc_job_queue"
TABLE_JOB_STAGE  = f"{SCHEMA_OUTPUT}.calc_job_stage"

async def ensure_calc_job_queue(con: psycopg.AsyncConnection) -> None:
    """Idempotentne utworzenie tabeli kolejki zadań."""
    async with con.cursor() as cur:
        await cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA_OUTPUT};

        CREATE TABLE IF NOT EXISTS {TABLE_JOBS_LOCAL} (
          job_id      uuid PRIMARY KEY,
          params_ts   timestamptz NOT NULL,
          status      text NOT NULL CHECK (status IN ('queued','running','done','error','skipped')),
          created_at  timestamptz NOT NULL DEFAULT now(),
          started_at  timestamptz,
          finished_at timestamptz,
          error       text
        );
        """)
        await cur.execute(f"""
        CREATE INDEX IF NOT EXISTS ix_calc_job_queue_status_created
          ON {TABLE_JOBS_LOCAL}(status, created_at DESC);
        """)
    await con.commit()

async def ensure_calc_job_stage(con: psycopg.AsyncConnection) -> None:
    """Idempotentne utworzenie tabeli statusów etapów kalkulacji."""
    async with con.cursor() as cur:
        await cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA_OUTPUT};

        CREATE TABLE IF NOT EXISTS {TABLE_JOB_STAGE} (
          job_id      uuid NOT NULL,
          calc_id     uuid NOT NULL,
          stage       text NOT NULL,   -- '00' | '01' | '02' | '03' | '04' | '05'
          status      text NOT NULL CHECK (status IN ('pending','running','done','error')),
          started_at  timestamptz NOT NULL DEFAULT now(),
          finished_at timestamptz,
          PRIMARY KEY (calc_id, stage)
        );
        """)
        await cur.execute(f"""
        CREATE INDEX IF NOT EXISTS ix_calc_job_stage_job_id
          ON {TABLE_JOB_STAGE}(job_id);
        """)

        # aktualny CHECK na 'stage' (dopuszcza '05')
        await cur.execute("""
        DO $$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM   information_schema.table_constraints
            WHERE  table_schema = 'output'
               AND table_name   = 'calc_job_stage'
               AND constraint_name = 'calc_job_stage_stage_check'
          ) THEN
            BEGIN
              ALTER TABLE output.calc_job_stage
                DROP CONSTRAINT calc_job_stage_stage_check;
            EXCEPTION WHEN undefined_object THEN
              NULL;
            END;
          END IF;

          ALTER TABLE output.calc_job_stage
            ADD CONSTRAINT calc_job_stage_stage_check
            CHECK (stage IN ('00','01','02','03','04','05'));
        END
        $$;
        """)
    await con.commit()


async def ensure_calc_status_structures(con: psycopg.AsyncConnection) -> None:
    """Tworzy obie struktury statusowe wymagane przez listener/runner."""
    await ensure_calc_job_queue(con)
    await ensure_calc_job_stage(con)

# ──────────────────────────────────────────────────────────────────────────────
# Snapshot: params.* → output.params_snapshot_calc
# ──────────────────────────────────────────────────────────────────────────────

async def ensure_params_snapshot_table(con: psycopg.AsyncConnection) -> None:
    """Idempotentne utworzenie tabeli snapshotów parametrów."""
    async with con.cursor() as cur:
        await cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA_OUTPUT};

        CREATE TABLE IF NOT EXISTS {TBL_SNAPSHOT}(
          calc_id      uuid        NOT NULL,
          params_ts    timestamptz NOT NULL,
          source_table text        NOT NULL,
          row          jsonb       NOT NULL,
          created_at   timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (calc_id, source_table)
        );
        """)
        await cur.execute(f"""
        CREATE INDEX IF NOT EXISTS ix_params_snapshot_calc_calc_created
          ON {TBL_SNAPSHOT}(calc_id, created_at DESC);
        """)
    await con.commit()

async def _detect_order_by(con: psycopg.AsyncConnection, schema: str, table: str) -> str:
    """
    Ustala bezpieczne ORDER BY dla "weź ostatni rekord":
      1) jeśli jest updated_at → 'updated_at DESC NULLS LAST'
      2) elif jest created_at → 'created_at DESC NULLS LAST'
      3) elif pojedynczy PK → '<pk> DESC'
      4) else → pierwsza kolumna tabeli DESC
    """
    con.row_factory = dict_row
    # dostępne kolumny
    async with con.cursor() as cur:
        await cur.execute("""
          SELECT column_name
          FROM information_schema.columns
          WHERE table_schema=%s AND table_name=%s
          ORDER BY ordinal_position
        """, (schema, table))
        cols = [r["column_name"] for r in await cur.fetchall()]

    lower = [c.lower() for c in cols]
    if "updated_at" in lower:
        return "updated_at DESC NULLS LAST"
    if "created_at" in lower:
        return "created_at DESC NULLS LAST"

    # spróbuj jednego PK
    regclass = f"{schema}.{table}"
    async with con.cursor() as cur:
        await cur.execute("""
          SELECT a.attname AS col
          FROM pg_index i
          JOIN pg_attribute a
            ON a.attrelid = i.indrelid
           AND a.attnum = ANY(i.indkey)
          WHERE i.indrelid = %s::regclass
            AND i.indisprimary
        """, (regclass,))
        pkcols = [r["col"] for r in await cur.fetchall()]
    if len(pkcols) == 1:
        return f"{pkcols[0]} DESC"

    # fallback: pierwsza kolumna (deterministycznie)
    return f"{cols[0]} DESC" if cols else "1"

async def dump_params_snapshot(con: psycopg.AsyncConnection, *, calc_id, params_ts) -> None:
    """Zrzuca ostatnie rekordy z KAŻDEJ tabeli params.* do output.params_snapshot_calc (po 1 na tabelę)."""
    await ensure_params_snapshot_table(con)
    con.row_factory = dict_row

    tables = await _list_param_tables(con)
    if not tables:
        raise RuntimeError("Brak tabel w schemacie 'params' — nie można zbudować snapshotu.")

    # czyścimy poprzedni snapshot dla calc_id
    async with con.cursor() as cur:
        await cur.execute(f"DELETE FROM {TBL_SNAPSHOT} WHERE calc_id=%s", (str(calc_id),))

    inserted = 0
    async with con.cursor() as cur:
        for t in tables:
            order_by = await _detect_order_by(con, SCHEMA_PARAMS, t)
            q = f"""
                SELECT row_to_json(x) AS j
                FROM (SELECT * FROM {SCHEMA_PARAMS}.{t} ORDER BY {order_by} LIMIT 1) x
            """
            await cur.execute(q)
            row = await cur.fetchone()
            if not row or row.get("j") is None:
                log.warning("[IO] Snapshot: tabela %s.%s pusta — pomijam.", SCHEMA_PARAMS, t)
                continue
            await cur.execute(
                f"INSERT INTO {TBL_SNAPSHOT}(calc_id, params_ts, source_table, row) VALUES (%s,%s,%s,%s)",
                (str(calc_id), params_ts, f"{SCHEMA_PARAMS}.{t}", json.dumps(row["j"])),
            )
            inserted += 1

    await con.commit()
    log.info("[IO] Snapshot zapisany: %d rekordów.", inserted)

# ──────────────────────────────────────────────────────────────────────────────
# Konsolidacja do params.__consolidated__
# ──────────────────────────────────────────────────────────────────────────────

async def ensure_params_consolidated_table(con: psycopg.AsyncConnection) -> None:
    """Idempotentne utworzenie tabeli params.__consolidated__ wymaganej do UPSERT-u."""
    async with con.cursor() as cur:
        await cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA_PARAMS};

        CREATE TABLE IF NOT EXISTS {TBL_CONSOL}(
          calc_id uuid PRIMARY KEY,
          payload jsonb NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now()
        );
        """)
    await con.commit()

async def _get_hours_in_month_local(con: psycopg.AsyncConnection, local_tz: str) -> Dict[str, int]:
    """
    Liczba godzin w miesiącu wg CZASU LOKALNEGO:
    wyznaczana z input.date_dim.ts_utc przeliczonym do :local_tz.
    """
    q = f"""
    SELECT to_char((ts_utc AT TIME ZONE %s), 'YYYY-MM') AS ym,
           COUNT(*)::int AS n
    FROM {SCHEMA_INPUT}.date_dim
    GROUP BY 1
    """
    con.row_factory = dict_row
    async with con.cursor() as cur:
        await cur.execute(q, (local_tz,))
        rows = await cur.fetchall()
    return {r["ym"]: r["n"] for r in rows}

async def build_params_snapshot_consolidated(con: psycopg.AsyncConnection, *, calc_id: str, params_ts=None) -> None:
    """
    Składa 'norm' — ujednolicone dane dla kalkulacji.
    Multiselecty zostają listami; przeliczenia procentów → udziały (0–1).
    """
    con.row_factory = dict_row

    # pobierz snapshoty FORMULARZY (konsolidat opiera się na form_*)
    async with con.cursor() as cur:
        await cur.execute(
            f"""
            SELECT source_table, row
            FROM {TBL_SNAPSHOT}
            WHERE calc_id=%s AND source_table LIKE %s
            """,
            (str(calc_id), "params.form_%"),
        )
        snap_rows = await cur.fetchall()

    forms: Dict[str, Mapping[str, Any]] = {}
    for r in snap_rows:
        st = r["source_table"].split(".", 1)[1]
        row: Mapping[str, Any] = r["row"] or {}
        payload = row.get("payload") if isinstance(row, dict) else None
        # MERGE: payload + kolumny (payload nadpisuje kolumny o tej samej nazwie)
        merged: Dict[str, Any] = dict(row)
        if isinstance(payload, dict):
            merged.update(payload)
        merged.pop("payload", None)
        forms[st] = merged

    def F(name: str) -> Mapping[str, Any]:
        return forms.get(name, {}) or {}

    # wybrane formularze
    zm         = F("form_zmienne")
    klient     = F("form_parametry_klienta")
    kon        = F("form_par_kontraktu")
    dystr      = F("form_oplaty_dystrybucyjne")
    sys_o      = F("form_oplaty_systemowe")
    fisk       = F("form_oplaty_fiskalne")
    arbi       = F("form_par_arbitrazu")
    lcoe_row   = F("form_lcoe")
    bess       = F("form_bess_param")           # parametry magazynu (BESS)
    kparam     = F("form_oplaty_sys_kparam")
    dyst_sched = F("form_oplaty_dyst_sched")
    moc_sched  = F("form_oplaty_sys_sched")

    # klient / kontrakt
    local_tz       = _req_str("local_tz", klient.get("local_tz"))
    moc_umowna_mw  = _req_num("klient_moc_umowna", klient.get("klient_moc_umowna"))

    # kalendarz (po wyznaczeniu local_tz)
    hours_in_month_local = await _get_hours_in_month_local(con, local_tz)

    # ─── ZMIENNE (fac*) — z formularza 'form_zmienne' ────────────────────────
    # fac.load z '%': zmiany_konsumpcji → mnożnik (1 + %/100)
    delta_kons_pct = _req_num("zmiany_konsumpcji", zm.get("zmiany_konsumpcji"))
    fac = {
        "load": 1.0 + (delta_kons_pct / 100.0),
        "pv_pp": _opt_num("moc_pv_pp", zm.get("moc_pv_pp"), 1.0),
        "pv_wz": _opt_num("moc_pv_wz", zm.get("moc_pv_wz"), 1.0),
        "wiatr": _opt_num("moc_wiatr", zm.get("moc_wiatr"), 1.0),
    }

    taryfa_wybrana = _normalize_tariff(kon.get("wybrana_taryfa"))
    typ_kontraktu  = _req_str("typ_kontraktu", kon.get("typ_kontraktu")).lower()

    par_kontraktu = {
        "wybrana_taryfa": taryfa_wybrana,     # B21/B22/B23 — bez aliasów
        "typ_kontraktu":  typ_kontraktu,      # 'stala'/'zmienna'
        "cena_energii_stała": _req_num("cena_energii_stała", kon.get("cena_energii_stała")),
        "proc_zmiana_ceny":   _opt_num("proc_zmiana_ceny", kon.get("proc_zmiana_ceny"), 0.0) / 100.0,
        "opłata_handlowa_stala":       _opt_num("opłata_handlowa_stala", kon.get("opłata_handlowa_stala"), 0.0),
        "opłata_handlowa_marza_stala": _opt_num("opłata_handlowa_marza_stala", kon.get("opłata_handlowa_marza_stala"), 0.0),
        "oplata_handlowa_marza_zmienna": _opt_num("oplata_handlowa_marza_zmienna", kon.get("oplata_handlowa_marza_zmienna"), 0.0) / 100.0,
    }

    oplaty_systemowe = {
        "stawka_oze_pln_mwh":      _opt_num("stawka_oze",           sys_o.get("stawka_oze"), 0.0),
        "stawka_kog_pln_mwh":      _opt_num("stawka_kogeneracyjna", sys_o.get("stawka_kogeneracyjna"), 0.0),
        "stawka_mocowa_pln_mwh":   _opt_num("stawka_mocowa",        sys_o.get("stawka_mocowa"), 0.0),
    }
    oplaty_fiskalne = {
        "akcyza_pln_mwh": _opt_num("akcyza", fisk.get("akcyza"), 0.0),
        "vat_udzial":     _opt_num("vat",    fisk.get("vat"),    0.0) / 100.0,
    }

    # dystrybucja (1:1 nazwy z formularza)
    oplaty_dystr = dict(dystr)

    # handlowa – miesięczna (na godzinę rozbije pricing_03)
    handlowa = {
        "oplata_handlowa_mies_pln": par_kontraktu["opłata_handlowa_stala"]
    }

    # arbitraż / bonusy — LISTY bez zmian + wymagane skalary
    par_arbitrazu = {
        "soc_high_threshold": _req_num("soc_high_threshold", arbi.get("soc_high_threshold")),
        "soc_low_threshold":  _req_num("soc_low_threshold",  arbi.get("soc_low_threshold")),
        "bonus_ch_window":    _req_num("bonus_ch_window",    arbi.get("bonus_ch_window")),
        "bonus_dis_window":   _req_num("bonus_dis_window",   arbi.get("bonus_dis_window")),

        "bonus_hrs_ch":        arbi.get("bonus_hrs_ch"),
        "bonus_hrs_dis":       arbi.get("bonus_hrs_dis"),
        "bonus_hrs_ch_free":   arbi.get("bonus_hrs_ch_free"),
        "bonus_hrs_dis_free":  arbi.get("bonus_hrs_dis_free"),

        "base_min_profit_pln_mwh": _req_num("base_min_profit_pln_mwh", arbi.get("base_min_profit_pln_mwh")),
        "cycles_per_day":          _req_num("cycles_per_day",          arbi.get("cycles_per_day")),
        "bonus_low_soc_ch":        _req_num("bonus_low_soc_ch",        arbi.get("bonus_low_soc_ch")),
        "bonus_high_soc_dis":      _req_num("bonus_high_soc_dis",      arbi.get("bonus_high_soc_dis")),
        "arbi_dis_to_load":        bool(arbi.get("arbi_dis_to_load", False)),
        "force_order":             bool(arbi.get("force_order", False)),
        "allow_carry_over":        bool(arbi.get("allow_carry_over", False)),
    }

    # ─── BESS — pojemność z form_zmienne.emax + udział z procent_arbitrazu ───
    if not bess:
        raise RuntimeError("[IO] Brak 'form_bess_param' w snapshot — wymagane do kalkulacji.")

    em_total = _req_num("emax (form_zmienne)", zm.get("emax"))  # [MWh]
    frac_arbi = _req_num("procent_arbitrazu", zm.get("procent_arbitrazu")) / 100.0
    if not (0.0 <= frac_arbi <= 1.0):
        raise RuntimeError("[IO] 'procent_arbitrazu' poza zakresem 0–100.")

    emax_arbi_mwh = em_total * frac_arbi
    emax_oze_mwh  = em_total - emax_arbi_mwh

    eta_ch  = _req_num("bess_charge_eff",      bess.get("bess_charge_eff"))
    eta_dis = _req_num("bess_discharge_eff",   bess.get("bess_discharge_eff"))

    # UWAGA: te dwa pola mają mylące nazwy ('c_rate'), ale przechowują CZAS [h] do pełna
    t_full_ch_h  = _req_num("bess_c_rate_charge (godziny)",    bess.get("bess_c_rate_charge"))
    t_full_dis_h = _req_num("bess_c_rate_discharge (godziny)", bess.get("bess_c_rate_discharge"))
    if t_full_ch_h <= 0 or t_full_dis_h <= 0:
        raise RuntimeError("[IO] Czas do pełna (charge/discharge) musi być > 0 h")

    # C-rate wyłącznie informacyjnie (nie używany do obliczeń)
    c_rate_ch  = 1.0 / t_full_ch_h
    c_rate_dis = 1.0 / t_full_dis_h

    # Maksymalna moc (stała) z czasu do pełna i emax
    p_ch_max_mw  = em_total / t_full_ch_h
    p_dis_max_mw = em_total / t_full_dis_h

    soc_start_pct = _req_num("bess_soc_start", bess.get("bess_soc_start")) / 100.0
    soc_total_start_mwh = em_total * soc_start_pct
    soc_arbi_start_mwh = soc_total_start_mwh * frac_arbi
    soc_oze_start_mwh  = soc_total_start_mwh - soc_arbi_start_mwh
    bess_lambda_month = _opt_num("bess_lambda_month", bess.get("bess_lambda_month"), 0.0)
    bess_lambda_h_frac = bess_lambda_month / 720.0

    # Capy NET/h (dla dt_h=1h) — slotowe przeskalowanie po dt_h robimy w 00
    e_ch_cap_net_mwh  = p_ch_max_mw  * eta_ch
    e_dis_cap_net_mwh = p_dis_max_mw * eta_dis

    bess_block = {
        "emax_total_mwh": em_total,
        "frac_oze": 1.0 - frac_arbi,
        "emax_oze_mwh":  emax_oze_mwh,
        "emax_arbi_mwh": emax_arbi_mwh,
        "eta_ch_frac":  eta_ch,
        "eta_dis_frac": eta_dis,

        # Moc maksymalna (stała)
        "p_ch_max_mw":  p_ch_max_mw,
        "p_dis_max_mw": p_dis_max_mw,

        # Informacyjnie C-rate (dla audytu; nie używane do obliczeń)
        "bess_c_rate_charge":    c_rate_ch,
        "bess_c_rate_discharge": c_rate_dis,

        # Czas do pełna (w polach o mylących nazwach)
        "bess_time_to_full_charge_h":    t_full_ch_h,
        "bess_time_to_full_discharge_h": t_full_dis_h,

        # Capy NET/h — przekazane 1:1 do norm (dla slotów 1h)
        "e_ch_cap_net_oze_mwh":   e_ch_cap_net_mwh,
        "e_dis_cap_net_oze_mwh":  e_dis_cap_net_mwh,
        "e_ch_cap_net_arbi_mwh":  e_ch_cap_net_mwh,
        "e_dis_cap_net_arbi_mwh": e_dis_cap_net_mwh,

        "soc_oze_start_mwh":  soc_oze_start_mwh,
        "soc_arbi_start_mwh": soc_arbi_start_mwh,
        "bess_lambda_h_frac": bess_lambda_h_frac
    }

    # ─── KPARAM (opłata mocowa – progi/k1_*, udziały, itp.) — PASS-THROUGH ───
    oplaty_sys_kparam = dict(kparam)  # 1:1 pass-through

    norm = {
        "local_tz": local_tz,
        "hours_in_month_local": hours_in_month_local,
        "fac": fac,
        "klient": dict(klient),
        "moc_umowna_mw": float(moc_umowna_mw),

        "par_kontraktu": par_kontraktu,
        "oplaty_systemowe": oplaty_systemowe,
        "oplaty_fiskalne":  oplaty_fiskalne,

        # top-level VAT w [0,1]
        "vat_udzial": oplaty_fiskalne["vat_udzial"] if "vat_udzial" in oplaty_fiskalne else 0.0,

        "oplaty_dystr":     oplaty_dystr,
        "handlowa": handlowa,

        # Harmonogramy (pass-through)
        "dyst_sched":   dyst_sched,
        "moc_sched":    moc_sched,

        # Parametry opłaty mocowej (progi k1 itp.) — pass-through
        "oplaty_sys_kparam": oplaty_sys_kparam,

        # Arbitraż
        **par_arbitrazu,

        # BESS
        **bess_block,

        # LCOE (pass-through)
        "lcoe": {
            "lcoe_pv_pp": _opt_num("lcoe_pv_pp", lcoe_row.get("lcoe_pv_pp"), 0.0),
            "lcoe_pv_wz": _opt_num("lcoe_pv_wz", lcoe_row.get("lcoe_pv_wz"), 0.0),
            "lcoe_wiatr": _opt_num("lcoe_wiatr", lcoe_row.get("lcoe_wiatr"), 0.0),
        },

        # Podpowiedzi typów (kontrakt dla 00/01…)
        "type_hints": {
            "scalars": IO_SCALAR_KEYS,
            "time_vectors": IO_TIME_VECTOR_KEYS,
        },
    }

    # Twarde egzekwowanie: wszystkie z IO_SCALAR_KEYS muszą być liczbami
    _assert_norm_scalars(norm)

    payload = {"norm": norm, "raw": forms}

    # upewnij się, że istnieje tabela konsolidacyjna
    await ensure_params_consolidated_table(con)

    # UPSERT do __consolidated__
    async with con.cursor() as cur:
        await cur.execute(
            f"""
            INSERT INTO {TBL_CONSOL}(calc_id, payload)
            VALUES (%s, %s)
            ON CONFLICT (calc_id) DO UPDATE SET payload = EXCLUDED.payload
            """,
            (str(calc_id), json.dumps(payload)),
        )

    # Zapisz też konsolidat do snapshotu (widoczne w CSV ze snapshotem)
    await ensure_params_snapshot_table(con)
    async with con.cursor() as cur:
        await cur.execute(
            f"""
            INSERT INTO {TBL_SNAPSHOT}(calc_id, params_ts, source_table, row)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (calc_id, source_table)
            DO UPDATE SET row = EXCLUDED.row, params_ts = EXCLUDED.params_ts
            """,
            (
                str(calc_id),
                params_ts,
                f"{SCHEMA_PARAMS}.__consolidated__",
                json.dumps({"payload": payload}),
            ),
        )

    await con.commit()
    log.info("[IO] Zapisano params.__consolidated__ (norm) oraz snapshot wiersz dla __consolidated__.")

# ──────────────────────────────────────────────────────────────────────────────
# Dostęp / asercje do konsolidatu
# ──────────────────────────────────────────────────────────────────────────────

async def assert_consolidated_norm_ready(con: psycopg.AsyncConnection, *, calc_id: str) -> None:
    """
    Sprawdza, czy istnieje wiersz w params.__consolidated__ dla calc_id
    i czy payload zawiera klucz 'norm'. W przeciwnym razie podnosi RuntimeError.
    """
    await ensure_params_consolidated_table(con)
    con.row_factory = dict_row
    q = f"SELECT 1 FROM {TBL_CONSOL} WHERE calc_id=%s AND (payload ? 'norm') LIMIT 1"
    async with con.cursor() as cur:
        await cur.execute(q, (str(calc_id),))
        row = await cur.fetchone()
    if not row:
        raise RuntimeError("[IO] Konsolidat niegotowy: brak params.__consolidated__ z kluczem 'norm' dla calc_id.")

# getter używany przez runner/pricing_03
async def get_consolidated_norm(con: psycopg.AsyncConnection, calc_id):
    con.row_factory = dict_row
    async with con.cursor() as cur:
        await cur.execute(f"SELECT payload FROM {TBL_CONSOL} WHERE calc_id=%s", (str(calc_id),))
        r = await cur.fetchone()
    if not r:
        raise RuntimeError("[IO] Brak params.__consolidated__ dla calc_id.")
    payload = r["payload"]
    if not isinstance(payload, Mapping) or "norm" not in payload:
        raise RuntimeError("[IO] params.__consolidated__: payload bez 'norm'.")
    return dict(payload["norm"])
