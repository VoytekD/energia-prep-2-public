# -*- coding: utf-8 -*-
"""
energia_prep2.calc.listener — KOORDYNATOR KOLEJKI / LISTENER
============================================================

Rola:
- Nasłuch kanału LISTEN/NOTIFY (np. 'ch_energy_rebuild').
- Koalestracja wielu powiadomień do jednego uruchomienia (debounce).
- Okresowe sondowanie kolejki (poll/tick).
- Odbiór jobów z kolejki i uruchamianie pipeline’u (runner.run_calc).
- Weryfikacja PREP (czy dane wejściowe i formularze są gotowe).

ENV (wyłącznie to wdrożono):
- CALC_POLL_SEC           → co ile sekund działa pętla poll (default 3.0)
- PERIODIC_TICK_SEC       → maks. odstęp między przeliczeniami „z zegara” (default 60.0; 0=wyłącz)
- DEBOUNCE_SECONDS        → opóźnienie po NOTIFY przed uruchomieniem drenu (default 2.0)
- CALC_RUN_ON_STARTUP     → czy na starcie wrzuć najnowszy params_ts do kolejki (default false)
- PREP_WAIT_SEC           → interwał sprawdzania gotowości PREP (default 10)
- PREP_HEARTBEAT_SEC      → jak często wypisywać heartbeat podczas oczekiwania (default max(15, 3*PREP_WAIT_SEC))
"""

from __future__ import annotations

import os
import time
import asyncio
import signal
from typing import Optional
import uuid

import psycopg
from psycopg.rows import dict_row
from psycopg import sql as _sql

from . import (
    log,                     # wspólny Logger skonfigurowany w pakiecie
    build_pg_dsn,            # składanie DSN z ENV (DB_*)
    create_async_pool,       # fabryka puli połączeń (psycopg_pool.AsyncConnectionPool)
    ensure_db_objects,       # tworzenie wymaganych obiektów DB
    CHANNEL_REBUILD,         # nazwa kanału LISTEN/NOTIFY (np. 'ch_energy_rebuild')
    parse_notify_payload,    # dekoder payloadu NOTIFY (json/tekst)
    TABLE_JOBS,              # nazwa tabeli kolejki: "output.calc_job_queue"
)
from .runner import run_calc  # uruchomienie właściwego pipeline’u

# ─────────────────────────────────────────────────────────────────────────────
# Zapytania SQL do obsługi kolejki
# ─────────────────────────────────────────────────────────────────────────────

SQL_PICK_COALESCE = f"""
WITH picked AS (
  SELECT job_id
  FROM {TABLE_JOBS}
  WHERE status = 'queued'
  ORDER BY created_at DESC
  FOR UPDATE SKIP LOCKED
  LIMIT 1
),
skipped AS (
  UPDATE {TABLE_JOBS} q
     SET status = 'skipped',
         finished_at = now()
  WHERE q.status = 'queued'
    AND q.job_id <> (SELECT job_id FROM picked)
)
UPDATE {TABLE_JOBS} j
   SET status = 'running',
       started_at = now(),
       error = NULL
FROM picked p
WHERE j.job_id = p.job_id
RETURNING j.job_id, j.params_ts;
"""

SQL_MARK_DONE = f"""
UPDATE {TABLE_JOBS}
SET status = 'done',
    finished_at = now(),
    error = NULL
WHERE job_id = %s;
"""

SQL_MARK_ERROR = f"""
UPDATE {TABLE_JOBS}
SET status = 'failed',
    finished_at = now(),
    error = %s
WHERE job_id = %s;
"""

SQL_EXISTS_FOR_PARAMS = f"""
SELECT 1
FROM {TABLE_JOBS}
WHERE params_ts = %s
LIMIT 1;
"""

SQL_INSERT_QUEUED = f"""
INSERT INTO {TABLE_JOBS} (job_id, params_ts, status, created_at)
VALUES (%s, %s, 'queued', now())
ON CONFLICT DO NOTHING;
"""

# ─────────────────────────────────────────────────────────────────────────────
# PREP — diagnostyka i oczekiwanie
# ─────────────────────────────────────────────────────────────────────────────

# Lista formularzy wymaganych do startu — komplet 12 (istnienie + ≥1 wiersz)
REQUIRED_FORMS = [
    "params.form_zmienne",
    "params.form_bess_param",
    "params.form_parametry_klienta",
    "params.form_par_arbitrazu",
    "params.form_lcoe",
    "params.form_oplaty_dyst_sched",
    "params.form_oplaty_dystrybucyjne",
    "params.form_oplaty_fiskalne",
    "params.form_oplaty_sys_sched",
    "params.form_oplaty_systemowe",
    "params.form_par_kontraktu",
    "params.form_oplaty_sys_kparam",
]

REQUIRED_INPUTS = [
    ("input", "date_dim", True),
]

SQL_LATEST_PARAMS_TS = """
WITH m AS (
  SELECT GREATEST(
    (SELECT COALESCE(MAX(updated_at), '-infinity'::timestamptz) FROM params.form_zmienne),
    (SELECT COALESCE(MAX(updated_at), '-infinity'::timestamptz) FROM params.form_bess_param),
    (SELECT COALESCE(MAX(updated_at), '-infinity'::timestamptz) FROM params.form_parametry_klienta),
    (SELECT COALESCE(MAX(updated_at), '-infinity'::timestamptz) FROM params.form_par_arbitrazu),
    (SELECT COALESCE(MAX(updated_at), '-infinity'::timestamptz) FROM params.form_lcoe),
    (SELECT COALESCE(MAX(updated_at), '-infinity'::timestamptz) FROM params.form_oplaty_dyst_sched),
    (SELECT COALESCE(MAX(updated_at), '-infinity'::timestamptz) FROM params.form_oplaty_dystrybucyjne),
    (SELECT COALESCE(MAX(updated_at), '-infinity'::timestamptz) FROM params.form_oplaty_fiskalne),
    (SELECT COALESCE(MAX(updated_at), '-infinity'::timestamptz) FROM params.form_oplaty_sys_sched),
    (SELECT COALESCE(MAX(updated_at), '-infinity'::timestamptz) FROM params.form_oplaty_systemowe),
    (SELECT COALESCE(MAX(updated_at), '-infinity'::timestamptz) FROM params.form_par_kontraktu),
    (SELECT COALESCE(MAX(updated_at), '-infinity'::timestamptz) FROM params.form_oplaty_sys_kparam)
  ) AS ts
)
SELECT ts FROM m;
"""

async def _log_db_identity(con: psycopg.AsyncConnection) -> None:
    """Loguje tożsamość połączenia oraz widoczne schematy (params/input/output)."""
    con.row_factory = dict_row
    async with con.cursor() as cur:
        await cur.execute(
            "select current_database() as db, current_user as usr, "
            "inet_server_addr()::text as host, inet_server_port() as port"
        )
        row = await cur.fetchone()
        log.info(
            "DB identity: db=%s user=%s host=%s port=%s",
            row["db"], row["usr"], row["host"], row["port"]
        )
        await cur.execute(
            """
            select schema_name
            from information_schema.schemata
            where schema_name in ('params','input','output')
            order by 1
            """
        )
        schemas = [r["schema_name"] for r in await cur.fetchall()]
        log.info("Visible schemas: %s", ", ".join(schemas) if schemas else "(none)")

async def _assert_prep_ready(con: psycopg.AsyncConnection) -> None:
    """Sprawdza istnienie i niepustość wymaganych tabel params + input.date_dim."""
    con.row_factory = dict_row
    miss, empty = [], []
    async with con.cursor() as cur:
        # Formularze
        for fqn in REQUIRED_FORMS:
            schema, table = fqn.split(".", 1)
            await cur.execute("SELECT to_regclass(%s) AS oid", (fqn,))
            row = await cur.fetchone()
            if not row or row["oid"] is None:
                miss.append(fqn)
                continue
            qualified = _sql.Identifier(schema, table).as_string(con)
            await cur.execute(f"SELECT COUNT(*) AS n FROM {qualified}")
            row_n = await cur.fetchone()
            if not row_n or row_n["n"] <= 0:
                empty.append(fqn)
        # Wejścia
        for schema, table, must_have_rows in REQUIRED_INPUTS:
            fqn = f"{schema}.{table}"
            await cur.execute("SELECT to_regclass(%s) AS oid", (fqn,))
            row = await cur.fetchone()
            if not row or row["oid"] is None:
                miss.append(fqn)
                continue
            if must_have_rows:
                qualified = _sql.Identifier(schema, table).as_string(con)
                await cur.execute(f"SELECT COUNT(*) AS n FROM {qualified}")
                row_n = await cur.fetchone()
                if not row_n or row_n["n"] <= 0:
                    empty.append(fqn)

    problems = []
    if miss:
        problems.append("brak tabel: " + ", ".join(miss))
    if empty:
        problems.append("brak danych (0 wierszy): " + ", ".join(empty))
    if problems:
        raise RuntimeError("PREP NOT READY → " + " | ".join(problems))

async def _wait_until_prep_ready(con: psycopg.AsyncConnection, interval_sec: float, heartbeat_sec: float) -> None:
    """Pętla oczekująca na gotowość PREP, z heartbeatem co `heartbeat_sec`."""
    await _log_db_identity(con)
    start = time.monotonic()
    tries = 0
    last_hb = start
    while True:
        try:
            await _assert_prep_ready(con)
            elapsed = time.monotonic() - start
            log.info("PREP READY — po %d próbach, %.1fs czekania.", tries, elapsed)
            return
        except Exception as e:
            tries += 1
            now = time.monotonic()
            if tries == 1 or (now - last_hb) >= heartbeat_sec:
                log.warning(
                    "PREP NOT READY (próba %d, %.0fs): %s ; ponowna próba za %.1fs",
                    tries, now - start, e, interval_sec
                )
                last_hb = now
            await asyncio.sleep(interval_sec)

# ─────────────────────────────────────────────────────────────────────────────
# CalcListener
# ─────────────────────────────────────────────────────────────────────────────

class CalcListener:
    """
    Koordynator kolejki obliczeń:
    - otwiera pulę połączeń do DB i połączenie LISTEN,
    - konsumuje NOTIFY (z debounce),
    - okresowo drenuje kolejkę (poll/tick),
    - uruchamia kalkulacje (runner.run_calc).

    Uwaga dot. PERIODIC_TICK_SEC:
    - przeliczenia ruszają najpóźniej co `tick_sec`, ale jeśli wcześniej przyjdzie
      NOTIFY z params → uruchomimy drain po `debounce_sec` i ZRESETUJEMY zegar.
    """

    def __init__(self, listen_channel: str = CHANNEL_REBUILD, poll_interval_sec: float | None = None) -> None:
        self.listen_channel = listen_channel

        # Config czasu – z ENV lub argumentów
        env_poll = os.getenv("CALC_POLL_SEC")
        self.poll_interval_sec = float(poll_interval_sec) if poll_interval_sec is not None \
            else (float(env_poll) if env_poll is not None else 3.0)

        env_debounce = os.getenv("DEBOUNCE_SECONDS")
        self.debounce_sec = float(env_debounce) if env_debounce is not None else 2.0

        env_tick = os.getenv("PERIODIC_TICK_SEC")
        self.tick_sec = float(env_tick) if env_tick is not None else 60.0

        self.prep_wait_sec = float(os.getenv("PREP_WAIT_SEC", "10"))
        default_hb = max(15.0, self.prep_wait_sec * 3.0)
        self.prep_heartbeat_sec = float(os.getenv("PREP_HEARTBEAT_SEC", str(default_hb)))

        # Pula połączeń (lazy)
        self._pool_dsn: Optional[str] = None
        self.pool = None  # AsyncConnectionPool

        # Połączenie LISTEN oraz sterowanie pętlami
        self._listen_conn: Optional[psycopg.AsyncConnection] = None
        self._listen_task: Optional[asyncio.Task] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._debounce_task: Optional[asyncio.Task] = None

        # Sygnalizacja i stan
        self._stop = asyncio.Event()
        self._notify_event = asyncio.Event()
        self._last_tick_ts: float = time.monotonic()
        self._busy: bool = False

        # Bootstrap (uruchomić job na starcie?)
        self._run_on_startup = (os.getenv("CALC_RUN_ON_STARTUP", "false").lower() in ("1", "true", "yes"))

        log.info(
            "Poll=%.1fs, Debounce=%.1fs, Tick=%.1fs; RUN_ON_STARTUP=%s; PREP_WAIT=%.1fs, PREP_HB=%.1fs",
            self.poll_interval_sec, self.debounce_sec, self.tick_sec,
            self._run_on_startup, self.prep_wait_sec, self.prep_heartbeat_sec
        )

    async def _open_pool_with_retry(self, attempts: int = 10, delay: float = 3.0) -> None:
        """Otwiera pulę z kilkoma próbami (typowy cold-start DB)."""
        last_exc = None
        for i in range(1, attempts + 1):
            try:
                await self.pool.open()
                log.info("Pool open OK (attempt %d/%d).", i, attempts)
                return
            except Exception as e:
                last_exc = e
                log.warning("Pool open failed (%d/%d): %s; retry in %.1fs", i, attempts, e, delay)
                await asyncio.sleep(delay)
        log.exception("Pool open failed after %d attempts: %s", attempts, last_exc)
        raise last_exc

    async def start(self) -> None:
        """Startuje główną pętlę słuchacza i wątki drain/poll."""
        # DSN budujemy dopiero TERAZ (aktualne ENV DB_*)
        if self._pool_dsn is None:
            # + keepalive’y dla stabilności połączenia
            self._pool_dsn = (
                build_pg_dsn()
                + " application_name=energia-prep-2:calc-listener"
                + " keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=3"
            )

        # Utworzenie puli (lazy) i otwarcie z retry
        if self.pool is None:
            self.pool = create_async_pool(self._pool_dsn, min_size=1, max_size=10, timeout=30.0)
            log.info("Async connection pool created (lazy init).")
        await self._open_pool_with_retry(attempts=10, delay=3.0)
        log.info("Connection pool opened successfully.")

        # Zapewnienie obiektów DB (DDL)
        await ensure_db_objects(self.pool)
        log.info("DB objects ensured (calc_job_queue, calc_job_stage, etc.).")

        # PREP READY (wejścia + formularze)
        async with self.pool.connection() as con:
            await _wait_until_prep_ready(con, self.prep_wait_sec, self.prep_heartbeat_sec)
        log.info("[PREP] READY — starting loops.")

        # Połączenie LISTEN/NOTIFY
        self._listen_conn = await self._open_listen_connection()

        # Start pętli
        self._listen_task = asyncio.create_task(self._listen_loop(), name="listen_loop")
        self._poll_task = asyncio.create_task(self._poll_loop(), name="poll_loop")

        # Bootstrap: dodaj job dla najnowszego params_ts (idempotencja) — bezpiecznie
        if self._run_on_startup:
            try:
                await self._bootstrap_enqueue_once()
                log.info("[BOOT] Enqueued latest params job on startup.")
            except Exception as e:
                # Nie zatrzymuj startu listenera przez brak params_ts lub tabeli kolejki
                log.warning("[BOOT] Skipping RUN_ON_STARTUP: %s", e)

        # Natychmiastowe opróżnienie kolejki po starcie
        await self._drain_queue()
        log.info("[DRAIN] Initial queue drain completed.")

    async def stop(self) -> None:
        """Zatrzymuje listener i domyka zasoby."""
        self._stop.set()
        if self._listen_conn is not None and not self._listen_conn.closed:
            try:
                await self._listen_conn.close()
            except Exception as e:
                log.warning("Problem while closing LISTEN conn: %s", e)
        if self.pool is not None:
            try:
                await self.pool.close()
            except Exception as e:
                log.warning("Problem while closing pool: %s", e)
        log.info("Listener stopped. Pool closed.")

    async def _open_listen_connection(self) -> psycopg.AsyncConnection:
        """Otwiera połączenie do DB używane wyłącznie do LISTEN/NOTIFY (autocommit ON)."""
        dsn = (
            build_pg_dsn()
            + " application_name=energia-prep-2:calc-listen-conn"
            + " keepalives=1 keepalives_idle=30 keepalives_interval=10 keepalives_count=3"
        )
        conn = await psycopg.AsyncConnection.connect(dsn)
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute("SET application_name = 'energia-prep-2-calc-listen-conn'")
        log.info("[LISTEN] connection established (autocommit ON)")
        return conn

    async def _listen_loop(self) -> None:
        """Obsługuje cykl LISTEN; na NOTIFY uruchamia debounce i markuje notify_event."""
        while not self._stop.is_set():
            try:
                con = self._listen_conn or await self._open_listen_connection()
                self._listen_conn = con
                async with con.cursor() as cur:
                    await cur.execute(f"LISTEN {self.listen_channel};")
                log.info("LISTEN on channel: %s", self.listen_channel)

                notifies = con.notifies()
                while not self._stop.is_set():
                    msg = await asyncio.wait_for(notifies.__anext__(), timeout=30.0)
                    payload = parse_notify_payload(getattr(msg, "payload", None))
                    job_id = payload.get("job_id")
                    status = payload.get("status")
                    action = payload.get("action")
                    log.info("NOTIFY %s: job_id=%s, status=%s, action=%s",
                             self.listen_channel, job_id, status, action)

                    # sygnał: przyszły zmiany → odpal debounce i zresetuj tick
                    self._notify_event.set()
                    if self._debounce_task is None or self._debounce_task.done():
                        self._debounce_task = asyncio.create_task(self._debounced_drain())
                    # reset zegara okresowego — bo ruszymy „zaraz”
                    self._last_tick_ts = time.monotonic()

            except asyncio.TimeoutError:
                # idle — spróbuj od nowa (utrzymuj połączenie świeże)
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("LISTEN loop soft error: %s — reconnecting soon.", e)
                try:
                    if self._listen_conn:
                        await self._listen_conn.close()
                except Exception:
                    pass
                self._listen_conn = None
                await asyncio.sleep(1.0)

    async def _debounced_drain(self) -> None:
        """Debounce po NOTIFY – odczekaj self.debounce_sec i zrób drain kolejki; resetuje zegar."""
        try:
            await asyncio.sleep(self.debounce_sec)
            await self._drain_queue()
        finally:
            self._notify_event.clear()
            # po przeliczeniu „na żądanie” ustaw bazę zegara na teraz
            self._last_tick_ts = time.monotonic()
            self._debounce_task = None

    async def _poll_loop(self) -> None:
        """
        Pętla poll/tick:
        - jeśli otrzymano NOTIFY → drain (poza listen-loop już mógł ruszyć debounce),
        - jeśli minął PERIODIC_TICK_SEC od ostatniego uruchomienia → drain.
        """
        while not self._stop.is_set():
            try:
                if self._notify_event.is_set():
                    log.debug("POLL: notify received → drain")
                    await self._drain_queue()
                    self._notify_event.clear()
                    self._last_tick_ts = time.monotonic()
                elif self.tick_sec > 0 and (time.monotonic() - self._last_tick_ts) >= self.tick_sec:
                    log.debug("POLL: periodic tick → drain")
                    await self._drain_queue()
                    self._last_tick_ts = time.monotonic()
            except Exception as e:
                log.exception("POLL error: %s", e)
            finally:
                await asyncio.sleep(self.poll_interval_sec)

    async def _drain_queue(self) -> None:
        """Zdejmuje joby z kolejki i uruchamia runner do skutku (koalestracja)."""
        if self._busy:
            log.debug("DRAIN: already busy — skip.")
            return
        self._busy = True
        try:
            log.debug("DRAIN: start")
            while True:
                row = await self._pick_one_job()
                if row is None:
                    log.debug("QUEUE: empty — nothing to run.")
                    return

                job_id   = row["job_id"]
                params_ts= row["params_ts"]
                log.info("RUN job_id=%s params_ts=%s", job_id, params_ts)

                try:
                    async with self.pool.connection() as con:
                        calc_id = await run_calc(con, job_id=job_id, params_ts=params_ts)
                    await self._mark_done(job_id)
                    log.info("DONE job_id=%s calc_id=%s", job_id, calc_id)
                except Exception as e:
                    await self._mark_error(job_id, error=str(e))
                    log.exception("ERROR job_id=%s: %s", job_id, e)
                    continue
        finally:
            self._busy = False
            log.debug("DRAIN: end")

    async def _pick_one_job(self) -> Optional[dict]:
        """Wybiera do uruchomienia najświeższy 'queued' i koalestruje resztę na 'skipped'."""
        async with self.pool.connection() as con:
            con.row_factory = dict_row
            async with con.cursor() as cur:
                await cur.execute(SQL_PICK_COALESCE)
                row = await cur.fetchone()
                await con.commit()
        return row

    async def _mark_done(self, job_id: str) -> None:
        """Ustawia status joba na 'done'."""
        async with self.pool.connection() as con:
            async with con.cursor() as cur:
                await cur.execute(SQL_MARK_DONE, (job_id,))
                await con.commit()

    async def _mark_error(self, job_id: str, error: str) -> None:
        """Ustawia status joba na 'failed' i zapisuje komunikat (ucięty do 8kB)."""
        err = (error or "").strip()
        if len(err) > 8000:
            err = err[:8000] + "…"
        async with self.pool.connection() as con:
            async with con.cursor() as cur:
                await cur.execute(SQL_MARK_ERROR, (err, job_id))
                await con.commit()

    async def _bootstrap_enqueue_once(self) -> None:
        """RUN_ON_STARTUP: dorzuć najnowszy params_ts do kolejki, jeśli go jeszcze nie ma."""
        async with self.pool.connection() as con:
            con.row_factory = dict_row
            async with con.cursor() as cur:
                await cur.execute(SQL_LATEST_PARAMS_TS)
                row = await cur.fetchone()
                if not row or row["ts"] is None:
                    raise RuntimeError("BOOTSTRAP: brak params_ts z formularzy (seed wymagany).")
                params_ts = row["ts"]

            async with con.cursor() as cur:
                await cur.execute(SQL_EXISTS_FOR_PARAMS, (params_ts,))
                exists = (await cur.fetchone()) is not None

            if exists:
                log.info("BOOTSTRAP: pomijam — istnieje job dla params_ts=%s", params_ts)
                return

            job_id = str(uuid.uuid4())
            async with con.cursor() as cur:
                await cur.execute(SQL_INSERT_QUEUED, (job_id, params_ts))
                await con.commit()
                log.info("BOOTSTRAP: dodano job queued (job_id=%s, params_ts=%s)", job_id, params_ts)

# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

async def _amain() -> None:
    """Async entrypoint dla `python -m energia_prep2.calc.listener`."""
    # ── jedyna zmiana: inicjalizacja logowania ───────────────────────────────
    import logging, os, sys
    lvl = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, lvl, logging.INFO),
        stream=sys.stdout,
        format="%(asctime)s %(levelname)s | %(name)s: %(message)s",
    )
    logging.getLogger("psycopg").setLevel(logging.WARNING)
    # ─────────────────────────────────────────────────────────────────────────

    listener = CalcListener()
    loop = asyncio.get_running_loop()

    def _graceful_shutdown() -> None:
        log.info("Odebrano sygnał zakończenia. Zatrzymuję listener…")
        asyncio.create_task(listener.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _graceful_shutdown)
        except NotImplementedError:
            pass

    await listener.start()

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        await listener.stop()

if __name__ == "__main__":
    asyncio.run(_amain())
