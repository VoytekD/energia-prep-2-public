# -*- coding: utf-8 -*-
"""
calc.listener
==============
Nasłuchuje kanału Postgres i uruchamia pipeline.

Zmiany:
- LATEST_PARAMS_TS: wyłącznie updated_at + COALESCE(..., '-infinity'::timestamptz)
- Preflight: weryfikujemy obecność i ≥1 wiersz dla wszystkich 12 formularzy.
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
    log,
    build_pg_dsn,
    create_async_pool,
    ensure_db_objects,
    CHANNEL_REBUILD,
    parse_notify_payload,
    TABLE_JOBS,  # "output.calc_job_queue"
)

# ─────────────────────────────────────────────────────────────────────────────
# Single-flight + koalestracja kolejki
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
  RETURNING 1
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
SET status = 'error',
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
# Najświeższy znacznik czasu parametrów — **12** formularzy, tylko updated_at
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

# Formularze wymagane do startu — **komplet 12**
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

# Wejścia używane przez 00: tylko oś czasu musi istnieć
REQUIRED_INPUTS = [
    ("input", "date_dim", True),
]


async def _assert_prep_ready(con: psycopg.AsyncConnection) -> None:
    """
    Preflight:
      - wymagane 12 formularzy params.form_* istnieją i mają ≥1 wiersz,
      - input.date_dim istnieje i ma >0 wierszy (series sprawdza 00).
    """
    con.row_factory = dict_row
    miss, empty = [], []

    async with con.cursor() as cur:
        # Forms (12 szt.)
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

        # Inputs (tylko date_dim)
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


async def _log_db_identity(con: psycopg.AsyncConnection) -> None:
    con.row_factory = dict_row
    async with con.cursor() as cur:
        await cur.execute(
            "select current_database() as db, current_user as usr, "
            "inet_server_addr()::text as host, inet_server_port() as port"
        )
        row = await cur.fetchone()
        log.info(
            "DB identity: db=%s user=%s host=%s port=%s",
            row["db"],
            row["usr"],
            row["host"],
            row["port"],
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


async def _wait_until_prep_ready(
    con: psycopg.AsyncConnection, interval_sec: float, heartbeat_sec: float
) -> None:
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
                    tries,
                    now - start,
                    e,
                    interval_sec,
                )
                last_hb = now
            await asyncio.sleep(interval_sec)


class CalcListener:
    def __init__(
        self, listen_channel: str = CHANNEL_REBUILD, poll_interval_sec: float | None = None
    ) -> None:
        self.listen_channel = listen_channel

        env_poll = os.getenv("CALC_POLL_SEC")
        if poll_interval_sec is not None:
            self.poll_interval_sec = float(poll_interval_sec)
        elif env_poll is not None:
            self.poll_interval_sec = float(env_poll)
        else:
            self.poll_interval_sec = 3.0

        env_debounce = os.getenv("DEBOUNCE_SECONDS")
        self.debounce_sec = float(env_debounce) if env_debounce is not None else 2.0

        env_tick = os.getenv("PERIODIC_TICK_SEC")
        self.tick_sec = float(env_tick) if env_tick is not None else 0.0

        self.prep_wait_sec = float(os.getenv("PREP_WAIT_SEC", "10"))
        default_hb = max(15.0, self.prep_wait_sec * 3.0)
        self.prep_heartbeat_sec = float(
            os.getenv("PREP_HEARTBEAT_SEC", str(default_hb))
        )

        # ─────────────── poprawka: tworzenie puli dopiero po await ───────────────
        self.pool = None
        self._pool_dsn = build_pg_dsn() + " application_name=energia-prep-2:calc-listener"
        # ─────────────────────────────────────────────────────────────────────────

        self._listen_conn: Optional[psycopg.AsyncConnection] = None
        self._stop = asyncio.Event()
        self._notify_event = asyncio.Event()

        self._last_tick_ts: float = time.monotonic()
        self._debounce_task: Optional[asyncio.Task] = None
        self._run_on_startup = (
            os.getenv("CALC_RUN_ON_STARTUP", "false").lower() in ("1", "true", "yes")
        )
        self._busy: bool = False  # single-flight

        log.info(
            f"Poll={self.poll_interval_sec:.1f}s, Debounce={self.debounce_sec:.1f}s, "
            f"Heartbeat={self.tick_sec:.1f}s (CALC_POLL_SEC={env_poll!r}, "
            f"DEBOUNCE_SECONDS={env_debounce!r}, PERIODIC_TICK_SEC={env_tick!r}); "
            f"RUN_ON_STARTUP={self._run_on_startup}; "
            f"PREP_WAIT_SEC={self.prep_wait_sec:.1f}, PREP_HEARTBEAT_SEC={self.prep_heartbeat_sec:.1f}"
        )

    async def start(self) -> None:
        # ─────────────── poprawka: async tworzenie puli ───────────────
        if self.pool is None:
            self.pool = await create_async_pool(self._pool_dsn)
        # ──────────────────────────────────────────────────────────────

        await self.pool.open()
        log.info("Pula połączeń otwarta.")
        await ensure_db_objects(self.pool)

        from . import io as calc_io
        async with self.pool.connection() as con:
            await calc_io.ensure_calc_status_structures(con)
            await calc_io.ensure_calc_job_queue(con)
            await _wait_until_prep_ready(
                con,
                interval_sec=self.prep_wait_sec,
                heartbeat_sec=self.prep_heartbeat_sec,
            )

        if self._run_on_startup:
            await self._bootstrap_enqueue_once()

        await self._drain_queue()
        log.info("Start pętli nasłuchu…")
        await self._run_forever()

    async def stop(self) -> None:
        self._stop.set()
        if self._listen_conn is not None and not self._listen_conn.closed:
            await self._listen_conn.close()
        if self.pool is not None:
            await self.pool.close()
        log.info("Pula połączeń zamknięta. Listener zatrzymany.")

    async def _open_listen_connection(self) -> psycopg.AsyncConnection:
        # ─────────────── poprawka: brak appname param ───────────────
        dsn = build_pg_dsn() + " application_name=energia-prep-2:calc-listen-conn"
        # ─────────────────────────────────────────────────────────────
        conn = await psycopg.AsyncConnection.connect(dsn)
        await conn.set_autocommit(True)
        return conn

    async def _run_forever(self) -> None:
        listen_task = asyncio.create_task(self._listen_loop(), name="listen_loop")
        poll_task = asyncio.create_task(self._poll_loop(), name="poll_loop")
        try:
            await self._stop.wait()
        finally:
            listen_task.cancel()
            poll_task.cancel()
            await asyncio.gather(listen_task, poll_task, return_exceptions=True)

    async def _debounced_drain(self) -> None:
        try:
            await asyncio.sleep(self.debounce_sec)
            await self._drain_queue()
        finally:
            self._debounce_task = None
            self._notify_event.clear()

    async def _consume_notifies(self, notifies) -> None:
        while not self._stop.is_set():
            msg = await asyncio.wait_for(notifies.__anext__(), timeout=30.0)
            payload: Optional[str] = getattr(msg, "payload", None)
            data = parse_notify_payload(payload)
            log.info(f"NOTIFY {self.listen_channel}: payload={data or '[empty]'}")
            self._notify_event.set()
            if self._debounce_task is None or self._debounce_task.done():
                self._debounce_task = asyncio.create_task(self._debounced_drain())

    async def _listen_loop(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                self._listen_conn = await self._open_listen_connection()
                con = self._listen_conn
                con.row_factory = dict_row

                stmt = _sql.SQL("LISTEN {}").format(_sql.Identifier(self.listen_channel))
                await con.execute(stmt)
                log.info(f"LISTEN on channel: {self.listen_channel}")

                notifies = con.notifies()
                await self._consume_notifies(notifies)

                await con.close()
                backoff = 1.0

            except asyncio.TimeoutError:
                continue
            except Exception as e:
                log.exception(f"Błąd w _listen_loop: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                did_run = False
                if self.tick_sec > 0:
                    now = time.monotonic()
                    if now - self._last_tick_ts >= self.tick_sec:
                        self._last_tick_ts = now
                        await self._drain_queue()
                        did_run = True

                if not did_run and not self._notify_event.is_set():
                    await self._drain_queue()
                else:
                    self._notify_event.clear()

            except Exception as e:
                log.exception(f"Błąd w _poll_loop: {e}")

            await asyncio.sleep(self.poll_interval_sec)

    async def _drain_queue(self) -> None:
        if self._busy:
            return
        self._busy = True
        try:
            while True:
                job_row = await self._pick_one_job()
                if job_row is None:
                    return
                job_id = job_row["job_id"]
                params_ts = job_row["params_ts"]
                log.info(f"RUN job_id={job_id} params_ts={params_ts}")
                try:
                    async with self.pool.connection() as con:
                        con.row_factory = dict_row
                        # Leniwy import, aby uniknąć problemów z importami top-level w runner.py
                        try:
                            from .runner import run_calc  # import lokalny
                        except Exception:
                            raise RuntimeError("Brak run_calc() w calc.runner")
                        calc_id = await run_calc(con, job_id=job_id, params_ts=params_ts)  # type: ignore[arg-type]
                        await self._mark_done(job_id)
                        log.info(f"DONE job_id={job_id} calc_id={calc_id}")
                except Exception as e:
                    await self._mark_error(job_id, error=str(e))
                    log.exception(f"ERROR job_id={job_id}: {e}")
                    continue
        finally:
            self._busy = False

    async def _pick_one_job(self) -> Optional[dict]:
        async with self.pool.connection() as con:
            con.row_factory = dict_row
            async with psycopg.AsyncCursor(con) as cur:  # type: ignore[attr-defined]
                await cur.execute(SQL_PICK_COALESCE)
                row = await cur.fetchone()
                await con.commit()
        return row

    async def _mark_done(self, job_id: str) -> None:
        async with self.pool.connection() as con:
            async with con.cursor() as cur:
                await cur.execute(SQL_MARK_DONE, (job_id,))
                await con.commit()

    async def _mark_error(self, job_id: str, error: str) -> None:
        err = (error or "").strip()
        if len(err) > 8000:
            err = err[:8000] + "…"
        async with self.pool.connection() as con:
            async with con.cursor() as cur:
                await cur.execute(SQL_MARK_ERROR, (err, job_id))
                await con.commit()

    async def _bootstrap_enqueue_once(self) -> None:
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

async def _amain() -> None:
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

def main() -> None:
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
