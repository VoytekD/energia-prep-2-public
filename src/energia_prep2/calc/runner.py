"""
calc.runner — orchestracja przebiegu (RAM-first):
- snapshot (PRZED liczeniem) → konsolidacja → 00→01→02→03,
- statusy: rejestrujemy etapy w output.calc_jobs / output.calc_job_stage,
- RAM-first: 00–02 liczą w pamięci (NumPy/Numba), 03 robi PERSIST do:
  output.stage00_ingest_calc,
  output.stage01_proposer_calc,
  output.stage02_commit_calc,
  output.stage04_report_calc,
- zero fallbacków/aliasów; agregacje w Grafanie; final = godzinówka (stage04_report_calc).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from time import perf_counter

import psycopg
from psycopg.rows import dict_row

from . import log
from . import io as calc_io


async def run_calc(
    con: psycopg.AsyncConnection,
    job_id: uuid.UUID,
    params_ts: datetime,
) -> uuid.UUID:
    """
    Nowy bieg:
      1) snapshot + konsolidacja (io.py),
      2) 00_ingest.run → H_buf,
      3) 01_proposer.run → P_buf (na razie: dummy zero-proposals),
      4) 02_commit.commit(H,P) → C_buf,
      5) 03_raport.persist(con, H_buf, P_buf, C_buf) → zapis godzin do output.stage04_report_calc,
    """
    con.row_factory = dict_row
    calc_id = uuid.uuid4()
    log.info("[RUNNER] start calc_id=%s", calc_id)

    # Struktury snapshotu (archiwum/źródło prawdy)
    await calc_io.ensure_params_snapshot_table(con)

    # Zarejestruj „pending” dla etapów 00..03 (wyłącznie per-etapowa tabela)
    async with con.cursor() as cur:
        stages = ('00','01','02','03')
        await cur.executemany("""
            INSERT INTO output.calc_job_stage (job_id, calc_id, stage, status, started_at)
            VALUES (%s, %s, %s, 'pending', now())
            ON CONFLICT (calc_id, stage) DO NOTHING
        """, [(str(job_id), str(calc_id), s) for s in stages])
    await con.commit()

    t_all0 = perf_counter()
    try:
        # Snapshot + konsolidacja parametrów
        t_s0 = perf_counter()
        await calc_io.dump_params_snapshot(con, calc_id=calc_id, params_ts=params_ts)
        await calc_io.build_params_snapshot_consolidated(con, calc_id=calc_id, params_ts=params_ts)
        await calc_io.assert_consolidated_norm_ready(con, calc_id=calc_id)
        await con.commit()
        t_s1 = perf_counter()

        # Import modułów po snapshot (unikamy importów top-level)
        from . import ingest_00, proposer_01, commit_02, raport_03

        # ===== 00 (RAM) =====
        async with con.cursor() as cur:
            await cur.execute(
                "UPDATE output.calc_job_stage SET status='running' WHERE calc_id=%s AND stage='00'",
                (str(calc_id),),
            )
        t00 = perf_counter()
        H_buf = await ingest_00.run(con, calc_id=calc_id, params_ts=params_ts)
        t01 = perf_counter()
        async with con.cursor() as cur:
            await cur.execute(
                "UPDATE output.calc_job_stage SET status='done', finished_at=now() WHERE calc_id=%s AND stage='00'",
                (str(calc_id),),
            )
        await con.commit()

        # ===== 01 (RAM) =====
        async with con.cursor() as cur:
            await cur.execute(
                "UPDATE output.calc_job_stage SET status='running' WHERE calc_id=%s AND stage='01'",
                (str(calc_id),),
            )
        t10 = perf_counter()
        P_buf = proposer_01.run(H_buf)  # dummy zero-proposals; pełna metodyka w kolejnym kroku
        t11 = perf_counter()
        async with con.cursor() as cur:
            await cur.execute(
                "UPDATE output.calc_job_stage SET status='done', finished_at=now() WHERE calc_id=%s AND stage='01'",
                (str(calc_id),),
            )
        await con.commit()

        # ===== 02 (RAM) =====
        async with con.cursor() as cur:
            await cur.execute(
                "UPDATE output.calc_job_stage SET status='running' WHERE calc_id=%s AND stage='02'",
                (str(calc_id),),
            )
        t20 = perf_counter()
        C_buf = commit_02.commit(H_buf, P_buf)
        t21 = perf_counter()
        async with con.cursor() as cur:
            await cur.execute(
                "UPDATE output.calc_job_stage SET status='done', finished_at=now() WHERE calc_id=%s AND stage='02'",
                (str(calc_id),),
            )
        await con.commit()

        # ===== 03 (PERSIST godzin → stage04_report_calc) =====
        async with con.cursor() as cur:
            await cur.execute(
                "UPDATE output.calc_job_stage SET status='running' WHERE calc_id=%s AND stage='03'",
                (str(calc_id),),
            )
        t30 = perf_counter()
        await raport_03.persist(con, H_buf, P_buf, C_buf)
        t31 = perf_counter()
        async with con.cursor() as cur:
            await cur.execute(
                "UPDATE output.calc_job_stage SET status='done', finished_at=now() WHERE calc_id=%s AND stage='03'",
                (str(calc_id),),
            )
        await con.commit()

        # ===== VALIDATE (auto, bez ENV) =====
        from . import validate
        t_v0 = perf_counter()
        await validate.run(con, params_ts)
        t_v1 = perf_counter()

        t_all1 = perf_counter()
        log.info(
            "[PERF] snapshot=%.0f ms | 00=%.0f ms | 01=%.0f ms | 02=%.0f ms | persist(03)=%.0f ms | validate=%.0f ms | total=%.0f ms",
            (t_s1 - t_s0) * 1000.0,
            (t01 - t00) * 1000.0,
            (t11 - t10) * 1000.0,
            (t21 - t20) * 1000.0,
            (t31 - t30) * 1000.0,
            (t_v1 - t_v0) * 1000.0,
            (t_all1 - t_all0) * 1000.0,
        )

        log.info("[RUNNER] done calc_id=%s", calc_id)
        return calc_id

    except Exception:
        log.exception("[RUNNER] ERROR calc_id=%s", calc_id)
        raise
