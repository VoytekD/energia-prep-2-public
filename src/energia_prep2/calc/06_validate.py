# 06_validate.py — DUMMY: tymczasowy walidator, który nic nie sprawdza.
from __future__ import annotations

import logging
import psycopg

log = logging.getLogger("energia-prep-2.calc")

async def run(con: psycopg.AsyncConnection, params_ts, calc_id) -> None:
    """
    Dummy validate: nie wykonuje żadnych zapytań, nie podnosi błędów.
    Zostawiamy tylko logi start/stop, by nie rozregulować pipeline'u.
    """
    log.info("[VALIDATE/DUMMY] start (params_ts=%s, calc_id=%s)", params_ts, calc_id)
    # brak checków — celowo pusto
    log.info("[VALIDATE/DUMMY] end (params_ts=%s, calc_id=%s)", params_ts, calc_id)