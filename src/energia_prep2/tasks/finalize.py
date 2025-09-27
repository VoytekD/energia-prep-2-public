from __future__ import annotations
from dataclasses import dataclass
from ..db import get_conn_app
from ..log import log

@dataclass
class Check:
    ok: bool
    detail: str

def run() -> None:
    checks: list[Check] = []
    with get_conn_app() as conn, conn.cursor() as cur:
        # Czy są jakiekolwiek wiersze w date_dim?
        cur.execute("SELECT count(*) FROM input.date_dim")
        n = cur.fetchone()[0]
        checks.append(Check(ok=(n > 0), detail=f"input.date_dim count={n}"))

        # Czy istnieje tabela tge.prices?
        cur.execute("""
            SELECT EXISTS (
              SELECT 1 FROM information_schema.tables WHERE table_schema='tge' AND table_name='prices'
            )
        """)
        exists = bool(cur.fetchone()[0])
        checks.append(Check(ok=exists, detail="tge.prices exists"))

        # Czy widok output.vw_energy_calc_input istnieje?
        cur.execute("""
            SELECT EXISTS (
              SELECT 1 FROM information_schema.views WHERE table_schema='output' AND table_name='vw_energy_calc_input'
            )
        """)
        exists = bool(cur.fetchone()[0])
        checks.append(Check(ok=exists, detail="output.vw_energy_calc_input exists"))

    any_fail = False
    for c in checks:
        level = "OK" if c.ok else "ERR"
        log.info("finalize: [%s] %s", level, c.detail)
        if not c.ok:
            any_fail = True

    if any_fail:
        raise RuntimeError("finalize: walidacja nie przeszła — sprawdź logi powyżej.")
    log.info("finalize: ALL GREEN")
