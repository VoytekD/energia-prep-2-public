from __future__ import annotations
from pathlib import Path
from ..cfg import settings
from ..db import get_conn_app, get_conn_super_app
from ..log import log

def run() -> None:
    sql_file = Path(settings.SQL_DIR) / "40_triggers" / "02_event_triggers.sql"
    views_file = Path(settings.SQL_DIR) / "30_views" / "01_output_vw_energy_calc_input.sql"
    grafana_views = Path(settings.SQL_DIR) / "30_views" / "02_output_vw_wykresy_grafana.sql"

    # Widoki – wykonaj zwykłym użytkownikiem
    for f in (views_file, grafana_views):
        if f.exists():
            log.info("views: APPLY %s", f)
            sql = f.read_text(encoding="utf-8")
            with get_conn_app() as conn, conn.cursor() as cur:
                cur.execute(sql)
            log.info("views: OK %s", f)
        else:
            log.warning("views: brak pliku %s", f)

    # Event trigger — próbuj jako superuser (jeżeli dostępny)
    if sql_file.exists():
        sql = sql_file.read_text(encoding="utf-8")
        try:
            with get_conn_super_app() as conn, conn.cursor() as cur:
                cur.execute(sql)
            log.info("event_trigger: OK (superuser)")
        except Exception as ex:
            log.warning("event_trigger: pominięty (brak superusera lub błąd: %s)", ex)
    else:
        log.warning("event_trigger: brak pliku %s", sql_file)
