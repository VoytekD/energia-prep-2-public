from __future__ import annotations

from energia_prep2.log import log
from energia_prep2.tasks import bootstrap, date_dim, csv_import, tge_fetch


def _maybe_call(func, label: str):
    if callable(func):
        log.info("%s: uruchamiam", label)
        return func()
    log.info("%s: pominięte (brak funkcji)", label)
    return None


def main():
    log.info("═══════════════════════════════════════════════════════════════════════════")
    log.info("■ energia-prep-2: START (pipeline)")
    log.info("═══════════════════════════════════════════════════════════════════════════")

    # 0) BOOTSTRAP SQL – schematy/tabele/triggery
    _maybe_call(bootstrap.run, "bootstrap")

    # 1) Wymiar czasu
    date_dim.build()

    # 2) Import CSV (konsumpcja/produkcja)
    _maybe_call(csv_import.run, "csv")

    # 3) TGE – pełny import przy cold-starcie; aktualizacje robi cron
    try:
        _maybe_call(tge_fetch.full_import, "tge")
    except Exception as e:
        log.error("tge: błąd krytyczny: %s (kontynuuję)", e)

    # 4) BOOTSTRAP_END / WALIDACJA po ETL
    _maybe_call(bootstrap.run_end, "validate")

    log.info("pipeline: OK")


if __name__ == "__main__":
    main()
