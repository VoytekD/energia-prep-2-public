from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import List, Tuple, Dict

import pandas as pd
from watchdog.events import FileSystemEventHandler, FileSystemEvent
from watchdog.observers import Observer

from ..cfg import settings
from ..db import get_conn_app
from ..log import log

# stałe nagłówków CSV (nie z ENV)
KONS_COLUMNS: List[str] = ["Timestamp", "Zużycie"]
PROD_COLUMNS: List[str] = ["Timestamp", "PV 1MW PP", "PV 1MW WZ", "Wind 1MWp"]

DATA_DIR = Path(settings.DATA_DIR).resolve()
KONS_PATH = DATA_DIR / settings.CSV_KONSUMPCJA
PROD_PATH = DATA_DIR / settings.CSV_PRODUKCJA
CSV_SEP = settings.CSV_SEP
CSV_DEC = settings.CSV_DEC
CSV_DAYFIRST = str(settings.CSV_DAYFIRST).lower() == "true"
TS_PARSE_FMT = "%d.%m.%Y %H:%M"


def _read_csv_fixed_headers(path: Path, expected_cols: List[str]) -> pd.DataFrame:
    tmp = pd.read_csv(path, sep=CSV_SEP, decimal=CSV_DEC, header=0,
                      encoding="utf-8-sig", dtype=str, nrows=1)
    if tmp.shape[1] != len(expected_cols):
        raise ValueError(
            f"Nieprawidłowa liczba kolumn w {path.name}: {tmp.shape[1]} "
            f"(oczekiwano {len(expected_cols)}: {expected_cols})"
        )
    df = pd.read_csv(path, sep=CSV_SEP, decimal=CSV_DEC, header=0,
                     names=expected_cols, encoding="utf-8-sig")
    return df


def _load_konsumpcja(path: Path) -> pd.DataFrame:
    df = _read_csv_fixed_headers(path, KONS_COLUMNS)
    df["ts_local"] = pd.to_datetime(df["Timestamp"], dayfirst=CSV_DAYFIRST, format=TS_PARSE_FMT, errors="raise")
    df.rename(columns={"Zużycie": "zuzycie_mw"}, inplace=True)
    return df[["ts_local", "zuzycie_mw"]]


def _load_produkcja(path: Path) -> pd.DataFrame:
    df = _read_csv_fixed_headers(path, PROD_COLUMNS)
    df["ts_local"] = pd.to_datetime(df["Timestamp"], dayfirst=CSV_DAYFIRST, format=TS_PARSE_FMT, errors="raise")
    df.rename(columns={
        "PV 1MW PP": "pv_pp_1mwp",
        "PV 1MW WZ": "pv_wz_1mwp",
        "Wind 1MWp": "wind_1mwp",
    }, inplace=True)
    return df[["ts_local", "pv_pp_1mwp", "pv_wz_1mwp", "wind_1mwp"]]


def _resolve_produkcja_columns() -> Tuple[str, str, str]:
    """
    Dopasowuje REALNE nazwy kolumn w input.produkcja do 3 pól produkcyjnych.
    Obsługujemy także warianty 'pv_pp_mw' i 'pv_wz_mw' (które masz w DB).
    """
    candidates: Dict[str, List[str]] = {
        "pp":   ["pv_pp_1mwp", "pv_pp_mw", "pv_pp_1mw", "pv_pp_mwp_1", "pv_1mwp_pp"],
        "wz":   ["pv_wz_1mwp", "pv_wz_mw", "pv_wz_1mw", "pv_wz_mwp_1", "pv_1mwp_wz"],
        "wiatr":["wind_1mwp", "wind_1mw", "wiatr_1mwp", "wiatr_1mw"],
    }
    found: Dict[str, str] = {}

    with get_conn_app() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='input' AND table_name='produkcja'
        """)
        cols = {r[0] for r in cur.fetchall()}

    for key, opts in candidates.items():
        for name in opts:
            if name in cols:
                found[key] = name
                break

    missing = [k for k in ["pp", "wz", "wiatr"] if k not in found]
    if missing:
        log.error("input.produkcja: nie znaleziono kolumn dla: %s. Dostępne kolumny: %s", missing, sorted(cols))
        raise ValueError("Brak kolumn do wstawienia produkcji — sprawdź definicję tabeli input.produkcja")

    return found["pp"], found["wz"], found["wiatr"]


def _insert_konsumpcja(df: pd.DataFrame) -> int:
    inserted = 0
    with get_conn_app() as conn, conn.cursor() as cur:
        for _, r in df.iterrows():
            cur.execute(
                """
                INSERT INTO input.konsumpcja (ts_utc, zuzycie_mw)
                SELECT d.ts_utc, %s
                FROM input.date_dim d
                WHERE d.ts_local = %s
                ON CONFLICT (ts_utc) DO UPDATE SET
                    zuzycie_mw = EXCLUDED.zuzycie_mw
                """,
                (r["zuzycie_mw"], r["ts_local"]),
            )
            inserted += cur.rowcount
        conn.commit()
    return inserted


def _insert_produkcja(df: pd.DataFrame) -> int:
    col_pp, col_wz, col_wind = _resolve_produkcja_columns()
    inserted = 0
    with get_conn_app() as conn, conn.cursor() as cur:
        for _, r in df.iterrows():
            cur.execute(
                f"""
                INSERT INTO input.produkcja (ts_utc, {col_pp}, {col_wz}, {col_wind})
                SELECT d.ts_utc, %s, %s, %s
                FROM input.date_dim d
                WHERE d.ts_local = %s
                ON CONFLICT (ts_utc) DO UPDATE SET
                    {col_pp} = EXCLUDED.{col_pp},
                    {col_wz} = EXCLUDED.{col_wz},
                    {col_wind} = EXCLUDED.{col_wind}
                """,
                (r["pv_pp_1mwp"], r["pv_wz_1mwp"], r["wind_1mwp"], r["ts_local"]),
            )
            inserted += cur.rowcount
        conn.commit()
    return inserted


def import_files() -> None:
    log.info("csv: wczytuję pliki: %s | %s", KONS_PATH, PROD_PATH)
    df_k = _load_konsumpcja(KONS_PATH)
    df_p = _load_produkcja(PROD_PATH)
    ins_k = _insert_konsumpcja(df_k)
    ins_p = _insert_produkcja(df_p)
    log.info("csv: załadowano konsumpcja=%s, produkcja=%s", len(df_k), len(df_p))
    log.info("csv: insert/merge konsumpcja=%s, produkcja=%s", ins_k, ins_p)


def run() -> None:
    log.info("csv: uruchamiam run()")
    import_files()


class _CsvHandler(FileSystemEventHandler):
    def __init__(self, kons_path: Path, prod_path: Path, debounce_s: float = 0.8):
        super().__init__()
        self.kons_path = kons_path.resolve()
        self.prod_path = prod_path.resolve()
        self._last_ts = 0.0
        self._debounce = debounce_s

    def _maybe_run_import(self):
        now = time.time()
        if now - self._last_ts < self._debounce:
            return
        self._last_ts = now
        try:
            import_files()
        except Exception as e:
            log.exception("csv: błąd importu po zmianie plików: %s", e)

    def on_modified(self, event: FileSystemEvent):
        if event.is_directory:
            return
        p = Path(event.src_path).resolve()
        if p == self.kons_path or p == self.prod_path:
            log.info("csv: wykryto modyfikację: %s", p)
            self._maybe_run_import()

    def on_created(self, event: FileSystemEvent):
        self.on_modified(event)


def run_watch() -> None:
    log.info("csv: watcher start – obserwuję katalog: %s", DATA_DIR)
    handler = _CsvHandler(KONS_PATH, PROD_PATH, debounce_s=0.8)
    observer = Observer()
    observer.schedule(handler, str(DATA_DIR), recursive=False)
    observer.start()
    try:
        import_files()
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--watch":
        run_watch()
    else:
        run()
