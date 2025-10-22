# src/energia_prep2/tasks/csv_import.py
from __future__ import annotations

from pathlib import Path
import pandas as pd

from ..cfg import settings
from ..db import get_conn_app
from ..log import log

# stałe nagłówków CSV (na sztywno)
KONS_COLUMNS = ["Timestamp", "Zużycie"]
PROD_COLUMNS = ["Timestamp", "PV 1MW PP", "PV 1MW WZ", "Wind 1MWp"]

DATA_DIR = Path(settings.DATA_DIR).resolve()
KONS_PATH = DATA_DIR / settings.CSV_KONSUMPCJA
PROD_PATH = DATA_DIR / settings.CSV_PRODUKCJA
CSV_SEP = settings.CSV_SEP
CSV_DEC = settings.CSV_DEC
CSV_DAYFIRST = str(settings.CSV_DAYFIRST).lower() == "true"
TS_PARSE_FMT = "%d.%m.%Y %H:%M"  # dopasowane do plików

def _read_csv_fixed_headers(path: Path, expected_cols: list[str]) -> pd.DataFrame:
    # twarda walidacja liczby kolumn w CSV
    first = pd.read_csv(
        path, sep=CSV_SEP, decimal=CSV_DEC, header=0, encoding="utf-8-sig", nrows=1
    )
    if first.shape[1] != len(expected_cols):
        raise ValueError(
            f"{path.name}: nieprawidłowa liczba kolumn {first.shape[1]} "
            f"(oczekiwano {len(expected_cols)}: {expected_cols})"
        )
    # dokładnie te nazwy – zero dorabiania/przycinania
    return pd.read_csv(
        path,
        sep=CSV_SEP,
        decimal=CSV_DEC,
        header=0,
        names=expected_cols,
        encoding="utf-8-sig",
    )

def _load_konsumpcja(path: Path) -> pd.DataFrame:
    df = _read_csv_fixed_headers(path, KONS_COLUMNS)
    df["ts_local"] = pd.to_datetime(
        df["Timestamp"], dayfirst=CSV_DAYFIRST, format=TS_PARSE_FMT, errors="raise"
    )
    df.rename(columns={"Zużycie": "zuzycie_mw"}, inplace=True)
    df["zuzycie_mw"] = pd.to_numeric(df["zuzycie_mw"], errors="coerce")
    df = df.dropna(subset=["ts_local"]).copy()
    return df[["ts_local", "zuzycie_mw"]]

def _load_produkcja(path: Path) -> pd.DataFrame:
    df = _read_csv_fixed_headers(path, PROD_COLUMNS)
    df["ts_local"] = pd.to_datetime(
        df["Timestamp"], dayfirst=CSV_DAYFIRST, format=TS_PARSE_FMT, errors="raise"
    )
    df.rename(
        columns={
            "PV 1MW PP": "pv_pp_1mwp",
            "PV 1MW WZ": "pv_wz_1mwp",
            "Wind 1MWp": "wind_1mwp",
        },
        inplace=True,
    )
    for c in ["pv_pp_1mwp", "pv_wz_1mwp", "wind_1mwp"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["ts_local"]).copy()
    return df[["ts_local", "pv_pp_1mwp", "pv_wz_1mwp", "wind_1mwp"]]

def _insert_konsumpcja(df: pd.DataFrame) -> int:
    rows = []
    for _, r in df.iterrows():
        rows.append((r["ts_local"].to_pydatetime(),
                     None if pd.isna(r["zuzycie_mw"]) else float(r["zuzycie_mw"])))
    with get_conn_app() as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO input.konsumpcja (ts_local, zuzycie_mw)
            VALUES (%s::timestamp, %s::numeric)
            ON CONFLICT (ts_local) DO UPDATE SET
              zuzycie_mw = EXCLUDED.zuzycie_mw
            """,
            rows,
        )
        conn.commit()
        return cur.rowcount

def _insert_produkcja(df: pd.DataFrame) -> int:
    rows = []
    for _, r in df.iterrows():
        rows.append((
            r["ts_local"].to_pydatetime(),
            None if pd.isna(r["pv_pp_1mwp"]) else float(r["pv_pp_1mwp"]),
            None if pd.isna(r["pv_wz_1mwp"]) else float(r["pv_wz_1mwp"]),
            None if pd.isna(r["wind_1mwp"])  else float(r["wind_1mwp"]),
        ))
    with get_conn_app() as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO input.produkcja (ts_local, pv_pp_1mwp, pv_wz_1mwp, wind_1mwp)
            VALUES (%s::timestamp, %s::numeric, %s::numeric, %s::numeric)
            ON CONFLICT (ts_local) DO UPDATE SET
              pv_pp_1mwp = EXCLUDED.pv_pp_1mwp,
              pv_wz_1mwp = EXCLUDED.pv_wz_1mwp,
              wind_1mwp  = EXCLUDED.wind_1mwp
            """,
            rows,
        )
        conn.commit()
        return cur.rowcount

def import_files() -> None:
    log.info("csv: wczytuję pliki: %s | %s", KONS_PATH, PROD_PATH)
    df_k = _load_konsumpcja(KONS_PATH)
    df_p = _load_produkcja(PROD_PATH)
    ins_k = _insert_konsumpcja(df_k)
    ins_p = _insert_produkcja(df_p)
    log.info("csv: załadowano wierszy (konsumpcja=%s, produkcja=%s)", len(df_k), len(df_p))
    log.info("csv: upsert (konsumpcja=%s, produkcja=%s)", ins_k, ins_p)

def run() -> None:
    import_files()
