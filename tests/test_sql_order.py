# tests/test_sql_order.py
# Sprawdza deterministyczną kolejność plików SQL zbieranych przez tasks.bootstrap._iter_sql_files
from pathlib import Path
import importlib

def test_iter_sql_files_order(tmp_path, monkeypatch):
    # Zbuduj tymczasowe drzewo katalogów z pustymi .sql
    root = tmp_path / "sql"
    (root / "00_init").mkdir(parents=True)
    (root / "10_schemas").mkdir()
    (root / "20_tables").mkdir()
    (root / "30_views (depreciated)").mkdir()
    (root / "40_triggers").mkdir()
    (root / "90_finalize").mkdir()

    # Utwórz kilka plików w każdej sekcji
    (root / "00_init" / "00_prechecks.sql").write_text("-- a", encoding="utf-8")
    (root / "00_init" / "01_util_schema.sql").write_text("-- b", encoding="utf-8")
    (root / "10_schemas" / "01_bootstrap_schemas.sql").write_text("-- c", encoding="utf-8")
    (root / "20_tables" / "01_input_tables.sql").write_text("-- d", encoding="utf-8")
    (root / "20_tables" / "02_tge_tables.sql").write_text("-- e", encoding="utf-8")
    (root / "30_views (depreciated)" / "01_output_vw_energy_calc_input.sql").write_text("-- f", encoding="utf-8")
    (root / "40_triggers" / "01_row_triggers.sql").write_text("-- g", encoding="utf-8")
    (root / "90_finalize" / "02_validate.sql").write_text("-- h", encoding="utf-8")

    # Podmień settings.SQL_DIR na tymczasowy
    from energia_prep2 import cfg
    importlib.reload(cfg)
    monkeypatch.setenv("SQL_DIR", str(root))
    importlib.reload(cfg)

    # Załaduj bootstrap z nowym settings i pobierz listę plików
    from energia_prep2.tasks import bootstrap
    importlib.reload(bootstrap)
    files = bootstrap._iter_sql_files()

    # Oczekiwana kolejność „po folderach”: 00_init → 10_schemas → 20_tables → 30_views (depreciated) → 40_triggers → 90_finalize
    rel = [str(p).split(str(root) + "/")[1] for p in files]
    assert rel == [
        "00_init/00_prechecks.sql",
        "00_init/01_util_schema.sql",
        "10_schemas/01_bootstrap_schemas.sql",
        "20_tables/01_input_tables.sql",
        "20_tables/02_tge_tables.sql",
        "30_views (depreciated)/01_output_vw_energy_calc_input.sql",
        "40_triggers/01_row_triggers.sql",
        "90_finalize/02_validate.sql",
    ]
