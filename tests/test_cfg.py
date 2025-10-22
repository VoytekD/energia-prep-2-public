# tests/test_cfg.py
# Testuje wczytanie środowiska do Settings (pydantic-settings).
import importlib
import os

def test_settings_loads(monkeypatch):
    # Ustaw minimalny zestaw zmiennych zanim zaimportujemy moduł
    monkeypatch.setenv("DB_HOST", "192.168.1.12")
    monkeypatch.setenv("DB_PORT", "5432")
    monkeypatch.setenv("DB_NAME", "energia")
    monkeypatch.setenv("DB_USER", "voytek")
    monkeypatch.setenv("DB_PASSWORD", "secret")
    monkeypatch.setenv("TZ", "Europe/Warsaw")

    # Import po ustawieniu env (i reload na wszelki wypadek)
    from energia_prep2 import cfg
    importlib.reload(cfg)

    s = cfg.settings
    assert s.DB_HOST == "192.168.1.12"
    assert s.DB_PORT == 5432
    assert s.DB_NAME == "energia"
    assert s.DB_USER == "voytek"
    assert s.DB_PASSWORD == "secret"
    assert s.SQL_DIR == "/app/sql"      # domyślna wartość
    assert s.TZ == "Europe/Warsaw"
