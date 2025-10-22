from __future__ import annotations

from datetime import date
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    # ── DB (wymagane)
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_SUPERUSER: str
    DB_SUPER_PASSWORD: str
    DB_ADMIN_DB: str
    DB_SSLMODE: str

    # ── LOG / TZ (wymagane)
    LOG_LEVEL: str
    TZ: str

    # ── Ścieżki / pliki (wymagane)
    SQL_DIR: str
    DATA_DIR: str
    FORM_CONFIG: str

    # ── Zakres dat (wymagane – ISO: YYYY-MM-DD)
    DATE_START: date
    DATE_END: date

    # ── CSV (częściowo z domyślnymi; nagłówki są stałe)
    CSV_KONSUMPCJA: str
    CSV_PRODUKCJA: str
    CSV_SEP: str
    CSV_DEC: str
    CSV_DAYFIRST: bool

    # Stałe nagłówki – nie wymagamy z ENV
    CSV_KONS_HEADERS: str = Field(default="Timestamp;Zużycie")
    CSV_PROD_HEADERS: str = Field(default="Timestamp;PV 1MW PP;PV 1MW WZ;Wind 1MWp")

    # ── TGE (wymagane)
    TGE_HISTORY_DAYS: int
    TGE_URL_BASE: str
    TGE_SOURCE: str

    # ── API (wymagane)
    API_HOST: str
    API_PORT: int
    API_KEY: str
    ALLOW_ORIGINS: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )


settings = Settings()
