# src/energia_prep2/db.py
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg

from .cfg import settings


def _connect_kwargs(app_user: bool = True) -> dict:
    """
    Buduje parametry połączenia do Postgresa zgodnie z wymaganymi polami z Settings:
    - tylko nowe pola (DB_PASSWORD, DB_SUPER_PASSWORD, DB_SSLMODE, itp.)
    - bez żadnych legacy aliasów.
    """
    if app_user:
        user = settings.DB_USER
        password = settings.DB_PASSWORD
        dbname = settings.DB_NAME
    else:
        user = settings.DB_SUPERUSER
        password = settings.DB_SUPER_PASSWORD
        dbname = settings.DB_ADMIN_DB

    if not user or not password:
        raise RuntimeError(
            "Brakuje poświadczeń DB: sprawdź DB_USER/DB_PASSWORD lub DB_SUPERUSER/DB_SUPER_PASSWORD"
        )

    return dict(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=dbname,
        user=user,
        password=password,
        sslmode=settings.DB_SSLMODE,
        application_name="energia-prep-2",
        autocommit=True,
    )


@contextmanager
def get_conn_app() -> Iterator[psycopg.Connection]:
    """Połączenie jako użytkownik aplikacyjny."""
    conn = psycopg.connect(**_connect_kwargs(app_user=True))
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_conn_super_app() -> Iterator[psycopg.Connection]:
    """
    Połączenie jako superuser (do operacji wymagających wyższych uprawnień).
    Wywołujący kod powinien używać warunkowo (np. event trigger).
    """
    conn = psycopg.connect(**_connect_kwargs(app_user=False))
    try:
        yield conn
    finally:
        conn.close()
