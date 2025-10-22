from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Tuple
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import requests

from ..cfg import settings
from ..db import get_conn_app
from ..log import log

# Endpoint bazowy z .env / cfg.py, np.:
# TGE_URL_BASE=https://energy-api.instrat.pl/api/prices/energy_price_rdn_hourly
BASE = settings.TGE_URL_BASE

# Wymagane przez API nagłówki (jak w starym skrypcie)
HEADERS = {
    "User-Agent": "energia-prep/1.0 (+local)",
    "Accept": "application/json",
}

# Docelowa tabela w schemacie input
TABLE_FULL = "input.ceny_godzinowe"


def _with_params(base: str, **params) -> str:
    """Dokłada / nadpisuje parametry w URL bez dublowania (np. all=1, days=365)."""
    parts = urlparse(base)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q.update({k: str(v) for k, v in params.items() if v is not None})
    new_qs = urlencode(q)
    return urlunparse(parts._replace(query=new_qs))


def _url_full() -> str:
    """Pełna historia – ZAWSZE z all=1."""
    return _with_params(BASE, all=1)


def _url_days(days: int) -> str:
    """Aktualizacja – ostatnie N dni (np. 365)."""
    return _with_params(BASE, days=int(days))


def _rows_from_payload(payload: Any) -> List[Tuple[datetime, float | None, float | None, float | None, float | None]]:
    """
    Oczekujemy formatu:
      {
        "date": "YYYY-MM-DDTHH:00:00Z",
        "fixing_i":  {"price": <float>, "volume": <float>},
        "fixing_ii": {"price": <float>, "volume": <float>}
      }
    Zwracamy listę:
      (ts_utc, fixing_i_price, fixing_i_volume, fixing_ii_price, fixing_ii_volume)
    """
    if isinstance(payload, dict):
        payload = payload.get("data", [])

    rows: List[Tuple[datetime, float | None, float | None, float | None, float | None]] = []
    if not isinstance(payload, list):
        return rows

    for rec in payload:
        if not isinstance(rec, dict) or "date" not in rec:
            continue

        try:
            ts_utc = datetime.fromisoformat(str(rec["date"]).replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            continue

        fi = rec.get("fixing_i") or {}
        fii = rec.get("fixing_ii") or {}

        def _to_float(x):
            try:
                return float(x) if x is not None else None
            except Exception:
                return None

        fi_price = _to_float(fi.get("price"))
        fi_vol = _to_float(fi.get("volume"))
        fii_price = _to_float(fii.get("price"))
        fii_vol = _to_float(fii.get("volume"))

        rows.append((ts_utc, fi_price, fi_vol, fii_price, fii_vol))

    rows.sort(key=lambda r: r[0])
    return rows


def _upsert_rows(rows: List[Tuple[datetime, float | None, float | None, float | None, float | None]]) -> int:
    """UPSERT do input.ceny_godzinowe 1:1 z API."""
    if not rows:
        return 0

    inserted = 0
    with get_conn_app() as conn, conn.cursor() as cur:
        sql = f"""
            INSERT INTO {TABLE_FULL}
                (ts_utc, fixing_i_price, fixing_i_volume, fixing_ii_price, fixing_ii_volume)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ts_utc) DO UPDATE SET
                fixing_i_price   = EXCLUDED.fixing_i_price,
                fixing_i_volume  = EXCLUDED.fixing_i_volume,
                fixing_ii_price  = EXCLUDED.fixing_ii_price,
                fixing_ii_volume = EXCLUDED.fixing_ii_volume
        """
        cur.executemany(sql, rows)
        inserted = cur.rowcount
        conn.commit()
    return inserted


def _fetch(url: str) -> Any:
    log.info("tge: fetch %s", url)
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json()


def full_import() -> None:
    """Pełna historia – używamy ?all=1."""
    url = _url_full()
    try:
        payload = _fetch(url)
    except Exception as e:
        log.error("tge: błąd krytyczny przy pełnym imporcie: %s", e)
        return

    rows = _rows_from_payload(payload)
    n = _upsert_rows(rows)

    if rows:
        log.info("tge: pełny import OK, upsert=%d, zakres=%s → %s",
                 n, rows[0][0].isoformat(), rows[-1][0].isoformat())
    else:
        log.info("tge: pełny import — brak danych")


def update_import() -> None:
    """Aktualizacja – ostatnie settings.TGE_HISTORY_DAYS dni przez ?days=..."""
    days = int(settings.TGE_HISTORY_DAYS)
    url = _url_days(days)
    try:
        payload = _fetch(url)
    except Exception as e:
        log.error("tge: błąd update: %s", e)
        return

    rows = _rows_from_payload(payload)
    n = _upsert_rows(rows)

    if rows:
        log.info("tge: update OK, upsert=%d, zakres=%s → %s",
                 n, rows[0][0].isoformat(), rows[-1][0].isoformat())
    else:
        log.info("tge: update — brak nowych danych")
