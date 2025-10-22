-- sql/20_tables/01_input_tables.sql

SET client_min_messages = WARNING;

CREATE SCHEMA IF NOT EXISTS input;

-- ── Tabela dat (oś po UTC; używamy tylko ts_utc) ─────────────────────────────
CREATE TABLE IF NOT EXISTS input.date_dim (
  ts_utc       timestamptz PRIMARY KEY,
  ts_local     timestamp    NOT NULL,
  year         integer      NOT NULL,
  month        integer      NOT NULL,
  day          integer      NOT NULL,
  dow          integer      NOT NULL,  -- 0=Sun..6=Sat (EXTRACT(DOW) po UTC)
  hour         integer      NOT NULL,  -- 0..23 (EXTRACT(HOUR) po UTC)
  is_workday   boolean      NOT NULL,
  is_holiday   boolean      NOT NULL DEFAULT false,
  holiday_name text,
  granularity  text         NOT NULL DEFAULT 'H',
  dt_h         double precision NOT NULL DEFAULT 1.0
);
CREATE INDEX IF NOT EXISTS ix_date_dim_ts ON input.date_dim (ts_utc);
CREATE INDEX IF NOT EXISTS ix_date_dim_ts_local ON input.date_dim (ts_local);

-- ── Konsumpcja (mapowanie po lokalnym czasie) ───────────────────────────────
CREATE TABLE IF NOT EXISTS input.konsumpcja (
  ts_local     timestamp PRIMARY KEY,
  zuzycie_mw   numeric(14,6) NOT NULL,
  inserted_at  timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_konsumpcja_ts ON input.konsumpcja (ts_local);

-- ── Produkcja (3 strumienie; nazwy na sztywno) ──────────────────────────────
CREATE TABLE IF NOT EXISTS input.produkcja (
  ts_local     timestamp PRIMARY KEY,
  pv_pp_1mwp   numeric(14,6),
  pv_wz_1mwp   numeric(14,6),
  wind_1mwp    numeric(14,6),
  inserted_at  timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_produkcja_ts ON input.produkcja (ts_local);