-- sql/20_tables/01_input_tables.sql

SET client_min_messages = WARNING;

-- Tabela dat (generowana przez util.build_date_dim)
CREATE TABLE IF NOT EXISTS input.date_dim (
  ts_utc      timestamptz PRIMARY KEY,
  granularity text NOT NULL,
  day_of_week int  NOT NULL,
  is_holiday  boolean NOT NULL DEFAULT false
);

-- Konsumpcja energii
CREATE TABLE IF NOT EXISTS input.konsumpcja (
  ts_utc       timestamptz PRIMARY KEY,
  zuzycie_mw   numeric(14,6) NOT NULL,
  inserted_at  timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- Produkcja energii (z CSV profil_prod.csv)
CREATE TABLE IF NOT EXISTS input.produkcja (
  ts_utc       timestamptz PRIMARY KEY,
  -- kolumny źródeł energii, zawsze obecne w CSV
  pv_pp_mw     numeric(14,6),
  pv_wz_mw     numeric(14,6),
  wind_1mwp    numeric(14,6),
  inserted_at  timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- Indeksy pomocnicze
CREATE INDEX IF NOT EXISTS ix_date_dim_ts ON input.date_dim (ts_utc);
CREATE INDEX IF NOT EXISTS ix_konsumpcja_ts ON input.konsumpcja (ts_utc);
CREATE INDEX IF NOT EXISTS ix_produkcja_ts ON input.produkcja (ts_utc);

-- PATCH: upewniamy się, że kolumny z CSV zawsze istnieją (idempotentnie)
ALTER TABLE input.produkcja
  ADD COLUMN IF NOT EXISTS pv_pp_mw  numeric(14,6),
  ADD COLUMN IF NOT EXISTS pv_wz_mw  numeric(14,6),
  ADD COLUMN IF NOT EXISTS wind_1mwp numeric(14,6);

-- Indeksy dodatkowe (idempotentnie)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname='input' AND indexname='ix_produkcja_pv_pp_mw') THEN
    EXECUTE 'CREATE INDEX ix_produkcja_pv_pp_mw ON input.produkcja (pv_pp_mw)';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname='input' AND indexname='ix_produkcja_pv_wz_mw') THEN
    EXECUTE 'CREATE INDEX ix_produkcja_pv_wz_mw ON input.produkcja (pv_wz_mw)';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname='input' AND indexname='ix_produkcja_wind_1mwp') THEN
    EXECUTE 'CREATE INDEX ix_produkcja_wind_1mwp ON input.produkcja (wind_1mwp)';
  END IF;
END$$;
