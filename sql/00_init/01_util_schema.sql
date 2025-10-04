-- 01_util_schema.sql
CREATE SCHEMA IF NOT EXISTS util;

-- Tabela czarna lista schematów, dla których pominiemy automatyczne triggery
CREATE TABLE IF NOT EXISTS util.trigger_blacklist (schema_name text PRIMARY KEY);
INSERT INTO util.trigger_blacklist(schema_name) VALUES
  ('public'),('output'),('util'),('information_schema'),('pg_catalog'),('pg_toast')
ON CONFLICT DO NOTHING;

-- Funkcja: czy schemat jest celem
CREATE OR REPLACE FUNCTION util.fn_is_target_schema(p_schema text)
RETURNS boolean
LANGUAGE plpgsql AS $$
BEGIN
  RETURN NOT EXISTS (SELECT 1 FROM util.trigger_blacklist b WHERE b.schema_name = p_schema);
END$$;

-- Helper: aktualizacja updated_at na update (jeśli kolumna istnieje)
CREATE OR REPLACE FUNCTION util.fn_set_updated_at()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = TG_TABLE_SCHEMA AND table_name = TG_TABLE_NAME
                 AND column_name = 'updated_at') THEN
      NEW.updated_at := now() AT TIME ZONE 'UTC';
    END IF;
  END IF;
  RETURN NEW;
END$$;
