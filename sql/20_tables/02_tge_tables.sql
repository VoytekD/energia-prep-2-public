-- ─────────────────────────────────────────────────────────────────────
-- INPUT: godzinowe ceny TGE 1:1 z API (fixing_i / fixing_ii)
-- ─────────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS input;

CREATE TABLE IF NOT EXISTS input.ceny_godzinowe (
    ts_utc            timestamptz PRIMARY KEY,      -- z pola "date" w API (UTC)
    fixing_i_price    numeric(12,4),
    fixing_i_volume   numeric(14,4),
    fixing_ii_price   numeric(12,4),
    fixing_ii_volume  numeric(14,4),
    inserted_at       timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now()
);

-- aktualizacja updated_at przy UPDATE
CREATE OR REPLACE FUNCTION input.set_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tg_input_ceny_updated ON input.ceny_godzinowe;
CREATE TRIGGER tg_input_ceny_updated
BEFORE UPDATE ON input.ceny_godzinowe
FOR EACH ROW
EXECUTE FUNCTION input.set_updated_at();

-- uprawnienia dla użytkownika aplikacyjnego (jeśli stosujesz tę rolę)
GRANT USAGE ON SCHEMA input TO "voytek";
GRANT SELECT, INSERT, UPDATE, DELETE ON input.ceny_godzinowe TO "voytek";

-- indeks pomocniczy po dacie (opcjonalnie)
CREATE INDEX IF NOT EXISTS idx_input_ceny_godzinowe_ts ON input.ceny_godzinowe (ts_utc);
