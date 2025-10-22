-- ======================================================================
-- 03_params_tables.sql
-- Tworzenie formularzy params.* z configu:
--  • payload (jsonb) = źródło prawdy
--  • kolumny GENERATED STORED wyprowadzone z payload wg typów JSON
--  • fail-fast: brak/pusty seed GUC ⇒ błąd z komunikatem
--  • walidacja kluczowych formularzy (form_zmienne, form_bess_param, form_par_arbitrazu)
-- ======================================================================

CREATE SCHEMA IF NOT EXISTS params;

-- Uwaga: zakładamy, że util.fn_set_updated_at() jest już utworzona w 00_init/01_util_schema.sql

CREATE OR REPLACE FUNCTION params.ensure_form_from_guc(p_slug text, p_schema text, p_table text)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_seed     jsonb;
  r          record;
  vtxt       text;
  qualified  text := quote_ident(p_schema) || '.' || quote_ident(p_table);
  trig_name  text := 't_' || p_table || '_updated_at';
  has_row    boolean;
  col_sql    text;
BEGIN
  -- 1) Tabela bazowa (payload jako źródło prawdy)
  EXECUTE format($f$
    CREATE TABLE IF NOT EXISTS %s (
      inserted_at   timestamptz NOT NULL DEFAULT now(),
      updated_at    timestamptz NOT NULL DEFAULT now(),
      payload       jsonb       NOT NULL,
      CONSTRAINT %I CHECK (jsonb_typeof(payload) = 'object')
    )$f$, qualified, 'chk_'||p_table||'_payload_obj');

  -- 2) Trigger updated_at (jeśli brak)
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE t.tgname = trig_name
      AND n.nspname = p_schema
      AND c.relname = p_table
  ) THEN
    EXECUTE format(
      'CREATE TRIGGER %I BEFORE UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION util.fn_set_updated_at()',
      trig_name, qualified
    );
  END IF;

  -- 3) Seed z GUC (ładowany przez bootstrap W TEJ SAMEJ SESJI)
  BEGIN
    EXECUTE format('SELECT current_setting(%L, true)::jsonb', 'energia.'||p_slug||'_json') INTO v_seed;
  EXCEPTION WHEN others THEN
    v_seed := NULL;
  END;
  IF v_seed IS NULL OR jsonb_typeof(v_seed) <> 'object' OR v_seed = '{}'::jsonb THEN
    RAISE EXCEPTION
      'params.ensure_form_from_guc: missing or empty GUC energia.%_json for %.%',
      p_slug, p_schema, p_table;
  END IF;

  -- 3b) Walidacja wymaganych kluczy dla kluczowych formularzy
  IF p_slug = 'form_zmienne' THEN
    IF EXISTS (
      SELECT 1 FROM (VALUES
        ('zmiany_konsumpcji'),('moc_pv_pp'),('moc_pv_wz'),
        ('moc_wiatr'),('emax'),('procent_arbitrazu')
      ) AS req(k) WHERE NOT (v_seed ? req.k)
    ) THEN
      RAISE EXCEPTION 'form_zmienne: missing required keys in energia.form_zmienne_json';
    END IF;

  ELSIF p_slug = 'form_bess_param' THEN
    IF NOT (v_seed ? 'bess_lambda_month') THEN
      RAISE EXCEPTION 'form_bess_param: missing key "bess_lambda_month" in energia.form_bess_param_json';
    END IF;

  ELSIF p_slug = 'form_par_arbitrazu' THEN
    -- (A) muszą istnieć wszystkie klucze
    IF EXISTS (
      SELECT 1 FROM (VALUES
        ('base_min_profit_pln_mwh'),('cycles_per_day'),('allow_carry_over'),('force_order'),
        ('bonus_ch_window'),('bonus_dis_window'),('bonus_low_soc_ch'),('bonus_high_soc_dis'),
        ('bonus_hrs_ch'),('bonus_hrs_dis'),('bonus_hrs_ch_free'),('bonus_hrs_dis_free'),
        ('soc_high_threshold'),('soc_low_threshold'),('arbi_dis_to_load')
      ) AS req(k) WHERE NOT (v_seed ? req.k)
    ) THEN
      RAISE EXCEPTION 'form_par_arbitrazu: missing required keys in energia.form_par_arbitrazu_json';
    END IF;

    -- (B) LISTY godzin muszą być JSON array (nie string)
    IF EXISTS (
      SELECT 1 FROM (VALUES
        ('bonus_hrs_ch'),('bonus_hrs_dis'),('bonus_hrs_ch_free'),('bonus_hrs_dis_free')
      ) AS a(k)
      WHERE jsonb_typeof(v_seed->a.k) <> 'array'
    ) THEN
      RAISE EXCEPTION 'form_par_arbitrazu: hour lists must be JSON arrays ([0..23]), not strings';
    END IF;
  END IF;

  -- 4) Kolumny GENERATED STORED wyprowadzone z payload (typ po typie JSON)
  FOR r IN SELECT key, value, jsonb_typeof(value) AS jtype FROM jsonb_each(v_seed)
  LOOP
    vtxt := (r.value)::text;

    IF r.jtype = 'number' THEN
      IF vtxt ~ '^[+-]?\d+$' THEN
        -- integer
        EXECUTE format(
          'ALTER TABLE %s ADD COLUMN IF NOT EXISTS %I integer GENERATED ALWAYS AS ((payload->>%L)::int) STORED',
          qualified, r.key, r.key
        );
      ELSE
        -- numeric (z częścią ułamkową)
        EXECUTE format(
          'ALTER TABLE %s ADD COLUMN IF NOT EXISTS %I numeric GENERATED ALWAYS AS ((payload->>%L)::numeric) STORED',
          qualified, r.key, r.key
        );
      END IF;

    ELSIF r.jtype = 'boolean' THEN
      EXECUTE format(
        'ALTER TABLE %s ADD COLUMN IF NOT EXISTS %I boolean GENERATED ALWAYS AS ((payload->>%L)::boolean) STORED',
        qualified, r.key, r.key
      );

    ELSIF r.jtype IN ('array','object') THEN
      -- dla tablic/obiektów używamy -> (nie ->>)
      EXECUTE format(
        'ALTER TABLE %s ADD COLUMN IF NOT EXISTS %I jsonb GENERATED ALWAYS AS ((payload->%L)::jsonb) STORED',
        qualified, r.key, r.key
      );

    ELSE
      -- tekst
      EXECUTE format(
        'ALTER TABLE %s ADD COLUMN IF NOT EXISTS %I text GENERATED ALWAYS AS ((payload->>%L)) STORED',
        qualified, r.key, r.key
      );
    END IF;
  END LOOP;

  -- 5) Jednorazowy insert (payload) jeśli tabela pusta
  EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s)', qualified) INTO has_row;
  IF NOT has_row THEN
    EXECUTE format('INSERT INTO %s (payload) VALUES ($1)', qualified) USING v_seed;
  END IF;
END;
$$;

-- ======================================================================
-- Wywołania dla wszystkich formularzy (spójne z bootstrapem)
-- ======================================================================
SELECT params.ensure_form_from_guc('form_zmienne',              'params','form_zmienne');
SELECT params.ensure_form_from_guc('form_bess_param',           'params','form_bess_param');
SELECT params.ensure_form_from_guc('form_parametry_klienta',    'params','form_parametry_klienta');
SELECT params.ensure_form_from_guc('form_oplaty_dystrybucyjne', 'params','form_oplaty_dystrybucyjne');
SELECT params.ensure_form_from_guc('form_oplaty_dyst_sched',    'params','form_oplaty_dyst_sched');
SELECT params.ensure_form_from_guc('form_oplaty_systemowe',     'params','form_oplaty_systemowe');
SELECT params.ensure_form_from_guc('form_oplaty_sys_sched',     'params','form_oplaty_sys_sched');
SELECT params.ensure_form_from_guc('form_oplaty_sys_kparam',    'params','form_oplaty_sys_kparam');
SELECT params.ensure_form_from_guc('form_par_kontraktu',        'params','form_par_kontraktu');
SELECT params.ensure_form_from_guc('form_oplaty_fiskalne',      'params','form_oplaty_fiskalne');
SELECT params.ensure_form_from_guc('form_lcoe',                 'params','form_lcoe');
SELECT params.ensure_form_from_guc('form_par_arbitrazu',        'params','form_par_arbitrazu');
