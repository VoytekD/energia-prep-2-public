-- 02_validate.sql (UTC-only, bez widoków 30_views (depreciated))
DO $$
DECLARE
  n_date bigint;
  n_price bigint;
  n_kons bigint;
  n_prod  bigint;
  has_seed boolean;
BEGIN
  -- 1) Oś czasu musi istnieć i mieć wiersze
  SELECT count(*) INTO n_date FROM input.date_dim;
  IF n_date <= 0 THEN
    RAISE EXCEPTION 'validate: input.date_dim is empty (rows=%).', n_date;
  ELSE
    RAISE NOTICE 'validate: input.date_dim rows=%', n_date;
  END IF;

  -- 2) Ceny TGE: używamy input.ceny_godzinowe (nie tge.prices)
  SELECT count(*) INTO n_price FROM input.ceny_godzinowe;
  IF n_price <= 0 THEN
    RAISE WARNING 'validate: input.ceny_godzinowe has no rows.';
  ELSE
    RAISE NOTICE 'validate: input.ceny_godzinowe rows=%', n_price;
  END IF;

  -- 3) CSV: podstawowe serie wejściowe (jeśli już coś załadowano)
  PERFORM 1 FROM information_schema.tables
   WHERE table_schema='input' AND table_name='konsumpcja';
  GET DIAGNOSTICS n_kons = ROW_COUNT;
  IF n_kons > 0 THEN
    SELECT count(*) INTO n_kons FROM input.konsumpcja;
    RAISE NOTICE 'validate: input.konsumpcja rows=%', n_kons;
  END IF;

  PERFORM 1 FROM information_schema.tables
   WHERE table_schema='input' AND table_name='produkcja';
  GET DIAGNOSTICS n_prod = ROW_COUNT;
  IF n_prod > 0 THEN
    SELECT count(*) INTO n_prod FROM input.produkcja;
    RAISE NOTICE 'validate: input.produkcja rows=%', n_prod;
  END IF;

  -- 4) Seed w params.* – sprawdź, że istnieje co najmniej 1 wiersz w kluczowych formach
  SELECT EXISTS (SELECT 1 FROM params.form_zmienne LIMIT 1)
    AND EXISTS (SELECT 1 FROM params.form_bess_param LIMIT 1)
    AND EXISTS (SELECT 1 FROM params.form_parametry_klienta LIMIT 1)
    AND EXISTS (SELECT 1 FROM params.form_par_arbitrazu LIMIT 1)
  INTO has_seed;

  IF NOT has_seed THEN
    RAISE WARNING 'validate: missing seed rows in one or more params.* tables.';
  ELSE
    RAISE NOTICE 'validate: params.* seed present.';
  END IF;
END$$;
