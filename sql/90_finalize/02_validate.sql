-- 02_validate.sql
DO $$
DECLARE
  n_date integer;
  v_exists boolean;
BEGIN
  SELECT count(*) INTO n_date FROM input.date_dim;
  RAISE NOTICE 'validate: input.date_dim rows=%', n_date;

  SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='tge' AND table_name='prices') INTO v_exists;
  IF NOT v_exists THEN
    RAISE WARNING 'Missing table: tge.prices';
  END IF;

  SELECT EXISTS (SELECT 1 FROM information_schema.views WHERE table_schema='output' AND table_name='vw_energy_calc_input') INTO v_exists;
  IF NOT v_exists THEN
    RAISE WARNING 'Missing view: output.vw_energy_calc_input';
  END IF;
END$$;
