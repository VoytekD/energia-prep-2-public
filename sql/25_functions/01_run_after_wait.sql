BEGIN;

CREATE SCHEMA IF NOT EXISTS util;

-- 1) Czekaj do końca walidacji (status 'done') i zwróć calc_id
--    p_not_before  → jeśli podasz, czekamy na pierwszy 'done' z finished_at >= p_not_before
--    p_timeout_ms  → maksymalny czas czekania (ms)
--    p_interval_ms → jak często sprawdzamy (ms)
--    p_fail_on_error → po timeout: true = wyjątek; false = fallback (ostatni zakończony persist)
CREATE OR REPLACE FUNCTION util.wait_final_stage_done(
  p_not_before   timestamptz DEFAULT NULL,
  p_timeout_ms   integer     DEFAULT 60000,
  p_interval_ms  integer     DEFAULT 300,
  p_fail_on_error boolean    DEFAULT true
) RETURNS uuid
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
  v_deadline     timestamptz := clock_timestamp() + (COALESCE(p_timeout_ms,0)::text || ' milliseconds')::interval;
  v_job_done_at  timestamptz;
  v_calc_id      uuid;
BEGIN
  LOOP
    -- A) Szukamy ZAKOŃCZONEGO joba ('done') po walidacji
    IF p_not_before IS NULL THEN
      -- najświeższy 'done'
      SELECT j.finished_at
        INTO v_job_done_at
      FROM output.calc_job_queue j
      WHERE j.status = 'done'
      ORDER BY j.finished_at DESC NULLS LAST
      LIMIT 1;
    ELSE
      -- pierwszy 'done' z finished_at >= p_not_before
      SELECT j.finished_at
        INTO v_job_done_at
      FROM output.calc_job_queue j
      WHERE j.status = 'done'
        AND j.finished_at >= p_not_before
      ORDER BY j.finished_at ASC NULLS LAST
      LIMIT 1;
    END IF;

    -- B) Jeśli mamy 'done', dopasuj calc_id z etapu FINAL (Stage 05)
    IF v_job_done_at IS NOT NULL THEN
      SELECT s.calc_id
        INTO v_calc_id
      FROM output.calc_job_stage s
      WHERE s.stage = '05' AND s.status = 'done'
        AND s.finished_at <= v_job_done_at
      ORDER BY s.finished_at DESC NULLS LAST
      LIMIT 1;

      IF v_calc_id IS NOT NULL THEN
        RETURN v_calc_id;
      END IF;
    END IF;

    -- C) Timeout?
    IF clock_timestamp() >= v_deadline THEN
      IF p_fail_on_error THEN
        RAISE EXCEPTION 'wait_final_stage_done: timeout (not_before=%, timeout_ms=%, interval_ms=%)',
          p_not_before, p_timeout_ms, p_interval_ms;
      END IF;

      -- Fallback: najświeższy zakończony FINAL (Stage 05)
      SELECT s.calc_id
        INTO v_calc_id
      FROM output.calc_job_stage s
      WHERE s.stage = '05' AND s.status = 'done'
      ORDER BY s.finished_at DESC NULLS LAST
      LIMIT 1;

      IF v_calc_id IS NULL THEN
        RAISE EXCEPTION 'wait_final_stage_done: brak zakończonego etapu 05.';
      END IF;

      RETURN v_calc_id;
    END IF;

    -- D) Poczekaj i sprawdź ponownie
    PERFORM pg_sleep(GREATEST(1, COALESCE(p_interval_ms, 100))::numeric / 1000.0);
  END LOOP;
END;
$$;

-- 2) Wrapper do paneli: zapamiętuje start zapytania i czeka na 'done' z finished_at >= start
CREATE OR REPLACE FUNCTION util.run_after_wait(
    sql_text       text,
    max_wait_ms    integer DEFAULT 120000,
    poll_ms        integer DEFAULT 200,
    raise_on_error boolean DEFAULT true
)
RETURNS SETOF record
LANGUAGE plpgsql
AS $$
DECLARE
  cid uuid;
  v_started_at timestamptz;
BEGIN
  PERFORM set_config('statement_timeout', '0', true);
  v_started_at := clock_timestamp();

  SELECT util.wait_final_stage_done(v_started_at, max_wait_ms, poll_ms, raise_on_error)::uuid
    INTO cid;

  IF sql_text IS NULL OR btrim(sql_text) = '' THEN
    RAISE EXCEPTION 'sql_text must not be empty';
  END IF;
  IF sql_text !~* '^\s*select\s' THEN
    RAISE EXCEPTION 'Only SELECT queries are allowed';
  END IF;

  RETURN QUERY EXECUTE sql_text USING cid;
END;
$$;

GRANT EXECUTE ON FUNCTION util.wait_final_stage_done(timestamptz, integer, integer, boolean) TO voytek;
GRANT EXECUTE ON FUNCTION util.run_after_wait(text, integer, integer, boolean)              TO voytek;

COMMIT;
