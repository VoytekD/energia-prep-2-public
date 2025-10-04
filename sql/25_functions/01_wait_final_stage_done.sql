-- NEW semantics: wait → return latest calc_id (uuid)
-- Breaking change: replaces old RETURNS TABLE(status text)

CREATE SCHEMA IF NOT EXISTS utils;


CREATE OR REPLACE FUNCTION utils.wait_final_stage_done(
  p_params_ts     timestamptz DEFAULT NULL,
  p_timeout_ms    integer     DEFAULT 60000,  -- 60 s
  p_interval_ms   integer     DEFAULT 300,    -- 0.3 s
  p_fail_on_error boolean     DEFAULT false
) RETURNS uuid
LANGUAGE plpgsql
STABLE
AS $func$
DECLARE
  v_params_ts timestamptz;
  v_deadline  timestamptz := clock_timestamp() + (p_timeout_ms::text || ' milliseconds')::interval;
  v_status    text;
  v_calc_id   uuid;
BEGIN
  -- Ustal params_ts: jeśli nie podano, weź najświeższy z kolejki.
  IF p_params_ts IS NULL THEN
    SELECT q.params_ts
      INTO v_params_ts
    FROM output.calc_job_queue q
    ORDER BY COALESCE(q.finished_at, q.started_at, q.created_at) DESC
    LIMIT 1;

    IF v_params_ts IS NULL THEN
      RAISE EXCEPTION 'wait_final_stage_done: brak rekordów w calc_job_queue.';
    END IF;
  ELSE
    v_params_ts := p_params_ts;
  END IF;

  -- Pętla: czekamy na finalny status (done/error) lub timeout.
  LOOP
    SELECT q.status
      INTO v_status
    FROM output.calc_job_queue q
    WHERE q.params_ts = v_params_ts
    ORDER BY COALESCE(q.finished_at, q.started_at, q.created_at) DESC
    LIMIT 1;

    IF v_status IN ('done','error') THEN
      EXIT;
    END IF;

    IF clock_timestamp() >= v_deadline THEN
      RAISE EXCEPTION
        'wait_final_stage_done: timeout po % ms (params_ts=%; status=%).',
        p_timeout_ms, v_params_ts, COALESCE(v_status, 'NULL');
    END IF;

    PERFORM pg_sleep(GREATEST(10, LEAST(p_interval_ms, 5000)) / 1000.0);
  END LOOP;

  -- Jeśli ERROR i życzysz sobie failować — podnieś wyjątek.
  IF v_status = 'error' AND p_fail_on_error THEN
    RAISE EXCEPTION 'wait_final_stage_done: job w stanie ERROR (params_ts=%).', v_params_ts;
  END IF;

  -- Najnowszy zakończony persist(03) → jego calc_id zwracamy
  SELECT s.calc_id
    INTO v_calc_id
  FROM output.calc_job_stage s
  WHERE s.stage = '03' AND s.status = 'done'
  ORDER BY s.finished_at DESC
  LIMIT 1;

  IF v_calc_id IS NULL THEN
    RAISE EXCEPTION 'wait_final_stage_done: nie znaleziono zakończonego etapu 03.';
  END IF;

  RETURN v_calc_id;
END;
$func$;
