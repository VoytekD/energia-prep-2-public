-- sql/20_tables/04_support_tables.sql
-- Struktury wsparcia przeniesione z ensure_* (idempotentnie).

BEGIN;

CREATE SCHEMA IF NOT EXISTS output;

-- 1) SNAPSHOT PARAMETRÓW (dokładnie jak w ensure_params_snapshot_table)
CREATE TABLE IF NOT EXISTS output.params_snapshot_calc (
  calc_id      uuid        NOT NULL,
  params_ts    timestamptz NOT NULL,
  source_table text        NOT NULL,
  row          jsonb       NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (calc_id, source_table)
);
CREATE INDEX IF NOT EXISTS idx_params_snapshot_calc__calc_created
  ON output.params_snapshot_calc (calc_id, created_at DESC);

-- 2) KOLEJKA JOBÓW (mirror z ensure_calc_job_queue)
CREATE TABLE IF NOT EXISTS output.calc_job_queue (
  job_id      uuid PRIMARY KEY
              DEFAULT (md5(random()::text || clock_timestamp()::text)::uuid),
  params_ts   timestamptz NOT NULL,
  status      text        NOT NULL DEFAULT 'queued'
                CHECK (status IN ('queued','running','done','error')),
  created_at  timestamptz NOT NULL DEFAULT now(),
  started_at  timestamptz,
  finished_at timestamptz,
  error       text
);
CREATE INDEX IF NOT EXISTS ix_calc_job_queue_status_created
  ON output.calc_job_queue (status, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_calc_job_queue_params_ts
  ON output.calc_job_queue (params_ts, created_at DESC);

-- 3) STATUSY STAGE’ÓW (mirror z ensure_calc_status_structures)
CREATE TABLE IF NOT EXISTS output.calc_job_stage (
  id           bigserial PRIMARY KEY,
  job_id       uuid,
  calc_id      uuid        NOT NULL,
  stage        text        NOT NULL CHECK (stage IN ('00','01','02','03','04')),
  status       text        NOT NULL CHECK (status IN ('pending','running','done','error')),
  created_at   timestamptz NOT NULL DEFAULT now(),
  started_at   timestamptz,
  finished_at  timestamptz,
  error        text,
  UNIQUE (calc_id, stage)
);
CREATE INDEX IF NOT EXISTS ix_calc_job_stage_calc_stage
  ON output.calc_job_stage (calc_id, stage, status, finished_at DESC);

COMMIT;
