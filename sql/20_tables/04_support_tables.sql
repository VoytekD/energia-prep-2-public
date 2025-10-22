-- sql/20_tables/04_support_tables.sql
-- Struktury wsparcia przeniesione z ensure_* (idempotentnie).

BEGIN;

CREATE SCHEMA IF NOT EXISTS output;

-- 1) SNAPSHOT PARAMETRÓW
CREATE TABLE IF NOT EXISTS output.snapshot (
  calc_id     uuid        PRIMARY KEY,
  params_ts   timestamptz NOT NULL,
  raw         jsonb       NOT NULL,  -- wejście (zrzut/połączenie formularzy)
  norm        jsonb       NOT NULL,  -- wyjście (skonsolidowana norma)
  created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_snapshot
  ON output.snapshot (calc_id, created_at DESC);

-- 2) KOLEJKA JOBÓW
CREATE TABLE IF NOT EXISTS output.calc_job_queue (
  job_id      uuid PRIMARY KEY
              DEFAULT (md5(random()::text || clock_timestamp()::text)::uuid),
  params_ts   timestamptz NOT NULL,
  status      text        NOT NULL DEFAULT 'queued'
                CHECK (status IN ('queued','running','done','failed','skipped')),
  created_at  timestamptz NOT NULL DEFAULT now(),
  started_at  timestamptz,
  finished_at timestamptz,
  error       text
);
CREATE INDEX IF NOT EXISTS ix_calc_job_queue
  ON output.calc_job_queue (status, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_calc_job_queue_params_ts
  ON output.calc_job_queue (params_ts, created_at DESC);

-- 3) STATUSY STAGE’ÓW (mirror z ensure_calc_status_structures)
CREATE TABLE IF NOT EXISTS output.calc_job_stage (
  id           bigserial PRIMARY KEY,
  job_id       uuid,
  calc_id      uuid        NOT NULL,
  stage        text        NOT NULL CHECK (stage IN ('00','01','02','03','04','05','06')),
  status       text        NOT NULL CHECK (status IN ('running','done','failed')),
  created_at   timestamptz NOT NULL DEFAULT now(),
  started_at   timestamptz,
  finished_at  timestamptz,
  error        text,
  UNIQUE (calc_id, stage)
);
CREATE INDEX IF NOT EXISTS ix_calc_job_stage
  ON output.calc_job_stage (calc_id, stage, status, finished_at DESC);

-- 4) GŁÓWNA TABELA SERWISOWA

CREATE TABLE IF NOT EXISTS output.health_service (
  id              bigserial PRIMARY KEY,
  -- 'prep' dla energy-prep, 'calc' dla energy-calc
  scope           text NOT NULL CHECK (scope IN ('prep','calc')),

  -- dla CALC: identyfikator biegu i znacznik parametrów
  calc_id         uuid NULL,
  params_ts       timestamptz NULL,

  -- status zbiorczy + treść sekcji (dowolnie zagnieżdżony JSON)
  overall_status  text NOT NULL CHECK (overall_status IN ('up','degraded','down')),
  sections        jsonb NOT NULL,               -- np. {"01_ingest":{...}, "04_pricing":{...}} / {"input":{...},"params":{...}}
  summary         text NULL,

  -- metadane
  created_at      timestamptz NOT NULL DEFAULT now()
);

-- INDEKSY POD NAJCZĘSTSZE ZAPYTANIA
CREATE INDEX IF NOT EXISTS idx_health_service_scope_created
  ON output.health_service (scope, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_health_service_calc
  ON output.health_service (calc_id);

-- (OPCJONALNIE) unikalność najnowszego snapshotu CALC per calc_id
-- jeśli w 06_validate robisz UPSERT po calc_id:
-- CREATE UNIQUE INDEX IF NOT EXISTS uq_health_service_calcid
--   ON output.health_service (calc_id)
--   WHERE scope='calc';

COMMIT;
