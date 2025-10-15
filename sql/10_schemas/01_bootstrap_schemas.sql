-- sql/10_schemas/01_bootstrap_schemas.sql
SET client_min_messages = WARNING;

-- (opcjonalny blok – zostawiony jak w Twojej wersji)
DO $do$
DECLARE
    app_user text := 'voytek';
BEGIN
    -- zakładamy istnienie roli app_user (no-op)
END
$do$ LANGUAGE plpgsql;

-- Tworzymy TYLKO: util, input, params, output
DO $do$
DECLARE
    s         text;
    schemata  text[] := ARRAY['util','input','params','output'];
    app_user  text   := 'voytek';
BEGIN
    FOREACH s IN ARRAY schemata LOOP
        EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I AUTHORIZATION %I;', s, app_user);
        EXECUTE format('GRANT USAGE, CREATE ON SCHEMA %I TO %I;', s, app_user);
    END LOOP;
END
$do$ LANGUAGE plpgsql;

-- search_path jak u Ciebie (bez analytics)
SET search_path TO util, input, params, output, public;

-- Default privileges dla roli aplikacyjnej (pętla po naszych schematach)
DO $do$
DECLARE
    s         text;
    schemata  text[] := ARRAY['util','input','params','output'];
    app_user  text   := 'voytek';
BEGIN
    FOREACH s IN ARRAY schemata LOOP
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I;', s, app_user);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO %I;', s, app_user);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT EXECUTE ON FUNCTIONS TO %I;', s, app_user);
    END LOOP;
END
$do$ LANGUAGE plpgsql;


