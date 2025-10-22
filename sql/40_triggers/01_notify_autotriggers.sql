CREATE SCHEMA IF NOT EXISTS util;

-- Funkcja row-level, która wysyła NOTIFY
CREATE OR REPLACE FUNCTION util.row_change_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_payload text;
BEGIN
    v_payload := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME || ':' || TG_OP;
    PERFORM pg_notify('ch_energy_rebuild', v_payload);

    IF TG_OP = 'DELETE' THEN
      RETURN OLD;
    ELSE
      RETURN NEW;
    END IF;
END;
$$;

-- Procedura zakładająca trigger na jednej tabeli
CREATE OR REPLACE PROCEDURE util.ensure_row_triggers(p_schema text, p_table text)
LANGUAGE plpgsql AS $$
DECLARE
    trig_name text := 'trg_row_change_notify';
    exists_trig boolean;
BEGIN
    IF p_schema NOT IN ('input','params') THEN
        RETURN;
    END IF;

    SELECT EXISTS(
      SELECT 1
      FROM pg_trigger t
      JOIN pg_class c ON c.oid = t.tgrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = p_schema
        AND c.relname = p_table
        AND t.tgname = trig_name
        AND NOT t.tgisinternal
    ) INTO exists_trig;

    IF NOT exists_trig THEN
        EXECUTE format($f$
          CREATE TRIGGER %I
          AFTER INSERT OR UPDATE OR DELETE ON %I.%I
          FOR EACH ROW
          EXECUTE FUNCTION util.row_change_notify()
        $f$, trig_name, p_schema, p_table);
    END IF;
END;
$$;

-- Event trigger: przy każdym CREATE TABLE uzbraja tabelę
DO $$
BEGIN
  CREATE OR REPLACE FUNCTION util.on_ddl_end_autotrig() RETURNS event_trigger
  LANGUAGE plpgsql AS $f$
  DECLARE
      r record;
      obj_schema text;
      obj_table  text;
  BEGIN
    FOR r IN
      SELECT *
      FROM pg_event_trigger_ddl_commands()
      WHERE command_tag ILIKE 'CREATE TABLE'
    LOOP
      obj_schema := split_part(r.object_identity, '.', 1);
      obj_table  := split_part(r.object_identity, '.', 2);

      IF obj_table IS NULL OR obj_table = '' THEN
          obj_schema := current_schema;
          obj_table  := r.object_identity;
      END IF;

      -- ✅ używamy CALL, bo ensure_row_triggers jest procedurą
      CALL util.ensure_row_triggers(obj_schema, obj_table);
    END LOOP;
  END;
  $f$;

  IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'trg_autotrig_on_ddl_end') THEN
      CREATE EVENT TRIGGER trg_autotrig_on_ddl_end
      ON ddl_command_end
      EXECUTE FUNCTION util.on_ddl_end_autotrig();
  END IF;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'event trigger not installed: %', SQLERRM;
END$$;
