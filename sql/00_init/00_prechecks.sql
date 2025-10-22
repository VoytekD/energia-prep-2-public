-- 00_prechecks.sql
DO $$
BEGIN
  RAISE NOTICE 'PostgreSQL version: %', current_setting('server_version');
  RAISE NOTICE 'Current database : %', current_database();
  RAISE NOTICE 'Current user     : %', current_user;
  RAISE NOTICE 'TimeZone         : %', current_setting('TimeZone', true);
END$$;
