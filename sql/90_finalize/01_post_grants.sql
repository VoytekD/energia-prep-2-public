-- 01_post_grants.sql
DO $$
BEGIN
  -- Bezpiecznie nadaj podstawowe uprawnienia bieżącemu użytkownikowi
  PERFORM 1;
  RAISE NOTICE 'post grants: nothing to do (placeholder)';
END$$;
