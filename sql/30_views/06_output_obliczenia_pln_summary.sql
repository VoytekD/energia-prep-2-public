-- =====================================================================================================
--  sql/30_views/06_output_obliczenia_pln_summary.sql
--  Godzinowe podsumowanie kosztów *w układzie kolumnowym per scenariusz*:
--   - 05a (mag_i_oze; bazuje na grid import):  energia / dystrybucja / systemowe / fiskalne
--   - 05b (oze; bazuje na delcie brutto):      energia / dystrybucja / systemowe / fiskalne
--   - 05c (konsumpcja; bazuje na zużyciu):     energia / dystrybucja / systemowe / fiskalne
--  Źródła:
--    output.obliczenia_pln_mag_i_oze
--    output.obliczenia_pln_oze
--    output.obliczenia_pln_grid
--  Uwaga:
--    • „energia” = opl_energia_sel_h_pln (po marżach, zawiera opłatę handlową h)
--    • „dystrybucja” = opl_dystr_zmienna_sel_h_pln + opl_dystr_stala_sel_h_pln
--    • „systemowe” = opl_oze_h_pln + opl_kog_h_pln + opl_mocowa_h_pln
--    • „fiskalne” = opl_akcyza_h_pln + opl_vat_h_pln
-- =====================================================================================================

CREATE SCHEMA IF NOT EXISTS output;

DROP VIEW IF EXISTS output.obliczenia_pln_summary;

CREATE VIEW output.obliczenia_pln_summary AS
WITH
-- Oś czasu jako baza (zapewnia komplet godzin)
dd AS (
  SELECT d.ts_utc, d.ts_local, d.year, d.month, d.day, d.dow, d.hour, d.is_workday::boolean AS is_workday
  FROM input.date_dim d
),

-- 05a: magazyn + OZE (grid import)
a AS (
  SELECT
    ts_utc,
    ROUND(opl_energia_sel_h_pln, 2)                                                        AS energia_mag_i_oze_pln,
    ROUND(opl_dystr_zmienna_sel_h_pln + opl_dystr_stala_sel_h_pln, 2)                      AS dystrybucja_mag_i_oze_pln,
    ROUND(opl_oze_h_pln + opl_kog_h_pln + opl_mocowa_h_pln, 2)                              AS systemowe_mag_i_oze_pln,
    ROUND(opl_akcyza_h_pln + opl_vat_h_pln, 2)                                              AS fiskalne_mag_i_oze_pln
  FROM output.obliczenia_pln_mag_i_oze
),

-- 05b: OZE (delta brutto -> pobór)
b AS (
  SELECT
    ts_utc,
    ROUND(opl_energia_sel_h_pln, 2)                                                        AS energia_oze_pln,
    ROUND(opl_dystr_zmienna_sel_h_pln + opl_dystr_stala_sel_h_pln, 2)                      AS dystrybucja_oze_pln,
    ROUND(opl_oze_h_pln + opl_kog_h_pln + opl_mocowa_h_pln, 2)                              AS systemowe_oze_pln,
    ROUND(opl_akcyza_h_pln + opl_vat_h_pln, 2)                                              AS fiskalne_oze_pln
  FROM output.obliczenia_pln_oze
),

-- 05c: KONSUMPCJA (zużycie przemnożone)
k AS (
  SELECT
    ts_utc,
    ROUND(opl_energia_sel_h_pln, 2)                                                        AS energia_kons_pln,
    ROUND(opl_dystr_zmienna_sel_h_pln + opl_dystr_stala_sel_h_pln, 2)                      AS dystrybucja_kons_pln,
    ROUND(opl_oze_h_pln + opl_kog_h_pln + opl_mocowa_h_pln, 2)                              AS systemowe_kons_pln,
    ROUND(opl_akcyza_h_pln + opl_vat_h_pln, 2)                                              AS fiskalne_kons_pln
  FROM output.obliczenia_pln_grid
)

SELECT
  -- kolumny czasu
  dd.ts_utc, dd.ts_local, dd.year, dd.month, dd.day, dd.dow, dd.hour, dd.is_workday,

  -- 05a (grid import + magazyn/OZE)
  COALESCE(a.energia_mag_i_oze_pln,     0)::numeric(18,6) AS energia_mag_i_oze_pln,
  COALESCE(a.dystrybucja_mag_i_oze_pln, 0)::numeric(18,6) AS dystrybucja_mag_i_oze_pln,
  COALESCE(a.systemowe_mag_i_oze_pln,   0)::numeric(18,6) AS systemowe_mag_i_oze_pln,
  COALESCE(a.fiskalne_mag_i_oze_pln,    0)::numeric(18,6) AS fiskalne_mag_i_oze_pln,

  -- 05b (delta brutto)
  COALESCE(b.energia_oze_pln,           0)::numeric(18,6) AS energia_oze_pln,
  COALESCE(b.dystrybucja_oze_pln,       0)::numeric(18,6) AS dystrybucja_oze_pln,
  COALESCE(b.systemowe_oze_pln,         0)::numeric(18,6) AS systemowe_oze_pln,
  COALESCE(b.fiskalne_oze_pln,          0)::numeric(18,6) AS fiskalne_oze_pln,

  -- 05c (konsumpcja)
  COALESCE(k.energia_kons_pln,          0)::numeric(18,6) AS energia_kons_pln,
  COALESCE(k.dystrybucja_kons_pln,      0)::numeric(18,6) AS dystrybucja_kons_pln,
  COALESCE(k.systemowe_kons_pln,        0)::numeric(18,6) AS systemowe_kons_pln,
  COALESCE(k.fiskalne_kons_pln,         0)::numeric(18,6) AS fiskalne_kons_pln

FROM dd
LEFT JOIN a ON a.ts_utc = dd.ts_utc
LEFT JOIN b ON b.ts_utc = dd.ts_utc
LEFT JOIN k ON k.ts_utc = dd.ts_utc
ORDER BY dd.ts_utc;
