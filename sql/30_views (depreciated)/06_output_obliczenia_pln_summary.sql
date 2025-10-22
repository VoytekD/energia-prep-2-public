-- =====================================================================================================
-- sql/30_views (depreciated)/06_output_obliczenia_pln_summary.sql  — WERSJA zoptymalizowana pod czas wykonania
--  • Minimalne kolumny z 05a/05b/05c + MATERIALIZED CTE
--  • Join po ts_utc do date_dim
--  • Opcjonalny filtr czasu (rok / okno) → podmieniaj w WHERE
-- =====================================================================================================

CREATE SCHEMA IF NOT EXISTS output;

DROP VIEW IF EXISTS output.obliczenia_pln_summary;

CREATE VIEW output.obliczenia_pln_summary AS
WITH
-- Oś czasu: tylko potrzebne kolumny (i ewentualny filtr)
dd AS MATERIALIZED (
  SELECT d.ts_utc,
         d.year, d.month, d.day, d.dow, d.hour,
         d.is_workday::boolean AS is_workday
  FROM input.date_dim d
  -- ▼ jeśli chcesz, tu możesz włączyć stały filtr uruchomieniowy, np. ostatnie 3 lata:
  -- WHERE d.ts_utc >= date_trunc('year', now() AT TIME ZONE 'UTC') - INTERVAL '3 years'
),

-- 05a: magazyn + OZE (tylko potrzebne pola)
a AS MATERIALIZED (
  SELECT
    ts_utc,
    ROUND(opl_energia_sel_h_pln, 2)                                                      AS energia_mag_i_oze_pln,
    ROUND(opl_dystr_zmienna_sel_h_pln + opl_dystr_stala_sel_h_pln, 2)                    AS dystrybucja_mag_i_oze_pln,
    ROUND(opl_oze_h_pln + opl_kog_h_pln + opl_mocowa_h_pln, 2)                            AS systemowe_mag_i_oze_pln,
    ROUND(opl_akcyza_h_pln + opl_vat_h_pln, 2)                                            AS fiskalne_mag_i_oze_pln
  FROM output.obliczenia_pln_mag_i_oze
),

-- 05b: OZE
b AS MATERIALIZED (
  SELECT
    ts_utc,
    ROUND(opl_energia_sel_h_pln, 2)                                                      AS energia_oze_pln,
    ROUND(opl_dystr_zmienna_sel_h_pln + opl_dystr_stala_sel_h_pln, 2)                    AS dystrybucja_oze_pln,
    ROUND(opl_oze_h_pln + opl_kog_h_pln + opl_mocowa_h_pln, 2)                            AS systemowe_oze_pln,
    ROUND(opl_akcyza_h_pln + opl_vat_h_pln, 2)                                            AS fiskalne_oze_pln
  FROM output.obliczenia_pln_oze
),

-- 05c: GRID (konsumpcja)
c AS MATERIALIZED (
  SELECT
    ts_utc,
    ROUND(opl_energia_sel_h_pln, 2)                                                      AS energia_kons_pln,
    ROUND(opl_dystr_zmienna_sel_h_pln + opl_dystr_stala_sel_h_pln, 2)                    AS dystrybucja_kons_pln,
    ROUND(opl_oze_h_pln + opl_kog_h_pln + opl_mocowa_h_pln, 2)                            AS systemowe_kons_pln,
    ROUND(opl_akcyza_h_pln + opl_vat_h_pln, 2)                                            AS fiskalne_kons_pln
  FROM output.obliczenia_pln_grid
)

SELECT
  d.ts_utc,
  d.year, d.month, d.day, d.dow, d.hour, d.is_workday,

  -- A: Mag+OZE
  COALESCE(a.energia_mag_i_oze_pln,     0)::numeric AS energia_mag_i_oze_pln,
  COALESCE(a.dystrybucja_mag_i_oze_pln, 0)::numeric AS dystrybucja_mag_i_oze_pln,
  COALESCE(a.systemowe_mag_i_oze_pln,   0)::numeric AS systemowe_mag_i_oze_pln,
  COALESCE(a.fiskalne_mag_i_oze_pln,    0)::numeric AS fiskalne_mag_i_oze_pln,

  -- B: OZE
  COALESCE(b.energia_oze_pln,           0)::numeric AS energia_oze_pln,
  COALESCE(b.dystrybucja_oze_pln,       0)::numeric AS dystrybucja_oze_pln,
  COALESCE(b.systemowe_oze_pln,         0)::numeric AS systemowe_oze_pln,
  COALESCE(b.fiskalne_oze_pln,          0)::numeric AS fiskalne_oze_pln,

  -- C: GRID (kons)
  COALESCE(c.energia_kons_pln,          0)::numeric AS energia_kons_pln,
  COALESCE(c.dystrybucja_kons_pln,      0)::numeric AS dystrybucja_kons_pln,
  COALESCE(c.systemowe_kons_pln,        0)::numeric AS systemowe_kons_pln,
  COALESCE(c.fiskalne_kons_pln,         0)::numeric AS fiskalne_kons_pln
FROM dd d
LEFT JOIN a ON a.ts_utc = d.ts_utc
LEFT JOIN b ON b.ts_utc = d.ts_utc
LEFT JOIN c ON c.ts_utc = d.ts_utc;
