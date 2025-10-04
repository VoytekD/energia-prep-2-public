-- ==========================================================================================
-- 30_views (depreciated)/00_output_energy_base.sql (final, bez fallbacków nazw) — WERSJA ze skalowaniem + date_dim.*
-- Wejścia:
--   • input.konsumpcja(ts_utc, zuzycie_mw)
--   • input.produkcja(ts_utc, pv_pp_mw, pv_wz_mw, wind_1mwp)
--   • input.ceny_godzinowe(ts_utc, fixing_i_price)
--   • input.date_dim(ts_utc, …pola pomocnicze…)
-- Parametry (skala):
--   • params.form_zmienne: moc_pv_pp, moc_pv_wz, moc_wiatr, zmiany_konsumpcji [%]
-- Wyjście:
--   • output.energy_base( date_dim.*, p_load_mw, p_pv_pp_mw, p_pv_wz_mw, p_wiatr_mw,
--                         p_gen_total_mw, delta_mw, e_delta_mwh, e_surplus_mwh,
--                         e_deficit_mwh, price_tge_pln, dt_h )
-- Zasady:
--   • pełna siatka godzinowa z date_dim,
--   • deduplikacja źródeł: AVG(...) GROUP BY ts_utc,
--   • interpolacja liniowa braków (prev/next); na brzegach najbliższa wartość,
--   • skalowanie po interpolacji (parametry z params.form_zmienne),
--   • dt_h = 1.0 (stała na kanonicznej siatce UTC).
-- ==========================================================================================

CREATE SCHEMA IF NOT EXISTS output;

DROP VIEW IF EXISTS output.energy_base CASCADE;

CREATE VIEW output.energy_base AS
WITH
-- 0) Parametry skali (ostatni wiersz z params.form_zmienne)
zmienne AS (
  SELECT
    COALESCE(z.moc_pv_pp, 1.0)::numeric       AS fac_pv_pp,
    COALESCE(z.moc_pv_wz, 1.0)::numeric       AS fac_pv_wz,
    COALESCE(z.moc_wiatr, 1.0)::numeric       AS fac_wind,
    (1.0 + COALESCE(z.zmiany_konsumpcji, 0)::numeric / 100.0) AS fac_load
  FROM params.form_zmienne z
  ORDER BY z.updated_at DESC NULLS LAST
  LIMIT 1
),

-- 1) Oś czasu: pełne godziny (UTC) z date_dim
axis AS (
  SELECT d.ts_utc
  FROM input.date_dim d
),

-- 2) Znormalizowane, zdeduplikowane szeregi źródłowe (AVG po ts_utc)
s_load AS (
  SELECT ts_utc, AVG(zuzycie_mw)::numeric AS v
  FROM input.konsumpcja
  GROUP BY ts_utc
),
s_pvpp AS (
  SELECT ts_utc, AVG(pv_pp_mw)::numeric AS v
  FROM input.produkcja
  GROUP BY ts_utc
),
s_pvwz AS (
  SELECT ts_utc, AVG(pv_wz_mw)::numeric AS v
  FROM input.produkcja
  GROUP BY ts_utc
),
s_wind AS (
  SELECT ts_utc, AVG(wind_1mwp)::numeric AS v
  FROM input.produkcja
  GROUP BY ts_utc
),
s_price AS (
  SELECT ts_utc, AVG(fixing_i_price)::numeric AS v
  FROM input.ceny_godzinowe
  GROUP BY ts_utc
),

-- 3) Siatka + wartości (mogą być NULL → luki do wypełnienia)
g AS (
  SELECT
    a.ts_utc,
    l.v  AS p_load_mw_raw,
    pp.v AS p_pv_pp_mw_raw,
    wz.v AS p_pv_wz_mw_raw,
    w.v  AS p_wiatr_mw_raw,
    pr.v AS price_raw
  FROM axis a
  LEFT JOIN s_load  l  USING (ts_utc)
  LEFT JOIN s_pvpp  pp USING (ts_utc)
  LEFT JOIN s_pvwz  wz USING (ts_utc)
  LEFT JOIN s_wind  w  USING (ts_utc)
  LEFT JOIN s_price pr USING (ts_utc)
),

-- 4) Znaczniki prev/next nie-NULL do interpolacji per kolumna
pn AS (
  SELECT
    ts_utc,

    MAX(CASE WHEN p_load_mw_raw  IS NOT NULL THEN ts_utc END) OVER (ORDER BY ts_utc
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ts_prev_load,
    MIN(CASE WHEN p_load_mw_raw  IS NOT NULL THEN ts_utc END) OVER (ORDER BY ts_utc
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)               AS ts_next_load,

    MAX(CASE WHEN p_pv_pp_mw_raw IS NOT NULL THEN ts_utc END) OVER (ORDER BY ts_utc
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ts_prev_pvpp,
    MIN(CASE WHEN p_pv_pp_mw_raw IS NOT NULL THEN ts_utc END) OVER (ORDER BY ts_utc
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)               AS ts_next_pvpp,

    MAX(CASE WHEN p_pv_wz_mw_raw IS NOT NULL THEN ts_utc END) OVER (ORDER BY ts_utc
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ts_prev_pvwz,
    MIN(CASE WHEN p_pv_wz_mw_raw IS NOT NULL THEN ts_utc END) OVER (ORDER BY ts_utc
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)               AS ts_next_pvwz,

    MAX(CASE WHEN p_wiatr_mw_raw IS NOT NULL THEN ts_utc END) OVER (ORDER BY ts_utc
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ts_prev_wind,
    MIN(CASE WHEN p_wiatr_mw_raw IS NOT NULL THEN ts_utc END) OVER (ORDER BY ts_utc
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)               AS ts_next_wind,

    MAX(CASE WHEN price_raw      IS NOT NULL THEN ts_utc END) OVER (ORDER BY ts_utc
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ts_prev_price,
    MIN(CASE WHEN price_raw      IS NOT NULL THEN ts_utc END) OVER (ORDER BY ts_utc
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)               AS ts_next_price

  FROM g
),

-- 5) Podłączenie wartości prev/next + współczynniki czasu (0..1) do interpolacji
gn AS (
  SELECT
    g.*,

    l_prev.v  AS v_prev_load,  l_next.v  AS v_next_load,
    pp_prev.v AS v_prev_pvpp,  pp_next.v AS v_next_pvpp,
    wz_prev.v AS v_prev_pvwz,  wz_next.v AS v_next_pvwz,
    w_prev.v  AS v_prev_wind,  w_next.v  AS v_next_wind,
    pr_prev.v AS v_prev_price, pr_next.v AS v_next_price,

    (EXTRACT(EPOCH FROM (g.ts_utc - pn.ts_prev_load)) /
     NULLIF(EXTRACT(EPOCH FROM (pn.ts_next_load - pn.ts_prev_load)),0))   AS t_load,
    (EXTRACT(EPOCH FROM (g.ts_utc - pn.ts_prev_pvpp)) /
     NULLIF(EXTRACT(EPOCH FROM (pn.ts_next_pvpp - pn.ts_prev_pvpp)),0))   AS t_pvpp,
    (EXTRACT(EPOCH FROM (g.ts_utc - pn.ts_prev_pvwz)) /
     NULLIF(EXTRACT(EPOCH FROM (pn.ts_next_pvwz - pn.ts_prev_pvwz)),0))   AS t_pvwz,
    (EXTRACT(EPOCH FROM (g.ts_utc - pn.ts_prev_wind)) /
     NULLIF(EXTRACT(EPOCH FROM (pn.ts_next_wind - pn.ts_prev_wind)),0))   AS t_wind,
    (EXTRACT(EPOCH FROM (g.ts_utc - pn.ts_prev_price)) /
     NULLIF(EXTRACT(EPOCH FROM (pn.ts_next_price - pn.ts_prev_price)),0)) AS t_price

  FROM g
  JOIN pn USING (ts_utc)

  LEFT JOIN s_load  l_prev  ON l_prev.ts_utc  = pn.ts_prev_load
  LEFT JOIN s_load  l_next  ON l_next.ts_utc  = pn.ts_next_load

  LEFT JOIN s_pvpp  pp_prev ON pp_prev.ts_utc = pn.ts_prev_pvpp
  LEFT JOIN s_pvpp  pp_next ON pp_next.ts_utc = pn.ts_next_pvpp

  LEFT JOIN s_pvwz  wz_prev ON wz_prev.ts_utc = pn.ts_prev_pvwz
  LEFT JOIN s_pvwz  wz_next ON wz_next.ts_utc = pn.ts_next_pvwz

  LEFT JOIN s_wind  w_prev  ON w_prev.ts_utc  = pn.ts_prev_wind
  LEFT JOIN s_wind  w_next  ON w_next.ts_utc  = pn.ts_next_wind

  LEFT JOIN s_price pr_prev ON pr_prev.ts_utc = pn.ts_prev_price
  LEFT JOIN s_price pr_next ON pr_next.ts_utc = pn.ts_next_price
),

-- 6) Interpolacja liniowa (na brzegach COALESCE do najbliższej wartości)
filled AS (
  SELECT
    ts_utc,

    COALESCE(
      p_load_mw_raw,
      CASE WHEN v_prev_load IS NOT NULL AND v_next_load IS NOT NULL
           THEN v_prev_load + (v_next_load - v_prev_load) * t_load
      ELSE NULL END,
      v_prev_load, v_next_load
    ) AS p_load_mw,

    COALESCE(
      p_pv_pp_mw_raw,
      CASE WHEN v_prev_pvpp IS NOT NULL AND v_next_pvpp IS NOT NULL
           THEN v_prev_pvpp + (v_next_pvpp - v_prev_pvpp) * t_pvpp
      ELSE NULL END,
      v_prev_pvpp, v_next_pvpp
    ) AS p_pv_pp_mw,

    COALESCE(
      p_pv_wz_mw_raw,
      CASE WHEN v_prev_pvwz IS NOT NULL AND v_next_pvwz IS NOT NULL
           THEN v_prev_pvwz + (v_next_pvwz - v_prev_pvwz) * t_pvwz
      ELSE NULL END,
      v_prev_pvwz, v_next_pvwz
    ) AS p_pv_wz_mw,

    COALESCE(
      p_wiatr_mw_raw,
      CASE WHEN v_prev_wind IS NOT NULL AND v_next_wind IS NOT NULL
           THEN v_prev_wind + (v_next_wind - v_prev_wind) * t_wind
      ELSE NULL END,
      v_prev_wind, v_next_wind
    ) AS p_wiatr_mw,

    COALESCE(
      price_raw,
      CASE WHEN v_prev_price IS NOT NULL AND v_next_price IS NOT NULL
           THEN v_prev_price + (v_next_price - v_prev_price) * t_price
      ELSE NULL END,
      v_prev_price, v_next_price
    ) AS price_tge_pln
  FROM gn
),

-- 7) Skalowanie po interpolacji (mnożniki z params.form_zmienne)
scaled AS (
  SELECT
    f.ts_utc,
    ROUND(f.p_load_mw  * (SELECT fac_load  FROM zmienne), 6) AS p_load_mw,
    ROUND(f.p_pv_pp_mw * (SELECT fac_pv_pp FROM zmienne), 6) AS p_pv_pp_mw,
    ROUND(f.p_pv_wz_mw * (SELECT fac_pv_wz FROM zmienne), 6) AS p_pv_wz_mw,
    ROUND(f.p_wiatr_mw * (SELECT fac_wind  FROM zmienne), 6) AS p_wiatr_mw,
    ROUND(f.price_tge_pln, 2)                                AS price_tge_pln,
    1.0::numeric                                             AS dt_h
  FROM filled f
),

-- 8) Obliczenia pochodne i zaokrąglenia
final AS (
  SELECT
    ts_utc,
    p_load_mw,
    p_pv_pp_mw,
    p_pv_wz_mw,
    p_wiatr_mw,
    ROUND(p_pv_pp_mw + p_pv_wz_mw + p_wiatr_mw, 6) AS p_gen_total_mw,
    ROUND((p_pv_pp_mw + p_pv_wz_mw + p_wiatr_mw) - p_load_mw, 6) AS delta_mw,
    price_tge_pln,
    dt_h
  FROM scaled
)
SELECT
  -- pełny kontekst kalendarza
  d.*,

  -- ▼ alias dla zgodności wstecz
  d.ts_utc AS ts,

  -- metryki energetyczne
  f.p_load_mw,
  f.p_pv_pp_mw,
  f.p_pv_wz_mw,
  f.p_wiatr_mw,
  f.p_gen_total_mw,
  f.delta_mw,
  (f.delta_mw * f.dt_h)                 AS e_delta_mwh,
  GREATEST(0.0,  f.delta_mw * f.dt_h)   AS e_surplus_mwh,
  GREATEST(0.0, -(f.delta_mw * f.dt_h)) AS e_deficit_mwh,
  f.price_tge_pln,
  f.dt_h
FROM final f
JOIN input.date_dim d
  ON d.ts_utc = f.ts_utc;
-- (sortuj tylko w zapytaniach użytkowych, np. ORDER BY d.ts_utc)

 /* ===========================
    INDEKSY – energia-prep-2 (FINAL)
    =========================== */

-- 1) Szeregi czasowe (wejścia) — szybkie joiny po ts_utc + spójność danych
CREATE UNIQUE INDEX IF NOT EXISTS uq_input_date_dim_ts   ON input.date_dim(ts_utc);
CREATE UNIQUE INDEX IF NOT EXISTS uq_input_konsumpcja_ts ON input.konsumpcja(ts_utc);
CREATE UNIQUE INDEX IF NOT EXISTS uq_input_produkcja_ts  ON input.produkcja(ts_utc);
CREATE UNIQUE INDEX IF NOT EXISTS uq_input_ceny_godz_ts  ON input.ceny_godzinowe(ts_utc);

-- BRIN dla dużych tabel append-only — szybkie skany zakresowe po czasie
CREATE INDEX IF NOT EXISTS brin_input_konsumpcja_ts     ON input.konsumpcja     USING BRIN (ts_utc) WITH (pages_per_range = 64);
CREATE INDEX IF NOT EXISTS brin_input_produkcja_ts      ON input.produkcja      USING BRIN (ts_utc) WITH (pages_per_range = 64);
CREATE INDEX IF NOT EXISTS brin_input_ceny_godzinowe_ts ON input.ceny_godzinowe USING BRIN (ts_utc) WITH (pages_per_range = 64);

-- 2) Ceny: „najnowszy wpis na godzinę” (DISTINCT ON / ROW_NUMBER po updated_at)
CREATE INDEX IF NOT EXISTS idx_ceny_godz_latest ON input.ceny_godzinowe (ts_utc, updated_at DESC);

-- 3) Parametry: pobieranie „najnowszego wiersza” (ORDER BY updated_at DESC LIMIT 1)
CREATE INDEX IF NOT EXISTS idx_params_form_zmienne_upd       ON params.form_zmienne       (updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_params_form_bess_param_upd    ON params.form_bess_param    (updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_params_form_par_arbitrazu_upd ON params.form_par_arbitrazu (updated_at DESC);

-- 4) Po imporcie/ETL — świeże statystyki dla planera zapytań
ANALYZE input.date_dim;
ANALYZE input.konsumpcja;
ANALYZE input.produkcja;
ANALYZE input.ceny_godzinowe;
ANALYZE params.form_zmienne;
ANALYZE params.form_bess_param;
ANALYZE params.form_par_arbitrazu;
