-- 01_output_energy_broker_detail.sql — BROKER: OZE/ARBI + import/eksport
-- Założenia:
--  • Brak wewnętrznego ograniczenia AC (falownik) — tylko c-rate i sprawności.
--  • PCC ogranicza wyłącznie import/eksport do/z sieci mocą umowną.
--  • Nadwyżka (delta ≥ 0): najpierw ładujemy OZE (NET), potem ARBI (NET); reszta do sieci (eksport, cap PCC).
--  • Deficyt (delta < 0): rozładowanie do odbiorcy OZE→(opcjonalnie ARBI), reszta z sieci (import, cap PCC).
--  • Rozładowanie do odbiorcy NIE jest limitowane PCC.

CREATE SCHEMA IF NOT EXISTS output;

CREATE OR REPLACE VIEW output.energy_broker_detail AS
WITH base AS (
    SELECT
      b.ts_utc AS ts,   -- <— kluczowa zmiana
      b.dt_h,
      b.e_delta_mwh::numeric   AS e_delta_mwh,
      b.e_surplus_mwh::numeric AS e_surplus_mwh,
      b.e_deficit_mwh::numeric AS e_deficit_mwh
    FROM output.energy_base b
),
bess AS (
  SELECT
    COALESCE((j->>'eta_ch')::numeric,       1.0) AS eta_ch,
    COALESCE((j->>'eta_dis')::numeric,      1.0) AS eta_dis,
    COALESCE((j->>'c_rate_ch_h')::numeric,  0.0) AS c_rate_ch_h,
    COALESCE((j->>'c_rate_dis_h')::numeric, 0.0) AS c_rate_dis_h,
    COALESCE((j->>'lambda_h')::numeric,     0.0) AS lambda_h
  FROM (SELECT current_setting('energia.form_bess_param_json', true)::json AS j) s
),
client AS (
  SELECT COALESCE((j->>'moc_umowna_mw')::numeric, 0.0) AS moc_umowna_mw
  FROM (SELECT current_setting('energia.form_parametry_klienta_json', true)::json AS j) s
),
zmienne AS (
  SELECT
    COALESCE(z.emax, 0)::numeric              AS emax_total_mwh,
    COALESCE(z.procent_arbitrazu, 0)::numeric AS procent_arbi
  FROM params.form_zmienne z
  ORDER BY z.updated_at DESC NULLS LAST
  LIMIT 1
),
buckets AS (
  SELECT
    z.emax_total_mwh,
    z.procent_arbi,
    ROUND(z.emax_total_mwh * (z.procent_arbi/100.0), 6)     AS emax_arbi_mwh,
    ROUND(z.emax_total_mwh * (1 - z.procent_arbi/100.0), 6) AS emax_oze_mwh
  FROM zmienne z
),
par_arbi AS (
  SELECT arbi_dis_to_load
  FROM params.form_par_arbitrazu
  ORDER BY updated_at DESC NULLS LAST
  LIMIT 1
),
caps AS (
  SELECT
    b.ts, b.dt_h,

    -- CAPy z c-rate (NET/DELIVER), bez ograniczeń PCC na rozładowanie do load
    CASE WHEN bs.c_rate_ch_h  > 0 THEN (bk.emax_total_mwh / bs.c_rate_ch_h)  * b.dt_h * bs.eta_ch ELSE 0 END AS e_ch_cap_net_mwh,
    CASE WHEN bs.c_rate_dis_h > 0 THEN (bk.emax_total_mwh / bs.c_rate_dis_h) * b.dt_h / bs.eta_dis ELSE 0 END AS e_dis_cap_net_mwh,
    CASE WHEN bs.c_rate_dis_h > 0 THEN (bk.emax_total_mwh / bs.c_rate_dis_h) * b.dt_h                   ELSE 0 END AS e_dis_cap_deliver_mwh,

    CASE WHEN bs.c_rate_ch_h  > 0 THEN (bk.emax_oze_mwh  / bs.c_rate_ch_h)  * b.dt_h * bs.eta_ch ELSE 0 END AS e_ch_cap_net_oze_mwh,
    CASE WHEN bs.c_rate_ch_h  > 0 THEN (bk.emax_arbi_mwh / bs.c_rate_ch_h)  * b.dt_h * bs.eta_ch ELSE 0 END AS e_ch_cap_net_arbi_mwh,

    CASE WHEN bs.c_rate_dis_h > 0 THEN (bk.emax_oze_mwh  / bs.c_rate_dis_h) * b.dt_h / bs.eta_dis ELSE 0 END AS e_dis_cap_net_oze_mwh,
    CASE WHEN bs.c_rate_dis_h > 0 THEN (bk.emax_arbi_mwh / bs.c_rate_dis_h) * b.dt_h / bs.eta_dis ELSE 0 END AS e_dis_cap_net_arbi_mwh,

    CASE WHEN bs.c_rate_dis_h > 0 THEN (bk.emax_oze_mwh  / bs.c_rate_dis_h) * b.dt_h ELSE 0 END AS e_dis_cap_deliver_oze_mwh,
    CASE WHEN bs.c_rate_dis_h > 0 THEN (bk.emax_arbi_mwh / bs.c_rate_dis_h) * b.dt_h ELSE 0 END AS e_dis_cap_deliver_arbi_mwh,

    -- PCC: limity tylko na import/eksport
    (cl.moc_umowna_mw * b.dt_h) AS grid_import_cap_mwh,
    (cl.moc_umowna_mw * b.dt_h) AS grid_export_cap_mwh,

    bs.eta_ch, bs.eta_dis, bs.lambda_h,
    bk.emax_total_mwh, bk.emax_oze_mwh, bk.emax_arbi_mwh,
    (SELECT arbi_dis_to_load FROM par_arbi) AS arbi_dis_to_load,
    b.e_delta_mwh, b.e_surplus_mwh, b.e_deficit_mwh
  FROM base b
  CROSS JOIN bess   bs
  CROSS JOIN client cl
  CROSS JOIN buckets bk
),
flows AS (
  SELECT
    c.*,

    -- (A) Nadwyżka (delta ≥ 0): OZE → ARBI → eksport
    -- 1) Ładowanie OZE (NET); brutto z nadwyżki i straty sprawności
    CASE WHEN c.e_delta_mwh >= 0
         THEN LEAST(c.e_ch_cap_net_oze_mwh, c.e_delta_mwh * c.eta_ch)
         ELSE 0 END AS e_oze_ch_in_mwh,  -- NET (→ SOC OZE)

    CASE WHEN c.e_delta_mwh >= 0
         THEN (LEAST(c.e_ch_cap_net_oze_mwh, c.e_delta_mwh * c.eta_ch) / NULLIF(c.eta_ch, 0))
         ELSE 0 END AS e_oze_ch_gross_from_surplus_mwh,

    CASE WHEN c.e_delta_mwh >= 0
         THEN LEAST(c.e_ch_cap_net_oze_mwh, c.e_delta_mwh * c.eta_ch) * (1.0 / NULLIF(c.eta_ch,1.0) - 1.0)
         ELSE 0 END AS e_oze_loss_eff_ch_mwh,

    -- 2) Pozostała nadwyżka po OZE (brutto) – wyliczona w LATERAL
    s.surplus_after_oze_gross_mwh,

    -- 3) Ładowanie ARBI (NET) z pozostałej nadwyżki
    CASE WHEN c.e_delta_mwh >= 0
         THEN LEAST(c.e_ch_cap_net_arbi_mwh, GREATEST(0, s.surplus_after_oze_gross_mwh * c.eta_ch))
         ELSE 0 END AS e_arbi_ch_in_mwh,   -- NET (→ SOC ARBI)

    CASE WHEN c.e_delta_mwh >= 0
         THEN (LEAST(c.e_ch_cap_net_arbi_mwh, GREATEST(0, s.surplus_after_oze_gross_mwh * c.eta_ch)) / NULLIF(c.eta_ch,0))
         ELSE 0 END AS e_arbi_ch_gross_from_surplus_mwh,

    CASE WHEN c.e_delta_mwh >= 0
         THEN LEAST(c.e_ch_cap_net_arbi_mwh, GREATEST(0, s.surplus_after_oze_gross_mwh * c.eta_ch)) * (1.0 / NULLIF(c.eta_ch,1.0) - 1.0)
         ELSE 0 END AS e_arbi_loss_eff_ch_mwh,

    -- (B) Deficyt (delta < 0): rozładowanie do odbiorcy i import
    CASE WHEN c.e_delta_mwh < 0
         THEN LEAST(c.e_dis_cap_deliver_oze_mwh, c.e_deficit_mwh)
         ELSE 0 END AS e_oze_dis_to_load_mwh,       -- DELIVER → odbiorca

    CASE WHEN c.e_delta_mwh < 0 AND c.arbi_dis_to_load
         THEN GREATEST(0, LEAST(c.e_dis_cap_deliver_arbi_mwh,
                                 c.e_deficit_mwh - LEAST(c.e_dis_cap_deliver_oze_mwh, c.e_deficit_mwh)))
         ELSE 0 END AS e_arbi_dis_to_load_mwh,      -- DELIVER → odbiorca

    -- straty efektywności rozładowania (po stronie SOC)
    CASE WHEN c.e_delta_mwh < 0
         THEN (LEAST(c.e_dis_cap_deliver_oze_mwh, c.e_deficit_mwh) / NULLIF(c.eta_dis,0)) - LEAST(c.e_dis_cap_deliver_oze_mwh, c.e_deficit_mwh)
         ELSE 0 END AS e_oze_loss_eff_dis_mwh,

    CASE WHEN c.e_delta_mwh < 0 AND c.arbi_dis_to_load
         THEN (LEAST(c.e_dis_cap_deliver_arbi_mwh,
                     GREATEST(0, c.e_deficit_mwh - c.e_dis_cap_deliver_oze_mwh)) / NULLIF(c.eta_dis,0))
              - LEAST(c.e_dis_cap_deliver_arbi_mwh,
                      GREATEST(0, c.e_deficit_mwh - c.e_dis_cap_deliver_oze_mwh))
         ELSE 0 END AS e_arbi_loss_eff_dis_mwh,

    -- Import z sieci (PCC cap)
    CASE WHEN c.e_delta_mwh < 0
         THEN LEAST(c.grid_import_cap_mwh,
                    GREATEST(0, c.e_deficit_mwh - LEAST(c.e_dis_cap_deliver_oze_mwh, c.e_deficit_mwh)
                                            - (CASE WHEN c.arbi_dis_to_load
                                                    THEN LEAST(c.e_dis_cap_deliver_arbi_mwh,
                                                               GREATEST(0, c.e_deficit_mwh - c.e_dis_cap_deliver_oze_mwh))
                                                    ELSE 0 END)))
         ELSE 0 END AS grid_import_mwh

  FROM caps c
  CROSS JOIN LATERAL (
    SELECT CASE
             WHEN c.e_delta_mwh >= 0 THEN
               GREATEST(
                 0,
                 c.e_delta_mwh
                 - (LEAST(c.e_ch_cap_net_oze_mwh, c.e_delta_mwh * c.eta_ch) / NULLIF(c.eta_ch,0))
               )
             ELSE 0
           END AS surplus_after_oze_gross_mwh
  ) s
),
flows2 AS (
  SELECT
    f.*,

    -- eksport przed capem PCC (tu możemy odwołać się do aliasów z flows)
    CASE WHEN f.e_delta_mwh >= 0
         THEN GREATEST(0, f.e_delta_mwh - f.e_oze_ch_gross_from_surplus_mwh - f.e_arbi_ch_gross_from_surplus_mwh)
         ELSE 0 END AS export_before_cap_mwh,

    -- eksport po capie PCC
    LEAST(
      CASE WHEN f.e_delta_mwh >= 0
           THEN GREATEST(0, f.e_delta_mwh - f.e_oze_ch_gross_from_surplus_mwh - f.e_arbi_ch_gross_from_surplus_mwh)
           ELSE 0 END,
      f.grid_export_cap_mwh
    ) AS grid_export_mwh,

    -- NET out (na potrzeby kolejnych widoków)
    CASE WHEN f.e_oze_dis_to_load_mwh  > 0 THEN f.e_oze_dis_to_load_mwh  / NULLIF(f.eta_dis,0) ELSE 0 END AS e_oze_dis_out_mwh,
    CASE WHEN f.e_arbi_dis_to_load_mwh > 0 THEN f.e_arbi_dis_to_load_mwh / NULLIF(f.eta_dis,0) ELSE 0 END AS e_arbi_dis_out_mwh
  FROM flows f
)

SELECT
  -- czas i parametry
  ts, dt_h,
  eta_ch, eta_dis, lambda_h,
  emax_total_mwh, emax_oze_mwh, emax_arbi_mwh,
  grid_import_cap_mwh, grid_export_cap_mwh,
  arbi_dis_to_load,

  -- wejście bazowe
  e_delta_mwh, e_surplus_mwh, e_deficit_mwh,

  -- CAPy
  e_ch_cap_net_mwh, e_dis_cap_net_mwh, e_dis_cap_deliver_mwh,
  e_ch_cap_net_oze_mwh, e_ch_cap_net_arbi_mwh,
  e_dis_cap_net_oze_mwh, e_dis_cap_net_arbi_mwh,
  e_dis_cap_deliver_oze_mwh, e_dis_cap_deliver_arbi_mwh,

  -- PRZEPŁYWY i straty
  ROUND(e_oze_ch_in_mwh, 6)                 AS e_oze_ch_in_mwh,             -- NET (→ 02)
  ROUND(e_arbi_ch_in_mwh, 6)                AS e_arbi_ch_in_mwh,            -- NET (→ 03)
  ROUND(e_oze_ch_gross_from_surplus_mwh,6)  AS e_oze_ch_gross_from_surplus_mwh,
  ROUND(e_arbi_ch_gross_from_surplus_mwh,6) AS e_arbi_ch_gross_from_surplus_mwh,
  ROUND(e_oze_loss_eff_ch_mwh, 6)           AS e_oze_loss_eff_ch_mwh,
  ROUND(e_arbi_loss_eff_ch_mwh, 6)          AS e_arbi_loss_eff_ch_mwh,

  ROUND(surplus_after_oze_gross_mwh, 6)     AS surplus_after_oze_gross_mwh,

  ROUND(e_oze_dis_to_load_mwh, 6)           AS e_oze_dis_to_load_mwh,       -- DELIVER
  ROUND(e_arbi_dis_to_load_mwh, 6)          AS e_arbi_dis_to_load_mwh,      -- DELIVER
  ROUND(e_oze_dis_out_mwh, 6)               AS e_oze_dis_out_mwh,           -- NET (→ 02)
  ROUND(e_arbi_dis_out_mwh, 6)              AS e_arbi_dis_out_mwh,          -- NET (→ 03)
  ROUND(e_oze_loss_eff_dis_mwh, 6)          AS e_oze_loss_eff_dis_mwh,
  ROUND(e_arbi_loss_eff_dis_mwh, 6)         AS e_arbi_loss_eff_dis_mwh,

  -- sieć + diagnostyka
  ROUND(export_before_cap_mwh, 6)           AS export_before_cap_mwh,
  ROUND(grid_import_mwh, 6)                 AS grid_import_mwh,
  ROUND(grid_export_mwh, 6)                 AS grid_export_mwh,
  ROUND(GREATEST(0, export_before_cap_mwh - grid_export_mwh), 6) AS curtailed_surplus_mwh,
  ROUND(GREATEST(0, e_deficit_mwh - e_oze_dis_to_load_mwh - e_arbi_dis_to_load_mwh - grid_import_mwh), 6) AS unserved_load_mwh

FROM flows2
ORDER BY ts;
