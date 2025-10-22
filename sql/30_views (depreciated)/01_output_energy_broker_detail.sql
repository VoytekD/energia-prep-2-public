-- 01_output_energy_broker_detail.sql — BROKER: OZE/ARBI + import/eksport (bez GUC) + date_dim.*
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
    b.ts_utc AS ts,
    b.dt_h,
    b.e_delta_mwh::numeric   AS e_delta_mwh,
    b.e_surplus_mwh::numeric AS e_surplus_mwh,
    b.e_deficit_mwh::numeric AS e_deficit_mwh
  FROM output.energy_base b
),

-- ► Parametry BESS z params.form_bess_param (bez GUC). Konwersje: eff%→ułamek; λ_mies→λ_h (30*24).
bess AS (
  SELECT
    COALESCE(b.bess_c_rate_charge::numeric,    0)                     AS c_rate_ch_h,
    COALESCE(b.bess_c_rate_discharge::numeric, 0)                     AS c_rate_dis_h,
    COALESCE(b.bess_charge_eff::numeric,     100) / 100.0             AS eta_ch,
    COALESCE(b.bess_discharge_eff::numeric,  100) / 100.0             AS eta_dis,
    COALESCE(b.bess_lambda_month::numeric,     0) / (30*24)           AS lambda_h
  FROM params.form_bess_param b
  ORDER BY b.updated_at DESC NULLS LAST
  LIMIT 1
),

-- ► Parametry klienta z params.form_parametry_klienta (bez GUC)
client AS (
  SELECT COALESCE(c.klient_moc_umowna::numeric, 0)::numeric AS moc_umowna_mw
  FROM params.form_parametry_klienta c
  ORDER BY c.updated_at DESC NULLS LAST
  LIMIT 1
),

-- ► Zmienne ogólne
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

-- ► Parametry arbitrażu
par_arbi AS (
  SELECT arbi_dis_to_load
  FROM params.form_par_arbitrazu
  ORDER BY updated_at DESC NULLS LAST
  LIMIT 1
),

caps AS (
  SELECT
    b.ts, b.dt_h,

    -- CAP-y z c-rate (NET/DELIVER), bez ograniczeń PCC na rozładowanie do load
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

    -- Capacity ładowania ARBI z sieci (NET), limitowane c-rate i PCC (import)
    LEAST(
      CASE WHEN bs.c_rate_ch_h > 0 THEN (bk.emax_arbi_mwh / bs.c_rate_ch_h) * b.dt_h * bs.eta_ch ELSE 0 END,
      (cl.moc_umowna_mw * b.dt_h)
    ) AS e_arbi_ch_from_grid_cap_net_mwh,

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
         THEN LEAST(c.e_ch_cap_net_oze_mwh, c.e_delta_mwh * c.eta_ch) * ((1.0 / NULLIF(c.eta_ch,1.0)) - 1.0)
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
         THEN LEAST(c.e_ch_cap_net_arbi_mwh, GREATEST(0, s.surplus_after_oze_gross_mwh * c.eta_ch)) * ((1.0 / NULLIF(c.eta_ch,1.0)) - 1.0)
         ELSE 0 END AS e_arbi_loss_eff_ch_mwh,

    -- (B) Deficyt (delta < 0): rozładowanie do odbiorcy i import
    CASE WHEN c.e_delta_mwh < 0
         THEN LEAST(c.e_dis_cap_deliver_oze_mwh, c.e_deficit_mwh)
         ELSE 0 END AS e_oze_dis_to_load_mwh,       -- DELIVER → odbiorca

    CASE WHEN c.e_delta_mwh < 0 AND c.arbi_dis_to_load
         THEN GREATEST(
                0,
                LEAST(
                  c.e_dis_cap_deliver_arbi_mwh,
                  GREATEST(0, c.e_deficit_mwh - LEAST(c.e_dis_cap_deliver_oze_mwh, c.e_deficit_mwh))
                )
              )
         ELSE 0 END AS e_arbi_dis_to_load_mwh,      -- DELIVER → odbiorca

    -- straty efektywności rozładowania (po stronie SOC)
    CASE WHEN c.e_delta_mwh < 0
         THEN (LEAST(c.e_dis_cap_deliver_oze_mwh, c.e_deficit_mwh) / NULLIF(c.eta_dis,0))
              - LEAST(c.e_dis_cap_deliver_oze_mwh, c.e_deficit_mwh)
         ELSE 0 END AS e_oze_loss_eff_dis_mwh,

    CASE WHEN c.e_delta_mwh < 0 AND c.arbi_dis_to_load
         THEN (LEAST(
                 c.e_dis_cap_deliver_arbi_mwh,
                 GREATEST(0, c.e_deficit_mwh - LEAST(c.e_dis_cap_deliver_oze_mwh, c.e_deficit_mwh))
               ) / NULLIF(c.eta_dis,0))
              - LEAST(
                  c.e_dis_cap_deliver_arbi_mwh,
                  GREATEST(0, c.e_deficit_mwh - LEAST(c.e_dis_cap_deliver_oze_mwh, c.e_deficit_mwh))
                )
         ELSE 0 END AS e_arbi_loss_eff_dis_mwh,

    -- Import z sieci (PCC cap)
    CASE WHEN c.e_delta_mwh < 0
         THEN LEAST(
                c.grid_import_cap_mwh,
                GREATEST(
                  0,
                  c.e_deficit_mwh
                  - LEAST(c.e_dis_cap_deliver_oze_mwh, c.e_deficit_mwh)
                  - CASE
                      WHEN c.arbi_dis_to_load THEN LEAST(
                        c.e_dis_cap_deliver_arbi_mwh,
                        GREATEST(0, c.e_deficit_mwh - c.e_dis_cap_deliver_oze_mwh)
                      )
                      ELSE 0
                    END
                )
              )
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

    -- eksport przed capem PCC
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
  -- pełny kontekst kalendarza
  d.*,

  -- ▼ alias dla zgodności wstecz
  d.ts_utc AS ts,

  -- parametry i limity
  x.dt_h,
  x.eta_ch,
  x.eta_dis,
  x.lambda_h,
  x.emax_total_mwh,
  x.emax_oze_mwh,
  x.emax_arbi_mwh,
  x.grid_import_cap_mwh,
  x.grid_export_cap_mwh,
  x.arbi_dis_to_load,

  -- capacity ładowania ARBI z sieci (NET)
  x.e_arbi_ch_from_grid_cap_net_mwh,

  -- balans i deficyt/nadwyżka z 00
  x.e_delta_mwh,
  x.e_surplus_mwh,
  x.e_deficit_mwh,

  -- limity krokowe
  x.e_ch_cap_net_mwh,
  x.e_dis_cap_net_mwh,
  x.e_dis_cap_deliver_mwh,
  x.e_ch_cap_net_oze_mwh,
  x.e_ch_cap_net_arbi_mwh,
  x.e_dis_cap_net_oze_mwh,
  x.e_dis_cap_net_arbi_mwh,
  x.e_dis_cap_deliver_oze_mwh,
  x.e_dis_cap_deliver_arbi_mwh,

  -- OZE/ARBI: charge/discharge + straty eff
  x.e_oze_ch_in_mwh,
  x.e_arbi_ch_in_mwh,
  x.e_oze_ch_gross_from_surplus_mwh,
  x.e_arbi_ch_gross_from_surplus_mwh,
  x.e_oze_loss_eff_ch_mwh,
  x.e_arbi_loss_eff_ch_mwh,

  -- nadwyżka po OZE (gross) → dostępna dla ARBI/export
  x.surplus_after_oze_gross_mwh,

  -- rozładowania do odbiorcy i wyjścia z SOC (NET)
  x.e_oze_dis_to_load_mwh,
  x.e_arbi_dis_to_load_mwh,
  x.e_oze_dis_out_mwh,
  x.e_arbi_dis_out_mwh,
  x.e_oze_loss_eff_dis_mwh,
  x.e_arbi_loss_eff_dis_mwh,

  -- eksport / import
  x.export_before_cap_mwh,
  x.grid_import_mwh,
  x.grid_export_mwh,

  -- wykorzystanie PCC przez nadwyżkę i „headroom”
  x.grid_export_mwh                                   AS grid_export_used_by_surplus_mwh,
  GREATEST(0, x.grid_export_cap_mwh - x.grid_export_mwh) AS grid_export_cap_remaining_mwh,

  -- diagnostyka systemowa
  GREATEST(0, x.export_before_cap_mwh - x.grid_export_mwh) AS curtailed_surplus_mwh,
  GREATEST(0, x.e_deficit_mwh - x.e_oze_dis_to_load_mwh - x.e_arbi_dis_to_load_mwh - x.grid_import_mwh) AS unserved_load_mwh
FROM flows2 x
JOIN input.date_dim d
  ON d.ts_utc = x.ts;

-- (sortuj tylko w zapytaniach użytkowych)
