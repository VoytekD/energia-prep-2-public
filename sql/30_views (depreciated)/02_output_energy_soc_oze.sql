-- 02_output_energy_soc_oze.sql — SOC OZE + spill do ARBI (spójne z 03)
-- Metodyka:
--  • λ podana „na miesiąc” → r_hour = (1-λ)^(1/H_miesiąca), r_step = r_hour^dt_h.
--  • SOC_{t+1} = clamp( SOC_t*r_step + e_oze_ch_in_mwh - e_oze_dis_out_mwh, 0, emax_oze_mwh ).
--  • „Spill” do ARBI = ile NET przycięła klamra u góry (clip_to_emax_mwh), ale tylko dla godzin z nadwyżką (delta ≥ 0).
--    Dodatkowo wystawiamy równoważnik po stronie AC: spill_gross = spill_net / eta_ch (z 01).

CREATE SCHEMA IF NOT EXISTS output;

CREATE OR REPLACE VIEW output.energy_soc_oze AS
WITH RECURSIVE
-- 0) Oś czasu + delta z 00
base AS MATERIALIZED (
  SELECT
    b.ts_utc AS ts,
    COALESCE(b.dt_h, 1.0)::numeric      AS dt_h,
    COALESCE(b.e_delta_mwh, 0)::numeric AS e_delta_mwh
  FROM output.energy_base b
),

-- 1) Wejścia z 01 (brokerski OZE)
bro AS MATERIALIZED (
  SELECT
    e.ts,
    COALESCE(e.emax_oze_mwh, 0)::numeric      AS emax_oze_mwh,
    COALESCE(e.e_oze_ch_in_mwh, 0)::numeric   AS e_ch_in_mwh,   -- NET → SOC
    COALESCE(e.e_oze_dis_out_mwh, 0)::numeric AS e_dis_out_mwh, -- NET ← SOC
    COALESCE(e.eta_ch, 1)::numeric            AS eta_ch         -- do spill_gross
  FROM output.energy_broker_detail e
),

-- 2) Parametr λ z params.form_bess_param (ostatni rekord)
param AS MATERIALIZED (
  SELECT LEAST(GREATEST(b.bess_lambda_month::numeric, 0), 0.999999) AS lambda_month
  FROM params.form_bess_param b
  ORDER BY b.updated_at DESC NULLS LAST
  LIMIT 1
),

-- 3) Złączenie
joined AS MATERIALIZED (
  SELECT
    b.ts, b.dt_h, b.e_delta_mwh,
    br.emax_oze_mwh, br.e_ch_in_mwh, br.e_dis_out_mwh, br.eta_ch,
    p.lambda_month
  FROM base b
  LEFT JOIN bro br ON br.ts = b.ts
  CROSS JOIN param p
),

-- 4) Liczba godzin w miesiącu dla każdej godziny ts (do przeliczenia λ→retencja)
month_hours AS MATERIALIZED (
  SELECT
    j.*,
    EXTRACT(EPOCH FROM (date_trunc('month', j.ts + interval '1 month')
                        - date_trunc('month', j.ts))) / 3600.0::numeric AS hours_in_month
  FROM joined j
),

-- 5) Retencje r_hour i r_step
retention AS MATERIALIZED (
  SELECT
    m.*,
    CASE WHEN m.hours_in_month > 0
         THEN power(1 - m.lambda_month, 1.0 / m.hours_in_month)
         ELSE 1.0
    END::numeric AS r_hour,
    CASE WHEN m.hours_in_month > 0
         THEN power(power(1 - m.lambda_month, 1.0 / m.hours_in_month), m.dt_h)
         ELSE 1.0
    END::numeric AS r_step
  FROM month_hours m
),

-- 6) Porządkowanie do rekurencji
ordered AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY ts) AS rn,
    ts, dt_h, e_delta_mwh, emax_oze_mwh, e_ch_in_mwh, e_dis_out_mwh, eta_ch,
    lambda_month, hours_in_month, r_hour, r_step
  FROM retention
),

-- 7) Rekurencja SOC OZE
rec AS (
  -- seed: SOC_0 = 0 (jeśli chcesz parametr startowy, łatwo wpiąć tu)
  SELECT
    o.rn, o.ts, o.dt_h, o.e_delta_mwh, o.emax_oze_mwh, o.e_ch_in_mwh, o.e_dis_out_mwh, o.eta_ch,
    o.lambda_month, o.hours_in_month, o.r_hour, o.r_step,
    0.0::numeric AS soc_prev_mwh,
    LEAST(o.emax_oze_mwh,
          GREATEST(0.0, 0.0 * o.r_step + o.e_ch_in_mwh - o.e_dis_out_mwh)
    )::numeric AS soc_curr_mwh
  FROM ordered o
  WHERE o.rn = 1

  UNION ALL

  SELECT
    o.rn, o.ts, o.dt_h, o.e_delta_mwh, o.emax_oze_mwh, o.e_ch_in_mwh, o.e_dis_out_mwh, o.eta_ch,
    o.lambda_month, o.hours_in_month, o.r_hour, o.r_step,
    r.soc_curr_mwh AS soc_prev_mwh,
    LEAST(o.emax_oze_mwh,
          GREATEST(0.0,
                   r.soc_curr_mwh * o.r_step
                   + o.e_ch_in_mwh - o.e_dis_out_mwh)
    )::numeric AS soc_curr_mwh
  FROM ordered o
  JOIN rec r ON o.rn = r.rn + 1
),

-- 8) Diagnostyka raw + „clip” (ile ucięła górna klamra)
calc AS (
  SELECT
    r.*,
    (r.soc_prev_mwh * r.r_step + r.e_ch_in_mwh - r.e_dis_out_mwh) AS soc_raw_mwh,
    GREATEST(0.0,
      (r.soc_prev_mwh * r.r_step + r.e_ch_in_mwh - r.e_dis_out_mwh) - r.soc_curr_mwh
    ) AS clip_to_emax_mwh
  FROM rec r
)

-- 9) Wyjście (surowe wartości — zaokrąglasz w 04)
SELECT
  ts,
  dt_h,
  emax_oze_mwh,

  -- Parametry λ i retencje
  lambda_month,
  hours_in_month,
  r_hour       AS retention_per_hour,
  r_step       AS retention_per_step,

  -- Wejścia/wyjścia względem SOC (z 01)
  e_ch_in_mwh   AS e_oze_ch_in_mwh,
  e_dis_out_mwh AS e_oze_dis_out_mwh,

  -- Strata „idle” w kroku (implikowana przez retencję): SOC_t * (1 - r_step)
  (soc_prev_mwh * (1 - r_step)) AS e_oze_loss_idle_mwh,

  -- Stany i diagnostyka
  soc_prev_mwh  AS soc_oze_prev_mwh,
  soc_raw_mwh   AS soc_oze_raw_mwh,
  soc_curr_mwh  AS soc_oze_mwh,
  clip_to_emax_mwh,

  -- *** SPILL DO ARBI ***
  -- NET do przelania (dokładnie to, co „uciął” limit OZE), ale tylko przy nadwyżce (delta ≥ 0):
  CASE WHEN e_delta_mwh >= 0 THEN clip_to_emax_mwh ELSE 0 END AS spill_oze_to_arbi_net_mwh,

  -- Równoważnik po stronie AC (brutto), przydatny gdy liczymy GROSS po eta_ch:
  CASE
    WHEN e_delta_mwh >= 0 AND eta_ch > 0 THEN clip_to_emax_mwh / eta_ch
    ELSE 0
  END AS spill_oze_to_arbi_gross_mwh,

  -- =========================
  -- NOWE: procenty zapełnienia
  -- =========================

  -- % zapełnienia OZE na swojej części magazynu
  CASE
    WHEN emax_oze_mwh > 0
      THEN ((100 * soc_curr_mwh::numeric) / emax_oze_mwh)::numeric(6,2)
    ELSE 0
  END AS oze_soc_pct,

  -- % zapełnienia OZE względem całego magazynu (globalny emax z params.form_zmienne)
  CASE
    WHEN (SELECT COALESCE(z.emax, 0)::numeric
          FROM params.form_zmienne z
          ORDER BY z.updated_at DESC NULLS LAST
          LIMIT 1) > 0
      THEN (
        (100 * soc_curr_mwh::numeric)
        / NULLIF(
            (SELECT COALESCE(z.emax, 0)::numeric
             FROM params.form_zmienne z
             ORDER BY z.updated_at DESC NULLS LAST
             LIMIT 1),
            0
          )
      )::numeric(6,2)
    ELSE 0
  END AS oze_soc_pct_total

FROM calc
