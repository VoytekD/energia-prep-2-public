-- 02_output_energy_soc_oze.sql — SOC OZE + spill do ARBI
-- Metodyka:
--  • λ podana „na miesiąc” → r_hour = (1-λ)^(1/H_miesiąca), r_step = r_hour^dt_h.
--  • SOC_{t+1} = clamp( SOC_t*r_step + e_oze_ch_in_mwh - e_oze_dis_out_mwh, 0, emax_oze_mwh ).
--  • „Spill” do ARBI = ile NET przycięła klamra u góry (clip_to_emax_mwh), ale tylko dla godzin z nadwyżką (delta ≥ 0).
--    Dodatkowo wystawiamy równoważnik po stronie AC: spill_gross = spill_net / eta_ch (z 01).
--
-- Wejścia:
--  • 00: output.energy_base(ts, dt_h, e_delta_mwh)
--  • 01: output.energy_broker_detail(ts, emax_oze_mwh, e_oze_ch_in_mwh, e_oze_dis_out_mwh, eta_ch)
-- Parametr:
--  • GUC energia.form_bess_param_json: lambda_month (ułamek) lub lambda_month_pct (procent).

CREATE SCHEMA IF NOT EXISTS output;

CREATE OR REPLACE VIEW output.energy_soc_oze AS
WITH RECURSIVE
base AS (
  SELECT
    b.ts_utc AS ts,
    COALESCE(b.dt_h, 1.0)::numeric      AS dt_h,
    COALESCE(b.e_delta_mwh, 0)::numeric AS e_delta_mwh
  FROM output.energy_base b
),
bro AS (
  SELECT
    e.ts,
    COALESCE(e.emax_oze_mwh, 0)::numeric      AS emax_oze_mwh,
    COALESCE(e.e_oze_ch_in_mwh, 0)::numeric   AS e_ch_in_mwh,   -- NET → SOC
    COALESCE(e.e_oze_dis_out_mwh, 0)::numeric AS e_dis_out_mwh, -- NET ← SOC
    COALESCE(e.eta_ch, 1)::numeric            AS eta_ch         -- do spill_gross
  FROM output.energy_broker_detail e
),
param AS (
  SELECT LEAST(GREATEST(b.bess_lambda_month::numeric, 0), 0.999999) AS lambda_month
  FROM params.form_bess_param b
  ORDER BY b.updated_at DESC NULLS LAST
  LIMIT 1
),
joined AS (
  SELECT
    b.ts, b.dt_h, b.e_delta_mwh,
    br.emax_oze_mwh, br.e_ch_in_mwh, br.e_dis_out_mwh, br.eta_ch,
    p.lambda_month
  FROM base b
  LEFT JOIN bro br ON br.ts = b.ts
  CROSS JOIN param p
  ORDER BY b.ts
),
month_hours AS (
  SELECT
    j.*,
    EXTRACT(EPOCH FROM (date_trunc('month', j.ts + interval '1 month')
                        - date_trunc('month', j.ts))) / 3600.0::numeric AS hours_in_month
  FROM joined j
),
retention AS (
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
ordered AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY ts) AS rn,
    ts, dt_h, e_delta_mwh, emax_oze_mwh, e_ch_in_mwh, e_dis_out_mwh, eta_ch,
    lambda_month, hours_in_month, r_hour, r_step
  FROM retention
  ORDER BY ts
),
rec AS (
  -- SOC_0 = 0 (jeśli chcesz parametr startowy, tu łatwo go podpiąć)
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
calc AS (
  SELECT
    r.*,
    -- surowe (przed klamrą) dla diagnostyki „ile ucięto”
    (r.soc_prev_mwh * r.r_step + r.e_ch_in_mwh - r.e_dis_out_mwh) AS soc_raw_mwh,

    -- ile NET przyciął limit górny (to kandydat na spill do ARBI)
    GREATEST(0.0,
      (r.soc_prev_mwh * r.r_step + r.e_ch_in_mwh - r.e_dis_out_mwh) - r.soc_curr_mwh
    ) AS clip_to_emax_mwh
  FROM rec r
)
SELECT
  ts,
  dt_h,
  emax_oze_mwh,

  -- Parametry λ i retencje
  ROUND(lambda_month, 8)   AS lambda_month,
  ROUND(hours_in_month,3)  AS hours_in_month,
  ROUND(r_hour, 10)        AS retention_per_hour,
  ROUND(r_step, 10)        AS retention_per_step,

  -- Wejścia/wyjścia względem SOC (z 01)
  ROUND(e_ch_in_mwh, 6)    AS e_oze_ch_in_mwh,
  ROUND(e_dis_out_mwh, 6)  AS e_oze_dis_out_mwh,

  -- Strata „idle” w kroku (implikowana przez retencję): SOC_t * (1 - r_step)
  ROUND(soc_prev_mwh * (1 - r_step), 6) AS e_oze_loss_idle_mwh,

  -- Stany i diagnostyka
  ROUND(soc_prev_mwh, 6)   AS soc_oze_prev_mwh,
  ROUND(soc_raw_mwh, 6)    AS soc_oze_raw_mwh,
  ROUND(soc_curr_mwh, 6)   AS soc_oze_mwh,
  ROUND(clip_to_emax_mwh,6) AS clip_to_emax_mwh,

  -- *** SPILL DO ARBI ***
  -- NET do przelania (dokładnie to, co „uciął” limit OZE), ale tylko przy nadwyżce (delta ≥ 0):
  CASE WHEN e_delta_mwh >= 0 THEN ROUND(clip_to_emax_mwh, 6) ELSE 0 END AS spill_oze_to_arbi_net_mwh,

  -- Równoważnik po stronie AC (brutto), użyteczne gdy 03 chce przeliczyć na własne eta_ch:
  CASE
    WHEN e_delta_mwh >= 0 AND eta_ch > 0 THEN ROUND(clip_to_emax_mwh / eta_ch, 6)
    ELSE 0
  END AS spill_oze_to_arbi_gross_mwh

FROM calc
ORDER BY ts;
