-- 03_output_energy_soc_arbi.sql — ARBITRAŻ wg parowania ekstremów (z diagnostyką)
-- Zmiany:
--  • dodano źródło ładowania z sieci: e_arbi_ch_from_grid_cap_net_mwh (capacity z 01)
--  • wyprowadzono faktycznie użyte e_arbi_ch_from_grid_used_net_mwh (priorytet: broker+spill → grid)
--  • gdy arbi_dis_to_load = false, extra-discharge do sieci capowane PCC:
--      cap_net_by_pcc = max(0, grid_export_cap_mwh - grid_export_used_by_surplus_mwh) / eta_dis

CREATE SCHEMA IF NOT EXISTS output;

CREATE OR REPLACE VIEW output.energy_soc_arbi AS
WITH RECURSIVE
-- 0) OŚ CZASU + CENY
base AS (
  SELECT
    b.ts_utc AS ts,
    COALESCE(b.dt_h, 1.0)::numeric        AS dt_h,
    COALESCE(b.price_tge_pln, 0)::numeric AS price_pln_mwh
  FROM output.energy_base b
),
with_day AS (
  SELECT
    ts, dt_h, price_pln_mwh,
    (ts AT TIME ZONE 'Europe/Warsaw')                         AS ts_local,
    (ts AT TIME ZONE 'Europe/Warsaw')::date                   AS day_key,
    EXTRACT(HOUR FROM (ts AT TIME ZONE 'Europe/Warsaw'))::int AS hour_local
  FROM base
),

-- 1) LIMITY/DECYZJE z 01
lim01 AS (
  SELECT
    e.ts,
    COALESCE(e.e_ch_cap_net_mwh, 0)::numeric                  AS e_ch_cap_net_mwh,
    COALESCE(e.e_dis_cap_net_mwh, 0)::numeric                 AS e_dis_cap_net_mwh,
    COALESCE(e.eta_ch, 1)::numeric                            AS eta_ch,
    COALESCE(e.eta_dis,1)::numeric                            AS eta_dis,

    COALESCE(e.e_arbi_ch_in_mwh, 0)::numeric                  AS e_broker_ch_net_mwh,
    COALESCE(e.e_arbi_dis_out_mwh, 0)::numeric                AS e_broker_dis_net_mwh,
    COALESCE(e.e_arbi_dis_to_load_mwh, 0)::numeric            AS e_broker_dis_deliver_mwh,

    -- NOWE: capacity ładowania z sieci (NET)
    COALESCE(e.e_arbi_ch_from_grid_cap_net_mwh, 0)::numeric   AS e_grid_ch_cap_net_mwh,

    -- NOWE: export PCC (cap i „zużyte przez nadwyżkę”)
    COALESCE(e.grid_export_cap_mwh, 0)::numeric               AS grid_export_cap_mwh,
    COALESCE(e.grid_export_used_by_surplus_mwh, 0)::numeric   AS grid_export_used_by_surplus_mwh
  FROM output.energy_broker_detail e
),

-- 2) SPILL i retencja z 02
oze02 AS (
  SELECT
    z.ts,
    COALESCE(z.spill_oze_to_arbi_net_mwh, 0)::numeric AS spill_oze_to_arbi_net_mwh,
    COALESCE(z.retention_per_step, 1)::numeric        AS r_step
  FROM output.energy_soc_oze z
),

-- 3) CAP (params.form_zmienne)
caps AS (
  SELECT
    ROUND(COALESCE(z.emax,0)::numeric * COALESCE(z.procent_arbitrazu,0)::numeric / 100.0, 6) AS emax_arbi_mwh
  FROM params.form_zmienne z
  ORDER BY z.updated_at DESC NULLS LAST
  LIMIT 1
),

-- 4) Parametry arbitrażu (params.form_par_arbitrazu) — wersja szybka: tablice INT[] i materializacja
par AS MATERIALIZED (
  SELECT
    base_min_profit_pln_mwh::numeric AS base_min_profit_pln_mwh,
    cycles_per_day::int              AS cycles_per_day,
    allow_carry_over::boolean        AS allow_carry_over,
    force_order::boolean             AS force_order,
    bonus_ch_window::numeric         AS bonus_ch_window,
    bonus_dis_window::numeric        AS bonus_dis_window,
    bonus_low_soc_ch::numeric        AS bonus_low_soc_ch,
    bonus_high_soc_dis::numeric      AS bonus_high_soc_dis,
    soc_low_threshold::numeric       AS soc_low_threshold,
    soc_high_threshold::numeric      AS soc_high_threshold,
    arbi_dis_to_load::boolean        AS arbi_dis_to_load,

    -- INT[] zamiast JSONB (odporne na NULL i TEXT/JSONB)
    COALESCE(
      ARRAY(SELECT (v)::int
            FROM jsonb_array_elements_text(COALESCE((bonus_hrs_ch)::jsonb, '[]'::jsonb)) AS v),
      '{}'
    ) AS bonus_hrs_ch_arr,
    COALESCE(
      ARRAY(SELECT (v)::int
            FROM jsonb_array_elements_text(COALESCE((bonus_hrs_dis)::jsonb, '[]'::jsonb)) AS v),
      '{}'
    ) AS bonus_hrs_dis_arr,
    COALESCE(
      ARRAY(SELECT (v)::int
            FROM jsonb_array_elements_text(COALESCE((bonus_hrs_ch_free)::jsonb, '[]'::jsonb)) AS v),
      '{}'
    ) AS bonus_hrs_ch_free_arr,
    COALESCE(
      ARRAY(SELECT (v)::int
            FROM jsonb_array_elements_text(COALESCE((bonus_hrs_dis_free)::jsonb, '[]'::jsonb)) AS v),
      '{}'
    ) AS bonus_hrs_dis_free_arr
  FROM params.form_par_arbitrazu
  ORDER BY updated_at DESC NULLS LAST
  LIMIT 1
),

-- 5) Parowanie Low↔High (ranking godzin w dobie)
ranked AS MATERIALIZED (
  SELECT
    wd.*,
    ROW_NUMBER() OVER (PARTITION BY wd.day_key ORDER BY wd.price_pln_mwh ASC,  wd.ts ASC)  AS rn_low,
    ROW_NUMBER() OVER (PARTITION BY wd.day_key ORDER BY wd.price_pln_mwh DESC, wd.ts ASC) AS rn_high
  FROM with_day wd
),
-- 6) Pary low↔high + flagi bonusowe z CTE par (tablice INT[])
pairs AS MATERIALIZED (
  SELECT
    l.day_key, l.rn_low AS pair_n,
    l.ts AS low_ts,  l.hour_local AS low_hour,  l.price_pln_mwh AS price_low_pln_mwh,
    h.ts AS high_ts, h.hour_local AS high_hour, h.price_pln_mwh AS price_high_pln_mwh,
    (h.price_pln_mwh - l.price_pln_mwh) AS delta_pln_mwh,

    (l.hour_local::int = ANY(par.bonus_hrs_ch_free_arr)) AS is_ch_free,
    (h.hour_local::int = ANY(par.bonus_hrs_dis_free_arr)) AS is_dis_free,
    (l.hour_local::int = ANY(par.bonus_hrs_ch_arr))       AS in_bonus_ch,
    (h.hour_local::int = ANY(par.bonus_hrs_dis_arr))      AS in_bonus_dis
  FROM ranked l
  JOIN ranked h
    ON h.day_key = l.day_key
   AND h.rn_high = l.rn_low
  CROSS JOIN par
),
-- 7) Progi dla par (bez scalar-subselectów)
pairs_threshold AS MATERIALIZED (
  SELECT
    p.*,
    par.base_min_profit_pln_mwh,
    par.bonus_ch_window,
    par.bonus_dis_window,
    par.cycles_per_day,
    par.allow_carry_over,
    par.force_order,
    CASE
      WHEN p.is_ch_free OR p.is_dis_free THEN 0::numeric
      ELSE GREATEST(
             0,
             par.base_min_profit_pln_mwh
             + CASE WHEN p.in_bonus_ch  THEN par.bonus_ch_window  ELSE 0 END
             + CASE WHEN p.in_bonus_dis THEN par.bonus_dis_window ELSE 0 END
           )
    END AS t_pair_base_pln_mwh
  FROM pairs p
  CROSS JOIN par
),
-- 8) Wybór par (kwalifikacja / forced fill)
pairs_selected AS MATERIALIZED (
  SELECT
    pt.*,
    (pt.delta_pln_mwh >= pt.t_pair_base_pln_mwh) AS qualifies_base,
    ROW_NUMBER() OVER (PARTITION BY pt.day_key ORDER BY pt.delta_pln_mwh DESC, pt.pair_n) AS delta_rank,
    COUNT(*) FILTER (WHERE pt.delta_pln_mwh >= pt.t_pair_base_pln_mwh)
      OVER (PARTITION BY pt.day_key) AS n_qualifying,
    (pt.delta_pln_mwh >= pt.t_pair_base_pln_mwh)
      OR (ROW_NUMBER() OVER (PARTITION BY pt.day_key ORDER BY pt.delta_pln_mwh DESC, pt.pair_n)
          <= par.cycles_per_day) AS selected_any,
    CASE
      WHEN (pt.delta_pln_mwh >= pt.t_pair_base_pln_mwh) THEN false
      ELSE (ROW_NUMBER() OVER (PARTITION BY pt.day_key ORDER BY pt.delta_pln_mwh DESC, pt.pair_n)
            <= par.cycles_per_day)
    END AS forced_fill
  FROM pairs_threshold pt
  CROSS JOIN par
),

-- 9) Oś godzinowa + parametry w jednej tabeli (CROSS JOIN par/caps, bez scalar-subselectów)
j AS (
  SELECT
    b.ts, b.dt_h, b.price_pln_mwh,
    wd.day_key, wd.hour_local,

    l.e_ch_cap_net_mwh, l.e_dis_cap_net_mwh, l.eta_ch, l.eta_dis,
    l.e_broker_ch_net_mwh, l.e_broker_dis_net_mwh, l.e_broker_dis_deliver_mwh,

    -- źródła ładowania
    l.e_grid_ch_cap_net_mwh,                          -- capacity grid→ARBI (NET)
    o.spill_oze_to_arbi_net_mwh, o.r_step,

    caps.emax_arbi_mwh,

    -- parametry arbitrażu (wprost z par)
    par.base_min_profit_pln_mwh,
    par.bonus_low_soc_ch,
    par.bonus_high_soc_dis,
    par.soc_low_threshold,
    par.soc_high_threshold,
    par.arbi_dis_to_load,

    -- export PCC
    l.grid_export_cap_mwh,
    l.grid_export_used_by_surplus_mwh,

    -- limit PCC po stronie NET (deliver/η_dis)
    GREATEST(0, l.grid_export_cap_mwh - COALESCE(l.grid_export_used_by_surplus_mwh,0)) / NULLIF(l.eta_dis,0)
      AS cap_net_by_pcc_mwh

  FROM base b
  JOIN with_day wd ON wd.ts = b.ts
  LEFT JOIN lim01 l ON l.ts = b.ts
  LEFT JOIN oze02 o ON o.ts = b.ts
  CROSS JOIN caps
  CROSS JOIN par
),

-- 7) FLAGI Low/High WYBRANEJ PARY — unikalny wiersz na godzinę
hour_flags AS (
  SELECT DISTINCT ON (j.ts)
    j.*,
    ps.pair_n,
    ps.price_low_pln_mwh, ps.price_high_pln_mwh, ps.delta_pln_mwh,
    ps.t_pair_base_pln_mwh,
    ps.qualifies_base, ps.forced_fill, ps.selected_any,
    (j.ts = ps.low_ts)  AS is_pair_low_hour,
    (j.ts = ps.high_ts) AS is_pair_high_hour
  FROM j
  LEFT JOIN pairs_selected ps
    ON ps.day_key = j.day_key
   AND (j.ts = ps.low_ts OR j.ts = ps.high_ts)
  ORDER BY j.ts, ps.selected_any DESC, ps.delta_pln_mwh DESC, ps.pair_n
),

-- 8) REKURENCJA SOC + realizacje
ordered AS (
  SELECT ROW_NUMBER() OVER (ORDER BY ts) AS rn, h.* FROM hour_flags h ORDER BY ts
),
rec AS (
  -- seed
  SELECT
    o.*,
    0.0::numeric AS soc_prev_mwh,
    0.0::numeric AS t_eff_low_pln_mwh,
    0.0::numeric AS t_eff_high_pln_mwh,
    -- wszystkie źródła ładowania dostępne w kroku:
    COALESCE(o.e_broker_ch_net_mwh,0) + COALESCE(o.spill_oze_to_arbi_net_mwh,0) + COALESCE(o.e_grid_ch_cap_net_mwh,0) AS src_total_ch_net_mwh,
    false AS sig_charge, false AS sig_discharge,
    0.0::numeric AS e_arbi_ch_in_mwh,
    0.0::numeric AS e_arbi_ch_from_grid_used_net_mwh,  -- NOWE: faktycznie użyte z sieci (NET)
    0.0::numeric AS e_arbi_dis_out_extra_mwh,
    COALESCE(o.e_broker_dis_net_mwh,0) AS e_arbi_dis_out_mwh,
    0.0::numeric AS headroom_mwh,
    0.0::numeric AS available_dis_mwh,
    0.0::numeric AS soc_arbi_mwh,
    0.0::numeric AS soc_raw_mwh,
    0.0::numeric AS clip_to_emax_mwh,
    0.0::numeric AS clip_to_zero_mwh,
    0.0::numeric AS e_arbi_loss_idle_mwh
  FROM ordered o
  WHERE o.rn = 1

  UNION ALL

  -- krok rekurencji
  SELECT
    o.*,
    r.soc_arbi_mwh AS soc_prev_mwh,

    -- progi efektywne
    CASE WHEN o.is_pair_low_hour  AND o.selected_any
         THEN GREATEST(0, o.t_pair_base_pln_mwh
                        + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) <= o.soc_low_threshold
                               THEN o.bonus_low_soc_ch ELSE 0 END)
         ELSE 0 END AS t_eff_low_pln_mwh,
    CASE WHEN o.is_pair_high_hour AND o.selected_any
         THEN GREATEST(0, o.t_pair_base_pln_mwh
                        + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) >= o.soc_high_threshold
                               THEN o.bonus_high_soc_dis ELSE 0 END)
         ELSE 0 END AS t_eff_high_pln_mwh,

    -- dostępne źródła ładowania w kroku
    COALESCE(o.e_broker_ch_net_mwh,0) + COALESCE(o.spill_oze_to_arbi_net_mwh,0) + COALESCE(o.e_grid_ch_cap_net_mwh,0) AS src_total_ch_net_mwh,

    -- sygnały
    (o.is_pair_low_hour  AND o.selected_any AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
       + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) <= o.soc_low_threshold THEN o.bonus_low_soc_ch ELSE 0 END)))  AS sig_charge,
    (o.is_pair_high_hour AND o.selected_any AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
       + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) >= o.soc_high_threshold THEN o.bonus_high_soc_dis ELSE 0 END))) AS sig_discharge,

    -- CHARGE: żądanie + split na (broker+spill) i (grid_used)
    CASE
      WHEN (o.is_pair_low_hour AND o.selected_any
            AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) <= o.soc_low_threshold THEN o.bonus_low_soc_ch ELSE 0 END)))
      THEN
        LEAST(
          o.e_ch_cap_net_mwh,
          GREATEST(0, o.emax_arbi_mwh - r.soc_arbi_mwh),                 -- headroom
          COALESCE(o.e_broker_ch_net_mwh,0) + COALESCE(o.spill_oze_to_arbi_net_mwh,0) + COALESCE(o.e_grid_ch_cap_net_mwh,0)
        )
      ELSE 0
    END AS e_arbi_ch_in_mwh,

    -- ile z powyższego pochodzi z sieci (NET)
    CASE
      WHEN (o.is_pair_low_hour AND o.selected_any
            AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) <= o.soc_low_threshold THEN o.bonus_low_soc_ch ELSE 0 END)))
      THEN
        LEAST(
          COALESCE(o.e_grid_ch_cap_net_mwh,0),
          GREATEST(
            0,
            LEAST(
              o.e_ch_cap_net_mwh,
              GREATEST(0, o.emax_arbi_mwh - r.soc_arbi_mwh),
              COALESCE(o.e_broker_ch_net_mwh,0) + COALESCE(o.spill_oze_to_arbi_net_mwh,0) + COALESCE(o.e_grid_ch_cap_net_mwh,0)
            )
            - (COALESCE(o.e_broker_ch_net_mwh,0) + COALESCE(o.spill_oze_to_arbi_net_mwh,0))
          )
        )
      ELSE 0
    END AS e_arbi_ch_from_grid_used_net_mwh,

    -- DISCHARGE EXTRA (NET) — z capem PCC, gdy kierunek to grid
    CASE
      WHEN (o.is_pair_high_hour AND o.selected_any
            AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) >= o.soc_high_threshold THEN o.bonus_high_soc_dis ELSE 0 END)))
      THEN
        LEAST(
          GREATEST(0, o.e_dis_cap_net_mwh - COALESCE(o.e_broker_dis_net_mwh,0)),
          GREATEST(0, r.soc_arbi_mwh - COALESCE(o.e_broker_dis_net_mwh,0)),
          CASE
            WHEN o.arbi_dis_to_load THEN 1e9::numeric
            ELSE COALESCE(o.cap_net_by_pcc_mwh, 0)
          END
        )
      ELSE 0
    END AS e_arbi_dis_out_extra_mwh,

    -- DISCHARGE total (NET)
    COALESCE(o.e_broker_dis_net_mwh,0)
      + CASE
          WHEN (o.is_pair_high_hour AND o.selected_any
                AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                    + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) >= o.soc_high_threshold THEN o.bonus_high_soc_dis ELSE 0 END)))
          THEN
            LEAST(
              GREATEST(0, o.e_dis_cap_net_mwh - COALESCE(o.e_broker_dis_net_mwh,0)),
              GREATEST(0, r.soc_arbi_mwh - COALESCE(o.e_broker_dis_net_mwh,0)),
              CASE
                WHEN o.arbi_dis_to_load THEN 1e9::numeric
                ELSE COALESCE(o.cap_net_by_pcc_mwh,0)
              END
            )
          ELSE 0
        END
    AS e_arbi_dis_out_mwh,

    -- diagnostyka SOC
    GREATEST(0, o.emax_arbi_mwh - r.soc_arbi_mwh) AS headroom_mwh,
    GREATEST(0, r.soc_arbi_mwh)                   AS available_dis_mwh,

    -- SOC (clamped)
    LEAST(o.emax_arbi_mwh,
          GREATEST(
            0,
            r.soc_arbi_mwh * o.r_step
            + (CASE
                 WHEN (o.is_pair_low_hour AND o.selected_any
                       AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                           + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) <= o.soc_low_threshold
                                  THEN o.bonus_low_soc_ch ELSE 0 END)))
                 THEN
                   LEAST(
                     o.e_ch_cap_net_mwh,
                     GREATEST(0, o.emax_arbi_mwh - r.soc_arbi_mwh),
                     COALESCE(o.e_broker_ch_net_mwh,0)
                       + COALESCE(o.spill_oze_to_arbi_net_mwh,0)
                       + COALESCE(o.e_grid_ch_cap_net_mwh,0)
                   )
                 ELSE 0
               END)
            - ( COALESCE(o.e_broker_dis_net_mwh,0)
                + CASE
                    WHEN (o.is_pair_high_hour AND o.selected_any
                          AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                              + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) >= o.soc_high_threshold
                                     THEN o.bonus_high_soc_dis ELSE 0 END)))
                    THEN
                      LEAST(
                        GREATEST(0, o.e_dis_cap_net_mwh - COALESCE(o.e_broker_dis_net_mwh,0)),
                        GREATEST(0, r.soc_arbi_mwh - COALESCE(o.e_broker_dis_net_mwh,0)),
                        CASE
                          WHEN o.arbi_dis_to_load THEN 1e9::numeric
                          ELSE COALESCE(o.cap_net_by_pcc_mwh,0)
                        END
                      )
                    ELSE 0
                  END )
          )
    ) AS soc_arbi_mwh,


    -- SOC (raw, przed clampem – pomocniczo)
    ( r.soc_arbi_mwh * o.r_step
      + (CASE
           WHEN (o.is_pair_low_hour AND o.selected_any
                 AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                     + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) <= o.soc_low_threshold
                            THEN o.bonus_low_soc_ch ELSE 0 END)))
           THEN
             LEAST(
               o.e_ch_cap_net_mwh,
               GREATEST(0, o.emax_arbi_mwh - r.soc_arbi_mwh),
               COALESCE(o.e_broker_ch_net_mwh,0)
                 + COALESCE(o.spill_oze_to_arbi_net_mwh,0)
                 + COALESCE(o.e_grid_ch_cap_net_mwh,0)
             )
           ELSE 0
         END)
      - ( COALESCE(o.e_broker_dis_net_mwh,0)
          + CASE
              WHEN (o.is_pair_high_hour AND o.selected_any
                    AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                        + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) >= o.soc_high_threshold
                               THEN o.bonus_high_soc_dis ELSE 0 END)))
              THEN
                LEAST(
                  GREATEST(0, o.e_dis_cap_net_mwh - COALESCE(o.e_broker_dis_net_mwh,0)),
                  GREATEST(0, r.soc_arbi_mwh - COALESCE(o.e_broker_dis_net_mwh,0)),
                  CASE
                    WHEN o.arbi_dis_to_load THEN 1e9::numeric
                    ELSE COALESCE(o.cap_net_by_pcc_mwh,0)
                  END
                )
              ELSE 0
            END )
    ) AS soc_raw_mwh,

    -- clip diagnostyczny
    clip.clip_to_emax_mwh AS clip_to_emax_mwh,
    clip.clip_to_zero_mwh AS clip_to_zero_mwh,

    -- idle-loss (na podstawie r_step)
    (r.soc_arbi_mwh * (1 - o.r_step)) AS e_arbi_loss_idle_mwh

  FROM ordered o
  JOIN rec r ON o.rn = r.rn + 1
  CROSS JOIN LATERAL (
    SELECT
      GREATEST(0, raw - clamped) AS clip_to_emax_mwh,
      GREATEST(0, clamped - raw) AS clip_to_zero_mwh
    FROM (
      SELECT
        raw,
        LEAST(o.emax_arbi_mwh, GREATEST(0, raw)) AS clamped
      FROM (
        SELECT
          r.soc_arbi_mwh * o.r_step
          + CASE
              WHEN (o.is_pair_low_hour AND o.selected_any
                    AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                        + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) <= o.soc_low_threshold THEN o.bonus_low_soc_ch ELSE 0 END)))
              THEN
                LEAST(
                  o.e_ch_cap_net_mwh,
                  GREATEST(0, o.emax_arbi_mwh - r.soc_arbi_mwh),
                  COALESCE(o.e_broker_ch_net_mwh,0) + COALESCE(o.spill_oze_to_arbi_net_mwh,0) + COALESCE(o.e_grid_ch_cap_net_mwh,0)
                )
              ELSE 0
            END
          - ( COALESCE(o.e_broker_dis_net_mwh,0)
              + CASE
                  WHEN (o.is_pair_high_hour AND o.selected_any
                        AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                            + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) >= o.soc_high_threshold THEN o.bonus_high_soc_dis ELSE 0 END)))
                  THEN
                    LEAST(
                      GREATEST(0, o.e_dis_cap_net_mwh - COALESCE(o.e_broker_dis_net_mwh,0)),
                      GREATEST(0, r.soc_arbi_mwh - COALESCE(o.e_broker_dis_net_mwh,0)),
                      CASE
                        WHEN o.arbi_dis_to_load THEN 1e9::numeric
                        ELSE COALESCE(o.cap_net_by_pcc_mwh,0)
                      END
                    )
                  ELSE 0
                END
            ) AS raw
      ) x
    ) y
  ) clip
),

-- 9) Wyjście
final AS (
  SELECT
    r.*,
    CASE
      WHEN r.e_arbi_ch_in_mwh > 0 OR r.e_arbi_dis_out_mwh > 0 THEN NULL
      ELSE CONCAT_WS('; ',
        CASE WHEN NOT r.selected_any THEN 'para_niewybrana' END,
        CASE WHEN r.is_pair_low_hour  AND r.delta_pln_mwh < r.t_eff_low_pln_mwh  THEN 'delta_ponizej_progu_low'  END,
        CASE WHEN r.is_pair_high_hour AND r.delta_pln_mwh < r.t_eff_high_pln_mwh THEN 'delta_ponizej_progu_high' END,
        CASE WHEN r.is_pair_low_hour  AND r.emax_arbi_mwh - r.soc_prev_mwh <= 0 THEN 'brak_headroom' END,
        CASE WHEN r.is_pair_high_hour AND r.soc_prev_mwh <= 0               THEN 'brak_energii'   END,
        CASE WHEN r.is_pair_low_hour  AND r.e_ch_cap_net_mwh  <= 0          THEN 'cap_ch_zero'    END,
        CASE WHEN r.is_pair_high_hour AND r.e_dis_cap_net_mwh <= 0          THEN 'cap_dis_zero'   END
      )
    END AS reason_no_move
  FROM rec r
)

SELECT
  ts, day_key, hour_local, pair_n,
  price_low_pln_mwh,
  price_high_pln_mwh,
  delta_pln_mwh,
  base_min_profit_pln_mwh,
  t_pair_base_pln_mwh,
  t_eff_low_pln_mwh,
  t_eff_high_pln_mwh,
  qualifies_base, selected_any, forced_fill,
  sig_charge, sig_discharge,

  src_total_ch_net_mwh,
  e_ch_cap_net_mwh,
  e_dis_cap_net_mwh,
  emax_arbi_mwh,
  r_step AS retention_per_step,
  soc_low_threshold, soc_high_threshold,
  bonus_low_soc_ch, bonus_high_soc_dis,
  arbi_dis_to_load,

  soc_prev_mwh,
  headroom_mwh,
  available_dis_mwh,

  e_arbi_ch_in_mwh,
  e_arbi_ch_from_grid_used_net_mwh,

  e_broker_dis_net_mwh     AS e_arbi_dis_out_broker_mwh,
  e_arbi_dis_out_extra_mwh AS e_arbi_dis_out_extra_mwh,
  e_arbi_dis_out_mwh       AS e_arbi_dis_out_mwh,

  soc_arbi_mwh,
  soc_raw_mwh,
  clip_to_emax_mwh,
  clip_to_zero_mwh,
  e_arbi_loss_idle_mwh,

  reason_no_move,

  -- =========================
  -- NOWE: procenty zapełnienia
  -- =========================

  -- % zapełnienia ARBI na swojej części magazynu
  CASE
    WHEN emax_arbi_mwh > 0
      THEN ((100 * soc_arbi_mwh::numeric) / emax_arbi_mwh)::numeric(6,2)
    ELSE 0
  END AS arbi_soc_pct,

  -- % zapełnienia ARBI względem całego magazynu (globalny emax z params.form_zmienne)
  CASE
    WHEN (SELECT COALESCE(z.emax, 0)::numeric
          FROM params.form_zmienne z
          ORDER BY z.updated_at DESC NULLS LAST
          LIMIT 1) > 0
      THEN (
        (100 * soc_arbi_mwh::numeric)
        / NULLIF(
            (SELECT COALESCE(z.emax, 0)::numeric
             FROM params.form_zmienne z
             ORDER BY z.updated_at DESC NULLS LAST
             LIMIT 1),
            0
          )
      )::numeric(6,2)
    ELSE 0
  END AS arbi_soc_pct_total

FROM final



