-- 03_output_energy_soc_arbi.sql — ARBITRAŻ wg parowania ekstremów (z diagnostyką)
-- Metodyka:
--   • Dla każdej doby: sortujemy ceny malejąco (High) i rosnąco (Low) → pary (Low[i], High[i]).
--   • Dla pary liczymy Δ = price(High) - price(Low).
--   • Próg T_pair = base_min_profit + bonus_ch_window(jeśli Low.hour ∈ bonus_hrs_ch)
--                    + bonus_dis_window(jeśli High.hour ∈ bonus_hrs_dis).
--     Dodatkowo „free hours”: jeśli Low ∈ bonus_hrs_ch_free lub High ∈ bonus_hrs_dis_free → T_pair := 0.
--     SOC-korekty są przykładane „w momencie” realizacji (w rekurencji): gdy
--        SOC_low% ≤ soc_low_threshold → T_eff_low  = T_pair + bonus_low_soc_ch
--        SOC_high% ≥ soc_high_threshold → T_eff_high = T_pair + bonus_high_soc_dis
--   • Wybieramy co najmniej cycles_per_day par: najpierw te z Δ ≥ T_pair; jeśli mniej → dobieramy top Δ (forced_fill=true).
--   • 03 zużywa wyłącznie energię, która przyjdzie jako źródło ładowania:
--        src_total = (e_arbi_ch_in_mwh z 01) + (spill_oze_to_arbi_net_mwh z 02),
--     a rozładowanie „extra” nigdy nie narusza rozładowania wymuszonego przez 01.

CREATE SCHEMA IF NOT EXISTS output;

CREATE OR REPLACE VIEW output.energy_soc_arbi AS
WITH RECURSIVE
-- ─────────────────────
-- 0) OŚ CZASU + CENY (00)
-- ─────────────────────
base AS (
  SELECT
    b.ts_utc AS ts,
    COALESCE(b.dt_h, 1.0)::numeric            AS dt_h,
    COALESCE(b.price_tge_pln, 0)::numeric     AS price_pln_mwh   -- ⬅⬅ poprawka: używamy price_tge_pln
  FROM output.energy_base b
),
with_day AS (
  SELECT
    ts,
    dt_h,
    price_pln_mwh,
    (ts AT TIME ZONE 'Europe/Warsaw')                         AS ts_local,
    (ts AT TIME ZONE 'Europe/Warsaw')::date                   AS day_key,
    EXTRACT(HOUR FROM (ts AT TIME ZONE 'Europe/Warsaw'))::int AS hour_local
  FROM base
),

-- ─────────────────────
-- 1) LIMITY I DECYZJE (01)
-- ─────────────────────
lim01 AS (
  SELECT
    e.ts,
    COALESCE(e.e_ch_cap_net_mwh, 0)::numeric   AS e_ch_cap_net_mwh,   -- cap ładowania (NET do SOC)
    COALESCE(e.e_dis_cap_net_mwh, 0)::numeric  AS e_dis_cap_net_mwh,  -- cap rozładowania (NET z SOC)
    COALESCE(e.eta_ch, 1)::numeric             AS eta_ch,
    COALESCE(e.eta_dis,1)::numeric             AS eta_dis,
    COALESCE(e.e_arbi_ch_in_mwh, 0)::numeric   AS e_broker_ch_net_mwh,    -- źródło 1: decyzja 01
    COALESCE(e.e_arbi_dis_out_mwh, 0)::numeric AS e_broker_dis_net_mwh,   -- rozładowanie wymuszone NET
    COALESCE(e.e_arbi_dis_to_load_mwh, 0)::numeric AS e_broker_dis_deliver_mwh
  FROM output.energy_broker_detail e
),

-- ─────────────────────
-- 2) SPILL + RETENCJA (02)
-- ─────────────────────
oze02 AS (
  SELECT
    z.ts,
    COALESCE(z.spill_oze_to_arbi_net_mwh, 0)::numeric AS spill_oze_to_arbi_net_mwh, -- źródło 2
    COALESCE(z.retention_per_step, 1)::numeric        AS r_step                      -- retencja kroku (λ miesięczna → h)
  FROM output.energy_soc_oze z
),

-- ─────────────────────
-- 3) CAPY POJEMNOŚCI (params.form_zmienne)
-- ─────────────────────
caps AS (
  SELECT
    ROUND(COALESCE(z.emax,0)::numeric * COALESCE(z.procent_arbitrazu,0)::numeric / 100.0, 6) AS emax_arbi_mwh
  FROM params.form_zmienne z
  ORDER BY z.updated_at DESC NULLS LAST
  LIMIT 1
),

-- ─────────────────────
-- 4) PARAMETRY ARBITRAŻU (params.form_par_arbitrazu)
-- ─────────────────────
par AS (
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
    bonus_hrs_ch::jsonb              AS bonus_hrs_ch,
    bonus_hrs_dis::jsonb             AS bonus_hrs_dis,
    bonus_hrs_ch_free::jsonb         AS bonus_hrs_ch_free,
    bonus_hrs_dis_free::jsonb        AS bonus_hrs_dis_free
  FROM params.form_par_arbitrazu
  ORDER BY updated_at DESC NULLS LAST
  LIMIT 1
),

-- ─────────────────────
-- 5) PARY EKSTREMÓW W DOBIE (Low↔High, bez SOC-korekt na tym etapie)
-- ─────────────────────
ranked AS (
  SELECT
    wd.*,
    ROW_NUMBER() OVER (PARTITION BY wd.day_key ORDER BY wd.price_pln_mwh ASC,  wd.ts ASC) AS rn_low,
    ROW_NUMBER() OVER (PARTITION BY wd.day_key ORDER BY wd.price_pln_mwh DESC, wd.ts ASC) AS rn_high
  FROM with_day wd
),
pairs AS (
  SELECT
    l.day_key, l.rn_low AS pair_n,
    l.ts AS low_ts,  l.hour_local AS low_hour,  l.price_pln_mwh AS price_low_pln_mwh,
    h.ts AS high_ts, h.hour_local AS high_hour, h.price_pln_mwh AS price_high_pln_mwh,
    (h.price_pln_mwh - l.price_pln_mwh) AS delta_pln_mwh,

    -- FREE/bonus membership (jsonb arrays of ints)
    EXISTS (SELECT 1 FROM jsonb_array_elements((SELECT bonus_hrs_ch_free  FROM par)) v(x)
            WHERE (v.x)::text::int = l.hour_local) AS is_ch_free,
    EXISTS (SELECT 1 FROM jsonb_array_elements((SELECT bonus_hrs_dis_free FROM par)) v(x)
            WHERE (v.x)::text::int = h.hour_local) AS is_dis_free,
    EXISTS (SELECT 1 FROM jsonb_array_elements((SELECT bonus_hrs_ch       FROM par)) v(x)
            WHERE (v.x)::text::int = l.hour_local) AS in_bonus_ch,
    EXISTS (SELECT 1 FROM jsonb_array_elements((SELECT bonus_hrs_dis      FROM par)) v(x)
            WHERE (v.x)::text::int = h.hour_local) AS in_bonus_dis
  FROM ranked l
  JOIN ranked h
    ON h.day_key = l.day_key
   AND h.rn_high = l.rn_low
),
pairs_threshold AS (
  -- próg bazowy + okna; SOC-korekt nie przykładamy jeszcze (w rekurencji, „w godzinie”)
  SELECT
    p.*,
    (SELECT base_min_profit_pln_mwh FROM par) AS base_min_profit_pln_mwh,
    (SELECT bonus_ch_window FROM par)         AS bonus_ch_window,
    (SELECT bonus_dis_window FROM par)        AS bonus_dis_window,
    (SELECT cycles_per_day FROM par)          AS cycles_per_day,
    (SELECT allow_carry_over FROM par)        AS allow_carry_over,
    (SELECT force_order FROM par)             AS force_order,

    CASE WHEN p.is_ch_free OR p.is_dis_free
         THEN 0::numeric
         ELSE GREATEST(
                0,
                (SELECT base_min_profit_pln_mwh FROM par)
                + CASE WHEN p.in_bonus_ch  THEN (SELECT bonus_ch_window FROM par)  ELSE 0 END
                + CASE WHEN p.in_bonus_dis THEN (SELECT bonus_dis_window FROM par) ELSE 0 END
              )
    END AS t_pair_base_pln_mwh
  FROM pairs p
),
pairs_selected AS (
  -- wybór co najmniej cycles_per_day par na dobę:
  -- 1) kandydaci z Δ ≥ T_base
  -- 2) jeśli < cycles_per_day → dobieramy top Δ (forced_fill)
  SELECT
    pt.*,
    (pt.delta_pln_mwh >= pt.t_pair_base_pln_mwh) AS qualifies_base,
    ROW_NUMBER() OVER (PARTITION BY pt.day_key ORDER BY pt.delta_pln_mwh DESC, pt.pair_n) AS delta_rank,
    COUNT(*) FILTER (WHERE pt.delta_pln_mwh >= pt.t_pair_base_pln_mwh)
      OVER (PARTITION BY pt.day_key) AS n_qualifying,
    (pt.delta_pln_mwh >= pt.t_pair_base_pln_mwh)
      OR (ROW_NUMBER() OVER (PARTITION BY pt.day_key ORDER BY pt.delta_pln_mwh DESC, pt.pair_n)
          <= (SELECT cycles_per_day FROM par)) AS selected_any,
    (CASE
       WHEN (pt.delta_pln_mwh >= pt.t_pair_base_pln_mwh) THEN false
       ELSE (ROW_NUMBER() OVER (PARTITION BY pt.day_key ORDER BY pt.delta_pln_mwh DESC, pt.pair_n)
             <= (SELECT cycles_per_day FROM par))
     END) AS forced_fill
  FROM pairs_threshold pt
),

-- ─────────────────────
-- 6) ZŁĄCZENIE WSZYSTKIEGO W OŚ GODZINOWĄ
-- ─────────────────────
j AS (
  SELECT
    b.ts, b.dt_h, b.price_pln_mwh,
    wd.day_key, wd.hour_local,

    l.e_ch_cap_net_mwh, l.e_dis_cap_net_mwh, l.eta_ch, l.eta_dis,
    l.e_broker_ch_net_mwh, l.e_broker_dis_net_mwh, l.e_broker_dis_deliver_mwh,

    o.spill_oze_to_arbi_net_mwh, o.r_step,
    (SELECT emax_arbi_mwh FROM caps) AS emax_arbi_mwh,

    -- parametry używane w rekurencji
    (SELECT base_min_profit_pln_mwh FROM par) AS base_min_profit_pln_mwh,
    (SELECT bonus_low_soc_ch FROM par)        AS bonus_low_soc_ch,
    (SELECT bonus_high_soc_dis FROM par)      AS bonus_high_soc_dis,
    (SELECT soc_low_threshold FROM par)       AS soc_low_threshold,
    (SELECT soc_high_threshold FROM par)      AS soc_high_threshold,
    (SELECT arbi_dis_to_load FROM par)        AS arbi_dis_to_load
  FROM base b
  JOIN with_day wd ON wd.ts = b.ts
  LEFT JOIN lim01 l ON l.ts = b.ts
  LEFT JOIN oze02 o ON o.ts = b.ts
),

-- ─────────────────────
-- 7) FLAGI „CZY TA GODZINA JEST LOW/HIGH WYBRANEJ PARY”
-- ─────────────────────
hour_flags AS (
  SELECT
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
),

-- ─────────────────────
-- 8) REKURENCJA SOC + REALIZACJA SYGNAŁÓW (SOC-korekty progów „w chwili” wykonania)
-- ─────────────────────
ordered AS (
  SELECT ROW_NUMBER() OVER (ORDER BY ts) AS rn, h.* FROM hour_flags h ORDER BY ts
),
rec AS (
  -- seed
  SELECT
    o.*,
    0.0::numeric AS soc_prev_mwh,

    -- efektywne progi w tej godzinie (na starcie nie liczą, ale raportujemy 0)
    0.0::numeric AS t_eff_low_pln_mwh,
    0.0::numeric AS t_eff_high_pln_mwh,

    -- źródła ładowania
    COALESCE(o.e_broker_ch_net_mwh,0) + COALESCE(o.spill_oze_to_arbi_net_mwh,0) AS src_total_ch_net_mwh,

    -- decyzje i przepływy
    false AS sig_charge, false AS sig_discharge,
    0.0::numeric AS e_arbi_ch_in_mwh,
    0.0::numeric AS e_arbi_dis_out_extra_mwh,
    COALESCE(o.e_broker_dis_net_mwh,0) AS e_arbi_dis_out_mwh, -- na starcie tylko „broker”

    -- diagnostyka limitów
    0.0::numeric AS headroom_mwh,
    0.0::numeric AS available_dis_mwh,

    -- SOC po kroku
    0.0::numeric AS soc_arbi_mwh,

    -- klamry i straty idle
    0.0::numeric AS soc_raw_mwh,
    0.0::numeric AS clip_to_emax_mwh,
    0.0::numeric AS clip_to_zero_mwh,
    0.0::numeric AS e_arbi_loss_idle_mwh

  FROM ordered o
  WHERE o.rn = 1

  UNION ALL

  -- kolejne godziny
  SELECT
    o.*,
    r.soc_arbi_mwh AS soc_prev_mwh,

    -- progi efektywne (SOC-aware) dla bieżącej godziny
    CASE
      WHEN o.is_pair_low_hour  AND o.selected_any
      THEN GREATEST(0, o.t_pair_base_pln_mwh
                       + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) <= o.soc_low_threshold
                              THEN o.bonus_low_soc_ch ELSE 0 END)
      ELSE 0
    END AS t_eff_low_pln_mwh,

    CASE
      WHEN o.is_pair_high_hour AND o.selected_any
      THEN GREATEST(0, o.t_pair_base_pln_mwh
                       + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) >= o.soc_high_threshold
                              THEN o.bonus_high_soc_dis ELSE 0 END)
      ELSE 0
    END AS t_eff_high_pln_mwh,

    -- źródła ładowania (const na godzinę)
    COALESCE(o.e_broker_ch_net_mwh,0) + COALESCE(o.spill_oze_to_arbi_net_mwh,0) AS src_total_ch_net_mwh,

    -- sygnały godzinowe (wymagają Δ oraz progu efektywnego i wybrania pary)
    (o.is_pair_low_hour  AND o.selected_any AND o.delta_pln_mwh >=
        (GREATEST(0, o.t_pair_base_pln_mwh
                     + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) <= o.soc_low_threshold
                            THEN o.bonus_low_soc_ch ELSE 0 END)))  AS sig_charge,

    (o.is_pair_high_hour AND o.selected_any AND o.delta_pln_mwh >=
        (GREATEST(0, o.t_pair_base_pln_mwh
                     + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) >= o.soc_high_threshold
                            THEN o.bonus_high_soc_dis ELSE 0 END))) AS sig_discharge,

    -- ładowanie
    CASE WHEN (o.is_pair_low_hour AND o.selected_any
               AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                       + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) <= o.soc_low_threshold
                              THEN o.bonus_low_soc_ch ELSE 0 END)))
         THEN LEAST(o.e_ch_cap_net_mwh,
                    GREATEST(0, o.emax_arbi_mwh - r.soc_arbi_mwh),
                    COALESCE(o.e_broker_ch_net_mwh,0) + COALESCE(o.spill_oze_to_arbi_net_mwh,0))
         ELSE 0 END AS e_arbi_ch_in_mwh,

    -- extra rozładowanie
    CASE WHEN (o.is_pair_high_hour AND o.selected_any
               AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                       + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) >= o.soc_high_threshold
                              THEN o.bonus_high_soc_dis ELSE 0 END)))
         THEN LEAST( GREATEST(0, o.e_dis_cap_net_mwh - COALESCE(o.e_broker_dis_net_mwh,0)),
                     GREATEST(0, r.soc_arbi_mwh - COALESCE(o.e_broker_dis_net_mwh,0)) )
         ELSE 0 END AS e_arbi_dis_out_extra_mwh,

    -- suma rozładowania NET
    COALESCE(o.e_broker_dis_net_mwh,0)
      + CASE WHEN (o.is_pair_high_hour AND o.selected_any
                   AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                           + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) >= o.soc_high_threshold
                                  THEN o.bonus_high_soc_dis ELSE 0 END)))
             THEN LEAST( GREATEST(0, o.e_dis_cap_net_mwh - COALESCE(o.e_broker_dis_net_mwh,0)),
                         GREATEST(0, r.soc_arbi_mwh - COALESCE(o.e_broker_dis_net_mwh,0)) )
             ELSE 0 END
    AS e_arbi_dis_out_mwh,

    -- diagnostyka limitów
    GREATEST(0, o.emax_arbi_mwh - r.soc_arbi_mwh) AS headroom_mwh,
    GREATEST(0, r.soc_arbi_mwh)                   AS available_dis_mwh,

    -- SOC po kroku (z retencją z 02)
    LEAST(o.emax_arbi_mwh,
          GREATEST(0,
                   r.soc_arbi_mwh * o.r_step
                   + CASE WHEN (o.is_pair_low_hour AND o.selected_any
                                AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                                        + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) <= o.soc_low_threshold
                                               THEN o.bonus_low_soc_ch ELSE 0 END)))
                          THEN LEAST(o.e_ch_cap_net_mwh,
                                     GREATEST(0, o.emax_arbi_mwh - r.soc_arbi_mwh),
                                     COALESCE(o.e_broker_ch_net_mwh,0) + COALESCE(o.spill_oze_to_arbi_net_mwh,0))
                          ELSE 0 END
                   - ( COALESCE(o.e_broker_dis_net_mwh,0)
                       + CASE WHEN (o.is_pair_high_hour AND o.selected_any
                                    AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                                            + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) >= o.soc_high_threshold
                                                   THEN o.bonus_high_soc_dis ELSE 0 END)))
                              THEN LEAST( GREATEST(0, o.e_dis_cap_net_mwh - COALESCE(o.e_broker_dis_net_mwh,0)),
                                          GREATEST(0, r.soc_arbi_mwh - COALESCE(o.e_broker_dis_net_mwh,0)) )
                              ELSE 0 END )
          )
    ) AS soc_arbi_mwh,

    -- klamry i idle loss (raport)
    (r.soc_arbi_mwh * o.r_step
       + CASE WHEN (o.is_pair_low_hour AND o.selected_any
                    AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                            + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) <= o.soc_low_threshold
                                   THEN o.bonus_low_soc_ch ELSE 0 END)))
              THEN LEAST(o.e_ch_cap_net_mwh,
                         GREATEST(0, o.emax_arbi_mwh - r.soc_arbi_mwh),
                         COALESCE(o.e_broker_ch_net_mwh,0) + COALESCE(o.spill_oze_to_arbi_net_mwh,0))
              ELSE 0 END
       - ( COALESCE(o.e_broker_dis_net_mwh,0)
           + CASE WHEN (o.is_pair_high_hour AND o.selected_any
                        AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                                + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) >= o.soc_high_threshold
                                       THEN o.bonus_high_soc_dis ELSE 0 END)))
                  THEN LEAST( GREATEST(0, o.e_dis_cap_net_mwh - COALESCE(o.e_broker_dis_net_mwh,0)),
                              GREATEST(0, r.soc_arbi_mwh - COALESCE(o.e_broker_dis_net_mwh,0)) )
                  ELSE 0 END )
    ) AS soc_raw_mwh,

    -- clipy liczone w LATERAL
    clip.clip_to_emax_mwh AS clip_to_emax_mwh,
    clip.clip_to_zero_mwh AS clip_to_zero_mwh,

    (r.soc_arbi_mwh * (1 - o.r_step)) AS e_arbi_loss_idle_mwh

  FROM ordered o
  JOIN rec r ON o.rn = r.rn + 1

  -- LATERAL: liczenie clipów (soc_raw → clamp[0, emax])
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
          + CASE WHEN (o.is_pair_low_hour AND o.selected_any
                       AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                               + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) <= o.soc_low_threshold
                                      THEN o.bonus_low_soc_ch ELSE 0 END)))
                 THEN LEAST(o.e_ch_cap_net_mwh,
                            GREATEST(0, o.emax_arbi_mwh - r.soc_arbi_mwh),
                            COALESCE(o.e_broker_ch_net_mwh,0) + COALESCE(o.spill_oze_to_arbi_net_mwh,0))
                 ELSE 0 END
          - ( COALESCE(o.e_broker_dis_net_mwh,0)
              + CASE WHEN (o.is_pair_high_hour AND o.selected_any
                           AND o.delta_pln_mwh >= (GREATEST(0, o.t_pair_base_pln_mwh
                                   + CASE WHEN (100.0 * r.soc_arbi_mwh / NULLIF(o.emax_arbi_mwh,0)) >= o.soc_high_threshold
                                          THEN o.bonus_high_soc_dis ELSE 0 END)))
                     THEN LEAST( GREATEST(0, o.e_dis_cap_net_mwh - COALESCE(o.e_broker_dis_net_mwh,0)),
                                 GREATEST(0, r.soc_arbi_mwh - COALESCE(o.e_broker_dis_net_mwh,0)) )
                     ELSE 0 END )
          AS raw
      ) x
    ) y
  ) clip
),

-- ─────────────────────
-- 9) WYJŚCIE + ANALITYKA
-- ─────────────────────
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
  -- identyfikacja
  ts, day_key, hour_local, pair_n,

  -- ceny i Δ
  ROUND(price_low_pln_mwh, 2)  AS price_low_pln_mwh,
  ROUND(price_high_pln_mwh, 2) AS price_high_pln_mwh,
  ROUND(delta_pln_mwh, 2)      AS delta_pln_mwh,

  -- progi
  ROUND(base_min_profit_pln_mwh, 2) AS base_min_profit_pln_mwh,
  ROUND(t_pair_base_pln_mwh, 2)     AS t_pair_base_pln_mwh,
  ROUND(t_eff_low_pln_mwh, 2)       AS t_eff_low_pln_mwh,
  ROUND(t_eff_high_pln_mwh, 2)      AS t_eff_high_pln_mwh,
  qualifies_base, selected_any, forced_fill,

  -- sygnały i źródła
  sig_charge, sig_discharge,
  ROUND(src_total_ch_net_mwh, 6) AS src_total_ch_net_mwh,

  -- capy/parametry
  ROUND(e_ch_cap_net_mwh, 6)  AS e_ch_cap_net_mwh,
  ROUND(e_dis_cap_net_mwh, 6) AS e_dis_cap_net_mwh,
  ROUND(emax_arbi_mwh, 6)     AS emax_arbi_mwh,
  ROUND(r_step, 10)           AS retention_per_step,
  soc_low_threshold, soc_high_threshold,
  bonus_low_soc_ch, bonus_high_soc_dis,
  arbi_dis_to_load,

  -- SOC i przepływy
  ROUND(soc_prev_mwh, 6)                AS soc_prev_mwh,
  ROUND(headroom_mwh, 6)                AS headroom_mwh,
  ROUND(available_dis_mwh, 6)           AS available_dis_mwh,
  ROUND(e_arbi_ch_in_mwh, 6)            AS e_arbi_ch_in_mwh,
  ROUND(e_broker_dis_net_mwh, 6)        AS e_arbi_dis_out_broker_mwh,
  ROUND(e_arbi_dis_out_extra_mwh, 6)    AS e_arbi_dis_out_extra_mwh,
  ROUND(e_arbi_dis_out_mwh, 6)          AS e_arbi_dis_out_mwh,
  ROUND(soc_arbi_mwh, 6)                AS soc_arbi_mwh,

  -- klamry i idle loss
  ROUND(soc_raw_mwh, 6)         AS soc_raw_mwh,
  ROUND(clip_to_emax_mwh, 6)    AS clip_to_emax_mwh,
  ROUND(clip_to_zero_mwh, 6)    AS clip_to_zero_mwh,
  ROUND(e_arbi_loss_idle_mwh,6) AS e_arbi_loss_idle_mwh,

  -- opis
  reason_no_move

FROM final
ORDER BY ts;
