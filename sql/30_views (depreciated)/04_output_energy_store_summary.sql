-- 04_output_energy_store_summary.sql — PODSUMOWANIE MAGAZYNU: przepływy, SOC, straty
-- Metodyka:
--  • Ładowania NET do magazynu: OZE→OZE (z 01), OZE→ARBI (spill użyty w 03), „surplus”→ARBI (część 01),
--    przeliczone też na GROSS (po stronie AC) przez eta_ch.
--  • Rozładowania NET z magazynu: OZE/ARBI (NET z SOC) oraz DELIVER: do odbiorcy / do sieci.
--    ARBI: część „brokerowa” (01) do odbiorcy, część „extra” (03) wg parametru: do sieci (arbi_dis_to_load=false) lub do odbiorcy.
--  • Straty:
--     - efektywności ładowania/rozładowania z 01 (OZE + ARBI broker),
--     - uzupełnienie strat ładowania dla części spill użytej w 03,
--     - strata rozładowania dla ARBI „extra” (03),
--     - „idle” z 02 (OZE) i 03 (ARBI).
--  • SOC: MWh oraz % względem koszyka i całości.
--  • Grid import/export: z 01 (po magazynie) + GROSS ładowania z sieci dla ARBI.
--
CREATE SCHEMA IF NOT EXISTS output;

CREATE OR REPLACE VIEW output.energy_store_summary AS
WITH
-- 00: baza czasu
b AS (
  SELECT
    eb.ts_utc AS ts,
    COALESCE(eb.dt_h, 1.0)::numeric              AS dt_h,
    COALESCE(eb.e_delta_mwh, 0)::numeric         AS e_delta_mwh,
    COALESCE(eb.e_surplus_mwh, 0)::numeric       AS e_surplus_mwh,
    COALESCE(eb.e_deficit_mwh, 0)::numeric       AS e_deficit_mwh
  FROM output.energy_base eb
),

-- 01: broker – flows, sprawności, grid, straty efektywności
br AS (
  SELECT
    e.ts,
    COALESCE(e.eta_ch,1)::numeric                AS eta_ch,
    COALESCE(e.eta_dis,1)::numeric               AS eta_dis,

    -- OZE: ładowanie/rozładowanie NET/DELIVER i straty eff
    COALESCE(e.e_oze_ch_in_mwh,0)::numeric       AS oze_ch_net_mwh,          -- NET → SOC OZE
    COALESCE(e.e_oze_dis_out_mwh,0)::numeric     AS oze_dis_net_mwh,         -- NET z SOC OZE
    COALESCE(e.e_oze_dis_to_load_mwh,0)::numeric AS oze_dis_deliver_mwh,     -- DELIVER do odbiorcy
    COALESCE(e.e_oze_loss_eff_ch_mwh,0)::numeric AS oze_loss_eff_ch_mwh,
    COALESCE(e.e_oze_loss_eff_dis_mwh,0)::numeric AS oze_loss_eff_dis_mwh,

    -- ARBI (część brokerowa): charge/discharge NET i DELIVER + straty eff (dla części brokerowej)
    COALESCE(e.e_arbi_ch_in_mwh,0)::numeric      AS arbi_ch_net_from01_mwh,  -- NET → SOC ARBI (z 01)
    COALESCE(e.e_arbi_dis_out_mwh,0)::numeric    AS arbi_dis_net_from01_mwh, -- NET z SOC ARBI (część brokerowa)
    COALESCE(e.e_arbi_dis_to_load_mwh,0)::numeric AS arbi_dis_deliver_from01_mwh, -- DELIVER do odbiorcy (broker)
    COALESCE(e.e_arbi_loss_eff_ch_mwh,0)::numeric AS arbi_loss_eff_ch_from01_mwh,
    COALESCE(e.e_arbi_loss_eff_dis_mwh,0)::numeric AS arbi_loss_eff_dis_from01_mwh,

    -- sieć po magazynie (bez ładowania ARBI z sieci — to dodamy niżej)
    COALESCE(e.grid_import_mwh,0)::numeric       AS grid_import_after_bess_mwh,
    COALESCE(e.grid_export_mwh,0)::numeric       AS grid_export_after_bess_mwh,

    -- diagnostyka systemowa
    COALESCE(e.curtailed_surplus_mwh,0)::numeric AS curtailed_surplus_mwh,
    COALESCE(e.unserved_load_mwh,0)::numeric     AS unserved_load_mwh,

    -- pojemności (opcjonalnie z 01)
    COALESCE(e.emax_oze_mwh,0)::numeric          AS emax_oze_from01_mwh,
    COALESCE(e.emax_arbi_mwh,0)::numeric         AS emax_arbi_from01_mwh,
    COALESCE(e.emax_total_mwh,0)::numeric        AS emax_total_from01_mwh
  FROM output.energy_broker_detail e
),

-- 02: SOC OZE + spill i idle
oz AS (
  SELECT
    z.ts,
    COALESCE(z.soc_oze_mwh,0)::numeric                 AS soc_oze_mwh,
    COALESCE(z.spill_oze_to_arbi_net_mwh,0)::numeric   AS spill_oze_to_arbi_net_mwh,
    COALESCE(z.retention_per_step,1)::numeric          AS r_step_oze,
    COALESCE(z.e_oze_loss_idle_mwh,0)::numeric         AS oze_loss_idle_mwh
  FROM output.energy_soc_oze z
),

-- 03: SOC ARBI + ładowanie (01+spill+grid) i extra rozładowanie + idle
ar AS (
  SELECT
    y.ts,
    COALESCE(y.soc_arbi_mwh,0)::numeric                AS soc_arbi_mwh,
    COALESCE(y.e_arbi_ch_in_mwh,0)::numeric            AS arbi_ch_net_total_mwh,           -- NET z 01+spill+grid
    COALESCE(y.e_arbi_ch_from_grid_used_net_mwh,0)::numeric AS arbi_ch_net_from_grid_mwh,  -- NET z sieci (03)
    COALESCE(y.e_arbi_dis_out_mwh,0)::numeric          AS arbi_dis_net_total_mwh,          -- NET z SOC (broker + extra)
    COALESCE(y.e_arbi_dis_out_extra_mwh,0)::numeric    AS arbi_dis_net_extra_mwh,          -- NET extra (03)
    COALESCE(y.e_arbi_loss_idle_mwh,0)::numeric        AS arbi_loss_idle_mwh,
    COALESCE(y.retention_per_step,1)::numeric          AS r_step_arbi
  FROM output.energy_soc_arbi y
),

-- Parametry pojemności (z params) i flaga kierunku extra-rozład. ARBI
caps AS (
  SELECT
    z.emax::numeric              AS emax_total_mwh,
    z.procent_arbitrazu::numeric AS procent_arbi
  FROM params.form_zmienne z
  ORDER BY z.updated_at DESC NULLS LAST
  LIMIT 1
),
emax AS (
  SELECT
    c.emax_total_mwh,
    ROUND(c.emax_total_mwh * (c.procent_arbi/100.0),6)                    AS emax_arbi_mwh,
    ROUND(c.emax_total_mwh * (1.0 - c.procent_arbi/100.0),6)              AS emax_oze_mwh
  FROM caps c
),
par AS (
  SELECT
    COALESCE(NULLIF(payload->>'arbi_dis_to_load','')::boolean, false) AS arbi_dis_to_load
  FROM params.form_par_arbitrazu
  ORDER BY updated_at DESC NULLS LAST
  LIMIT 1
),

-- Złączenie
j AS (
  SELECT
    b.ts, b.dt_h, b.e_delta_mwh, b.e_surplus_mwh, b.e_deficit_mwh,

    -- z br (bez ts)
    br.eta_ch, br.eta_dis,
    br.oze_ch_net_mwh, br.oze_dis_net_mwh, br.oze_dis_deliver_mwh,
    br.oze_loss_eff_ch_mwh, br.oze_loss_eff_dis_mwh,
    br.arbi_ch_net_from01_mwh, br.arbi_dis_net_from01_mwh, br.arbi_dis_deliver_from01_mwh,
    br.arbi_loss_eff_ch_from01_mwh, br.arbi_loss_eff_dis_from01_mwh,
    br.grid_import_after_bess_mwh, br.grid_export_after_bess_mwh,
    br.curtailed_surplus_mwh, br.unserved_load_mwh,
    br.emax_oze_from01_mwh, br.emax_arbi_from01_mwh, br.emax_total_from01_mwh,

    -- z oz (bez ts)
    oz.soc_oze_mwh, oz.spill_oze_to_arbi_net_mwh, oz.r_step_oze, oz.oze_loss_idle_mwh,

    -- z ar (bez ts)
    ar.soc_arbi_mwh, ar.arbi_ch_net_total_mwh, ar.arbi_dis_net_total_mwh,
    ar.arbi_dis_net_extra_mwh, ar.arbi_loss_idle_mwh, ar.r_step_arbi,
    ar.arbi_ch_net_from_grid_mwh,  -- NOWE: NET ładowania ARBI z sieci (03)

    emax.emax_total_mwh, emax.emax_oze_mwh, emax.emax_arbi_mwh,
    (SELECT arbi_dis_to_load FROM par) AS arbi_dis_to_load
  FROM b
  LEFT JOIN br  ON br.ts  = b.ts
  LEFT JOIN oz  ON oz.ts  = b.ts
  LEFT JOIN ar  ON ar.ts  = b.ts
  CROSS JOIN emax
),

-- Rozbicie źródeł ładowania ARBI: 01 vs spill (02) (grid dodamy osobno)
split_arbi_ch AS (
  SELECT
    j.*,
    LEAST(j.arbi_ch_net_total_mwh, j.arbi_ch_net_from01_mwh) AS arbi_ch_net_used_from01_mwh,
    GREATEST(
      0,
      LEAST( GREATEST(0, j.arbi_ch_net_total_mwh - LEAST(j.arbi_ch_net_total_mwh, j.arbi_ch_net_from01_mwh)),
             j.spill_oze_to_arbi_net_mwh )
    ) AS arbi_ch_net_used_from_spill_mwh,

    -- niewykorzystane źródła (diagnostyka)
    GREATEST(0, j.arbi_ch_net_from01_mwh - LEAST(j.arbi_ch_net_total_mwh, j.arbi_ch_net_from01_mwh)) AS arbi_ch_net_unused_from01_mwh,
    GREATEST(0, j.spill_oze_to_arbi_net_mwh
                - GREATEST(0, LEAST( GREATEST(0, j.arbi_ch_net_total_mwh - LEAST(j.arbi_ch_net_total_mwh, j.arbi_ch_net_from01_mwh)),
                                     j.spill_oze_to_arbi_net_mwh ))) AS spill_oze_to_arbi_net_unused_mwh
  FROM j
),

-- Przepływy DELIVER z ARBI extra: do sieci lub do odbiorcy wg parametru
deliver_arbi AS (
  SELECT
    s.*,
    -- DELIVER wynikający z extra rozład. (03), rozdział wg arbi_dis_to_load
    CASE WHEN s.arbi_dis_to_load
         THEN (s.arbi_dis_net_extra_mwh * s.eta_dis) ELSE 0 END AS arbi_dis_extra_deliver_to_load_mwh,
    CASE WHEN s.arbi_dis_to_load
         THEN 0 ELSE (s.arbi_dis_net_extra_mwh * s.eta_dis) END AS arbi_dis_extra_deliver_to_grid_mwh
  FROM split_arbi_ch s
),

-- Straty efektywności: częściowo z 01, częściowo dopliczane dla spill i extra
losses AS (
  SELECT
    d.*,

    -- 1) Ładowanie ARBI: część z 01 (przeskalowana do faktycznie użytej) + część ze spill (obliczona)
    CASE
      WHEN d.arbi_ch_net_from01_mwh > 0
      THEN (d.arbi_ch_net_used_from01_mwh / d.arbi_ch_net_from01_mwh) * d.arbi_loss_eff_ch_from01_mwh
      ELSE 0 END AS arbi_loss_eff_ch_used_from01_mwh,

    CASE
      WHEN d.eta_ch > 0
      THEN d.arbi_ch_net_used_from_spill_mwh * (1.0 / d.eta_ch - 1.0)
      ELSE 0 END AS arbi_loss_eff_ch_from_spill_mwh,

    -- 2) Rozładowanie ARBI extra (03): strata = NET - DELIVER = NET*(1-eta_dis)
    (d.arbi_dis_net_extra_mwh * (1 - d.eta_dis)) AS arbi_loss_eff_dis_extra_mwh

  FROM deliver_arbi d
),

-- Zbiorcze straty
loss_sum AS (
  SELECT
    l.*,
    -- straty OZE z 01 + idle z 02
    (l.oze_loss_eff_ch_mwh + l.oze_loss_eff_dis_mwh) AS oze_loss_eff_total_mwh,
    -- straty ARBI: z 01 (broker) + z 03 (spill charge + extra discharge) + idle z 03
    (l.arbi_loss_eff_ch_used_from01_mwh
       + l.arbi_loss_eff_ch_from_spill_mwh
       + l.arbi_loss_eff_dis_from01_mwh
       + l.arbi_loss_eff_dis_extra_mwh) AS arbi_loss_eff_total_mwh,

    -- suma „idle”
    (l.oze_loss_idle_mwh + l.arbi_loss_idle_mwh) AS idle_loss_total_mwh,

    -- suma wszystkich strat
    (l.oze_loss_eff_ch_mwh + l.oze_loss_eff_dis_mwh
       + l.arbi_loss_eff_ch_used_from01_mwh
       + l.arbi_loss_eff_ch_from_spill_mwh
       + l.arbi_loss_eff_dis_from01_mwh
       + l.arbi_loss_eff_dis_extra_mwh
       + l.oze_loss_idle_mwh + l.arbi_loss_idle_mwh) AS loss_total_mwh
  FROM losses l
),

-- Przepływy do/z magazynu (NET) oraz DELIVER (do odbiorcy / do sieci)
flows AS (
  SELECT
    x.*,

    -- Wejścia NET do magazynu (po stronie SOC)
    x.oze_ch_net_mwh                                              AS in_oze_to_oze_net_mwh,
    x.arbi_ch_net_used_from01_mwh                                 AS in_surplus_to_arbi_net_mwh, -- „z 01”
    x.arbi_ch_net_used_from_spill_mwh                             AS in_oze_to_arbi_net_mwh,     -- spill użyty w 03
    (x.oze_ch_net_mwh + x.arbi_ch_net_total_mwh)                  AS in_total_store_net_mwh,

    -- NOWE: Wejście NET z sieci do ARBI (z 03)
    x.arbi_ch_net_from_grid_mwh                                   AS in_grid_to_arbi_net_mwh,

    -- Wejścia GROSS po stronie AC
    CASE WHEN x.eta_ch > 0 THEN x.oze_ch_net_mwh / x.eta_ch ELSE 0 END                               AS in_oze_to_oze_gross_mwh,
    CASE WHEN x.eta_ch > 0 THEN x.arbi_ch_net_used_from01_mwh / x.eta_ch ELSE 0 END                  AS in_surplus_to_arbi_gross_mwh,
    CASE WHEN x.eta_ch > 0 THEN x.arbi_ch_net_used_from_spill_mwh / x.eta_ch ELSE 0 END              AS in_oze_to_arbi_gross_mwh,
    CASE WHEN x.eta_ch > 0 THEN (x.oze_ch_net_mwh + x.arbi_ch_net_total_mwh) / x.eta_ch ELSE 0 END   AS in_total_store_gross_mwh,

    -- NOWE: GROSS poboru z sieci na ładowanie ARBI
    CASE WHEN x.eta_ch > 0 THEN x.arbi_ch_net_from_grid_mwh / x.eta_ch ELSE 0 END                    AS in_grid_to_arbi_gross_mwh,

    -- Wyjścia NET z magazynu (z SOC)
    x.oze_dis_net_mwh                                              AS out_oze_net_mwh,
    x.arbi_dis_net_total_mwh                                       AS out_arbi_net_mwh,
    (x.oze_dis_net_mwh + x.arbi_dis_net_total_mwh)                 AS out_total_store_net_mwh,

    -- Wyjścia DELIVER (po stronie odbiorcy / sieci)
    x.oze_dis_deliver_mwh                                          AS to_load_from_oze_mwh,
    (x.arbi_dis_deliver_from01_mwh + x.arbi_dis_extra_deliver_to_load_mwh) AS to_load_from_arbi_mwh,
    x.arbi_dis_extra_deliver_to_grid_mwh                           AS to_grid_from_arbi_mwh,

    -- Suma DELIVER
    (x.oze_dis_deliver_mwh
       + x.arbi_dis_deliver_from01_mwh
       + x.arbi_dis_extra_deliver_to_load_mwh)                     AS deliver_total_to_load_mwh,
    x.arbi_dis_extra_deliver_to_grid_mwh                           AS deliver_total_to_grid_mwh,

    -- NOWE: Import z sieci po magazynie + ładowanie ARBI z sieci (GROSS)
    (x.grid_import_after_bess_mwh + CASE WHEN x.eta_ch > 0 THEN x.arbi_ch_net_from_grid_mwh / x.eta_ch ELSE 0 END)
      AS grid_import_incl_grid_charge_mwh

  FROM loss_sum x
),

-- Wymiary czasu
dd AS (
  SELECT *
  FROM input.date_dim
)

SELECT
  -- wymiary czasu
  dd.ts_utc, dd.ts_local, dd.year, dd.month, dd.day, dd.dow, dd.hour, dd.is_workday,

  -- oś bazowa
  f.ts, f.dt_h,
  f.e_delta_mwh, f.e_surplus_mwh, f.e_deficit_mwh,

  -- SOC (MWh)
  ROUND(f.soc_oze_mwh, 6)  AS soc_oze_mwh,
  ROUND(f.soc_arbi_mwh, 6) AS soc_arbi_mwh,
  ROUND(f.soc_oze_mwh + f.soc_arbi_mwh, 6) AS soc_total_mwh,

  -- SOC (%) względem koszyka
  CASE WHEN f.emax_oze_mwh  > 0 THEN ROUND(100.0 * f.soc_oze_mwh  / f.emax_oze_mwh, 2)  END AS soc_oze_pct,
  CASE WHEN f.emax_arbi_mwh > 0 THEN ROUND(100.0 * f.soc_arbi_mwh / f.emax_arbi_mwh, 2) END AS soc_arbi_pct,

  -- SOC (%) względem CAŁOŚCI magazynu
  CASE WHEN f.emax_total_mwh> 0 THEN ROUND(100.0 * (f.soc_oze_mwh + f.soc_arbi_mwh) / f.emax_total_mwh, 2) END AS soc_total_pct,
  CASE WHEN f.emax_total_mwh> 0 THEN ROUND(100.0 * f.soc_oze_mwh  / f.emax_total_mwh, 2) END AS soc_oze_pct_of_total,
  CASE WHEN f.emax_total_mwh> 0 THEN ROUND(100.0 * f.soc_arbi_mwh / f.emax_total_mwh, 2) END AS soc_arbi_pct_of_total,

  -- Emax (dla referencji)
  ROUND(f.emax_oze_mwh, 6)  AS emax_oze_mwh,
  ROUND(f.emax_arbi_mwh, 6) AS emax_arbi_mwh,
  ROUND(f.emax_total_mwh,6) AS emax_total_mwh,

  -- WEJŚCIA do magazynu (NET i GROSS)
  ROUND(f.in_oze_to_oze_net_mwh, 6)        AS in_oze_to_oze_net_mwh,
  ROUND(f.in_oze_to_arbi_net_mwh, 6)       AS in_oze_to_arbi_net_mwh,
  ROUND(f.in_surplus_to_arbi_net_mwh, 6)   AS in_surplus_to_arbi_net_mwh,
  ROUND(f.in_total_store_net_mwh, 6)       AS in_total_store_net_mwh,

  ROUND(f.in_oze_to_oze_gross_mwh, 6)      AS in_oze_to_oze_gross_mwh,
  ROUND(f.in_oze_to_arbi_gross_mwh, 6)     AS in_oze_to_arbi_gross_mwh,
  ROUND(f.in_surplus_to_arbi_gross_mwh, 6) AS in_surplus_to_arbi_gross_mwh,
  ROUND(f.in_total_store_gross_mwh, 6)     AS in_total_store_gross_mwh,

  -- NOWE: ładowanie ARBI z sieci
  ROUND(f.in_grid_to_arbi_net_mwh, 6)      AS in_grid_to_arbi_net_mwh,
  ROUND(f.in_grid_to_arbi_gross_mwh, 6)    AS in_grid_to_arbi_gross_mwh,

  -- WYJŚCIA z magazynu (NET i DELIVER)
  ROUND(f.out_oze_net_mwh, 6)              AS out_oze_net_mwh,
  ROUND(f.out_arbi_net_mwh, 6)             AS out_arbi_net_mwh,
  ROUND(f.out_total_store_net_mwh, 6)      AS out_total_store_net_mwh,

  ROUND(f.to_load_from_oze_mwh, 6)         AS to_load_from_oze_mwh,
  ROUND(f.to_load_from_arbi_mwh, 6)        AS to_load_from_arbi_mwh,
  ROUND(f.deliver_total_to_load_mwh, 6)    AS to_load_total_mwh,

  ROUND(f.to_grid_from_arbi_mwh, 6)        AS to_grid_from_arbi_mwh,
  ROUND(f.deliver_total_to_grid_mwh, 6)    AS to_grid_total_mwh,

  -- GRID
  ROUND(f.grid_import_after_bess_mwh, 6)           AS grid_import_mwh,                -- z 01 (tylko load-deficyt)
  ROUND(f.grid_export_after_bess_mwh, 6)           AS grid_export_mwh,
  ROUND(f.grid_import_incl_grid_charge_mwh, 6)     AS grid_import_incl_grid_charge_mwh, -- NOWE: + GROSS ładowania z sieci
  ROUND(f.curtailed_surplus_mwh, 6)                AS curtailed_surplus_mwh,
  ROUND(f.unserved_load_mwh, 6)                    AS unserved_load_mwh,

  -- STRATY
  ROUND(f.oze_loss_eff_ch_mwh, 6)          AS oze_loss_eff_ch_mwh,
  ROUND(f.oze_loss_eff_dis_mwh, 6)         AS oze_loss_eff_dis_mwh,
  ROUND(f.oze_loss_idle_mwh, 6)            AS oze_loss_idle_mwh,
  ROUND(f.oze_loss_eff_total_mwh, 6)       AS oze_loss_eff_total_mwh,

  ROUND(f.arbi_loss_eff_ch_used_from01_mwh, 6)  AS arbi_loss_eff_ch_used_from01_mwh,
  ROUND(f.arbi_loss_eff_ch_from_spill_mwh, 6)   AS arbi_loss_eff_ch_from_spill_mwh,
  ROUND(f.arbi_loss_eff_dis_from01_mwh, 6)      AS arbi_loss_eff_dis_from01_mwh,
  ROUND(f.arbi_loss_eff_dis_extra_mwh, 6)       AS arbi_loss_eff_dis_extra_mwh,
  ROUND(f.arbi_loss_idle_mwh, 6)                AS arbi_loss_idle_mwh,
  ROUND(f.arbi_loss_eff_total_mwh, 6)           AS arbi_loss_eff_total_mwh,

  ROUND(f.idle_loss_total_mwh, 6)          AS idle_loss_total_mwh,
  ROUND(f.loss_total_mwh, 6)               AS loss_total_mwh,

  -- DIAGNOSTYKA (co nie weszło i dlaczego)
  ROUND(f.arbi_ch_net_unused_from01_mwh, 6)    AS arbi_ch_net_unused_from01_mwh,
  ROUND(f.spill_oze_to_arbi_net_unused_mwh, 6) AS spill_oze_to_arbi_net_unused_mwh,
  f.arbi_dis_to_load                            AS arbi_dis_to_load_flag
FROM flows f
JOIN dd ON dd.ts_utc = f.ts

