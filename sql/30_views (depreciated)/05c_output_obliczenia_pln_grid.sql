-- =====================================================================================================
--  Scenariusz C: GRID (konsumpcja)
--  IDENTYCZNY schemat jak 05a/05b. JEDYNA różnica: źródło poboru (CTE base).
--  Poniżej domyślnie używam zużycia: p_load_mw * dt_h z output.energy_base, ale:
--  >>> TU PODMIEŃ ŹRÓDŁO, jeśli wolisz inne (np. import bez magazynu) <<<
-- =====================================================================================================

CREATE SCHEMA IF NOT EXISTS output;
DROP VIEW IF EXISTS output.obliczenia_pln_grid;

CREATE VIEW output.obliczenia_pln_grid AS
WITH
dd AS (
  SELECT
    eb.ts_utc,
    eb.ts_local,
    eb.year, eb.month, eb.day, eb.dow, eb.hour,
    eb.is_workday::boolean AS is_workday
  FROM output.energy_base eb
),
p_kontrakt_raw AS (SELECT payload FROM params.form_par_kontraktu ORDER BY updated_at DESC NULLS LAST LIMIT 1),
p_kontrakt AS (
  SELECT
    UPPER(regexp_replace(replace(COALESCE(payload->>'wybrana_taryfa','wybrana_b23'),'"',''), '^wybrana_', ''))::text AS taryfa_wybrana,
    LOWER(replace(COALESCE(payload->>'typ_kontraktu','stala'),'"',''))::text                                   AS model_ceny_wybrany,
    (COALESCE(payload->>'proc_zmiana_ceny','0'))::numeric / 100.0                                                AS zmiana_ceny_udzial,
    (COALESCE(payload->>'cena_energii_stała','0'))::numeric                                                      AS cena_stala_pln_mwh,
    (COALESCE(payload->>'opłata_handlowa_marza_stala','0'))::numeric                                             AS marza_stala_pln_mwh,
    (COALESCE(payload->>'oplata_handlowa_marza_zmienna','0'))::numeric / 100.0                                   AS marza_zmienna_udzial,
    (COALESCE(payload->>'opłata_handlowa_stala','0'))::numeric                                                   AS oplata_handlowa_mies_pln
  FROM p_kontrakt_raw
),
p_dystr AS (
  SELECT
    (payload->>'dyst_abon_b21')::numeric AS dyst_abon_b21,   (payload->>'dyst_fixed_b21')::numeric AS dyst_fixed_b21,
    (payload->>'dyst_trans_b21')::numeric AS dyst_trans_b21, (payload->>'dyst_qual_b21')::numeric  AS dyst_qual_b21,
    (payload->>'dyst_var_b21_morn')::numeric AS var_b21_morn,(payload->>'dyst_var_b21_aft')::numeric AS var_b21_aft,
    (payload->>'dyst_var_b21_other')::numeric AS var_b21_other,

    (payload->>'dyst_abon_b22')::numeric AS dyst_abon_b22,   (payload->>'dyst_fixed_b22')::numeric AS dyst_fixed_b22,
    (payload->>'dyst_trans_b22')::numeric AS dyst_trans_b22, (payload->>'dyst_qual_b22')::numeric  AS dyst_qual_b22,
    (payload->>'dyst_var_b22_morn')::numeric AS var_b22_morn,(payload->>'dyst_var_b22_aft')::numeric AS var_b22_aft,
    (payload->>'dyst_var_b22_other')::numeric AS var_b22_other,

    (payload->>'dyst_abon_b23')::numeric AS dyst_abon_b23,   (payload->>'dyst_fixed_b23')::numeric AS dyst_fixed_b23,
    (payload->>'dyst_trans_b23')::numeric AS dyst_trans_b23, (payload->>'dyst_qual_b23')::numeric  AS dyst_qual_b23,
    (payload->>'dyst_var_b23_morn')::numeric AS var_b23_morn,(payload->>'dyst_var_b23_aft')::numeric AS var_b23_aft,
    (payload->>'dyst_var_b23_other')::numeric AS var_b23_other
  FROM params.form_oplaty_dystrybucyjne
  ORDER BY updated_at DESC NULLS LAST
  LIMIT 1
),
p_sys AS (
  SELECT
    (payload->>'stawka_oze')::numeric           AS stawka_oze_pln_mwh,
    (payload->>'stawka_kogeneracyjna')::numeric AS stawka_kog_pln_mwh,
    (payload->>'stawka_mocowa')::numeric        AS stawka_mocowa_pln_mwh
  FROM params.form_oplaty_systemowe
  ORDER BY updated_at DESC NULLS LAST
  LIMIT 1
),
p_fisk AS (
  SELECT (payload->>'akcyza')::numeric AS akcyza_pln_mwh,
         (payload->>'vat')::numeric / 100.0 AS vat_udzial
  FROM params.form_oplaty_fiskalne
  ORDER BY updated_at DESC NULLS LAST
  LIMIT 1
),
p_klient AS (
  SELECT (payload->>'klient_moc_umowna')::numeric AS moc_umowna_mw
  FROM params.form_parametry_klienta
  ORDER BY updated_at DESC NULLS LAST
  LIMIT 1
),
p_sched_dyst AS (SELECT payload FROM params.form_oplaty_dyst_sched ORDER BY updated_at DESC NULLS LAST LIMIT 1),
p_sched_moc  AS (SELECT payload FROM params.form_oplaty_sys_sched  ORDER BY updated_at DESC NULLS LAST LIMIT 1),

-- ▼▼▼ JEDYNY PUNKT RÓŻNICY: ŹRÓDŁO POBORU ▼▼▼
base AS (
  SELECT
    d.*,
    COALESCE(eb.p_load_mw, 0) * COALESCE(eb.dt_h, 1.0)::numeric AS pobor_mwh
  FROM dd d
  LEFT JOIN output.energy_base eb ON eb.ts_utc = d.ts_utc
),
-- ▲▲▲ KONIEC: ŹRÓDŁO POBORU ▲▲▲

-- LCOE OZE (identycznie jak 05a/05b)
base_oze AS (
  SELECT
    b.ts_utc,
    ROUND(COALESCE(eb.p_pv_pp_mw,0) * COALESCE(eb.dt_h,1), 6)::numeric AS oze_gen_pv_pp_mwh,
    ROUND(COALESCE(eb.p_pv_wz_mw,0) * COALESCE(eb.dt_h,1), 6)::numeric AS oze_gen_pv_wz_mwh,
    ROUND(COALESCE(eb.p_wiatr_mw,0) * COALESCE(eb.dt_h,1), 6)::numeric AS oze_gen_wiatr_mwh
  FROM base b
  LEFT JOIN output.energy_base eb ON eb.ts_utc = b.ts_utc
),
-- ZAMIANA: p_lcoe_raw zostaje jak było
p_lcoe_raw AS (
  SELECT *
  FROM params.form_lcoe
  ORDER BY updated_at DESC NULLS LAST
  LIMIT 1
),

-- NOWA WERSJA: tylko z payloadu, stabilne nazwy kluczy
p_lcoe AS (
  SELECT
    COALESCE(NULLIF(l.payload->>'lcoe_pv_pp',   '')::numeric, 0.0) AS lcoe_pv_pp_pln_mwh,
    COALESCE(NULLIF(l.payload->>'lcoe_pv_wz',   '')::numeric, 0.0) AS lcoe_pv_wz_pln_mwh,
    COALESCE(NULLIF(l.payload->>'lcoe_wiatr',   '')::numeric, 0.0) AS lcoe_wiatr_pln_mwh
  FROM p_lcoe_raw l
),
lcoe AS (
  SELECT
    o.ts_utc,
    o.oze_gen_pv_pp_mwh,
    o.oze_gen_pv_wz_mwh,
    o.oze_gen_wiatr_mwh,
    (o.oze_gen_pv_pp_mwh + o.oze_gen_pv_wz_mwh + o.oze_gen_wiatr_mwh) AS oze_gen_total_mwh,
    (o.oze_gen_pv_pp_mwh * pl.lcoe_pv_pp_pln_mwh
   + o.oze_gen_pv_wz_mwh * pl.lcoe_pv_wz_pln_mwh
   + o.oze_gen_wiatr_mwh * pl.lcoe_wiatr_pln_mwh)                       AS lcoe_cost_h_pln,
    CASE WHEN (o.oze_gen_pv_pp_mwh + o.oze_gen_pv_wz_mwh + o.oze_gen_wiatr_mwh) > 0
         THEN (
           (o.oze_gen_pv_pp_mwh * pl.lcoe_pv_pp_pln_mwh
          + o.oze_gen_pv_wz_mwh * pl.lcoe_pv_wz_pln_mwh
          + o.oze_gen_wiatr_mwh * pl.lcoe_wiatr_pln_mwh)
           / NULLIF((o.oze_gen_pv_pp_mwh + o.oze_gen_pv_wz_mwh + o.oze_gen_wiatr_mwh),0)
         )
    END AS lcoe_blended_pln_mwh
  FROM base_oze o
  CROSS JOIN p_lcoe pl
),

-- Dalej identycznie jak 05a/05b
ceny_dyn AS (
  SELECT d.ts_utc, ROUND(c.fixing_i_price::numeric, 6) AS cena_dyn_pln_mwh
  FROM dd d
  LEFT JOIN input.ceny_godzinowe c ON c.ts_utc = d.ts_utc
),
ceny AS (
  SELECT
    b.ts_utc, b.ts_local,
    ROUND(k.cena_stala_pln_mwh * (1 + k.marza_zmienna_udzial) * (1 + k.zmiana_ceny_udzial) + k.marza_stala_pln_mwh, 2) AS cena_stala_final_pln_mwh,
    ROUND(cd.cena_dyn_pln_mwh   * (1 + k.marza_zmienna_udzial) * (1 + k.zmiana_ceny_udzial) + k.marza_stala_pln_mwh, 2) AS cena_dyn_final_pln_mwh,
    k.model_ceny_wybrany, k.taryfa_wybrana, k.marza_zmienna_udzial, k.marza_stala_pln_mwh, k.zmiana_ceny_udzial,
    k.oplata_handlowa_mies_pln
  FROM base b
  LEFT JOIN ceny_dyn cd ON cd.ts_utc = b.ts_utc
  CROSS JOIN p_kontrakt k
),
godziny_mies AS (SELECT year, month, COUNT(*)::numeric AS godz_w_mies FROM base GROUP BY year, month),
prad_h AS (
  SELECT
    c.ts_utc, c.ts_local,
    c.cena_stala_final_pln_mwh, c.cena_dyn_final_pln_mwh,
    c.model_ceny_wybrany, c.taryfa_wybrana,
    c.marza_zmienna_udzial, c.marza_stala_pln_mwh, c.zmiana_ceny_udzial,
    (c.oplata_handlowa_mies_pln / NULLIF(gm.godz_w_mies,0)) AS handlowa_h
  FROM ceny c
  JOIN godziny_mies gm
    ON gm.year = EXTRACT(YEAR FROM c.ts_local)::int
   AND gm.month= EXTRACT(MONTH FROM c.ts_local)::int
),
-- strefy / dystrybucja
month_map_dyst AS (
  SELECT 1 AS m,'jan' AS mon UNION ALL SELECT 2,'feb' UNION ALL SELECT 3,'mar' UNION ALL SELECT 4,'apr'
  UNION ALL SELECT 5,'may' UNION ALL SELECT 6,'jun' UNION ALL SELECT 7,'jul' UNION ALL SELECT 8,'aug'
  UNION ALL SELECT 9,'sep' UNION ALL SELECT 10,'oct' UNION ALL SELECT 11,'nov' UNION ALL SELECT 12,'dec'
),
okno_dyst AS (
  SELECT
    b.ts_local, b.month, b.is_workday,
    (p.payload->>(format('dyst_sched_b21_%s_%s_morn_start', mm.mon, CASE WHEN b.is_workday THEN 'work' ELSE 'free' END)))::int AS b21_morn_st,
    (p.payload->>(format('dyst_sched_b21_%s_%s_morn_end',   mm.mon, CASE WHEN b.is_workday THEN 'work' ELSE 'free' END)))::int AS b21_morn_en,
    (p.payload->>(format('dyst_sched_b21_%s_%s_aft_start',  mm.mon, CASE WHEN b.is_workday THEN 'work' ELSE 'free' END)))::int AS b21_aft_st,
    (p.payload->>(format('dyst_sched_b21_%s_%s_aft_end',    mm.mon, CASE WHEN b.is_workday THEN 'work' ELSE 'free' END)))::int AS b21_aft_en,

    (p.payload->>(format('dyst_sched_b22_%s_%s_morn_start', mm.mon, CASE WHEN b.is_workday THEN 'work' ELSE 'free' END)))::int AS b22_morn_st,
    (p.payload->>(format('dyst_sched_b22_%s_%s_morn_end',   mm.mon, CASE WHEN b.is_workday THEN 'work' ELSE 'free' END)))::int AS b22_morn_en,
    (p.payload->>(format('dyst_sched_b22_%s_%s_aft_start',  mm.mon, CASE WHEN b.is_workday THEN 'work' ELSE 'free' END)))::int AS b22_aft_st,
    (p.payload->>(format('dyst_sched_b22_%s_%s_aft_end',    mm.mon, CASE WHEN b.is_workday THEN 'work' ELSE 'free' END)))::int AS b22_aft_en,

    (p.payload->>(format('dyst_sched_b23_%s_%s_morn_start', mm.mon, CASE WHEN b.is_workday THEN 'work' ELSE 'free' END)))::int AS b23_morn_st,
    (p.payload->>(format('dyst_sched_b23_%s_%s_morn_end',   mm.mon, CASE WHEN b.is_workday THEN 'work' ELSE 'free' END)))::int AS b23_morn_en,
    (p.payload->>(format('dyst_sched_b23_%s_%s_aft_start',  mm.mon, CASE WHEN b.is_workday THEN 'work' ELSE 'free' END)))::int AS b23_aft_st,
    (p.payload->>(format('dyst_sched_b23_%s_%s_aft_end',    mm.mon, CASE WHEN b.is_workday THEN 'work' ELSE 'free' END)))::int AS b23_aft_en
  FROM base b
  JOIN month_map_dyst mm ON mm.m = b.month
  CROSS JOIN p_sched_dyst p
),
strefy AS (
  SELECT
    o.ts_local,
    CASE WHEN o.b21_morn_st IS NOT NULL AND EXTRACT(HOUR FROM o.ts_local)::int BETWEEN o.b21_morn_st AND o.b21_morn_en-1 THEN 'morn'
         WHEN o.b21_aft_st  IS NOT NULL AND EXTRACT(HOUR FROM o.ts_local)::int BETWEEN o.b21_aft_st  AND o.b21_aft_en-1  THEN 'aft'
         ELSE 'other'
    END AS b21_strefa,
    CASE WHEN o.b22_morn_st IS NOT NULL AND EXTRACT(HOUR FROM o.ts_local)::int BETWEEN o.b22_morn_st AND o.b22_morn_en-1 THEN 'morn'
         WHEN o.b22_aft_st  IS NOT NULL AND EXTRACT(HOUR FROM o.ts_local)::int BETWEEN o.b22_aft_st  AND o.b22_aft_en-1  THEN 'aft'
         ELSE 'other'
    END AS b22_strefa,
    CASE WHEN o.b23_morn_st IS NOT NULL AND EXTRACT(HOUR FROM o.ts_local)::int BETWEEN o.b23_morn_st AND o.b23_morn_en-1 THEN 'morn'
         WHEN o.b23_aft_st  IS NOT NULL AND EXTRACT(HOUR FROM o.ts_local)::int BETWEEN o.b23_aft_st  AND o.b23_aft_en-1  THEN 'aft'
         ELSE 'other'
    END AS b23_strefa
  FROM okno_dyst o
),
dystr_zm AS (
  SELECT
    s.ts_local,
    (CASE s.b21_strefa WHEN 'morn' THEN p.var_b21_morn WHEN 'aft' THEN p.var_b21_aft ELSE p.var_b21_other END + p.dyst_qual_b21) AS b21_var_pln_mwh,
    (CASE s.b22_strefa WHEN 'morn' THEN p.var_b22_morn WHEN 'aft' THEN p.var_b22_aft ELSE p.var_b22_other END + p.dyst_qual_b22) AS b22_var_pln_mwh,
    (CASE s.b23_strefa WHEN 'morn' THEN p.var_b23_morn WHEN 'aft' THEN p.var_b23_aft ELSE p.var_b23_other END + p.dyst_qual_b23) AS b23_var_pln_mwh
  FROM strefy s
  CROSS JOIN p_dystr p
),
-- dystrybucja stała
godziny_mies2 AS (SELECT year, month, COUNT(*)::numeric AS godz_w_mies FROM base GROUP BY year, month),
dystr_stale AS (
  SELECT
    b.ts_local, gm.year, gm.month,
    ((p.dyst_fixed_b21 + p.dyst_trans_b21) * (COALESCE(pkli.moc_umowna_mw,0) * 1000.0) + p.dyst_abon_b21) / NULLIF(gm.godz_w_mies,0) AS b21_stala_h,
    ((p.dyst_fixed_b22 + p.dyst_trans_b22) * (COALESCE(pkli.moc_umowna_mw,0) * 1000.0) + p.dyst_abon_b22) / NULLIF(gm.godz_w_mies,0) AS b22_stala_h,
    ((p.dyst_fixed_b23 + p.dyst_trans_b23) * (COALESCE(pkli.moc_umowna_mw,0) * 1000.0) + p.dyst_abon_b23) / NULLIF(gm.godz_w_mies,0) AS b23_stala_h
  FROM base b
  JOIN godziny_mies2 gm ON gm.year=b.year AND gm.month=b.month
  CROSS JOIN p_dystr p
  CROSS JOIN p_klient pkli
),
-- okna mocy
month_map_moc AS (
  SELECT 1 AS m,'sty' AS mon UNION ALL SELECT 2,'lut' UNION ALL SELECT 3,'mar' UNION ALL SELECT 4,'kwi'
  UNION ALL SELECT 5,'maj' UNION ALL SELECT 6,'cze' UNION ALL SELECT 7,'lip' UNION ALL SELECT 8,'sie'
  UNION ALL SELECT 9,'wrz' UNION ALL SELECT 10,'paz' UNION ALL SELECT 11,'lis' UNION ALL SELECT 12,'gru'
),
moc_okna AS (
  SELECT
    b.ts_local,
    (ps.payload->>(format('opl_moc_%s_%s_st',  mm.mon, CASE WHEN b.is_workday THEN 'work' ELSE 'free' END)))::int AS st_h,
    (ps.payload->>(format('opl_moc_%s_%s_kon', mm.mon, CASE WHEN b.is_workday THEN 'work' ELSE 'free' END)))::int AS en_h
  FROM base b
  JOIN month_map_moc mm ON mm.m = b.month
  CROSS JOIN p_sched_moc ps
),
moc_kwal AS (
  SELECT
    m.ts_local,
    CASE WHEN m.st_h IS NOT NULL AND EXTRACT(HOUR FROM m.ts_local)::int BETWEEN m.st_h AND m.en_h-1 THEN true ELSE false END AS is_moc_kwal
  FROM moc_okna m
),
-- złożenie + j2 identycznie jak 05a/05b
joined AS (
  SELECT
    b.*,
    ph.cena_stala_final_pln_mwh, ph.cena_dyn_final_pln_mwh, ph.model_ceny_wybrany, ph.taryfa_wybrana,
    ph.marza_zmienna_udzial, ph.marza_stala_pln_mwh, ph.zmiana_ceny_udzial, ph.handlowa_h,
    dz.b21_var_pln_mwh, dz.b22_var_pln_mwh, dz.b23_var_pln_mwh,
    ds.b21_stala_h, ds.b22_stala_h, ds.b23_stala_h,
    ps.stawka_oze_pln_mwh, ps.stawka_kog_pln_mwh, ps.stawka_mocowa_pln_mwh,
    pf.akcyza_pln_mwh, pf.vat_udzial,
    mk.is_moc_kwal
  FROM base b
  LEFT JOIN prad_h      ph ON ph.ts_utc   = b.ts_utc
  LEFT JOIN dystr_zm    dz ON dz.ts_local = b.ts_local
  LEFT JOIN dystr_stale ds ON ds.ts_local = b.ts_local
  CROSS JOIN p_sys ps
  CROSS JOIN p_fisk pf
  LEFT JOIN moc_kwal mk    ON mk.ts_local = b.ts_local
),
j2 AS (
  SELECT
    j.*,
    ROUND(j.b21_stala_h, 2) AS b21_stala_h_r,
    ROUND(j.b22_stala_h, 2) AS b22_stala_h_r,
    ROUND(j.b23_stala_h, 2) AS b23_stala_h_r,
    ROUND(j.handlowa_h,  2) AS handlowa_h_r,
    CASE WHEN j.is_moc_kwal
         THEN ROUND( j.pobor_mwh * COALESCE(j.stawka_mocowa_pln_mwh,0), 2 )
         ELSE 0
    END::numeric AS opl_mocowa_h_r
  FROM joined j
)

SELECT
  j.ts_utc, j.ts_local, j.year, j.month, j.day, j.dow, j.hour, j.is_workday,
  j.pobor_mwh,

  j.marza_zmienna_udzial, j.marza_stala_pln_mwh, j.zmiana_ceny_udzial,
  j.cena_stala_final_pln_mwh, j.cena_dyn_final_pln_mwh, j.model_ceny_wybrany, j.taryfa_wybrana,
  ROUND(j.pobor_mwh * COALESCE(j.cena_stala_final_pln_mwh,0), 2) + j.handlowa_h_r AS opl_energia_stala_h_pln,
  ROUND(j.pobor_mwh * COALESCE(j.cena_dyn_final_pln_mwh,0), 2)   + j.handlowa_h_r AS opl_energia_dyn_h_pln,
  CASE WHEN j.model_ceny_wybrany = 'stala'
       THEN ROUND(j.pobor_mwh * COALESCE(j.cena_stala_final_pln_mwh,0), 2) + j.handlowa_h_r
       ELSE ROUND(j.pobor_mwh * COALESCE(j.cena_dyn_final_pln_mwh,0), 2)   + j.handlowa_h_r
  END AS opl_energia_sel_h_pln,

  ROUND(j.pobor_mwh * COALESCE(j.b21_var_pln_mwh,0), 2) AS opl_dystr_zmienna_b21_h_pln,
  ROUND(j.pobor_mwh * COALESCE(j.b22_var_pln_mwh,0), 2) AS opl_dystr_zmienna_b22_h_pln,
  ROUND(j.pobor_mwh * COALESCE(j.b23_var_pln_mwh,0), 2) AS opl_dystr_zmienna_b23_h_pln,

  j.b21_stala_h_r AS opl_dystr_stala_b21_h_pln,
  j.b22_stala_h_r AS opl_dystr_stala_b22_h_pln,
  j.b23_stala_h_r AS opl_dystr_stala_b23_h_pln,

  CASE j.taryfa_wybrana
    WHEN 'B21' THEN ROUND(j.pobor_mwh * COALESCE(j.b21_var_pln_mwh,0), 2)
    WHEN 'B22' THEN ROUND(j.pobor_mwh * COALESCE(j.b22_var_pln_mwh,0), 2)
    WHEN 'B23' THEN ROUND(j.pobor_mwh * COALESCE(j.b23_var_pln_mwh,0), 2)
    ELSE 0
  END AS opl_dystr_zmienna_sel_h_pln,
  CASE j.taryfa_wybrana
    WHEN 'B21' THEN j.b21_stala_h_r
    WHEN 'B22' THEN j.b22_stala_h_r
    WHEN 'B23' THEN j.b23_stala_h_r
    ELSE 0
  END AS opl_dystr_stala_sel_h_pln,

  ROUND(j.pobor_mwh * COALESCE(j.stawka_oze_pln_mwh,0), 2) AS opl_oze_h_pln,
  ROUND(j.pobor_mwh * COALESCE(j.stawka_kog_pln_mwh,0), 2) AS opl_kog_h_pln,
  CAST(j.opl_mocowa_h_r AS numeric(18,6)) AS opl_mocowa_h_pln,

  ROUND(j.pobor_mwh * COALESCE(j.akcyza_pln_mwh, 0), 2)::numeric(18,6) AS opl_akcyza_h_pln,
  ROUND(
    COALESCE(j.vat_udzial,0) * (
      (
        CASE WHEN j.model_ceny_wybrany = 'stala'
             THEN ROUND(j.pobor_mwh * COALESCE(j.cena_stala_final_pln_mwh,0), 2) + j.handlowa_h_r
             ELSE ROUND(j.pobor_mwh * COALESCE(j.cena_dyn_final_pln_mwh,0), 2)   + j.handlowa_h_r
        END
        + CASE j.taryfa_wybrana
            WHEN 'B21' THEN j.b21_stala_h_r
            WHEN 'B22' THEN j.b22_stala_h_r
            WHEN 'B23' THEN j.b23_stala_h_r
            ELSE 0
          END
        + CASE j.taryfa_wybrana
            WHEN 'B21' THEN j.pobor_mwh * COALESCE(j.b21_var_pln_mwh,0)
            WHEN 'B22' THEN j.pobor_mwh * COALESCE(j.b22_var_pln_mwh,0)
            WHEN 'B23' THEN j.pobor_mwh * COALESCE(j.b23_var_pln_mwh,0)
            ELSE 0
          END
        + j.pobor_mwh * COALESCE(j.stawka_oze_pln_mwh,0)
        + j.pobor_mwh * COALESCE(j.stawka_kog_pln_mwh,0)
        + j.opl_mocowa_h_r
      )
      + (j.pobor_mwh * COALESCE(j.akcyza_pln_mwh, 0))
    )
  , 2)::numeric(18,6) AS opl_vat_h_pln,

  ROUND(
    (
      CASE WHEN j.model_ceny_wybrany = 'stala'
           THEN ROUND(j.pobor_mwh * COALESCE(j.cena_stala_final_pln_mwh,0), 2) + j.handlowa_h_r
           ELSE ROUND(j.pobor_mwh * COALESCE(j.cena_dyn_final_pln_mwh,0), 2)   + j.handlowa_h_r
      END
      + CASE j.taryfa_wybrana
          WHEN 'B21' THEN j.b21_stala_h_r
          WHEN 'B22' THEN j.b22_stala_h_r
          WHEN 'B23' THEN j.b23_stala_h_r
          ELSE 0
        END
      + CASE j.taryfa_wybrana
          WHEN 'B21' THEN j.pobor_mwh * COALESCE(j.b21_var_pln_mwh,0)
          WHEN 'B22' THEN j.pobor_mwh * COALESCE(j.b22_var_pln_mwh,0)
          WHEN 'B23' THEN j.pobor_mwh * COALESCE(j.b23_var_pln_mwh,0)
          ELSE 0
        END
      + j.pobor_mwh * COALESCE(j.stawka_oze_pln_mwh,0)
      + j.pobor_mwh * COALESCE(j.stawka_kog_pln_mwh,0)
      + j.opl_mocowa_h_r
    )
  , 2)::numeric(18,6) AS koszt_h_sum_netto_pln,

  ROUND(
    (
      (
        CASE WHEN j.model_ceny_wybrany = 'stala'
             THEN ROUND(j.pobor_mwh * COALESCE(j.cena_stala_final_pln_mwh,0), 2) + j.handlowa_h_r
             ELSE ROUND(j.pobor_mwh * COALESCE(j.cena_dyn_final_pln_mwh,0), 2)   + j.handlowa_h_r
        END
        + CASE j.taryfa_wybrana
            WHEN 'B21' THEN j.b21_stala_h_r
            WHEN 'B22' THEN j.b22_stala_h_r
            WHEN 'B23' THEN j.b23_stala_h_r
            ELSE 0
          END
        + CASE j.taryfa_wybrana
            WHEN 'B21' THEN j.pobor_mwh * COALESCE(j.b21_var_pln_mwh,0)
            WHEN 'B22' THEN j.pobor_mwh * COALESCE(j.b22_var_pln_mwh,0)
            WHEN 'B23' THEN j.pobor_mwh * COALESCE(j.b23_var_pln_mwh,0)
            ELSE 0
          END
        + j.pobor_mwh * COALESCE(j.stawka_oze_pln_mwh,0)
        + j.pobor_mwh * COALESCE(j.stawka_kog_pln_mwh,0)
        + j.opl_mocowa_h_r
      )
      + (j.pobor_mwh * COALESCE(j.akcyza_pln_mwh, 0))
      + COALESCE(j.vat_udzial,0) * (
          (
            CASE WHEN j.model_ceny_wybrany = 'stala'
                 THEN ROUND(j.pobor_mwh * COALESCE(j.cena_stala_final_pln_mwh,0), 2) + j.handlowa_h_r
                 ELSE ROUND(j.pobor_mwh * COALESCE(j.cena_dyn_final_pln_mwh,0), 2)   + j.handlowa_h_r
            END
            + CASE j.taryfa_wybrana
                WHEN 'B21' THEN j.b21_stala_h_r
                WHEN 'B22' THEN j.b22_stala_h_r
                WHEN 'B23' THEN j.b23_stala_h_r
                ELSE 0
              END
            + CASE j.taryfa_wybrana
                WHEN 'B21' THEN j.pobor_mwh * COALESCE(j.b21_var_pln_mwh,0)
                WHEN 'B22' THEN j.pobor_mwh * COALESCE(j.b22_var_pln_mwh,0)
                WHEN 'B23' THEN j.pobor_mwh * COALESCE(j.b23_var_pln_mwh,0)
                ELSE 0
              END
            + j.pobor_mwh * COALESCE(j.stawka_oze_pln_mwh,0)
            + j.pobor_mwh * COALESCE(j.stawka_kog_pln_mwh,0)
            + j.opl_mocowa_h_r
          )
          + (j.pobor_mwh * COALESCE(j.akcyza_pln_mwh, 0))
        )
    )
  , 2)::numeric(18,6) AS koszt_h_sum_brutto_pln,

  -- LCOE OZE – raport
  l.oze_gen_pv_pp_mwh,
  l.oze_gen_pv_wz_mwh,
  l.oze_gen_wiatr_mwh,
  l.oze_gen_total_mwh,
  (SELECT lcoe_pv_pp_pln_mwh FROM p_lcoe)  AS lcoe_pv_pp_pln_mwh,
  (SELECT lcoe_pv_wz_pln_mwh FROM p_lcoe)  AS lcoe_pv_wz_pln_mwh,
  (SELECT lcoe_wiatr_pln_mwh FROM p_lcoe)  AS lcoe_wiatr_pln_mwh,
  ROUND(COALESCE(l.lcoe_cost_h_pln,0), 2)::numeric       AS lcoe_cost_h_pln,
  ROUND(COALESCE(l.lcoe_blended_pln_mwh,0), 2)::numeric  AS lcoe_blended_pln_mwh

FROM j2 j
LEFT JOIN lcoe l ON l.ts_utc = j.ts_utc
