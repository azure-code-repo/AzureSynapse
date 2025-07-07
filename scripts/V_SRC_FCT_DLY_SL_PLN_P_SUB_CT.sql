CREATE VIEW stg.V_SRC_FCT_DLY_SL_PLN_P_SUB_CT
AS
 SELECT DISTINCT
                                 ISNULL(dimGEO.GEO_HST_SKY, -1) AS GEO_HST_SKY,
								 dimTM.TM_SKY,
								 ISNULL(BYR_HST_SKY, -1) AS BYR_HST_SKY,
                                 b.mjr_p_sub_ct_id as P_SUB_CT_ID,
                                 aa.ut_id ,
                                 aa.day_dt ,
								 b.pky_id as FNC_PKY_ID,
								 b.mprs_ct_id as FNC_MPRS_CT_ID,
                                 Sum(aa.tg_slspn_am)                      AS SLS_FNC_AN_PLN_AM,
                                 Sum(aa.tg_dmgn_pln_am)                   AS DM_FNC_AN_PLN_AM ,
                                 Sum(aa.tg_stl_pln_am+aa.tg_twaw_pln_am)  AS INV_SHRK_AN_PLN
                                 --Sum(CASE WHEN dimGEO.ST_CLS = 'COMP STORE'
                                 --THEN tg_slspn_am ELSE 0 END)             AS SLS_FNC_COMP_PLN,
                                 --Sum(CASE WHEN dimGEO.ST_CLS = 'NON-COMP STORE'
                                 --THEN tg_slspn_am ELSE 0 END)             AS SLS_FNC_NCOMP_PLN
                      FROM       dwh.fnc_slspn_mprs_ut_day_inf AS aa

                      INNER JOIN (SELECT * FROM dwh.byr_emp_pky_mprs_currprev_inf WHERE INS_DT = (SELECT MAX(INS_DT) FROM dwh.byr_emp_pky_mprs_currprev_inf))     AS b
                      ON         b.pky_id=aa.pky_id
                      AND        b.mprs_ct_id=aa.mprs_ct_id

					  LEFT JOIN DM.DIM_TM as dimTM
					  ON CAST(dimTM.DAY_DT as date) = CAST(aa.day_dt as date)


					  LEFT JOIN (SELECT GEO_HST_SKY, UT_ID, ST_CLS, VLD_FROM, VLD_TO FROM DM.DIM_GEO ) as dimGEO
					  ON CAST(dimGEO.UT_ID as varchar(5)) = CAST(aa.ut_id as varchar(5))
                      AND aa.day_dt BETWEEN DimGeo.VLD_FROM AND ISNULL(DimGeo.VLD_TO, CAST('2099-01-01' as date))


					  LEFT JOIN (SELECT BYR_HST_SKY, P_SUB_CT_ID, VLD_FROM, VLD_TO   FROM DM.DIM_BYR_P_SUB_CT) BYR
					  ON BYR.P_SUB_CT_ID = b.mjr_p_sub_ct_id
                      AND aa.day_dt BETWEEN BYR.VLD_FROM AND ISNULL(BYR.VLD_TO, CAST('2099-01-01' as date))

					  WHERE
                              (

                                            aa.tg_slspn_am <>0
                                 OR         aa.tg_dmgn_pln_am <>0
                                 OR         aa.tg_stl_pln_am <> 0
                                 OR         aa.tg_twaw_pln_am <> 0

                              )
                      GROUP BY
					      b.mjr_p_sub_ct_id ,
                                 aa.ut_id ,
                                 aa.day_dt,
								 b.pky_id,
								 b.mprs_ct_id,
								 dimTM.TM_SKY,
								 dimGEO.GEO_HST_SKY,
								 BYR_HST_SKY
