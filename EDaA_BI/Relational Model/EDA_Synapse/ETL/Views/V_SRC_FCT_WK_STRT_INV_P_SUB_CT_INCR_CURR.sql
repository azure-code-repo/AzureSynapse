CREATE VIEW [ETL].[V_SRC_FCT_WK_STRT_INV_P_SUB_CT_INCR_CURR]
AS select FCT_WKLY.GEO_HST_SKY,
G.GEO_SKY,
FCT_WKLY.TM_SKY,
FCT_WKLY.BYR_HST_SKY,
B.BYR_SKY,
BYR.mjr_p_sub_ct_id as P_SUB_CT_ID,
CURR_MPRS_CT_ID AS FNC_MPRS_CT_ID,
CURR_PKY_ID AS FNC_PKY_ID,
FCT_WKLY.UT_ID,
SUM(FCT_WKLY.TG_INV_CST_AM) AS TG_INV_CST_AM,
SUM(FCT_WKLY.TG_CLRN_INV_CST_AM) AS TG_CLRN_INV_CST_AM,
SUM(FCT_WKLY.INV_CLRN_QTY) AS INV_CLRN_QTY,
SUM(FCT_WKLY.TG_INV_QT) AS TG_INV_QTY
 from DM.FCT_WK_STRT_INV_P_SUB_CT FCT_WKLY

  -------- get current FNC_MPRS_CT_ID and FNC_PKY_ID from buyer employee bridge table -----
 INNER JOIN
 (

    SELECT pky_id, mprs_ct_id, LTRIM(RTRIM(mjr_p_sub_ct_id)) AS mjr_p_sub_ct_id, CURR_MPRS_CT_ID, CURR_PKY_ID
	FROM  dm_edw.byr_emp_pky_mprs_currprev_inf
    WHERE CAST(INS_DT as datetime) =
    (SELECT CAST(MAX(INS_DT) as datetime) AS INS_DT FROM dm_edw.byr_emp_pky_mprs_currprev_inf)
    AND mjr_p_sub_ct_id != ''
 ) AS BYR
 ON BYR.mprs_ct_id = FCT_WKLY.FNC_MPRS_CT_ID AND BYR.pky_id = FCT_WKLY.FNC_PKY_ID
 LEFT JOIN DM.DIM_GEO G ON G.GEO_HST_SKY = FCT_WKLY.GEO_HST_SKY
 LEFT JOIN DM.DIM_BYR_P_SUB_CT B ON B.BYR_HST_SKY = FCT_WKLY.BYR_HST_SKY

  where FCT_WKLY.TM_SKY
  IN (
  select tm_sky from (
  select  tm_sky, count(1) as Records
  from
   /*

      [ETL].[FCT_WK_STRT_INV_P_SUB_CT_INCR_KEYS] contains all dimension keys to be considered in the current incremental load
      This table is populated in a previous etl step by the stored procedure [ETL].[GET_FCT_WK_STRT_INV_P_SUB_CT_INCR_KEYS]

  */
  [ETL].[FCT_WK_STRT_INV_P_SUB_CT_INCR_KEYS]
  GROUP BY tm_sky) as x
  )

  GROUP BY
FCT_WKLY.GEO_HST_SKY,
G.GEO_SKY,
FCT_WKLY.TM_SKY,
FCT_WKLY.BYR_HST_SKY,
B.BYR_SKY,
BYR.mjr_p_sub_ct_id,
FCT_WKLY.UT_ID,
CURR_MPRS_CT_ID,
CURR_PKY_ID
