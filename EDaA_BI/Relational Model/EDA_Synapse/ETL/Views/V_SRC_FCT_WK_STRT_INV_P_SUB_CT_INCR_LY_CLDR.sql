﻿CREATE VIEW [ETL].[V_SRC_FCT_WK_STRT_INV_P_SUB_CT_INCR_LY_CLDR] AS select
FCT_CLDR.TM_SKY as TM_SKY_LY_CLDR,
TM.DAY_DT_LY, TM.DAY_DT, TM.TM_SKY as TM_SKY,
G.GEO_SKY,
B.BYR_SKY,
FCT_CLDR.UT_ID,
BYR.mjr_p_sub_ct_id as P_SUB_CT_ID,
CURR_MPRS_CT_ID AS FNC_MPRS_CT_ID,
CURR_PKY_ID AS FNC_PKY_ID,
SUM(FCT_CLDR.TG_INV_CST_AM) AS TG_INV_CST_AM_LY_CLDR,
SUM(FCT_CLDR.TG_CLRN_INV_CST_AM) AS TG_CLRN_INV_CST_AM_LY_CLDR,
SUM(FCT_CLDR.INV_CLRN_QTY) AS INV_CLRN_QTY_LY_CLDR,
SUM(FCT_CLDR.TG_INV_QT) AS TG_INV_QTY_LY_CLDR
from DM.FCT_WK_STRT_INV_P_SUB_CT AS FCT_CLDR
----- LEFT join to get last year calendar data
LEFT JOIN
DM.DIM_TM as TM ON  FCT_CLDR.TM_SKY = TM.TM_SKY_LY_CLDR

 -------- get current FNC_MPRS_CT_ID and FNC_PKY_ID from buyer employee bridge table -----
 INNER JOIN
 (

    SELECT pky_id, mprs_ct_id, LTRIM(RTRIM(mjr_p_sub_ct_id)) AS mjr_p_sub_ct_id, CURR_MPRS_CT_ID, CURR_PKY_ID
	FROM  dm_edw.byr_emp_pky_mprs_currprev_inf
    WHERE CAST(INS_DT as datetime) =
    (SELECT CAST(MAX(INS_DT) as datetime) AS INS_DT FROM dm_edw.byr_emp_pky_mprs_currprev_inf)
    AND mjr_p_sub_ct_id != ''
 ) AS BYR
 ON BYR.mprs_ct_id = FCT_CLDR.FNC_MPRS_CT_ID AND BYR.pky_id = FCT_CLDR.FNC_PKY_ID


----- join to get dimension skys
LEFT JOIN DM.DIM_GEO AS G ON G.GEO_HST_SKY = FCT_CLDR.GEO_HST_SKY
LEFT JOIN DM.DIM_BYR_P_SUB_CT AS B ON B.BYR_HST_SKY = FCT_CLDR.BYR_HST_SKY
where tm.tm_sky IN (select distinct tm_sky from ETL.FCT_WK_STRT_INV_P_SUB_CT_INCR_KEYS)
GROUP BY
FCT_CLDR.TM_SKY,
TM.DAY_DT_LY, TM.DAY_DT, TM.TM_SKY,
G.GEO_SKY,
B.BYR_SKY,
FCT_CLDR.UT_ID,
BYR.mjr_p_sub_ct_id,
CURR_MPRS_CT_ID,
CURR_PKY_ID;
GO
