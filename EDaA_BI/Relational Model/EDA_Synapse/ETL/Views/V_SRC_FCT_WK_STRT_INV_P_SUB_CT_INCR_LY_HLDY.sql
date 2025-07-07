CREATE VIEW [ETL].[V_SRC_FCT_WK_STRT_INV_P_SUB_CT_INCR_LY_HLDY] AS select
FCT_HLDY.TM_SKY as TM_SKY_LY_HLDY,
LY_HLDY_DAY_DT as DAY_DT_LY, TM.DAY_DT, TM.TM_SKY as TM_SKY,
ISNULL(G.GEO_SKY, -1) as GEO_SKY,
ISNULL(B.BYR_SKY, -1) as BYR_SKY,
FCT_HLDY.UT_ID,
BYR.mjr_p_sub_ct_id as P_SUB_CT_ID,
CURR_MPRS_CT_ID AS FNC_MPRS_CT_ID,
CURR_PKY_ID AS FNC_PKY_ID,
SUM(FCT_HLDY.TG_INV_CST_AM) AS TG_INV_CST_AM_LY_HLDY,
SUM(FCT_HLDY.TG_CLRN_INV_CST_AM) AS TG_CLRN_INV_CST_AM_LY_HLDY,
SUM(FCT_HLDY.INV_CLRN_QTY) AS INV_CLRN_QTY_LY_HLDY,
SUM(FCT_HLDY.TG_INV_QT) AS TG_INV_QTY_LY_HLDY
from DM.FCT_WK_STRT_INV_P_SUB_CT AS FCT_HLDY

----- inner join to get last year holiday data
INNER JOIN
(
SELECT TM_SKY, case when LY_HLDY_DAY_DT is null then
[FCL_DAY_DT_LY] else [LY_HLDY_DAY_DT] end as LY_HLDY_DAY_DT
, DAY_DT, case when TM_SKY_LY_HLDY is null then [TM_SKY_LY_FSC] else
TM_SKY_LY_HLDY end as TM_SKY_LY_HLDY
FROM DM.DIM_TM
) as TM
ON  FCT_HLDY.TM_SKY = TM.TM_SKY_LY_HLDY

----- get current mprs_ct_id and pky_id
 INNER JOIN
 (

    SELECT pky_id, mprs_ct_id, LTRIM(RTRIM(mjr_p_sub_ct_id)) AS mjr_p_sub_ct_id, CURR_MPRS_CT_ID, CURR_PKY_ID
	FROM  dm_edw.byr_emp_pky_mprs_currprev_inf
    WHERE CAST(INS_DT as datetime) =
    (SELECT CAST(MAX(INS_DT) as datetime) AS INS_DT FROM dm_edw.byr_emp_pky_mprs_currprev_inf)
    AND mjr_p_sub_ct_id != ''
 ) AS BYR
 ON BYR.mprs_ct_id = FCT_HLDY.FNC_MPRS_CT_ID AND BYR.pky_id = FCT_HLDY.FNC_PKY_ID


----- join to get dimension skys
LEFT JOIN DM.DIM_GEO AS G ON G.GEO_HST_SKY = FCT_HLDY.GEO_HST_SKY
LEFT JOIN DM.DIM_BYR_P_SUB_CT AS B ON B.BYR_HST_SKY = FCT_HLDY.BYR_HST_SKY

where tm.tm_sky IN (select distinct tm_sky from ETL.FCT_WK_STRT_INV_P_SUB_CT_INCR_KEYS)

GROUP BY
FCT_HLDY.TM_SKY,
LY_HLDY_DAY_DT, TM.DAY_DT, TM.TM_SKY,
G.GEO_SKY,
B.BYR_SKY,
FCT_HLDY.UT_ID,
BYR.mjr_p_sub_ct_id,
CURR_MPRS_CT_ID,
CURR_PKY_ID;
