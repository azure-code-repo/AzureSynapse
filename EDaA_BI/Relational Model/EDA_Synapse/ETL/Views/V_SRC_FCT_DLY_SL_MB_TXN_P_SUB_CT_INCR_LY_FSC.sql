CREATE VIEW [ETL].[V_SRC_FCT_DLY_SL_MB_TXN_P_SUB_CT_INCR_LY_FSC] AS select
FCT_FSC.TM_SKY as TM_SKY_LY_FSC,
TM.DAY_DT_LY, TM.DAY_DT, TM.TM_SKY as TM_SKY
, ISNULL(G.GEO_SKY, -1) AS GEO_SKY
,ISNULL(C.CHNL_SKY, -1) AS CHNL_SKY
,FCT_FSC.UT_ID,
FCT_FSC.P_CT_BUS_KEY,
FNC_TN_CNT as FNC_TN_CNT_LY_FSC
from DM.FCT_DLY_SL_MB_TXN_P_SUB_CT AS FCT_FSC
----- inner join to get last year fiscal data
INNER JOIN
DM.DIM_TM as TM ON  FCT_FSC.TM_SKY = TM.TM_SKY_LY_FSC
----- join to get dimension skys
LEFT JOIN DM.DIM_GEO AS G ON G.GEO_HST_SKY = FCT_FSC.GEO_HST_SKY
LEFT JOIN DM.DIM_CHNL AS C ON C.CHNL_HST_SKY = FCT_FSC.CHNL_HST_SKY
----  Inner join to get changed and updated skys

/******* CHANGE 11/9 = comment below section ***/
/*
INNER JOIN ETL.FCT_DLY_SL_MB_TXN_P_SUB_CT_INCR_KEYS AS INCR
ON INCR.UT_ID = FCT_FSC.UT_ID AND INCR.CHNL_SKY = C.CHNL_SKY AND INCR.P_CT_BUS_KEY = FCT_FSC.P_CT_BUS_KEY
AND INCR.TM_SKY = TM.TM_SKY
*/

/******* CHANGE 11/9 = introduce below condition to check only those last year dates corresponding to current incremental***/
where TM.TM_SKY In (

----select distinct tm_sky from ETL.FCT_DLY_SL_P_SUB_CT_INCR_KEYS

/*fix last year records for future dates*/

select distinct tm_sky from (
 select tm_sky from (
  select   tm_sky, count(1) as Records from [ETL].[FCT_DLY_SL_MB_TXN_P_SUB_CT_INCR_KEYS]
  GROUP BY tm_sky
  ) as x
UNION

select tm_sky from dm.dim_tm where tm_sky between
(

SELECT  min([TM_SKY])
  FROM [PRES].[DIM_TM_CALC]
  where [PRE_DEF_DT_FLTR] = 'Current Wk'

)
AND
(
SELECT  max([TM_SKY])
  FROM [PRES].[DIM_TM_CALC]
  where [PRE_DEF_DT_FLTR] = 'Current Wk'

)

) as x




);
