CREATE VIEW [ETL].[V_SRC_FCT_DLY_SL_MB_TXN_P_SUB_CT_INCR_LY_HLDY] AS select
FCT_HLDY.TM_SKY as TM_SKY_LY_HLDY,
LY_HLDY_DAY_DT as DAY_DT_LY, TM.DAY_DT, TM.TM_SKY as TM_SKY
, ISNULL(G.GEO_SKY, -1) AS GEO_SKY
,ISNULL(C.CHNL_SKY, -1) AS CHNL_SKY
,FCT_HLDY.UT_ID,
FCT_HLDY.P_CT_BUS_KEY,
FNC_TN_CNT as FNC_TN_CNT_LY_HLDY
from DM.FCT_DLY_SL_MB_TXN_P_SUB_CT AS FCT_HLDY
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
----- join to get dimension skys
LEFT JOIN DM.DIM_GEO AS G ON G.GEO_HST_SKY = FCT_HLDY.GEO_HST_SKY
LEFT JOIN DM.DIM_CHNL AS C ON C.CHNL_HST_SKY = FCT_HLDY.CHNL_HST_SKY
----  Inner join to get changed and updated skys

/******* CHANGE 11/9 = comment below section ***/
/*
INNER JOIN ETL.FCT_DLY_SL_MB_TXN_P_SUB_CT_INCR_KEYS AS INCR
ON INCR.UT_ID = FCT_HLDY.UT_ID AND INCR.CHNL_SKY = C.CHNL_SKY AND INCR.P_CT_BUS_KEY = FCT_HLDY.P_CT_BUS_KEY
AND INCR.TM_SKY = TM.TM_SKY
*/

/******* CHANGE 11/9 = introduce below condition to check only those last year dates corresponding to current incremental***/
where TM.TM_SKY In (

--select distinct tm_sky from ETL.FCT_DLY_SL_MB_TXN_P_SUB_CT_INCR_KEYS
--UNION
--select tm_sky from dm.dim_tm where tm_sky between
--(select cast(replace([WK_END_DT], '-', '') as int)  from dm.dim_tm where [DAY_DT] = cast(getdate() as date)) -6
--AND
--(select cast(replace([WK_END_DT], '-', '') as int)  from dm.dim_tm where [DAY_DT] = cast(getdate() as date))

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
