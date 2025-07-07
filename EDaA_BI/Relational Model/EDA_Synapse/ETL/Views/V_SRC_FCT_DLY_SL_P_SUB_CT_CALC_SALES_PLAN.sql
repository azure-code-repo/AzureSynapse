/*


This view returns sales and plan data for the current increment (or full load)
as well as sales and plan data for all dates of the current week.

*/



CREATE VIEW [ETL].[V_SRC_FCT_DLY_SL_P_SUB_CT_CALC_SALES_PLAN] AS with sales as (
SELECT  [GEO_HST_SKY]
      ,[TM_SKY]
      ,[BYR_HST_SKY]
      ,[P_SUB_CT_ID]
      ,[FNC_MPRS_CT_ID]
      , [FNC_PKY_ID]
      ,[UT_ID]
      ,[DM_FNC_AM]
      ,[DM_FNC_CLRN_AM]
      ,[DM_FNC_PROMO_AM]
      ,[SLS_FNC_AM]
      ,[SLS_FNC_QTY]
      ,[SLS_FNC_PROMO_AM]
      ,[SLS_FNC_PROMO_MKDN_AM]
      ,[SLS_FNC_CLRN_QTY]
      ,[SLS_FNC_CLRN_AM]
      ,[SLS_FNC_CLRN_MKDN_AM]
      ,[SLS_FNC_SCN_QTY]
      ,[SLS_FNC_AM_COMP]
      ,[SLS_FNC_AM_NCOMP]
      ,[INV_SHRK_AM]
      ,[INV_CLRN_QTY]

  FROM [DM].[FCT_DLY_SL_P_SUB_CT] AS SLS


  where tm_sky
  IN (
  select tm_sky from (
  select  tm_sky, count(1) as Records
  from
   /*

      [ETL].[FCT_DLY_SL_P_SUB_CT_INCR_KEYS] contains all dimension keys to be considered in the current incremental load
      This table is populated in a previous etl step by the stored procedure [ETL].[GET_FCT_DLY_SL_P_SUB_CT_INCR_KEYS]

  */
  [ETL].[FCT_DLY_SL_P_SUB_CT_INCR_KEYS]
  GROUP BY tm_sky) as x
  )

  ),
  salesplan as
(
   SELECT  [GEO_HST_SKY]
      ,[TM_SKY]
      ,[BYR_HST_SKY]
      ,[P_SUB_CT_ID]
      ,[FNC_MPRS_CT_ID]
      , [FNC_PKY_ID]
      ,[UT_ID]
      ,[SLS_FNC_AN_PLN_AM]
      ,[DM_FNC_AN_PLN_AM]
      ,[INV_SHRK_AN_PLN_AM]
      ,[SLS_PLN_QT]
  FROM [DM].[FCT_DLY_SL_PLN_P_SUB_CT] AS PLN


  where tm_sky IN (




select distinct tm_sky from (
 select tm_sky from (
  select   tm_sky, count(1) as Records

  from

  /*

      [ETL].[FCT_DLY_SL_P_SUB_CT_INCR_KEYS] contains all dimension keys to be considered in the current incremental load
      This table is populated in a previous etl step by the stored procedure [ETL].[GET_FCT_DLY_SL_P_SUB_CT_INCR_KEYS]

  */
  [ETL].[FCT_DLY_SL_P_SUB_CT_INCR_KEYS]

  GROUP BY tm_sky
  ) as x
UNION

/*Bring in data for all dates in current week*/

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
)
  )
  select
  coalesce(sales.[GEO_HST_SKY], salesplan.[GEO_HST_SKY]) as [GEO_HST_SKY]
      ,coalesce(sales.[TM_SKY], salesplan.tm_sky) as tm_sky
      ,coalesce(sales.[BYR_HST_SKY], salesplan.[BYR_HST_SKY]) as [BYR_HST_SKY]
      ,coalesce(sales.[P_SUB_CT_ID], salesplan.[P_SUB_CT_ID]) as [P_SUB_CT_ID]
      ,coalesce(sales.[FNC_MPRS_CT_ID], salesplan.[FNC_MPRS_CT_ID]) as [FNC_MPRS_CT_ID]
      ,coalesce(sales.[FNC_PKY_ID], salesplan.[FNC_PKY_ID]) as [FNC_PKY_ID]
      ,coalesce(sales.[UT_ID], salesplan.UT_ID) as UT_ID
      ,isnull(sales.[DM_FNC_AM], 0) as [DM_FNC_AM]
      ,isnull(sales.[DM_FNC_CLRN_AM], 0) as [DM_FNC_CLRN_AM]
      ,isnull(sales.[DM_FNC_PROMO_AM], 0) as [DM_FNC_PROMO_AM]
      ,isnull(sales.[SLS_FNC_AM], 0) as [SLS_FNC_AM]
      ,isnull(sales.[SLS_FNC_QTY], 0) as [SLS_FNC_QTY]
      ,isnull(sales.[SLS_FNC_PROMO_AM], 0) as [SLS_FNC_PROMO_AM]
      ,isnull(sales.[SLS_FNC_PROMO_MKDN_AM], 0) as [SLS_FNC_PROMO_MKDN_AM]
      ,isnull(sales.[SLS_FNC_CLRN_QTY], 0) as [SLS_FNC_CLRN_QTY]
      ,isnull(sales.[SLS_FNC_CLRN_AM], 0) as [SLS_FNC_CLRN_AM]
      ,isnull(sales.[SLS_FNC_CLRN_MKDN_AM], 0) as [SLS_FNC_CLRN_MKDN_AM]
      ,isnull(sales.[SLS_FNC_SCN_QTY], 0) as [SLS_FNC_SCN_QTY]
      ,isnull(sales.[SLS_FNC_AM_COMP], 0) as [SLS_FNC_AM_COMP]
      ,isnull(sales.[SLS_FNC_AM_NCOMP], 0) as [SLS_FNC_AM_NCOMP]
      ,isnull(sales.[INV_SHRK_AM], 0) as [INV_SHRK_AM]
      ,isnull(sales.[INV_CLRN_QTY], 0) as [INV_CLRN_QTY]
	  ,ISNULL([SLS_FNC_AN_PLN_AM], 0) as [SLS_FNC_AN_PLN_AM]
      ,ISNULL([DM_FNC_AN_PLN_AM], 0) as [DM_FNC_AN_PLN_AM]
      ,ISNULL([INV_SHRK_AN_PLN_AM], 0) as [INV_SHRK_AN_PLN_AM]
      ,ISNULL([SLS_PLN_QT], 0) as [SLS_PLN_QT]
	  ,(ISNULL(SLS_FNC_AM, 0) - ISNULL(DM_FNC_AM, 0)) as COGS_FNC
,((ISNULL(DM_FNC_AM, 0) - ISNULL(DM_FNC_PROMO_AM, 0) ) - ISNULL(DM_FNC_CLRN_AM, 0)) as DM_FNC_RRTL_AM
,(ISNULL(SLS_FNC_PROMO_MKDN_AM, 0) + ISNULL(SLS_FNC_CLRN_MKDN_AM, 0)) as SLS_FNC_MKDN_AM
,((ISNULL(SLS_FNC_AM, 0) - ISNULL(SLS_FNC_PROMO_AM, 0)) - ISNULL(SLS_FNC_CLRN_AM, 0)) as SLS_FNC_RRTL_MKDN_AM
,((ISNULL(SLS_FNC_AM, 0) - ISNULL(SLS_FNC_CLRN_AM, 0))) AS SLS_FNC_NCLRN_AM
,((ISNULL(DM_FNC_AM, 0) - ISNULL(DM_FNC_CLRN_AM, 0))) AS DM_FNC_NCLRN_AM
	  from sales
      /* full outer join to get
      1.) sales data with plan data
      2.) Sales data for which plan data doesn't exist
      3.) Plan data for which sales data doesn't exist
      */
	  full outer join
	  salesplan on
	  sales.UT_ID = salesplan.UT_ID
 and sales.TM_SKY = salesplan.TM_SKY
 and sales.FNC_MPRS_CT_ID = salesplan.FNC_MPRS_CT_ID
 and sales.FNC_PKY_ID = salesplan.FNC_PKY_ID and sales.p_sub_ct_id = salesplan.p_sub_ct_id
