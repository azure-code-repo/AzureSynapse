CREATE VIEW [V_EDAA_DW].[FCT_DLY_STR_ITMSKU_INV_EXP]
AS SELECT
[DT_SK]
	  ,[PROD_HIST_SK]
      ,[GEO_HIST_SK]
      ,[STR_ID]
      ,[ITM_SKU]
      /*,[INV_ROW_START_DT]
      ,[INV_ROW_END_DT] */
      ,CAST([INV_EACH_AMT] AS DECIMAL(18,2)) AS [INV_EACH_AMT]
      ,CAST([INV_FNC_CHRG_AMT] AS DECIMAL(18,2)) AS [INV_FNC_CHRG_AMT]
      ,CAST([INV_FNC_CR_AMT] AS DECIMAL(18,2)) AS [INV_FNC_CR_AMT]
      ,CAST([INV_STRGE_CHRG_AMT] AS DECIMAL(18,2)) AS [INV_STRGE_CHRG_AMT]
      ,CAST([INV_FRT_AMT] AS DECIMAL(18,2)) AS [INV_FRT_AMT]
      ,CAST([INV_DIST_CNTR_HNDL_AMT] AS DECIMAL(18,2)) AS [INV_DIST_CNTR_HNDL_AMT]
      ,CAST([INV_AMT] AS DECIMAL(18,2)) AS [INV_AMT]
      ,[INV_QTY]
      ,[INV_CLRNC_IND]
  FROM [EDAA_DW].[FCT_DLY_STR_ITMSKU_INV] e,  [EDAA_DW].[DIM_DLY_CAL] d
  where d.dt_sk between replace(inv_row_start_dt,'-','') and replace(inv_row_end_dt,'-','')
  and d.dt_sk <= format(getdate(),'yyyyMMdd');
