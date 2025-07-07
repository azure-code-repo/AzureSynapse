CREATE VIEW [EDAA_PRES].[V_PERF_BRF_FCT_HRLY_SLS_CALC]
AS SELECT

       [Lvl3_Prod_Sub_Ctgry_Id]
      ,[Dt_Tm_Hr]
      ,[Byr_L3_Sk]
      ,[Geo_Sk]
      ,[Str_Id]
      ,[Sls_Amt]
      ,[Prm_To_Prm_Drct_Mgn_Amt]
      ,[Sls_Qty]
      ,[Prm_To_Sls_Amt]
	  	,Mprk_Sls_Amt
	,Mprk_Prm_To_Sls_Amt
	,Mprk_Prm_To_Prm_Drct_Mgn_Amt
      ,[Lst_Yr_Fsc_Sls_Amt]
      ,[Lst_Yr_Fsc_Prm_To_Prm_Drct_Mgn_Amt]
      ,[Lst_Yr_Fsc_Sls_Qty]
      ,[Lst_Yr_Fsc_Prm_To_Sls_Amt]
      ,[Lst_Yr_Hldy_Sls_Amt]
      ,[Lst_Yr_Hldy_Prm_To_Prm_Drct_Mgn_Amt]
      ,[Lst_Yr_Hldy_Sls_Qty]
      ,[Lst_Yr_Hldy_Prm_To_Sls_Amt]
      ,[Pln_Sls_Amt]

	   ,ISNULL([Mprk_Sls_Amt],0) - ISNULL([Lst_Yr_Fsc_Sls_Amt],0) AS [Diff_Curr_Yr_Lst_Yr_Fsc_Sls_Amt]
      ,ISNULL([Mprk_Prm_To_Prm_Drct_Mgn_Amt],0) -ISNULL([Lst_Yr_Fsc_Prm_To_Prm_Drct_Mgn_Amt],0)  AS [Diff_Curr_Yr_Lst_Yr_Fsc_Prm_To_Prm_Drct_Mgn_Amt]
      ,ISNULL([Sls_Qty],0) - ISNULL([Lst_Yr_Fsc_Sls_Qty],0) AS  [Diff_Curr_Yr_Lst_Yr_Fsc_Sls_Qty]
      ,ISNULL([Mprk_Prm_To_Sls_Amt],0) - ISNULL([Lst_Yr_Fsc_Prm_To_Sls_Amt],0) AS [Diff_Lst_Yr_Fsc_Prm_To_Sls_Amt]

	  ,ISNULL([Mprk_Sls_Amt],0) - ISNULL([Lst_Yr_Hldy_Sls_Amt],0) AS [Diff_Curr_Yr_Lst_Yr_Hldy_Sls_Amt]
      ,ISNULL([Mprk_Prm_To_Prm_Drct_Mgn_Amt],0) - ISNULL([Lst_Yr_Hldy_Prm_To_Prm_Drct_Mgn_Amt],0) AS [Diff_Curr_Yr_Lst_Yr_Hldy_Prm_To_Prm_Drct_Mgn_Amt]
      ,ISNULL([Sls_Qty],0) - ISNULL([Lst_Yr_Hldy_Sls_Qty],0) AS  [Diff_Curr_Yr_Lst_Yr_Hldy_Sls_Qty]
      ,ISNULL([Mprk_Prm_To_Sls_Amt],0) - ISNULL([Lst_Yr_Hldy_Prm_To_Sls_Amt],0) AS [Diff_Lst_Yr_Hldy_Prm_To_Sls_Amt]
	  ,ISNULL([Mprk_Sls_Amt],0) - ISNULL([Pln_Sls_Amt],0) AS [Diff_Curr_Pln_Sls_Amt]
  FROM [EDAA_PRES].[Fct_Hrly_Prod_Sub_Ctgry_Sls_Calc]
