CREATE VIEW [EDAA_PRES].[V_PERF_BRF_FCT_HRLY_SLS_FCST_CALC]
AS SELECT
       [Dt_Tm_Hr]
      ,[Geo_Sk]
      ,[Str_Id]
      ,[Fcst_Sls_Amt]

  FROM [EDAA_PRES].[Fct_Hrly_Sls_Fcst_Calc];
