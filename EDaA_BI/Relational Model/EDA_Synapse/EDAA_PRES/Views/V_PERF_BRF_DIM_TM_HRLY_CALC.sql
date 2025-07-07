CREATE VIEW [EDAA_PRES].[V_PERF_BRF_DIM_TM_HRLY_CALC]
AS SELECT [Dt_Tm_Hr]
      ,[Cal_Typ_Id]
      ,[Pre_Def_Fltr_Id]
      ,[Cal_Dt]
      ,[Cal_Dt_Nm]
      ,[Lst_Yr_Fsc_Dt]
      ,[Lst_Yr_Hldy_Dt]
      ,[Lst_Yr_Cal_Dt]
      ,[Hldy_Dt_Desc]
      ,[Dy_Of_Wk_Nm]
      ,[Cal_Typ]
      ,[Dy_Tm_Intrvl_Id]
      ,[Pre_Def_Fltr]
     ,Dateadd(hour, 1,[Dt_Tm_Hr])  AS Hr
	   ,Datepart(hour, [Dt_Tm_Hr]) + 1 AS Mil_Tm
  FROM [EDAA_PRES].[Dim_Hrly_Cal_Calc];
