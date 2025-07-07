CREATE VIEW [EDAA_PRES].[V_PERF_BRF_DIM_USR]
AS SELECT  'US\' + [Usr_Id] AS [Usr_Id]
      ,[Str_Id]
      ,[Usr_Rl_Nm]
      ,[Mkt_Id]
      ,[Rgn_Id]
  FROM [EDAA_DW].[Dim_Usr];
