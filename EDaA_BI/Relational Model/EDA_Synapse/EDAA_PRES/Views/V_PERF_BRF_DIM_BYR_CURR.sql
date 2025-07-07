CREATE VIEW [EDAA_PRES].[V_PERF_BRF_DIM_BYR_CURR]
AS SELECT  [Byr_L3_Hist_Sk]
      ,[Byr_L3_Sk]
      ,[Byr_Id]
      ,[Lvl3_Prod_Sub_Ctgry_Id]
      ,[Lvl3_Prod_Sub_Ctgry_Desc]
      ,[Grp_Vp_Nm]
      ,[Mngr_Nm]
      ,[Vp_Nm]
      ,[Byr_Nm]
  FROM [EDAA_PRES].[Dim_Byr]
  where
  [Is_Curr_Ind] = 1;
