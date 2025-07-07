CREATE VIEW [PRES].[V_DIM_GEO_CURR]
AS
SELECT
      [GEO_SKY]
      ,[UT_ID]
      ,case when [UT_DSC] = 'N/A' or [UT_DSC] = '' THEN
       CAST(UT_ID as varchar(10)) + ' - ' else [UT_DSC] END AS [UT_DSC]
      ,[UNIT_SQF]
	  ---- Power BI bug, region name is CENTRL in PMM source data. Remapped to CENTRAL
	  , case when
      replace( right(TRIM([RGN]), len(TRIM([RGN])) - charindex('(', TRIM([RGN]))), ')', '') = 'CENTRL'
	  then 'CENTRAL' ELSE
	  replace( right(TRIM([RGN]), len(TRIM([RGN])) - charindex('(', TRIM([RGN]))), ')', '')
	  END as [RGN]

	  ,[OPR_MKT]
      ,[UT_LNGT]
      ,[UT_LAT]
      ,[ST_CLS_ID]
      ,[ST_CLS]
      ,[ST]
      ,[CNTY]
      ,[CITY]
      ,[ZIP]
      ,[DIV_ID]
      ,[DIV_NM]
      ,[NW_STO_TY_FLG]
      ,[NW_STO_LY_FLG]
      ,[NW_STO_2LY_FLG]
  FROM [DM].[DIM_GEO]
  WHERE IS_CURR = 1;
