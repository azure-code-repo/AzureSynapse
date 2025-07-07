CREATE VIEW [EDAA_PRES].[V_PERF_BRF_DIM_PROD_L3_CURR]
AS SELECT
[Lvl3_Prod_Sub_Ctgry_Id]
      ,[Lvl3_Prod_Sub_Ctgry_Desc]
      ,[Lvl4_Prod_Ctgry_Id]
      ,[Lvl4_Prod_Ctgry_Desc]
      ,[Lvl5_Bsns_Sgmt_Id]
      ,[Lvl5_Bsns_Sgmt_Desc]


   ,case when [Lvl5_Bsns_Sgmt_Id] = 'L5-000078' then 'L6-000003'
	  when [Lvl5_Bsns_Sgmt_Id] = 'L5-000105' then 'L6-000003'
	  when [Lvl5_Bsns_Sgmt_Id] = 'L5-000106' then 'L6-000003'
	  when [Lvl5_Bsns_Sgmt_Id] = 'L5-000108' then 'L6-000003'
	  when [Lvl5_Bsns_Sgmt_Id] = 'L5-000107' then 'L6-000003'
	  ELSE [Lvl6_MDSArea_Id] end as [Lvl6_MDSArea_Id]
	   /* bug fix MMA key for gas station from System L5/L6 to Gas station*/
	    ,case when [Lvl5_Bsns_Sgmt_Id] = 'L5-000078'  then 'GAS STATION'
	  when [Lvl5_Bsns_Sgmt_Id] = 'L5-000105' then 'GAS STATION'
	  when [Lvl5_Bsns_Sgmt_Id] = 'L5-000106' then 'GAS STATION'
	  when [Lvl5_Bsns_Sgmt_Id] = 'L5-000108' then 'GAS STATION'
	  when [Lvl5_Bsns_Sgmt_Id] = 'L5-000107' then 'GAS STATION'
	  ELSE [Lvl6_MDSArea_Desc] end as [Lvl6_MDSArea_Desc]

      ,[Str_Area_Dtl_Id]
      ,[Str_Area_Dtl_Desc]
      ,[Str_Area_Summ_Id]
      ,[Str_Area_Summ_Desc]
      ,[Str_Dept_Dtl_Id]
      ,[Str_Dept_Dtl_Desc]
      ,[Str_Dept_Summ_Id]
      ,[Str_Dept_Summ_Desc]
	  FROM
(SELECT
      distinct
      ROW_NUMBER() OVER  (PARTITION BY [Lvl3_Prod_Sub_Ctgry_Id] ORDER BY Prod_L3_Hist_Sk, Prod_L3_Sk desc) AS RwNr
       ,[Lvl3_Prod_Sub_Ctgry_Id]
      ,[Lvl3_Prod_Sub_Ctgry_Desc]
      ,[Lvl4_Prod_Ctgry_Id]
      ,[Lvl4_Prod_Ctgry_Desc]


	   ,Case when [Lvl4_Prod_Ctgry_Id]='L4-999079' then 'L5-000078'
        when [Lvl4_Prod_Ctgry_Id]='L4-999379' then 'L5-000105'
        when [Lvl4_Prod_Ctgry_Id]='L4-999733' then 'L5-000106'
        when [Lvl4_Prod_Ctgry_Id]='L4-999073' then 'L5-000108'
        when [Lvl4_Prod_Ctgry_Id]='L4-999791' then 'L5-000107'
        else [Lvl5_Bsns_Sgmt_Id] end as [Lvl5_Bsns_Sgmt_Id]
		  ,Case when [Lvl4_Prod_Ctgry_Id]='L4-999079' then 'GAS'
        when [Lvl4_Prod_Ctgry_Id]='L4-999379' then 'CAR WASH'
        when [Lvl4_Prod_Ctgry_Id]='L4-999733' then 'GAS STATION PHONE CARDS'
        when [Lvl4_Prod_Ctgry_Id]='L4-999073' then 'CONVENIENCE STORE'
        when [Lvl4_Prod_Ctgry_Id]='L4-999791' then 'KEROSENE'
        else [Lvl5_Bsns_Sgmt_Desc] end as [Lvl5_Bsns_Sgmt_Desc]
      ,[Lvl6_MDSArea_Id]
      ,[Lvl6_MDSArea_Desc]
      ,[Str_Area_Dtl_Id]
      ,[Str_Area_Dtl_Desc]
      ,[Str_Area_Summ_Id]
      ,[Str_Area_Summ_Desc]
      ,[Str_Dept_Dtl_Id]
      ,[Str_Dept_Dtl_Desc]
      ,[Str_Dept_Summ_Id]
      ,[Str_Dept_Summ_Desc]
	  ,990000 AS MJR_CORP_ID
  FROM [EDAA_PRES].[Dim_Prod_L3]
  where [Is_Curr_Ind] = 1) as x
    where x.RwNr = 1
