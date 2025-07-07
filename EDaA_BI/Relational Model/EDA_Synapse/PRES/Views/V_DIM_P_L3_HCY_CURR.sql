CREATE VIEW [PRES].[V_DIM_P_L3_HCY_CURR] AS select [P_SUB_CT_ID]
,[P_SUB_CT_DSC]
,[P_CT_ID]
,[P_CT_DSC]
,[BUS_SEG_ID]
,[BUS_SEG_DSC]

 /* bug fix MMA key for gas station from System L5/L6 to Gas station*/
,case when [BUS_SEG_ID] = 'L5-000078' then 'L6-000003'
when [BUS_SEG_ID] = 'L5-000105' then 'L6-000003'
when [BUS_SEG_ID] = 'L5-000106' then 'L6-000003'
when [BUS_SEG_ID] = 'L5-000108' then 'L6-000003'
when [BUS_SEG_ID] = 'L5-000107' then 'L6-000003'
ELSE [MDS_ARE_ID] end as [MDS_ARE_ID]
/* bug fix MMA key for gas station from System L5/L6 to Gas station*/
,case when [BUS_SEG_ID] = 'L5-000078' then 'GAS STATION'
when [BUS_SEG_ID] = 'L5-000105' then 'GAS STATION'
when [BUS_SEG_ID] = 'L5-000106' then 'GAS STATION'
when [BUS_SEG_ID] = 'L5-000108' then 'GAS STATION'
when [BUS_SEG_ID] = 'L5-000107' then 'GAS STATION'
ELSE [MDS_ARE_DSC] end as [MDS_ARE_DSC]
/*Bug fix FUEL flag*/
,CASE when BUS_SEG_ID='L5-000078' then 'Y' else 'N' end as FUEL
,[MJR_CORP_ID]

 from (

SELECT distinct
/* BUG fix duplicates in AAS view */
ROW_NUMBER() OVER (PARTITION BY [P_SUB_CT_ID] ORDER BY P_SKY, P_HST_SKY desc) AS RwNr
,[P_SUB_CT_ID]
,[P_SUB_CT_DSC]
,[P_CT_ID]
,[P_CT_DSC]
,Case when P_CT_ID='L4-999079' then 'L5-000078'
when P_CT_ID='L4-999379' then 'L5-000105'
when P_CT_ID='L4-999733' then 'L5-000106'
when P_CT_ID='L4-999073' then 'L5-000108'
when P_CT_ID='L4-999791' then 'L5-000107'
else [BUS_SEG_ID] end as [BUS_SEG_ID]
,Case when P_CT_ID='L4-999079' then 'GAS'
when P_CT_ID='L4-999379' then 'CAR WASH'
when P_CT_ID='L4-999733' then 'GAS STATION PHONE CARDS'
when P_CT_ID='L4-999073' then 'CONVENIENCE STORE'
when P_CT_ID='L4-999791' then 'KEROSENE'
else [BUS_SEG_DSC] end as [BUS_SEG_DSC]
,[MDS_ARE_ID]
,[MDS_ARE_DSC]
,CASE when BUS_SEG_ID='L5-000078' then 'Y' else 'N' end as FUEL
,990000 AS MJR_CORP_ID
FROM [PRES].[DIM_P_L3] AS P
  WHERE IS_CURR = 1

) as x
where x.RwNr = 1;
GO
