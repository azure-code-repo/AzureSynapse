CREATE VIEW [PRES].[V_DIM_P_L3_CURR]
AS SELECT
       [P_SKY]
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
      ,[FNC_MPRS_CT_ID]
      ,[FNC_MPRS_CT_DSC]
      ,[FNC_PKY_ID]
      ,[FNC_PKY_DSC]
      ,[FNC_ARE_ID]
      ,[FNC_ARE_DSC]
	  ,CASE when BUS_SEG_ID='L5-000078' then 'Y' else 'N' end as FUEL
  FROM [PRES].[DIM_P_L3]
  WHERE IS_CURR = 1;
