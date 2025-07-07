CREATE VIEW [PRES].[V_DIM_BYR_P_SUB_CT_CURR]
AS SELECT  DISTINCT
      D.[BYR_SKY]
      ,D.[BYR_ID]
      ,D.[BYR]
      ,D.[P_SUB_CT_ID]
      ,D.[P_SUB_CT_DSC]
      ,D.[GVP]
      ,D.[MGR]
      ,D.[VP]
  FROM [DM].[DIM_BYR_P_SUB_CT]  AS D
  WHERE IS_CURR = 1;
