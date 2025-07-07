/****** Object:  View [PRES].[V_FCT_DLY_SL_MB_TXN_P_SUB_CT_CALC_LLY_TEST]    Script Date: 5/21/2021 12:44:16 PM ******/
/*
  This view is created to calculate LLY Data for Market Basket transaction
  ---------------------------------------------------------------------------------------------------------
  DATE: 24th May, 2021
  MB.IS_ATV=1 added for Bug 656285
*/
CREATE VIEW [PRES].[V_GET_FCT_DLY_SL_MB_TXN_P_SUB_CT_CALC_LLY]
AS
SELECT [GEO_HST_SKY]
      ,[GEO_SKY]
      ,TM.[TM_SKY]
      ,[CHNL_HST_SKY]
      ,[CHNL_SKY]
      ,[UT_ID]
      ,[P_CT_BUS_KEY]
      ,MB.[FNC_TN_CNT] AS [FNC_TN_CNT_LLY_FSC]


  FROM [PRES].[FCT_DLY_SL_MB_TXN_P_SUB_CT_CALC_ARCHIVE] AS MB WITH(NOLOCK)
    INNER JOIN
  (
		 Select DISTINCT A.[TM_SKY],
		  B.[TM_SKY] AS 'TM_SKY_LY',
		  Replace(B.[FCL_DAY_DT_LY],'-','') AS 'TM_SKY_LLY'
		  FROM [PRES].[DIM_TM_CALC] as A with(nolock)
		  INNER JOIN [PRES].[DIM_TM_CALC] as B with(nolock) ON
		  Replace(A.[FCL_DAY_DT_LY],'-','') = B.[DAY_DT] AND B.CLDR_TYP='Fiscal'
		  where A.FCL_YR_ID = (Select Distinct FCL_YR_ID from PRES.DIM_TM_CALC WITH(NOLOCK) WHERE DAY_DT = CONVERT(VARCHAR,DATEADD(D,-1,GETDATE()),112))
		  -- order by  A.[TM_SKY] desc
  )as TM
  ON MB.[TM_SKY] = TM.TM_SKY_LLY AND MB.IS_ATV=1;
