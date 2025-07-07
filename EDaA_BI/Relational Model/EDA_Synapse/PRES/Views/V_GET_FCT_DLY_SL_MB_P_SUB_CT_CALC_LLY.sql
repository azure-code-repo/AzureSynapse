/****** Object:  View [PRES].[V_GET_FCT_DLY_SL_MB_P_SUB_CT_CALC_LLY]    Script Date: 5/21/2021 11:32:46 AM ******/
/*
  This view is created to calculate LLY Data for Market Basket Sales
  ---------------------------------------------------------------------------------------------------------
  DATE: 24th May, 2021
  SLL.IS_ATV=1 added for Bug 656285
*/
CREATE VIEW [PRES].[V_GET_FCT_DLY_SL_MB_P_SUB_CT_CALC_LLY]
AS
SELECT [GEO_HST_SKY]
      ,[GEO_SKY]
      ,TM.[TM_SKY]
      ,[CHNL_HST_SKY]
      ,[CHNL_SKY]
      ,[P_SUB_CT_ID]
      ,[BYR_HST_SKY]
      ,[BYR_SKY]
      ,[UT_ID]
	  ,[SLS_MKT_BSKT_AM] as [SLS_MKT_BSKT_AM_LLY_FSC]
      ,[SLS_MKT_BSKT_QTY] as [SLS_MKT_BSKT_QTY_LLY_FSC]
      ,[DMGN_MKT_BSKT_AM] as [DMGN_MKT_BSKT_AM_LLY_FSC]
      ,[COGS_MKT_BSKT] as [COGS_MKT_BSKT_LLY_FSC]
      ,[DGTL_UT_SLS_AM] as [DGTL_UT_SLS_AM_LLY_FSC]
      ,[CALC_CIRC_AM] as [CALC_CIRC_AM_LLY_FSC]
      ,[CALC_CLRN_AM] as [CALC_CLRN_AM_LLY_FSC]
      ,[CALC_DGTL_CIRC_AM] as [CALC_DGTL_CIRC_AM_LLY_FSC]
      ,[CALC_HOOK_AM] as [CALC_HOOK_AM_LLY_FSC]
      ,[CALC_DUM_AM] as [CALC_DUM_AM_LLY_FSC]
      ,[CALC_MDWK_AM] as [CALC_MDWK_AM_LLY_FSC]
      ,[CALC_MPRKS_AM] as [CALC_MPRKS_AM_LLY_FSC]
      ,[CALC_OTH_AM] as [CALC_OTH_AM_LLY_FSC]
      ,[CALC_PERS_RWRD_AM] as [CALC_PERS_RWRD_AM_LLY_FSC]
      ,[CALC_SHLF_EDG_AM] as [CALC_SHLF_EDG_AM_LLY_FSC]
      ,[CALC_SP_EVNT_AM] as [CALC_SP_EVNT_AM_LLY_FSC]
      ,[CALC_TEAM_MMB_AM] as [CALC_TEAM_MMB_AM_LLY_FSC]
      ,[DGTL_SHOP_SLS_AM] as [DGTL_SHOP_SLS_AM_LLY_FSC]
      ,[SHOP_AND_SCN_STO_SLS_AM] as [SHOP_AND_SCN_STO_SLS_AM_LLY_FSC]
      ,[SHOP_AND_SCN_SLS_AM] as [SHOP_AND_SCN_SLS_AM_LLY_FSC]
      ,[SLS_MKT_BSKT_AM_COMP] as [SLS_MKT_BSKT_AM_COMP_LLY_FSC]
      ,[SLS_MKT_BSKT_AM_NCOMP] as [SLS_MKT_BSKT_AM_NCOMP_LLY_FSC]
  FROM [PRES].[FCT_DLY_SL_MB_P_SUB_CT_CALC_ARCHIVE] AS S WITH(NOLOCK)
  INNER JOIN
  (
		  Select DISTINCT A.[TM_SKY],
		  B.[TM_SKY] AS 'TM_SKY_LY',
		  Replace(B.[FCL_DAY_DT_LY],'-','') AS 'TM_SKY_LLY'
		  FROM [PRES].[DIM_TM_CALC] as A with(nolock)
		  INNER JOIN [PRES].[DIM_TM_CALC] as B with(nolock) ON
		  Replace(A.[FCL_DAY_DT_LY],'-','') = B.[DAY_DT] AND B.CLDR_TYP='Fiscal'
		  --where A.FCL_YR_ID = (Select Distinct FCL_YR_ID from PRES.DIM_TM_CALC WITH(NOLOCK) WHERE DAY_DT = CONVERT(VARCHAR,DATEADD(M,-1,GETDATE()),112)) //Actual
          where A.FCL_YR_ID = (Select Distinct FCL_YR_ID from PRES.DIM_TM_CALC WITH(NOLOCK) WHERE DAY_DT = CONVERT(VARCHAR,DATEADD(M,-1,GETDATE()),112))
		  -- order by  A.[TM_SKY] desc
  )as TM
  ON  S.TM_SKY = TM.TM_SKY_LLY AND S.IS_ATV=1;
