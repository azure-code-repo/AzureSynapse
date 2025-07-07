CREATE TABLE [EDAA_STG].[PRCM_FUEL_PCHG_DTL]
(
	[STR_ID] [smallint] NULL,
	[CM_STE_STR_ID] [varchar](25) NULL,
	[CM_STE_STR_NM] [varchar](100) NULL,
	[GLBL_P_TYP_ID] [smallint] NULL,
	[GLBL_P_TYP_NM] [varchar](25) NULL,
	[PRCM_AM] [decimal](8, 3) NULL,
	[PR_EFF_TMS] [datetime] NULL,
	[PR_CRT_TMS] [datetime] NULL,
	[PR_UDT_TMS] [datetime] NULL,
	[CM_STE_L1_AD] [varchar](70) NULL,
	[CM_STE_L2_AD] [varchar](70) NULL,
	[CM_STE_L3_AD] [varchar](70) NULL,
	[CM_STE_POL_ZN_ID] [varchar](30) NULL,
	[CM_STE_LTTD_ID] [decimal](15, 10) NULL,
	[CM_STE_LNGTD_ID] [decimal](15, 10) NULL,
	[DIST_TO_NEAR_OWN_STE_QTY] [decimal](15, 10) NULL,
	[PR_ENTY_ID] [varchar](60) NULL,
	[P_CURR_SRC_TX] [varchar](50) NULL,
	[PR_SRC_TX] [varchar](50) NULL,
	[OWN_STE_ENTY_ID] [varchar](60) NULL,
	[STR_NM] [varchar](100) NULL,
	[OWN_STE_L1_AD] [varchar](70) NULL,
	[OWN_STE_L2_AD] [varchar](70) NULL,
	[OWN_STE_L3_AD] [varchar](70) NULL,
	[OWN_STE_POL_ZN_ID] [varchar](30) NULL,
	[OWN_STE_LTTD_ID] [decimal](15, 10) NULL,
	[OWN_STE_LNGTD_ID] [decimal](15, 10) NULL,
	[CM_STE_ENTY_ID] [varchar](60) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
