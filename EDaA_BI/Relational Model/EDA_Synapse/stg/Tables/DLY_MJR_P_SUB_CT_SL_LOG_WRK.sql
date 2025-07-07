CREATE TABLE [stg].[DLY_MJR_P_SUB_CT_SL_LOG_WRK]
(
	[DAY_DT] [datetime2](0) NULL,
	[MJR_P_SUB_CT_ID] [nvarchar](15) NULL,
	[UT_ID] [smallint] NULL,
	[SHOP_CHNL_CT] [smallint] NULL,
	[PVDR_ID] [int] NULL,
	[MBL_SLF_CHKOT_FLG] [nvarchar](1) NULL,
	[MPERK_TN_CT] [nvarchar](1) NULL,
	[TG_SL_ADJ_AM] [decimal](18, 4) NULL,
	[TG_CST_ADJ_AM] [decimal](15, 2) NULL,
	[TG_UOM_ADJ_QT] [decimal](15, 2) NULL,
	[PRSNL_RWRD_DCT_AM] [decimal](15, 2) NULL,
	[CLRN_PR_CHG_AM] [decimal](15, 2) NULL,
	[CIRC_SVGS_AM] [decimal](15, 2) NULL,
	[MID_WK_SVGS_AM] [decimal](15, 2) NULL,
	[SUPER_EVNT_SVGS_AM] [decimal](15, 2) NULL,
	[DGTL_CIRC_SVGS_AM] [decimal](15, 2) NULL,
	[SHLF_EDGE_SVGS_AM] [decimal](15, 2) NULL,
	[HOOK_DCT_AM] [decimal](15, 2) NULL,
	[MPERK_RX_CPN_DCT_AM] [decimal](15, 2) NULL,
	[CMKT_LGX_OTH_DCT_AM] [decimal](15, 2) NULL,
	[TMMB_DCT_AM] [decimal](15, 2) NULL,
	[KEY_DMP_CPN_DCT_AM] [decimal](15, 2) NULL,
	[DGTL_UT_FLG] [smallint] NULL,
	[SHOP_AND_SCN_UT_FLG] [smallint] NULL,
	[RW_ACT_CT] [nvarchar](1) NULL,
	[RW_CRT_TS] [datetime2](0) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
