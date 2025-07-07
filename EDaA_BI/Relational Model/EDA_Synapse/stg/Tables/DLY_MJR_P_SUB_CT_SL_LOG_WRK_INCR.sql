CREATE TABLE [stg].[DLY_MJR_P_SUB_CT_SL_LOG_WRK_INCR]
(
	[DAY_DT] datetime NOT NULL,
	[MJR_P_SUB_CT_ID] [varchar](15) NOT NULL,
	[UT_ID] [smallint] NOT NULL,
	[SHOP_CHNL_CT] [int] NOT NULL,
	[MBL_SLF_CHKOT_FLG] [char](1) NOT NULL,
	[MPERK_TN_CT] [char](1) NOT NULL,
	[TG_SL_ADJ_AM] [decimal](38, 4) NULL,
	[TG_CST_ADJ_AM] [decimal](38, 2) NULL,
	[TG_UOM_ADJ_QT] [decimal](38, 2) NULL,
	[PRSNL_RWRD_DCT_AM] [decimal](38, 2) NULL,
	[CLRN_PR_CHG_AM] [decimal](38, 2) NULL,
	[CIRC_SVGS_AM] [decimal](38, 2) NULL,
	[MID_WK_SVGS_AM] [decimal](38, 2) NULL,
	[SUPER_EVNT_SVGS_AM] [decimal](38, 2) NULL,
	[DGTL_CIRC_SVGS_AM] [decimal](38, 2) NULL,
	[SHLF_EDGE_SVGS_AM] [decimal](38, 2) NULL,
	[HOOK_DCT_AM] [decimal](38, 2) NULL,
	[MPERK_RX_CPN_DCT_AM] [decimal](38, 2) NULL,
	[CMKT_LGX_OTH_DCT_AM] [decimal](38, 2) NULL,
	[TMMB_DCT_AM] [decimal](38, 2) NULL,
	[KEY_DMP_CPN_DCT_AM] [decimal](38, 2) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
