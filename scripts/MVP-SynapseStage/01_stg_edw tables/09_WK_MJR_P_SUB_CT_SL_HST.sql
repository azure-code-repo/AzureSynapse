/****** Object:  Table [stg_edw].[WK_MJR_P_SUB_CT_SL_HST]    Script Date: 9/9/2020 3:19:46 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stg_edw].[WK_MJR_P_SUB_CT_SL_HST]
(
	[WK_END_DT] [date] NOT NULL,
	[MJR_P_SUB_CT_ID] [varchar](15) NOT NULL,
	[UT_ID] [smallint] NOT NULL,
	[SHOP_CHNL_CT] [int] NOT NULL,
	[PVDR_ID] [int] NOT NULL,
	[MBL_SLF_CHKOT_FLG] [char](1) NOT NULL,
	[MPERK_TN_CT] [char](1) NOT NULL,
	[TG_SL_ADJ_AM] [decimal](18, 4) NOT NULL,
	[TG_CST_ADJ_AM] [decimal](15, 2) NOT NULL,
	[TG_UOM_ADJ_QT] [decimal](15, 2) NOT NULL,
	[PRSNL_RWRD_DCT_AM] [decimal](15, 2) NOT NULL,
	[CLRN_PR_CHG_AM] [decimal](15, 2) NOT NULL,
	[CIRC_SVGS_AM] [decimal](15, 2) NOT NULL,
	[MID_WK_SVGS_AM] [decimal](15, 2) NOT NULL,
	[SUPER_EVNT_SVGS_AM] [decimal](15, 2) NOT NULL,
	[DGTL_CIRC_SVGS_AM] [decimal](15, 2) NOT NULL,
	[SHLF_EDGE_SVGS_AM] [decimal](15, 2) NOT NULL,
	[HOOK_DCT_AM] [decimal](15, 2) NOT NULL,
	[MPERK_RX_CPN_DCT_AM] [decimal](15, 2) NOT NULL,
	[CMKT_LGX_OTH_DCT_AM] [decimal](15, 2) NOT NULL,
	[TMMB_DCT_AM] [decimal](15, 2) NOT NULL,
	[KEY_DMP_CPN_DCT_AM] [decimal](15, 2) NOT NULL,
	[DGTL_UT_FLG] [smallint] NOT NULL,
	[SHOP_AND_SCN_UT_FLG] [smallint] NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
