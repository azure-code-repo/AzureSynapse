CREATE TABLE [EDAA_STG].[UT_UPC_DLY_SL_HST]
(
	[POS_STO_POS_BUS_DT] [char](8) NULL,
	[POS_UT_ID] [smallint] NULL,
	[POS_UPC_ID] [decimal](18, 0) NULL,
	[POS_PKY_ID] [smallint] NULL,
	[POS_MPRS_PRC_ID] [char](1) NULL,
	[POS_SSK_T_SL_AM] [decimal](11, 2) NULL,
	[POS_SSK_T_SL_QT] [decimal](9, 0) NULL,
	[POS_SSK_T_SL_WGHT] [decimal](8, 2) NULL,
	[POS_SSK_SL_PTS_AM] [decimal](11, 2) NULL,
	[POS_SSK_SL_PTS_QT] [decimal](9, 0) NULL,
	[POS_SSK_SL_PTS_WGHT] [decimal](8, 2) NULL,
	[POS_SSK_SL_PTS_MKDN_AM] [decimal](9, 2) NULL,
	[POS_SSK_SL_CLRN_AM] [decimal](11, 2) NULL,
	[POS_SSK_SL_CLRN_QT] [decimal](9, 0) NULL,
	[POS_SSK_SL_CLRN_WGHT] [decimal](8, 2) NULL,
	[POS_SSK_SL_CLR_MKDN_AM] [decimal](9, 2) NULL,
	[POS_SSK_CGS_FNC_CHR_AM] [decimal](11, 4) NULL,
	[POS_SSK_CGS_FNC_CR_AM] [decimal](11, 4) NULL,
	[POS_SSK_CGS_STG_CHR_AM] [decimal](11, 4) NULL,
	[POS_SSK_T_SL_DMGN_AM] [decimal](11, 4) NULL,
	[POS_SSK_SL_PTS_DMGN_AM] [decimal](11, 4) NULL,
	[POS_SSK_SL_CLR_DMGN_AM] [decimal](11, 4) NULL,
	[POS_SSK_CGS_FRT_AM] [decimal](11, 4) NULL,
	[POS_SSK_CGS_DC_HDL_AM] [decimal](11, 4) NULL,
	[POS_MPRS_CT_ID] [smallint] NULL,
	[POS_SBT_FLAG] [char](1) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
);
