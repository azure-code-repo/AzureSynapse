CREATE TABLE [EDAA_STG].[UT_UPC_DLY_SL_ADJ_HST]
(
	[DA_UPC_ID] [decimal](18, 0) NULL,
	[DA_UT_ID] [smallint] NULL,
	[DA_BUS_DT] [char](8) NULL,
	[DA_RTL_T_AM] [decimal](13, 2) NULL,
	[DA_UPC_T_QT] [decimal](11, 2) NULL,
	[DA_UPC_T_WGT_QT] [decimal](8, 2) NULL,
	[DA_UPC_PTS_AM] [decimal](13, 2) NULL,
	[DA_UPC_PTS_QT] [decimal](11, 2) NULL,
	[DA_UPC_PTS_WGT_QT] [decimal](8, 2) NULL,
	[DA_UPC_PTS_MKDN_AM] [decimal](13, 2) NULL,
	[DA_UPC_PTC_AM] [decimal](13, 2) NULL,
	[DA_UPC_PTC_QT] [decimal](11, 2) NULL,
	[DA_UPC_PTC_WGT_QT] [decimal](8, 2) NULL,
	[DA_UPC_PTC_MKDN_AM] [decimal](13, 2) NULL,
	[DA_CGS_P_CST_ADJ_AM] [decimal](15, 4) NULL,
	[DA_CGS_FNC_CHRG_AM] [decimal](15, 4) NULL,
	[DA_CGS_FNC_CR_AM] [decimal](15, 4) NULL,
	[DA_CGS_STG_CHRG_AM] [decimal](15, 4) NULL,
	[DA_CGS_FRT_AM] [decimal](15, 4) NULL,
	[DA_CGS_DC_HDL_AM] [decimal](15, 4) NULL,
	[DA_UPC_T_DMGN_AM] [decimal](15, 4) NULL,
	[DA_UPC_PTS_DMGN_AM] [decimal](15, 4) NULL,
	[DA_UPC_PTC_DMGN_AM] [decimal](15, 4) NULL,
	[DA_CRT_DT] [char](8) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
);
