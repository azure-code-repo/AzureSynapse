CREATE TABLE [EDAA_STG].[UT_UPC_DLY_SL_ADJ_HST_INITIAL]
(
	[UPC_S_ID] [varchar](31) NULL,
	[UT_ID] [smallint] NULL,
	[BUS_DT] [date] NULL,
	[RW_CRT_DT] [date] NULL,
	[RW_CRT_TM] [int] NULL,
	[UPC_ID] [decimal](18, 0) NULL,
	[T_SL_ADJ_AM] [decimal](13, 2) NULL,
	[T_PROMO_SL_ADJ_AM] [decimal](13, 2) NULL,
	[T_CLRN_SL_ADJ_AM] [decimal](13, 2) NULL,
	[T_PROMO_MKDN_ADJ_AM] [decimal](13, 2) NULL,
	[T_CLRN_MKDN_ADJ_AM] [decimal](13, 2) NULL,
	[T_DMGN_ADJ_AM] [decimal](15, 4) NULL,
	[T_PROMO_DMGN_ADJ_AM] [decimal](15, 4) NULL,
	[T_CLRN_DMGN_ADJ_AM] [decimal](15, 4) NULL,
	[T_CGS_P_CST_ADJ_AM] [decimal](15, 4) NULL,
	[T_CGS_FNC_CHRG_ADJ_AM] [decimal](15, 4) NULL,
	[T_CGS_FNC_CR_ADJ_AM] [decimal](15, 4) NULL,
	[T_CGS_STG_CHRG_ADJ_AM] [decimal](15, 4) NULL,
	[T_CGS_FRT_ADJ_AM] [decimal](15, 4) NULL,
	[T_CGS_DF_HDL_ADJ_AM] [decimal](15, 4) NULL,
	[T_SL_ADJ_QT] [decimal](11, 2) NULL,
	[T_PROMO_SL_ADJ_QT] [decimal](11, 2) NULL,
	[T_CLRN_SL_ADJ_QT] [decimal](11, 2) NULL,
	[T_WGT_ADJ_QT] [decimal](8, 2) NULL,
	[T_PROMO_WGT_ADJ_QT] [decimal](8, 2) NULL,
	[T_CLRN_WGT_ADJ_QT] [decimal](8, 2) NULL,
	[BAT_ID] [int] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
);
