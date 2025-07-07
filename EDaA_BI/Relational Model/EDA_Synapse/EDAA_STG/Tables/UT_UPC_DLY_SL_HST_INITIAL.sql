CREATE TABLE [EDAA_STG].[UT_UPC_DLY_SL_HST_INITIAL]
(
	[UPC_S_ID] [varchar](31) NULL,
	[UT_ID] [smallint] NULL,
	[BUS_DT] [date] NULL,
	[UPC_ID] [decimal](18, 0) NULL,
	[T_SL_AM] [decimal](11, 2) NULL,
	[T_PROMO_SL_AM] [decimal](11, 2) NULL,
	[T_CLRN_SL_AM] [decimal](11, 2) NULL,
	[T_PROMO_MKDN_AM] [decimal](9, 2) NULL,
	[T_CLRN_MKDN_AM] [decimal](9, 2) NULL,
	[T_DMGN_AM] [decimal](11, 4) NULL,
	[T_PROMO_DMGN_AM] [decimal](11, 4) NULL,
	[T_CLRN_DMGN_AM] [decimal](11, 4) NULL,
	[T_CGS_FNC_CHRG_AM] [decimal](11, 4) NULL,
	[T_CGS_FNC_CR_AM] [decimal](11, 4) NULL,
	[T_CGS_STG_CHRG_AM] [decimal](11, 4) NULL,
	[T_CGS_FRT_AM] [decimal](11, 4) NULL,
	[T_CGS_DF_HDL_AM] [decimal](11, 4) NULL,
	[T_SL_QT] [decimal](9, 0) NULL,
	[T_PROMO_SL_QT] [decimal](9, 0) NULL,
	[T_CLRN_SL_QT] [decimal](9, 0) NULL,
	[T_WGT_QT] [decimal](8, 2) NULL,
	[T_PROMO_WGT_QT] [decimal](8, 2) NULL,
	[T_CLRN_WGT_QT] [decimal](8, 2) NULL,
	[RW_CRT_DT] [date] NULL,
	[RW_CRT_TM] [int] NULL,
	[BAT_ID] [int] NULL,
	[T_CGS_P_CST_AM] [decimal](11, 4) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
);
