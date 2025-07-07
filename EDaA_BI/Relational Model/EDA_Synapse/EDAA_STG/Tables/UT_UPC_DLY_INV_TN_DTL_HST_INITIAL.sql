CREATE TABLE [EDAA_STG].[UT_UPC_DLY_INV_TN_DTL_HST_INITIAL]
(
	[UPC_S_ID] [nvarchar](31) NULL,
	[UT_ID] [smallint] NULL,
	[BUS_DT] [datetime2](0) NULL,
	[INV_TN_TYP_CT_ID] [smallint] NULL,
	[INV_TN_TYP_ID] [smallint] NULL,
	[RW_CRT_DT] [datetime2](0) NULL,
	[RW_CRT_TM] [int] NULL,
	[UPC_ID] [decimal](18, 0) NULL,
	[T_INV_TN_AM] [decimal](17, 4) NULL,
	[T_INV_TN_QT] [decimal](11, 2) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
);
