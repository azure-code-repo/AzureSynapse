CREATE TABLE [EDAA_STG].[TEMP_INV_TY_LY_FSC]
(
	[STR_ID] [smallint] NOT NULL,
	[ITM_SKU] [decimal](18, 0) NOT NULL,
	[DT_SK] [int] NULL,
	[LY_DT_SK] [int] NULL,
	[LY_DT_SK_HLDY] [int] NULL,
	[LY_DT_SK_CAL] [int] NULL,
	[INV_AMT_TY] [decimal](14, 4) NULL,
	[INV_QTY_TY] [decimal](12, 2) NULL,
	[INV_AMT_LY] [decimal](14, 4) NULL,
	[INV_QTY_LY] [decimal](12, 2) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
