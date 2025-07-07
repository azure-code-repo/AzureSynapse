﻿CREATE TABLE [EDAA_STG].[TEMP_FCT_DLY_STR_ITMSKU_INV_TY_LY_FSC_DLY]
(
    [ROW_ID_VAL] [bigint] NULL,
	[PRE_DEF_ID] [int] NULL,
	[DT_VAL] [date] NULL,
	[ITM_SKU] [decimal](18, 0) NOT NULL,
	[STR_ID] [smallint] NOT NULL,
	[INV_AMT_TY_FSC] [decimal](13, 4) NULL,
	[INV_QTY_TY_FSC] [decimal](11, 2) NULL,
	[INV_AMT_LY_FSC] [decimal](13, 4) NULL,
	[INV_QTY_LY_FSC] [decimal](11, 2) NULL,
	[INV_AMT_LY_HLDY] [decimal](13, 4) NULL,
	[INV_QTY_LY_HLDY] [decimal](11, 2) NULL,
	[INV_AMT_LY_CAL] [decimal](13, 4) NULL,
	[INV_QTY_LY_CAL] [decimal](11, 2) NULL,
	[UPDATEDTIMESTAMP] [datetime] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
