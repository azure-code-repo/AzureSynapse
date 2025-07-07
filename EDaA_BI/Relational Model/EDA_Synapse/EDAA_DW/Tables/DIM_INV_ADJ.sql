/****** Object:  Table [EDAA_DW].[DIM_INV_ADJ]    Script Date: 8/25/2021 1:53:04 AM ******/
CREATE TABLE [EDAA_DW].[DIM_INV_ADJ]
(
	[INV_ADJ_SK] [int] NOT NULL,
	[ADJ_TYP_ID] [smallint] NULL,
	[ADJ_TYP] [char](3) NOT NULL,
	[ADJ_TYP_DESC] [varchar](100) NULL,
	[TXN_TYP] [smallint] NOT NULL,
	[INV_ADJ_DESC] [varchar](40) NULL,
	[IS_DMY_IND] [bit] NULL,
	[IS_EMB_IND] [bit] NULL,
	[ETL_ACTN] [varchar](100) NULL,
	[AUD_INS_SK] [bigint] NULL,
	[AUD_UPD_SK] [bigint] NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	HEAP
);
