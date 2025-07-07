CREATE TABLE [EDAA_STG_CUST].[SRVY]
(
	[SRVY_ID] [bigint] NOT NULL,
	[SRVY_STRT_DT] [datetime] NOT NULL,
	[SRVY_END_DT] [datetime] NOT NULL,
	[DGTL_ORDR_ID] [varchar](36) NULL,
	[TXN_DT] [date] NULL,
	[TXN_TM] [varchar](8) NULL,
	[STR_ID] [int] NULL,
	[TXN_LN_ID] [int] NULL,
	[TXN_NUM] [int] NULL,
	[SRVY_TYP_ID] [int] NOT NULL,
	[TXN_ID] [decimal](21, 0) NULL,
	[SHOP_CHL_CTGRY_ID] [int] NULL,
	[LLTY_ACCT_ID] [varchar](36) NULL,
	[UDT_TMS] [datetime] NOT NULL,
	[UDT_BY] [varchar](64) NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
