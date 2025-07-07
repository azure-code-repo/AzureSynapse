CREATE TABLE [stg].[DLY_P_MDS_HCY_LVL_TN_LOG_WRK]
(
	[DAY_DT] [datetime2](0) NULL,
	[P_MDS_HCY_LVL_ID] [nvarchar](15) NULL,
	[UT_ID] [smallint] NULL,
	[SHOP_CHNL_CT] [smallint] NULL,
	[MBL_SLF_CHKOT_FLG] [nvarchar](1) NULL,
	[MPERK_TN_CT] [nvarchar](1) NULL,
	[DGTL_UT_FLG] [smallint] NULL,
	[SHOP_AND_SCN_UT_FLG] [smallint] NULL,
	[GUEST_SRVY_POS_CNT] [int] NULL,
	[GUEST_SRVY_RSP_CNT] [int] NULL,
	[FNC_TN_CNT] [int] NULL,
	[RW_ACT_CT] [nvarchar](1) NULL,
	[RW_CRT_TS] [datetime2](0) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
