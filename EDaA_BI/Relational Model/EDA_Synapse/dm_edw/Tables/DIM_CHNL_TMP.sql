CREATE TABLE [dm_edw].[DIM_CHNL_TMP]
(
	[CHNL_HST_SKY] [int] NULL,
	[CHNL_SKY] [int] NULL,
	[CHNL_BUS_KEY] [nvarchar](256) NULL,
	[SHOP_CHNL_CT] [int] NULL,
	[SHOP_CHNL_GRP_CT] [int] NULL,
	[SHOP_CHNL_CLS_CT] [nvarchar](256) NULL,
	[SHOP_CHNL_CLS_CT_NM] [nvarchar](256) NULL,
	[MBL_SLF_CHKOT_CT] [nvarchar](256) NULL,
	[MBL_SLF_CHKOT_DSC] [nvarchar](256) NULL,
	[MPRKS] [nvarchar](256) NULL,
	[MPRKS_TN_CT] [nvarchar](256) NULL,
	[MPERKS_TN_DSC] [nvarchar](256) NULL,
	[CHNL_DTL] [nvarchar](256) NULL,
	[SHOP_CHNL] [nvarchar](256) NULL,
	[SHOP_AND_SCN] [nvarchar](256) NULL,
	[SHOP_AND_SCN_FLG] [int] NULL,
	[DGTL_UT_FLG] [nvarchar](256) NULL,
	[VLD_FROM] [date] NULL,
	[VLD_TO] [date] NULL,
	[IS_CURR] [int] NULL,
	[IS_DMY] [int] NULL,
	[IS_EMBR] [int] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
;
