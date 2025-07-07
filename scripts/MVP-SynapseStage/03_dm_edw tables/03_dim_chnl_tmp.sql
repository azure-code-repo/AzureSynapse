/****** Object:  Table [dm_edw].[DIM_CHNL_TMP]    Script Date: 9/9/2020 1:17:21 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dm_edw].[DIM_CHNL_TMP]
(
	[CHNL_HST_SKY] [int] NOT NULL,
	[CHNL_SKY] [int] NOT NULL,
	[CHNL_BUS_KEY] [varchar](10) NOT NULL,
	[SHOP_CHNL_CT] [smallint] NOT NULL,
	[SHOP_CHNL_GRP_CT] [smallint] NOT NULL,
	[SHOP_CHNL_CLS_CT] [varchar](3) NULL,
	[SHOP_CHNL_CLS_CT_NM] [varchar](40) NULL,
	[MBL_SLF_CHKOT_CT] [varchar](3) NOT NULL,
	[MBL_SLF_CHKOT_DSC] [varchar](60) NOT NULL,
	[MPRKS] [varchar](10) NOT NULL,
	[MPRKS_TN_CT] [varchar](3) NOT NULL,
	[MPERKS_TN_DSC] [varchar](100) NOT NULL,
	[CHNL_DTL] [varchar](50) NOT NULL,
	[SHOP_CHNL] [varchar](50) NOT NULL,
	[SHOP_AND_SCN] [varchar](30) NOT NULL,
	[SHOP_AND_SCN_FLG] [bit] NOT NULL,
	[DGTL_UT_FLG] [smallint] NOT NULL,
	[VLD_FROM] [datetime] NOT NULL,
	[VLD_TO] [datetime] NULL,
	[IS_CURR] [bit] NOT NULL,
	[IS_DMY] [bit] NOT NULL,
	[IS_EMBR] [bit] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
