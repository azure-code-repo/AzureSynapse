/****** Object:  Table [dm_edw].[DIM_P_TMP]    Script Date: 9/9/2020 1:45:39 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dm_edw].[DIM_P_TMP]
(
	[P_HST_SKY] [bigint] NOT NULL,
	[P_SKY] [bigint] NOT NULL,
	[UPC_ID] [varchar](100) NOT NULL,
	[UPC_DSC] [varchar](500) NOT NULL,
	[P_ID] [varchar](10) NOT NULL,
	[P_DSC] [varchar](80) NOT NULL,
	[P_CLS_ID] [varchar](10) NOT NULL,
	[P_CLS_DSC] [varchar](80) NOT NULL,
	[P_SUB_CT_ID] [varchar](10) NOT NULL,
	[P_SUB_CT_DSC] [varchar](80) NOT NULL,
	[P_CT_ID] [varchar](10) NOT NULL,
	[P_CT_DSC] [varchar](80) NOT NULL,
	[BUS_SEG_ID] [varchar](10) NOT NULL,
	[BUS_SEG_DSC] [varchar](80) NOT NULL,
	[MDS_ARE_ID] [varchar](10) NOT NULL,
	[MDS_ARE_DSC] [varchar](80) NOT NULL,
	[FNC_P_CAT_ID] [varchar](10) NOT NULL,
	[FNC_P_CAT_DSC] [varchar](80) NOT NULL,
	[FNC_MPRS_CT_ID] [smallint] NOT NULL,
	[FNC_MPRS_CT_DSC] [varchar](80) NOT NULL,
	[FNC_PKY_ID] [smallint] NOT NULL,
	[FNC_PKY_DSC] [varchar](80) NOT NULL,
	[FNC_ARE_ID] [varchar](15) NOT NULL,
	[FNC_ARE_DSC] [varchar](80) NOT NULL,
	[VLD_FROM] [datetime] NOT NULL,
	[VLD_TO] [datetime] NULL,
	[IS_CURR] [bit] NULL,
	[IS_DMY] [bit] NULL,
	[IS_EMBR] [bit] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
