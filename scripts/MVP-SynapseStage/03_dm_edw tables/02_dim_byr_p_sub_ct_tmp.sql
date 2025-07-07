/****** Object:  Table [dm_edw].[DIM_BYR_P_SUB_CT_TMP]    Script Date: 9/9/2020 1:13:47 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dm_edw].[DIM_BYR_P_SUB_CT_TMP]
(
	[BYR_HST_SKY] [int] NOT NULL,
	[BYR_SKY] [int] NOT NULL,
	[BYR_ID] [int] NULL,
	[BYR] [varchar](200) NULL,
	[P_SUB_CT_ID] [varchar](200) NULL,
	[P_SUB_CT_DSC] [varchar](200) NULL,
	[GVP] [varchar](200) NULL,
	[MGR] [varchar](200) NULL,
	[VP] [varchar](200) NULL,
	[VLD_FROM] [datetime] NULL,
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
