/****** Object:  Table [dm_edw].[DIM_P_L3_tmp]    Script Date: 9/9/2020 1:47:44 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dm_edw].[DIM_P_L3_tmp]
(
	[P_HST_SKY] [int] NULL,
	[P_SKY] [int] NULL,
	[P_SUB_CT_ID] [nvarchar](256) NULL,
	[P_SUB_CT_DSC] [nvarchar](256) NULL,
	[P_CT_ID] [nvarchar](256) NULL,
	[P_CT_DSC] [nvarchar](256) NULL,
	[BUS_SEG_ID] [nvarchar](256) NULL,
	[BUS_SEG_DSC] [nvarchar](256) NULL,
	[MDS_ARE_ID] [nvarchar](256) NULL,
	[MDS_ARE_DSC] [nvarchar](256) NULL,
	[FNC_MPRS_CT_ID] [int] NULL,
	[FNC_MPRS_CT_DSC] [nvarchar](256) NULL,
	[FNC_PKY_ID] [int] NULL,
	[FNC_PKY_DSC] [nvarchar](256) NULL,
	[FNC_ARE_ID] [nvarchar](256) NULL,
	[FNC_ARE_DSC] [nvarchar](256) NULL,
	[VLD_FROM] [date] NULL,
	[VLD_TO] [date] NULL,
	[IS_CURR] [int] NULL,
	[IS_DMY] [int] NULL,
	[IS_EMBR] [int] NULL,
	[Hash] [nvarchar](256) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
