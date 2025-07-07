/****** Object:  Table [dm_edw].[DIM_GEO_TMP]    Script Date: 9/9/2020 1:43:00 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dm_edw].[DIM_GEO_TMP]
(
	[GEO_HST_SKY] [int] NOT NULL,
	[GEO_SKY] [int] NOT NULL,
	[UT_ID] [int] NOT NULL,
	[UT_DSC] [varchar](150) NOT NULL,
	[UNIT_SQF] [int] NULL,
	[RGN] [varchar](100) NOT NULL,
	[OPR_MKT] [varchar](35) NOT NULL,
	[UT_LNGT] [decimal](9, 6) NULL,
	[UT_LAT] [decimal](9, 6) NULL,
	[ST_CLS_ID] [smallint] NULL,
	[ST_CLS] [varchar](15) NULL,
	[ST] [varchar](3) NULL,
	[CNTY] [varchar](20) NULL,
	[CITY] [varchar](50) NULL,
	[ZIP] [varchar](5) NULL,
	[DIV_ID] [varchar](3) NOT NULL,
	[DIV_NM] [varchar](40) NOT NULL,
	[NW_STO_TY_FLG] [smallint] NULL,
	[NW_STO_LY_FLG] [smallint] NULL,
	[NW_STO_2LY_FLG] [smallint] NULL,
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
