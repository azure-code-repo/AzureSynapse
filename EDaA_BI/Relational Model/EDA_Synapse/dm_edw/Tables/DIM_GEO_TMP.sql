CREATE TABLE [dm_edw].[DIM_GEO_TMP]
(
	[GEO_HST_SKY] [int] NULL,
	[GEO_SKY] [int] NULL,
	[UT_ID] [int] NULL,
	[UT_DSC] [nvarchar](256) NULL,
	[UNIT_SQF] [int] NULL,
	[RGN] [nvarchar](256) NULL,
	[OPR_MKT] [nvarchar](256) NULL,
	[UT_LNGT] [decimal](9, 6) NULL,
	[UT_LAT] [decimal](9, 6) NULL,
	[ST_CLS_ID] [int] NULL,
	[ST_CLS] [nvarchar](256) NULL,
	[ST] [nvarchar](256) NULL,
	[CNTY] [nvarchar](256) NULL,
	[CITY] [nvarchar](256) NULL,
	[ZIP] [nvarchar](256) NULL,
	[DIV_ID] [nvarchar](256) NULL,
	[DIV_NM] [nvarchar](256) NULL,
	[NW_STO_TY_FLG] [int] NULL,
	[NW_STO_LY_FLG] [int] NULL,
	[NW_STO_2LY_FLG] [int] NULL,
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
);
