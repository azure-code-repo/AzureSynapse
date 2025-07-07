/*
New Table Created for Data Driven Alert
*/

CREATE TABLE [dm_edw].[UT_DLY_SL_STS]
(
	[UT_ID] [int] NULL,
	[PHM_FLG] [nvarchar](1024) NULL,
	[TM_SKY_PRELIM_SL] [int] NULL,
	[FLG_LOADED] [nvarchar](1024) NULL,
	[LAST_REFRESHED] [datetime2](7) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
