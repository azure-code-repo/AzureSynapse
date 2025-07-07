/****** Object:  Table [EDAA_PRES].[BUCKET_METRICDESC]    Script Date: 9/28/2022 2:15:43 AM ******/

CREATE TABLE [EDAA_PRES].[BUCKET_METRICDESC]
(
	[Bucket] [int] NULL,
	[Metric_Type] [varchar](250) NULL,
	[Metric_Description] [varchar](1000) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
