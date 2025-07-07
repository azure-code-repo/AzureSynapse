/****** Object:  Table [EDAA_PRES].[Metric_Type]    Script Date: 9/27/2022 11:06:47 AM ******/

CREATE TABLE [EDAA_PRES].[Metric_Type]
(
	[Metric_Type] [varchar](250) NOT NULL,
	[Sort_Order] [int] NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
