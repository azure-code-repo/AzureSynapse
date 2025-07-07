CREATE TABLE [EDAA_STG].[ADV_ANALYT_OSA_PREDICT]
(
	[day_dt] [date] NULL,
	[ut_id] [int] NULL,
	[p_id] [nvarchar](256) NULL,
	[p_nm] [nvarchar](256) NULL,
	[upc_id] [float] NULL,
	[boh] [float] NULL,
	[alert_type] [nvarchar](256) NULL,
	[fcst] [float] NULL,
	[it_loc_asl_id] [float] NULL,
	[it_loc_sctn_id] [float] NULL,
	[it_loc_pstn_id] [float] NULL,
	[promo_flg] [nvarchar](256) NULL,
	[oos_hits] [float] NULL,
	[dsd_flag] [float] NULL,
	[alert_table_creation_ts] [datetime2](7) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
