CREATE TABLE [EDAA_PRES].[ADVANCED_ANALYTICS_OSA_PREDICTIONS_HIST]
(
	[day_dt] [date] NOT NULL,
	[ut_id] [int] NOT NULL,
	[p_id] [varchar](8) NOT NULL,
	[p_nm] [varchar](100) NULL,
	[upc_id] [decimal](18,0) NULL,
	[boh] [real] NULL,
	[alert_type] [varchar](50) NULL,
	[fcst] [real] NULL,
	[it_loc_asl_id] [real] NULL,
	[it_loc_sctn_id] [real] NULL,
	[it_loc_pstn_id] [real] NULL,
	[promo_flg] [varchar](50) NULL,
	[oos_hits] [real] NULL,
	[dsd_flag] [real] NULL,
	[alert_table_creation_ts] [datetime] NULL
)
WITH
(
	DISTRIBUTION = HASH ( [upc_id] ),
	CLUSTERED COLUMNSTORE INDEX
)
