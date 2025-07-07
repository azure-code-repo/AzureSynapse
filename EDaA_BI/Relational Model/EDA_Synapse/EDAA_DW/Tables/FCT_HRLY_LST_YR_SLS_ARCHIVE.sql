CREATE TABLE [EDAA_DW].[FCT_HRLY_LST_YR_SLS_ARCHIVE]
(
	[Dt_Tm_Hr] [datetime2](7) NULL,
	[Lvl3_Prod_Sub_Ctgry_Id] [nvarchar](4000) NULL,
	[Str_Id] [int] NULL,
	[Sls_Amt] [decimal](15, 4) NULL,
	[Drct_Mgn_Amt] [decimal](15, 4) NULL,
	[Sls_Qty] [int] NULL,
	[Prm_To_Sls_Amt] [decimal](15, 4) NULL,
	[Dt_Sk] [int] NOT NULL,
	[Geo_Hist_Sk] [int] NOT NULL,
	[Aud_Ins_Sk] [bigint] NULL,
	[Aud_Upd_Sk] [bigint] NULL,
	[Row_Archv_ts] [datetime] NOT NULL
)
WITH
(
	DISTRIBUTION = HASH ( [Dt_Sk] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO
