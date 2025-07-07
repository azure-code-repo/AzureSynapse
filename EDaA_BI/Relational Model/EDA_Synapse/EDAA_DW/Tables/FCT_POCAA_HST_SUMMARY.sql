CREATE TABLE [EDAA_DW].[FCT_POCAA_HST_SUMMARY]
(
	[Dt_Sk] [int] NOT NULL,
	[Geo_Hist_Sk] [int] NOT NULL,
	[Prod_Hist_Sk] [decimal](18, 0) NOT NULL,
	[SwellAndDefectiveAlw] [decimal](22, 5) NOT NULL,
	[Bat_Prcs_Tms] [datetime2](7) NOT NULL,
	[Bat_Prcs_Id] [varchar](11) NOT NULL,
	[Aud_Ins_Sk] [int] NOT NULL,
	[Aud_Upd_Sk] [int] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
