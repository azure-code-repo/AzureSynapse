CREATE TABLE [EDAA_DW].[FCT_BLBK_SUMMARY]
(
	[Dt_Sk] [int] NOT NULL,
	[Geo_Hist_Sk] [int] NOT NULL,
	[Prod_Hist_Sk] [decimal](18, 0) NOT NULL,
	[CostOfGoodsReductionAmount] [decimal](15, 4) NOT NULL,
	[Bat_Prcs_Tms] [datetime2](7) NOT NULL,
	[Bat_Prcs_Id] [varchar](10) NOT NULL,
	[Aud_Ins_Sk] [int] NOT NULL,
	[Aud_Upd_Sk] [int] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
