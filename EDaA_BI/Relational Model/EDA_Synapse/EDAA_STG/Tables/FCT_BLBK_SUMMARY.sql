

CREATE TABLE [EDAA_STG].[FCT_BLBK_SUMMARY]
(
	[Dt_Sk] [int] NOT NULL,
	[Str_Id] [int] NOT NULL,
	[Itm_Sku] [decimal](18, 0) NOT NULL,
	[CostOfGoodsReductionAmount] [decimal](15, 4) NOT NULL,
	[Bat_Prcs_Tms] [datetime2](7) NOT NULL,
	[Bat_Prcs_Id] [varchar](10) NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
