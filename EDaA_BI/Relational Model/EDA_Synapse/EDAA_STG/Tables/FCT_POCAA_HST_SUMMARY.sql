CREATE TABLE [EDAA_STG].[FCT_POCAA_HST_SUMMARY]
(
	[Dt_Sk] [int] NOT NULL,
	[Str_Id] [int] NOT NULL,
	[Itm_Sku] [decimal](18, 0) NOT NULL,
	[SwellAndDefectiveAlw] [decimal](22, 5) NOT NULL,
	[Bat_Prcs_Tms] [datetime2](7) NOT NULL,
	[Bat_Prcs_Id] [varchar](11) NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
