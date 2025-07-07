
CREATE TABLE [EDAA_STG].[FCT_DW_STG_HRLY_SLS_PLN]
(
	[Dt_Tm_hr] [datetime2](0) NOT NULL,
	[Lvl3_Prod_Sub_Ctgry_Id] [varchar](10) NOT NULL,
	[Str_Id] [int] NOT NULL,
	[Pln_Sls_Amt] [decimal](16, 2) NULL,
	[Aud_Ins_Sk] [int] NULL,
	[Aud_Upd_Sk] [int] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
