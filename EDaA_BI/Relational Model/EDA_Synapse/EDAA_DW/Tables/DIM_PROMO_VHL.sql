CREATE TABLE [EDAA_DW].[DIM_PROMO_VHL]
(
	[Promo_Vhl_Id] [int] NOT NULL,
	[Promo_Vhl_Ct] [varchar](5) NOT NULL,
	[Promo_Vhl_Ct_Dsc] [varchar](50) NULL,
	[Promo_Vhl_Struc_Id] [int] NULL,
	[Promo_Vhl_Prt_Ind] [char](1) NULL,
 CONSTRAINT [PK_DIM_VEHICLE_PROMOTION] PRIMARY KEY NONCLUSTERED
	(
		[Promo_Vhl_Id] ASC,
		[Promo_Vhl_Ct] ASC
	) NOT ENFORCED
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
