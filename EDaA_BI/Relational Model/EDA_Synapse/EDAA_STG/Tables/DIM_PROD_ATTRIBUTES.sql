CREATE TABLE [EDAA_STG].[DIM_PROD_ATTRIBUTES]
(
	[Itm_sku] [decimal](18, 0) NOT NULL,
	[Itm_Prim_Ind] [char](1) NULL,
	[Prod_Size_Desc] [varchar](500) NULL,
	[Brnd_Nm] [varchar](250) NULL,
	[Brnd_Desc] [varchar](250) NULL,
	[Brnd_Ctgry] [varchar](250) NULL,
	[Prod_Desc] [varchar](500) NULL,
	[Prod_Desc_Note] [varchar](500) NULL,
	[Outdate] [date] NULL,
	[Byr_Id] [int] NULL,
	[Prod_Sts] [varchar](100) NULL,
	[Prod_Sts_Note] [varchar](500) NULL,
	[Vdr_Id] [int] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
);
