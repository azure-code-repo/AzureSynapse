CREATE TABLE [EDAA_DW].[FCT_PROMO_ADV_BLK]
(
	[Promo_Adv_Blk_Id] [int] NOT NULL,
	[Promo_Adv_Sn_Vhl_Id] [int] NOT NULL,
	[Promo_Vhl_Id] [int] NULL,
	[Promo_Adv_Pg_Id] [int] NULL,
	[Promo_Adv_Blk_Nbr_Id] [int] NULL,
	[Promo_Adv_Blk_Strt_Dt] [date] NULL,
	[Promo_Adv_Blk_End_Dt] [date] NULL,
	[Promo_Adv_Blk_Nm] [nvarchar](4000) NULL,
	[Promo_Adv_Blk_Dsc] [nvarchar](4000) NULL,
	[Promo_Adv_Sz_Qty] [decimal](5, 2) NULL,
	[Promo_Adv_Asgn_Byr_Id] [int] NULL,
	[Promo_Adv_Blk_Thm] [nvarchar](4000) NULL,
	[Promo_Adv_Blk_Sts_Ct] [int] NULL,
	[Promo_Adv_Prop_Vhl_Ct] [nvarchar](4000) NULL,
	[Promo_Adv_Prop_Pg_Id] [int] NULL,
	[Promo_Adv_As_Buy_At_Least_Ofr_Ind] [nvarchar](4000) NULL,
	[UpdatedDate] [datetime2](7) NULL,

	CONSTRAINT [PK_FCT_PROMO_ADV_BLK] PRIMARY KEY NONCLUSTERED
	(
		[Promo_Adv_Blk_Id] ASC,
		[Promo_Adv_Sn_Vhl_Id] ASC
	) NOT ENFORCED
)
WITH
  (
    DISTRIBUTION = HASH ( Promo_Adv_Sn_Vhl_Id ),
    CLUSTERED COLUMNSTORE INDEX
  )
