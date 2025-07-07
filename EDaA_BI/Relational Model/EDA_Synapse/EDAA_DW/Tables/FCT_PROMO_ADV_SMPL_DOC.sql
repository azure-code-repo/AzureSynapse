CREATE TABLE [EDAA_DW].[FCT_PROMO_ADV_SMPL_DOC]
(
	[Promo_Adv_Blk_Id] [int] NOT NULL,
	[Promo_Adv_P_Grp_Id] [int] NOT NULL,
	[Promo_Adv_P_Sub_Grp_Id] [int] NOT NULL,
	[P_Item_Sku_Doc_Id] [decimal](18, 0) NOT NULL,
	[Promo_Adv_Doc_Trtmt_Id] [int] NULL,
	[Promo_Adv_Smpl_Src_Id] [int] NULL,
	[Promo_Adv_Cpn_Req_Ind] [nvarchar](4000) NULL,
	[Promo_Adv_Sgn_Req_Ind] [nvarchar](4000) NULL,
	[Promo_Adv_Ut_Avl_Qty] [int] NULL,
	[Promo_Adv_On_Clrn_Qty] [int] NULL,
	[UpdatedDate] [datetime2](7) NULL,
 CONSTRAINT [PK_FCT_PROMO_ADV_SMPL_DOC] PRIMARY KEY NONCLUSTERED
	(
		[Promo_Adv_Blk_Id] ASC,
		[Promo_Adv_P_Grp_Id] ASC,
		[Promo_Adv_P_Sub_Grp_Id] ASC,
		[P_Item_Sku_Doc_Id] ASC
	) NOT ENFORCED
)
WITH
  (
    DISTRIBUTION = HASH ( P_Item_Sku_Doc_Id ),
    CLUSTERED COLUMNSTORE INDEX
  )
