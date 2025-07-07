CREATE TABLE [EDAA_DW].[FCT_PROMO_BLK_P_SGRP_HST]
(
	[Promo_Adv_Blk_Id] [int] NOT NULL,
	[Promo_Vhl_P_Grp_Id] [int] NOT NULL,
	[Promo_Vhl_P_Sub_Grp_Id] [int] NOT NULL,
	[Promo_Vhl_P_Sub_Grp_Dsc] [nvarchar](4000) NULL,
	[Promo_Vhl_P_Sub_Grp_Nte] [nvarchar](4000) NULL,
	[Fcst_Vdr_Lump_Sum_Amt] [decimal](11, 4) NULL,
	[Fcst_Promo_Vdr_Alw_Amt] [decimal](11, 4) NULL,
	[Fcst_T_Sl_Qty] [decimal](9, 2) NULL,
	[Fcst_T_Sl_Amt] [decimal](14, 4) NULL,
	[Fcst_T_Dmgn_Amt] [decimal](14, 4) NULL,
	[Fcst_Avg_Cst_Amt] [decimal](9, 2) NULL,
	[Fcst_Avg_Sl_Qty] [decimal](9, 2) NULL,
	[Fcst_Ut_Qty] [int] NULL,
	[Fcst_Itm_Sku_Qty] [int] NULL,
	[Ut_Qty] [int] NULL,
	[Itm_Sku_Qty] [decimal](9, 2) NULL,
	[Promo_T_Sl_Qty] [decimal](9, 2) NULL,
	[Promo_T_Sl_Amt] [decimal](9, 2) NULL,
	[Promo_T_Dmgn_Amt] [decimal](9, 2) NULL,
	[Promo_Avg_Rtl_Amt] [decimal](9, 2) NULL,
	[Promo_Min_Rtl_Amt] [decimal](9, 2) NULL,
	[Promo_Max_Rtl_Amt] [decimal](9, 2) NULL,
	[Avg_Rgl_Rtl_Amt] [decimal](9, 2) NULL,
	[Avg_Cst_Amt] [decimal](9, 2) NULL,
	[Rwb_Fcst_T_Sl_Qty] [decimal](9, 2) NULL,
	[Rwb_Fcst_Bsln_Sl_Qty] [decimal](9, 2) NULL,
	[Tg_Lost_Sl_Qty] [decimal](9, 2) NULL,
	[Tg_Begn_Inv_Qty] [int] NULL,
	[Tg_End_Inv_Qty] [int] NULL,
	[Incr_T_Sl_Qty] [decimal](9, 2) NULL,
	[Incr_T_Sl_Amt] [decimal](14, 4) NULL,
	[Incr_T_Dmgn_Amt] [decimal](14, 4) NULL,
	[Tg_Pts_Sl_Qty] [int] NULL,
	[Tg_Pts_Sl_Amt] [decimal](14, 2) NULL,
	[Tg_Pts_Dmgn_Amt] [decimal](14, 2) NULL,
	[Tg_Clrn_Sl_Qty] [int] NULL,
	[Tg_Clrn_Sl_Amt] [decimal](9, 2) NULL,
	[Tg_Clrn_Dmgn_Amt] [decimal](9, 2) NULL,
 CONSTRAINT [PK_FCT_PROMO_BLK_P_SGRP_HST] PRIMARY KEY NONCLUSTERED
	(
		[Promo_Adv_Blk_Id] ASC,
		[Promo_Vhl_P_Grp_Id] ASC,
		[Promo_Vhl_P_Sub_Grp_Id] ASC
	) NOT ENFORCED
)
WITH
(
	DISTRIBUTION = HASH ( [Promo_Vhl_P_Sub_Grp_Id] ),
	CLUSTERED COLUMNSTORE INDEX
)
