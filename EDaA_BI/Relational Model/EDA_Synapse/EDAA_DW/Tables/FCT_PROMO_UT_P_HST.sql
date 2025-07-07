CREATE TABLE [EDAA_DW].[FCT_PROMO_UT_P_HST]
(
	[ProductItemId] [decimal](18, 0) NULL,
	[StoreId] [int] NULL,
	[Prod_Hist_Sk] [bigint] NULL,
	[Geo_Hist_Sk] [bigint] NULL,
	[Promo_Adv_Blk_Id] [int] NULL,
	[Promo_P_Grp_Id] [int] NULL,
	[Promo_P_Sub_Grp_Id] [int] NULL,
	[Promo_Adv_Blk_Str_Strt_Dt] [date] NULL,
	[Dt_Sk] [int] NULL,
	[Promo_Adv_Blk_Str_End_Dt] [date] NULL,
	[Promo_T_Sl_Qty] [decimal](9, 2) NULL,
	[Promo_T_Sl_Amt] [decimal](14, 4) NULL,
	[Promo_T_Dmgn_Amt] [decimal](14, 4) NULL,
	[Promo_Avg_Rtl_Amt] [decimal](9, 2) NULL,
	[Avg_Reg_Rtl_Amt] [decimal](9, 2) NULL,
	[Avg_Cst_Amt] [decimal](9, 2) NULL,
	[Rwb_Fcst_T_Sl_Qty] [decimal](14, 2) NULL,
	[Rwb_Fcst_Bsln_Sl_Qty] [decimal](14, 2) NULL,
	[Tg_Lost_Sl_Qty] [decimal](9, 2) NULL,
	[Tg_Begn_Inv_Qty] [int] NULL,
	[Tg_End_Inv_Qty] [int] NULL,
	[Incr_T_Sl_Qty] [decimal](9, 2) NULL,
	[Incr_T_Sl_Amt] [decimal](14, 4) NULL,
	[Incr_T_Dmgn_Amt] [decimal](14, 4) NULL,
	[Tg_Pts_Sl_Qty] [int] NULL,
	[Tg_Pts_Sl_Amt] [decimal](9, 2) NULL,
	[Tg_Pts_Dmgn_Amt] [decimal](9, 2) NULL,
	[Tg_Clrn_Sl_Qty] [int] NULL,
	[Tg_Clrn_Sl_Amt] [decimal](9, 2) NULL,
	[Tg_Clrn_Dmgn_Amt] [decimal](9, 2) NULL,
	[UpdatedDate] [datetime2](7) NULL
)
WITH
  (
    DISTRIBUTION = HASH ( ProductItemId ),
    CLUSTERED COLUMNSTORE INDEX
  )
