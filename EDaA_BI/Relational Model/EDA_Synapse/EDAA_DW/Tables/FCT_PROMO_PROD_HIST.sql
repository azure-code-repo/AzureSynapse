CREATE TABLE [EDAA_DW].[FCT_PROMO_PROD_HIST]
(
	[Prod_Hist_Sk] [bigint] NULL,
	[ProductItemId] [decimal](18, 0) NULL,
	[Promo_Adv_Blk_Id] [int] NULL,
	[Promo_P_Grp_Id] [int] NULL,
	[Promo_P_Sub_Grp_Id] [int] NULL,
	[Dt_Sk] [int] NULL,
	[Promo_Blk_Min_Strt_Dt] [date] NULL,
	[Promo_Blk_Max_End_Dt] [date] NULL,
	[Ut_Qty] [int] NULL,
	[Promo_Rck_Id] [int] NULL,
	[Promo_T_Sl_Qty] [decimal](9, 2) NULL,
	[Promo_T_Sl_Amt] [decimal](14, 4) NULL,
	[Promo_T_Dmgn_Amt] [decimal](14, 4) NULL,
	[Promo_Avg_Rtl_Amt] [decimal](9, 2) NULL,
	[Promo_Min_Rtl_Amt] [decimal](9, 2) NULL,
	[Promo_Max_Rtl_Amt] [decimal](9, 2) NULL,
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
