CREATE TABLE [EDAA_DW].[FCT_RWB_UT_P_FCST_HST]
(
	[Promo_Adv_Blk_Id] [int] NULL,
	[Promo_Vhl_P_Grp_Id] [int] NOT NULL,
	[Promo_Vhl_P_Sub_Grp_Id] [int] NOT NULL,
	[Geo_Hist_Sk] [int] NULL,
	[Dt_Sk] [int] NULL,
	[P_ID] [decimal](18, 0) NULL,
	[UT_ID] [int] NULL,
	[Rwb_Blk_Min_P_Ut_Strt_Dt] [date] NULL,
	[Rwb_Blk_Max_P_Ut_End_Dt] [date] NULL,
	[Rwb_Fcst_T_Sl_Qty] [decimal](9, 2) NULL,
	[Rwb_Fcst_Bsln_Sl_Qty] [decimal](9, 2) NULL,
	[Rwb_Fcst_T_Sl_Amt] [decimal](14, 4) NULL,
	[Rwb_Fcst_T_Dmgn_Amt] [decimal](14, 4) NULL,
	[Rwb_Avg_Rtl_Amt] [decimal](9, 2) NULL,
	[Rgl_Avg_Cst_Amt] [decimal](9, 2) NULL,
	[Rgl_Avg_Rtl_Pr_Amt] [decimal](9, 2) NULL,
	[Promo_Byr_Sts_Ct] [varchar](1) NULL,
	[Promo_Rplnm_Sts_Ct] [varchar](1) NULL,
	[Promo_Rplnm_Appr_Ind] [varchar](1) NULL,
	[Promo_Rplnm_Snt_Lift_Manu_Ind] [int] NULL,
	[Promo_Rplnm_Ovlp_Ind] [int] NULL,
	[Promo_Rplnm_Extrl_Promo_Buf_Ind] [int] NULL,
	[Promo_Rplnm_Sbmt_Qty] [decimal](11, 2) NULL,
 CONSTRAINT [PK_FCT_RWB_UT_P_FCST_HST] PRIMARY KEY NONCLUSTERED
	(
		[Promo_Vhl_P_Grp_Id] ASC,
		[Promo_Vhl_P_Sub_Grp_Id] ASC
	) NOT ENFORCED
)
WITH
(
	DISTRIBUTION = HASH ( [P_ID] ),
	CLUSTERED COLUMNSTORE INDEX
)
