/****** Object:  Table [EDAA_PRES].[FCT_PRODUCT_MPRS_COSTRCV_TY_LY_FSC_42DAYS]    Script Date: 9/17/2022 12:19:54 PM ******/

CREATE TABLE [EDAA_PRES].[FCT_PRODUCT_MPRS_COSTRCV_TY_LY_FSC_42DAYS]
(
	[ROW_ID_VAL] [int] NOT NULL,
	[CLN_TYP_ID] [smallint] NOT NULL,
	[PRE_DEF_ID] [int] NOT NULL,
	[DT_VAL] [date] NOT NULL,
	[Geo_Hist_Sk] [int] NOT NULL,
	[Prod_Hist_Sk] [int] NOT NULL,
	[Itm_Sku] [decimal](18, 0) NOT NULL,
	[Mjr_Pkg_Ut_Qty_TY_FSC] [decimal](24, 4) NULL,
	[Mjr_Pkg_Ut_Qty_LY_FSC] [decimal](24, 4) NULL,
	[Itm_Grss_Cst_Amt_TY_FSC] [decimal](12, 5) NULL,
	[Itm_Grss_Cst_Amt_LY_FSC] [decimal](12, 5) NULL,
	[Itm_Net_Cst_Amt_TY_FSC] [decimal](12, 5) NULL,
	[Itm_Net_Cst_Amt_LY_FSC] [decimal](12, 5) NULL,
	[Itm_Chrg_Ea_Amt_TY_FSC] [decimal](12, 5) NULL,
	[Itm_Chrg_Ea_Amt_LY_FSC] [decimal](12, 5) NULL,
	[Itm_Alw_Ea_Amt_TY_FSC] [decimal](12, 5) NULL,
	[Itm_Alw_Ea_Amt_LY_FSC] [decimal](12, 5) NULL,
	[Po_Chrg_Ea_Amt_TY_FSC] [decimal](12, 5) NULL,
	[Po_Chrg_Ea_Amt_LY_FSC] [decimal](12, 5) NULL,
	[Po_Alw_Ea_Amt_TY_FSC] [decimal](12, 5) NULL,
	[Po_Alw_Ea_Amt_LY_FSC] [decimal](12, 5) NULL,
	[Csh_Dct_Ea_Amt_TY_FSC] [decimal](12, 5) NULL,
	[Csh_Dct_Ea_Amt_LY_FSC] [decimal](12, 5) NULL,
	[Frt_Chrg_Ea_Amt_TY_FSC] [decimal](12, 5) NULL,
	[Frt_Chrg_Ea_Amt_LY_FSC] [decimal](12, 5) NULL,
	[Frt_Alw_Ea_Amt_TY_FSC] [decimal](12, 5) NULL,
	[Frt_Alw_Ea_Amt_LY_FSC] [decimal](12, 5) NULL,
	[Frt_Unld_Chrg_Amt_TY_FSC] [decimal](9, 6) NULL,
	[Frt_Unld_Chrg_Amt_LY_FSC] [decimal](9, 6) NULL,
	[Ipt_Ld_Chrg_Amt_TY_FSC] [decimal](9, 4) NULL,
	[Ipt_Ld_Chrg_Amt_LY_FSC] [decimal](9, 4) NULL,
	[Bkh_Frt_Chrg_Amt_TY_FSC] [decimal](10, 6) NULL,
	[Bkh_Frt_Chrg_Amt_LY_FSC] [decimal](10, 6) NULL,
	[Extd_Bkh_Frt_Chrg_Amt_TY_FSC] [decimal](11, 4) NULL,
	[Extd_Bkh_Frt_Chrg_Amt_LY_FSC] [decimal](11, 4) NULL,
	[CostOfGoodsReductionAmount_TY_FSC] [decimal](15, 4) NULL,
	[CostOfGoodsReductionAmount_LY_FSC] [decimal](15, 4) NULL,
	[SwellAndDefectiveAlw_TY_FSC] [decimal](22, 5) NULL,
	[SwellAndDefectiveAlw_LY_FSC] [decimal](22, 5) NULL,
	[Create_Date] [datetime] NULL,
	[Update_Date] [datetime] NULL
)
WITH
(
	DISTRIBUTION = HASH ( [Itm_Sku] ),
	CLUSTERED COLUMNSTORE INDEX
)
