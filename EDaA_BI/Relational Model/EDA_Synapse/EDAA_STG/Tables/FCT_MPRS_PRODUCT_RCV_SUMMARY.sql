/****** Object:  Table [EDAA_STG].[FCT_MPRS_PRODUCT_RCV_SUMMARY]    Script Date: 7/12/2022 7:32:08 AM ******/


CREATE TABLE [EDAA_STG].[FCT_MPRS_PRODUCT_RCV_SUMMARY]
(
	[Itm_Sku] [decimal](18, 0) NOT NULL,
	[Str_Id] [int] NOT NULL,
	[Dt_Sk] [int] NOT NULL,
	[Strt_Dt] [date] NOT NULL,
	[End_Dt] [date] NOT NULL,
	[Day_Dt] [date] NOT NULL,
	[Is_Curr_Ind] [int] NOT NULL,
	[Mjr_Pkg_Ut_Qty] [decimal](24, 4) NOT NULL,
	[Itm_Grss_Cst_Amt] [decimal](12, 5) NOT NULL,
	[Itm_Net_Cst_Amt] [decimal](12, 5) NOT NULL,
	[Itm_Chrg_Ea_Amt] [decimal](12, 5) NOT NULL,
	[Itm_Alw_Ea_Amt] [decimal](12, 5) NOT NULL,
	[Po_Chrg_Ea_Amt] [decimal](12, 5) NOT NULL,
	[Po_Alw_Ea_Amt] [decimal](12, 5) NOT NULL,
	[Csh_Dct_Ea_Amt] [decimal](12, 5) NOT NULL,
	[Frt_Chrg_Ea_Amt] [decimal](12, 5) NOT NULL,
	[Frt_Alw_Ea_Amt] [decimal](12, 5) NOT NULL,
	[Frt_Unld_Chrg_Amt] [decimal](9, 6) NOT NULL,
	[Ipt_Ld_Chrg_Amt] [decimal](9, 4) NOT NULL,
	[Bkh_Frt_Chrg_Amt] [decimal](10, 6) NOT NULL,
	[Extd_Bkh_Frt_Chrg_Amt] [decimal](11, 4) NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
