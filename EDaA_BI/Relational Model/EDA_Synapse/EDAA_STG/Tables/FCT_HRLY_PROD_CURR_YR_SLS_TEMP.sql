CREATE TABLE [EDAA_STG].[FCT_HRLY_PROD_CURR_YR_SLS_TEMP]
(
	[Itm_Sku] [varchar](150) NOT NULL,
	[Str_Id] [int] NOT NULL,
	[Day_dt] [date] NOT NULL,
	[Scn_Bsd_Trdng_Ind] [char](1) NOT NULL,
	[apd_upc_id] [varchar](150) NOT NULL,
	[Mprs_Src_Ctgry] [char](1) NULL,
	[Mprs_Prcs_Id] [char](1) NULL,
	[Str_Sls_Key_Ind] [smallint] NULL,
	[Fnc_Lvl3_Pky_Id] [smallint] NULL,
	[Prod_Drct_Mgn_Ind] [char](1) NULL,
	[Sls_Amt] [decimal](11, 2) NULL,
	[Sls_Qty] [decimal](9, 0) NULL,
	[Wgtd_Qty] [decimal](8, 2) NULL,
	[Prm_To_Sls_Amt] [decimal](11, 2) NULL,
	[Prm_To_Sls_Qty] [decimal](9, 0) NULL,
	[Prm_To_Sls_Wgtd_Qty] [decimal](8, 2) NULL,
	[Prm_To_Sls_Mkdn_Amt] [decimal](9, 2) NULL,
	[Prm_To_Clrnc_Amt] [decimal](11, 2) NULL,
	[Prm_To_Clrnc_Qty] [decimal](9, 0) NULL,
	[Prm_To_Clrnc_Wgtd_Qty] [decimal](8, 2) NULL,
	[Prm_To_Clrnc_Mkdn_Amt] [decimal](9, 2) NULL,
	[Cogs_Fnc_Chrg_Amt] [decimal](11, 4) NULL,
	[Cogs_Fnc_Cr_Amt] [decimal](11, 4) NULL,
	[Cogs_Strge_Chrg_Amt] [decimal](11, 4) NULL,
	[Cogs_Frt_Amt] [decimal](11, 4) NULL,
	[Cogs_Dist_Fclt_Hdl_Amt] [decimal](11, 4) NULL,
	[Str_Hdl_Mrkng_Amt] [decimal](11, 4) NULL,
	[Str_Hdl_Rcvng_Amt] [decimal](11, 4) NULL,
	[Str_Hdl_Chckng_Amt] [decimal](11, 4) NULL,
	[Str_Hdl_Stckng_Amt] [decimal](11, 4) NULL,
	[Prm_To_Prm_Drct_Mgn_Amt] [decimal](11, 4) NULL,
	[Prm_To_Sls_Drct_Mgn_Amt] [decimal](11, 4) NULL,
	[Prm_To_Clrnc_Drct_Mgn_Amt] [decimal](11, 4) NULL,
	[Mprs_Ctgry_Id] [smallint] NULL,
	[Edi_Prm_To_Prm_Appld_Qty] [decimal](9, 0) NULL,
	[Edi_Prm_To_Prm_Appld_Wgt_Qty] [decimal](8, 2) NULL,
	[Dgtl_Cst_Ctgry] [varchar](3) NULL,
	[Row_Updt_ts] [datetime] NULL,
	[Dt_Tm_Hr] [datetime] NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
