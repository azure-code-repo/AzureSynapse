CREATE TABLE [EDAA_STG].[CUST_FLYBUY]
(

	[CRBSD_PCK_ID] [int] NOT NULL,
	[ORDR_ID] [nvarchar](25) NOT NULL,
	[CRBSD_PCK_PARTY_ID] [int] NULL,
	[STR_ID] [int] NULL,
	[APL_PLTFRM_ID] [int] NULL,
	[LOC_ATH_ID] [int] NULL,
	[CRBSD_PCK_PARTY_NM] [nvarchar](129) NULL,
	[PKP_CMT_TMS] [nvarchar](100) NULL,
	[PKP_CMT_TM_ZN_TMS] [nvarchar](100) NULL,
	[CUST_RDM_TMS] [datetime] NULL,
	[CUST_RDM_TM_ZN_TMS] [datetime] NULL,
	[PKP_STRT_TMS] [datetime] NULL,
	[PKP_STRT_TM_ZN_TMS] [datetime] NULL,
	[PKP_FFL_TMS] [datetime] NULL,
	[PKP_FFL_TM_ZN_TMS] [datetime] NULL,
	[CUST_TRP_DIST_METR] [int] NULL,
	[CUST_WT_TM] [int] NULL,
	[UPD_DT] [datetime] NULL,
	[UPD_BY_FLG] [nvarchar](2) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
