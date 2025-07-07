/****** Object:  Table [stg].[WK_DC_MPRS_CT_HST_INV]    Script Date: 9/9/2020 3:30:10 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stg].[WK_DC_MPRS_CT_HST_INV]
(
	[PKY_ID] [nvarchar](4000) NULL,
	[UT_ID] [nvarchar](4000) NULL,
	[FILLER] [nvarchar](4000) NULL,
	[MPRS_CT_ID] [nvarchar](4000) NULL,
	[WK_END_DT] [nvarchar](4000) NULL,
	[TG_INV_CS_QT] [nvarchar](4000) NULL,
	[TG_CLRN_INV_CS_QT] [nvarchar](4000) NULL,
	[TG_INV_CST_AM] [nvarchar](4000) NULL,
	[TG_CLRN_INV_CST_AM] [nvarchar](4000) NULL,
	[TG_FNC_CHRG_AM] [nvarchar](4000) NULL,
	[TG_CLRN_FNC_CHRG_AM] [nvarchar](4000) NULL,
	[TG_FNC_CR_AM] [nvarchar](4000) NULL,
	[TG_CLRN_FNC_CR_AM] [nvarchar](4000) NULL,
	[TG_STG_CHRG_AM] [nvarchar](4000) NULL,
	[TG_CLRN_STG_CHRG_AM] [nvarchar](4000) NULL,
	[TG_INV_QT] [nvarchar](4000) NULL,
	[TG_CLRN_INV_QT] [nvarchar](4000) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
