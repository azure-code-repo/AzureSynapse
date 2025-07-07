/****** Object:  Table [stg].[WK_UT_MPRS_CT_INV_ADJ_HST_ADJ]    Script Date: 9/9/2020 3:33:01 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stg].[WK_UT_MPRS_CT_INV_ADJ_HST_ADJ]
(
	[P_UPC_ID] [nvarchar](4000) NULL,
	[UT_ID] [smallint] NOT NULL,
	[MPRS_CT_ID] [smallint] NOT NULL,
	[PKY_ID] [smallint] NOT NULL,
	[WK_END_DT] [nvarchar](4000) NOT NULL,
	[TN_TYP] [smallint] NULL,
	[ADJ_TYP] [char](3) NOT NULL,
	[TG_INV_QT] [int] NULL,
	[TG_INV_ADJ_QT] [int] NULL,
	[TG_INV_CST_AM] [decimal](13, 2) NULL,
	[FILL_CC] [nvarchar](4000) NULL,
	[TG_INV_T_INV_ADJ_AM] [decimal](13, 2) NULL,
	[FILL_DD] [nvarchar](4000) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
