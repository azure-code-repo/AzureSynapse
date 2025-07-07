/****** Object:  Table [stg].[WK_DC_MPRS_CT_HST_ADJ]    Script Date: 9/9/2020 3:29:37 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stg].[WK_DC_MPRS_CT_HST_ADJ]
(
	[PKY_ID] [smallint] NULL,
	[UT_ID] [smallint] NULL,
	[FILLER] [varchar](20) NULL,
	[MPRS_CT_ID] [smallint] NULL,
	[WK_END_DT] [varchar](30) NULL,
	[TG_INV_QT] [int] NULL,
	[TG_CLRN_INV_QT] [int] NULL,
	[TG_INV_CST_AM] [decimal](13, 2) NULL,
	[TG_CLRN_INV_CST_AM] [decimal](13, 2) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
