/****** Object:  Table [stg_edw].[WK_UT_MPRS_CT_INV_ADJ_HST]    Script Date: 9/9/2020 3:21:43 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stg_edw].[WK_UT_MPRS_CT_INV_ADJ_HST]
(
	[PKY_ID] [smallint] NOT NULL,
	[MPRS_CT_ID] [smallint] NOT NULL,
	[UT_ID] [smallint] NOT NULL,
	[WK_END_DT] [date] NOT NULL,
	[ADJ_TYP] [char](3) NOT NULL,
	[TN_TYP] [smallint] NULL,
	[TG_INV_QT] [int] NULL,
	[TG_INV_ADJ_QT] [int] NULL,
	[TG_INV_CST_AM] [decimal](13, 2) NULL,
	[TG_INV_T_INV_ADJ_AM] [decimal](13, 2) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
