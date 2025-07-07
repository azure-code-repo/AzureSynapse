/****** Object:  Table [stg_edw].[WK_DC_MPRS_CT_HST]    Script Date: 9/9/2020 3:19:08 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stg_edw].[WK_DC_MPRS_CT_HST]
(
	[PKY_ID] [smallint] NOT NULL,
	[MPRS_CT_ID] [smallint] NOT NULL,
	[UT_ID] [smallint] NOT NULL,
	[WK_END_DT] [date] NOT NULL,
	[TG_INV_CST_AM] [decimal](11, 2) NOT NULL,
	[TG_INV_QT] [int] NOT NULL,
	[TG_CLRN_INV_CST_AM] [decimal](11, 2) NOT NULL,
	[TG_CLRN_INV_QT] [int] NOT NULL,
	[TG_FNC_CR_AM] [decimal](11, 4) NOT NULL,
	[TG_CLRN_FNC_CR_AM] [decimal](11, 4) NOT NULL,
	[TG_FNC_CHRG_AM] [decimal](11, 4) NOT NULL,
	[TG_CLRN_FNC_CHRG_AM] [decimal](11, 4) NOT NULL,
	[TG_STG_CHRG_AM] [decimal](11, 4) NOT NULL,
	[TG_CLRN_STG_CHRG_AM] [decimal](11, 4) NOT NULL,
	[TG_INV_CS_QT] [int] NOT NULL,
	[TG_CLRN_INV_CS_QT] [int] NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
