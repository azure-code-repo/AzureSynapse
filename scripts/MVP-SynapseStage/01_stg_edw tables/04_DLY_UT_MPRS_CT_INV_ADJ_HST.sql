/****** Object:  Table [stg_edw].[DLY_UT_MPRS_CT_INV_ADJ_HST]    Script Date: 9/9/2020 3:10:09 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stg_edw].[DLY_UT_MPRS_CT_INV_ADJ_HST]
(
	[PKY_ID] [smallint] NOT NULL,
	[MPRS_CT_ID] [smallint] NOT NULL,
	[UT_ID] [smallint] NOT NULL,
	[DAY_DT] [date] NOT NULL,
	[ADJ_TYP] [char](3) NOT NULL,
	[TN_TYP] [smallint] NULL,
	[TG_INV_QT] [int] NOT NULL,
	[TG_INV_ADJ_QT] [int] NOT NULL,
	[TG_INV_CST_AM] [decimal](11, 2) NOT NULL,
	[TG_INV_T_INV_ADJ_AM] [decimal](11, 2) NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
