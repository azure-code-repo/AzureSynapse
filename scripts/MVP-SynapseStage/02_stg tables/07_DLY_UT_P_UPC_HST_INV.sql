/****** Object:  Table [stg].[DLY_UT_P_UPC_HST_INV]    Script Date: 9/9/2020 3:27:14 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stg].[DLY_UT_P_UPC_HST_INV]
(
	[UT_ID] [int] NULL,
	[P_UPC_ID] [varchar](18) NULL,
	[DAY_DT] [varchar](8) NULL,
	[TG_INV_QT] [int] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
