/****** Object:  Table [stg_edw].[DLY_UT_P_UPC_PR_HST]    Script Date: 9/9/2020 3:17:06 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stg_edw].[DLY_UT_P_UPC_PR_HST]
(
	[UT_ID] [smallint] NOT NULL,
	[P_UPC_ID] [decimal](18, 0) NOT NULL,
	[DAY_DT] [date] NOT NULL,
	[RGL_PR_AM] [decimal](11, 2) NULL,
	[PROMO_PR_AM] [decimal](11, 2) NULL,
	[CLRN_PR_AM] [decimal](11, 2) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
