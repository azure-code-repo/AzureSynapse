/****** Object:  Table [stg_edw].[FNC_SLSPN_MPRS_UT_DAY_INF]    Script Date: 9/9/2020 3:18:24 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stg_edw].[FNC_SLSPN_MPRS_UT_DAY_INF]
(
	[MPRS_CT_ID] [smallint] NOT NULL,
	[PKY_ID] [smallint] NOT NULL,
	[UT_ID] [smallint] NOT NULL,
	[DAY_DT] [date] NOT NULL,
	[TG_SLSPN_AM] [decimal](13, 4) NULL,
	[TG_DMGN_PLN_AM] [decimal](13, 4) NULL,
	[TG_STL_PLN_AM] [decimal](16, 4) NULL,
	[TG_TWAW_PLN_AM] [decimal](16, 4) NULL,
	[TG_EXPD_LOSS_AM] [decimal](15, 4) NULL,
	[TG_UNEXPD_LOSS_AM] [decimal](15, 4) NULL,
	[TG_SLSPN_QT] [decimal](12, 2) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
