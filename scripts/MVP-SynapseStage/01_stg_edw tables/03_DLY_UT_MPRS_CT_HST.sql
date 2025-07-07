/****** Object:  Table [stg_edw].[DLY_UT_MPRS_CT_HST]    Script Date: 9/9/2020 3:09:11 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stg_edw].[DLY_UT_MPRS_CT_HST]
(
	[PKY_ID] [smallint] NOT NULL,
	[MPRS_CT_ID] [smallint] NOT NULL,
	[UT_ID] [smallint] NOT NULL,
	[DAY_DT] [date] NOT NULL,
	[TG_SL_AM] [decimal](11, 2) NOT NULL,
	[TG_SL_QT] [int] NOT NULL,
	[TG_DMGN_AM] [decimal](11, 2) NOT NULL,
	[TG_CGS_FNC_CR_AM] [decimal](9, 2) NOT NULL,
	[TG_CGS_FNC_CHR_AM] [decimal](9, 2) NOT NULL,
	[TG_CGS_WRHG_AM] [decimal](9, 2) NOT NULL,
	[TG_CGS_TDC_HDL_AM] [decimal](9, 2) NOT NULL,
	[TG_CGS_STO_HDL_AM] [decimal](9, 2) NOT NULL,
	[TG_CGS_P_CST_AM] [decimal](9, 2) NOT NULL,
	[TG_PTS_AM] [decimal](9, 2) NOT NULL,
	[TG_PTS_QT] [int] NOT NULL,
	[TG_PTS_DMGN_AM] [decimal](9, 2) NOT NULL,
	[TG_PTS_MKDN_AM] [decimal](9, 2) NOT NULL,
	[TG_CLRN_AM] [decimal](9, 2) NOT NULL,
	[TG_CLRN_QT] [int] NOT NULL,
	[TG_CLRN_DMGN_AM] [decimal](9, 2) NOT NULL,
	[TG_CLRN_MKDN_AM] [decimal](9, 2) NOT NULL,
	[TG_T_MKDN_AM] [decimal](9, 2) NOT NULL,
	[TG_SCN_QT] [int] NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
