/****** Object:  Table [stg].[DLY_UT_P_UPC_HST_ADJ]    Script Date: 9/9/2020 3:26:43 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [stg].[DLY_UT_P_UPC_HST_ADJ]
(
	[P_UPC_ID] [varchar](18) NULL,
	[UT_ID] [int] NULL,
	[MPRS_CT_ID] [varchar](20) NULL,
	[PKY_ID] [varchar](20) NULL,
	[DAY_DT] [varchar](8) NULL,
	[TG_SL_AM] [decimal](9, 2) NOT NULL,
	[TG_SL_QT] [int] NOT NULL,
	[TG_DMGN_AM] [decimal](9, 2) NULL,
	[TG_CGS_FNC_CR_AM] [decimal](9, 2) NULL,
	[TG_CGS_FNC_CHR_AM] [decimal](9, 2) NULL,
	[TG_CGS_WRHG_AM] [decimal](9, 2) NULL,
	[TG_CGS_TDC_HDL_AM] [decimal](9, 2) NULL,
	[TG_CGS_STO_HDL_AM] [decimal](9, 2) NULL,
	[TG_CGS_P_CST_AM] [decimal](9, 2) NULL,
	[TG_PTS_AM] [decimal](9, 2) NULL,
	[TG_PTS_QT] [int] NOT NULL,
	[TG_PTS_DMGN_AM] [decimal](9, 2) NOT NULL,
	[TG_PTS_MKDN_AM] [decimal](9, 2) NOT NULL,
	[TG_CLRN_AM] [decimal](9, 2) NOT NULL,
	[TG_CLRN_QT] [int] NOT NULL,
	[TG_CLRN_DMGN_AM] [decimal](9, 2) NULL,
	[TG_CLRN_MKDN_AM] [decimal](9, 2) NOT NULL,
	[TG_T_MKDN_AM] [decimal](9, 2) NOT NULL,
	[TG_SCN_QT] [int] NOT NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
GO
