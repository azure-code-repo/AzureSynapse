CREATE TABLE [EDAA_DW].[FCT_WKND_PKY_MPRK_ADJ_ARCHIVE]
(
	[Fnc_Lvl3_Pky_Id] [varchar](15) NOT NULL,
	[Dt_Sk] [int] NOT NULL,
	[Mprk_Adj_Prcntg] [decimal](11, 8) NULL,
	[Aud_Ins_Sk] [bigint] NULL,
	[Aud_Upd_Sk] [bigint] NULL,
	[Row_Archv_ts] [datetime] NOT NULL
)
WITH
(
	DISTRIBUTION = HASH ( [Fnc_Lvl3_Pky_Id] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO
