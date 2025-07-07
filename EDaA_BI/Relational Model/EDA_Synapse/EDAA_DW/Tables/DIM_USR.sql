CREATE TABLE [EDAA_DW].[DIM_USR]
(
	[Usr_Id] [nvarchar](4000) NULL,
	[Str_Id] [int] NULL,
	[Usr_Rl_Nm] [nvarchar](4000) NULL,
	[Mkt_Id] [int] NULL,
	[Rgn_Id] [int] NULL,
	[Aud_Ins_Sk] [bigint] NULL,
	[Aud_Upd_Sk] [bigint] NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	CLUSTERED COLUMNSTORE INDEX
)
GO
