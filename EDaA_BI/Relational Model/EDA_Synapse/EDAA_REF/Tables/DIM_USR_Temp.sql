CREATE TABLE [EDAA_REF].[DIM_USR_Temp]
(
	[Usr_Id] [nvarchar](4000) NULL,
	[Str_Id] [int] NULL,
	[Usr_Rl_Nm] [nvarchar](4000) NULL,
	[Mkt_Id] [int] NULL,
	[Rgn_Id] [int] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
GO
