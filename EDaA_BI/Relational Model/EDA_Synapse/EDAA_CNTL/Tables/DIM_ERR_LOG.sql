/****** Object:  Table [EDAA_CNTL].[DIM_ERR_LOG]    Script Date: 5/5/2023 2:29:11 PM ******/

CREATE TABLE [EDAA_CNTL].[DIM_ERR_LOG]
(
	[EXEC_JOB] [varchar](500) NULL,
	[ERR_PROC_NM] [varchar](500) NULL,
	[ERR_LN] [int] NULL,
	[ERR_MSG] [nvarchar](max) NULL,
	[ERR_LOG_TS] [datetime] NULL
)
WITH
(
	DISTRIBUTION = HASH ( [ERR_PROC_NM] ),
	HEAP
)
