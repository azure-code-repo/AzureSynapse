/*
New Table Created for Data Driven Alert
*/

CREATE TABLE [dm_edw].[UT_DLY_SL_STS_ALRT]
(
	[SALESDT] [int] NULL,
	[TOTUT] [int] NULL,
	[TOTPHRM] [int] NULL,
	[UTSALES] [int] NULL,
	[PHRMSALES] [int] NULL,
	[STRUTNO] [varchar](2000) NULL,
	[PHRMUTNO] [varchar](2000) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	HEAP
)
