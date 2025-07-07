CREATE TABLE [EDAA_STG].[FCT_DSTB_CTR_INV_QTY]
(
	[StoreId] [int] NULL,
	[ProductId] [int] NULL,
	[DistributionCenterDirectStoreDeliveryCategory] [nvarchar](4000) NULL,
	[DistributionCenterPalletCategory] [int] NULL,
	[DistributionCenterMaximumTierStackQuantity] [int] NULL,
	[DistributionCenterMaximumTierQuantity] [int] NULL,
	[DistributionCenterFinanceChargeAmount] [decimal](21, 4) NULL,
	[DistributionCenterFinanceCreditAmount] [decimal](21, 4) NULL,
	[DistributionCenterStorageChargeAmount] [decimal](21, 4) NULL,
	[EstimatedDeliveryCostAmount] [decimal](21, 4) NULL,
	[TransferQuantity] [bigint] NULL,
	[WeeklyOutofStockQuantity] [bigint] NULL,
	[WeeklyOutofStockSubstituteQuantity] [bigint] NULL,
	[WeeklyOverrideSubstituteQuantity] [bigint] NULL,
	[WeeklyAdjustedQuantity] [bigint] NULL,
	[WeeklyDistributionCenterMovementQuantity] [bigint] NULL,
	[WeeklyMovementCostAmount] [decimal](19, 2) NULL,
	[WeeklyMovementRetailAmount] [decimal](19, 2) NULL,
	[WeeklyReceivedQuantity] [bigint] NULL,
	[MeijerPackQuantity] [bigint] NULL,
	[DistributionCenterBalanceOnHandQuantity] [bigint] NULL,
	[RetailServiceCenterBalanceOnHandQuantity] [bigint] NULL,
	[DistributionCenterCostAmount] [decimal](19, 4) NULL,
	[MeijerPackCubeQuantity] [decimal](19, 4) NULL,
	[UpdatedTimestamp] [datetime2](7) NULL,
	[UpdatedBy] [nvarchar](4000) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
