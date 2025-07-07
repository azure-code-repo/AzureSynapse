CREATE TABLE [EDAA_STG].[FCT_PROMO_PROD_HIST]
(
	[UpdatedDate] [datetime2](7) NULL,
	[UpdatedBy] [nvarchar](4000) NULL,
	[ProductItemId] [decimal](18, 0) NULL,
	[PromotionAdvertisementBlockId] [int] NULL,
	[PromotionVehicleProductGroupId] [int] NULL,
	[PromotionVehicleProductSubGroupId] [int] NULL,
	[PromotionBlockMinimumStartDate] [date] NULL,
	[PromotionBlockMaximumEndDate] [date] NULL,
	[UnitQuantity] [int] NULL,
	[PromotionRainCheckId] [int] NULL,
	[PromotionTotalSalesQuantity] [decimal](9, 2) NULL,
	[PromotionTotalSalesAmount] [decimal](14, 4) NULL,
	[PromotionTotalDirectMarginAmount] [decimal](14, 4) NULL,
	[PromotionAverageRetailAmount] [decimal](9, 2) NULL,
	[PromotionMinimumRetailAmount] [decimal](9, 2) NULL,
	[PromotionMaximumRetailAmount] [decimal](9, 2) NULL,
	[AverageRegisterRetailAmount] [decimal](9, 2) NULL,
	[AverageCostAmount] [decimal](9, 2) NULL,
	[ReplenishmentWorkbenchForecastTotalSalesQuantity] [decimal](14, 2) NULL,
	[ReplenishmentWorkbenchForecastBaselineSalesQuantity] [decimal](14, 2) NULL,
	[TotalAggregateLostSalesQuantity] [decimal](9, 2) NULL,
	[TotalAggregateBeginInventoryQuantity] [int] NULL,
	[TotalAggregateEndInventoryQuantity] [int] NULL,
	[IncrementTotalSalesQuantity] [decimal](9, 2) NULL,
	[IncrementTotalSalesAmount] [decimal](14, 4) NULL,
	[IncrementTotalDirectMarginAmount] [decimal](14, 4) NULL,
	[TotalAggregatePermToSaleQuantity] [int] NULL,
	[TotalAggregatePermToSaleAmount] [decimal](9, 2) NULL,
	[TotalAggregatePermToSaleDirectMarginAmount] [decimal](9, 2) NULL,
	[TotalAggregateClearanceQuantity] [int] NULL,
	[TotalAggregateClearanceAmount] [decimal](9, 2) NULL,
	[TotalAggregateClearanceDirectMarginAmount] [decimal](9, 2) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
