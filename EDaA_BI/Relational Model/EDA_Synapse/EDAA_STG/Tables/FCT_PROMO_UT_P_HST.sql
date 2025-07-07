CREATE TABLE [EDAA_STG].[FCT_PROMO_UT_P_HST]
(
	[UpdatedDate] [datetime2](7) NULL,
	[UpdatedBy] [nvarchar](4000) NULL,
	[ProductItemId] [decimal](18, 0) NULL,
	[StoreId] [int] NULL,
	[PromotionAdvertisementBlockId] [int] NULL,
	[PromotionVehicleProductGroupId] [int] NULL,
	[PromotionVehicleProductSubGroupId] [int] NULL,
	[PromotionAdvertisementBlockStoreStartDate] [date] NULL,
	[PromotionAdvertisementBlockStoreEndDate] [date] NULL,
	[PromotionTotalSalesQuantity] [decimal](9, 2) NULL,
	[PromotionTotalSalesAmount] [decimal](14, 4) NULL,
	[PromotionTotalDirectMarginAmount] [decimal](14, 4) NULL,
	[PromotionAverageRetailAmount] [decimal](9, 2) NULL,
	[AverageRegisterRetailAmount] [decimal](9, 2) NULL,
	[AverageCostAmount] [decimal](9, 2) NULL,
	[ReplenishmentWorkbenchForecastTotalSalesQuantity] [decimal](14, 2) NULL,
	[ReplenishmentWorkbenchForecastBaselineSalesQuantity] [decimal](14, 2) NULL,
	[TotalAggregateLostSalesQuantity] [decimal](9, 2) NULL,
	[TotalAggregateBeginInventoryQuantity] [int] NULL,
	[TotalAggregateEndInventoryQuantity] [int] NULL,
	[IncrementalTotalSalesQuantity] [decimal](9, 2) NULL,
	[IncrementalTotalSalesAmount] [decimal](14, 4) NULL,
	[IncrementalTotalDirectMarginAmount] [decimal](14, 4) NULL,
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
