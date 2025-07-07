CREATE TABLE [EDAA_STG].[DIM_PROMO_VHL]
(
	[UpdatedDate] [datetime2](7) NULL,
	[UpdatedBy] [nvarchar](4000) NULL,
	[PromotionVehicleId] [int] NULL,
	[PromotionVehicleCategory] [nvarchar](4000) NULL,
	[PromotionVehicleCategoryDescription] [nvarchar](4000) NULL,
	[PromotionVehicleStructureId] [int] NULL,
	[PromotionVehiclePrintIndicator] [nvarchar](4000) NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
