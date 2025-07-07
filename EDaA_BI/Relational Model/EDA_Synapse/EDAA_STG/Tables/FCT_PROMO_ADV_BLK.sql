CREATE TABLE [EDAA_STG].[FCT_PROMO_ADV_BLK]
(
	[UpdatedDate] [datetime2](7) NULL,
	[UpdatedBy] [nvarchar](4000) NULL,
	[PromotionAdvertisementBlockId] [int] NULL,
	[PromotionAdvertisementSeasonVehicleId] [int] NULL,
	[PromotionAdvertisementPageId] [int] NULL,
	[PromotionAdvertisementBlockNumberId] [int] NULL,
	[PromotionAdvertisementBlockStartDate] [date] NULL,
	[PromotionAdvertisementBlockEndDate] [date] NULL,
	[PromotionAdvertisementBlockName] [nvarchar](4000) NULL,
	[PromotionAdvertisementBlockDescription] [nvarchar](4000) NULL,
	[PromotionAdvertisementSizeQuantity] [decimal](5, 2) NULL,
	[PromotionAdvertisementAssignBuyerId] [int] NULL,
	[PromotionAdvertisementBlockTheme] [nvarchar](4000) NULL,
	[PromotionAdvertisementBlockStatusCategory] [int] NULL,
	[PromotionAdvertisementProposedVehicleCategory] [nvarchar](4000) NULL,
	[PromotionAdvertisementProposedPageId] [int] NULL,
	[PromotionAdvertisementAsBuyAtLeastOfferIndicator] [nvarchar](4000) NULL,
	[PromotionVehicleId] [int] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
