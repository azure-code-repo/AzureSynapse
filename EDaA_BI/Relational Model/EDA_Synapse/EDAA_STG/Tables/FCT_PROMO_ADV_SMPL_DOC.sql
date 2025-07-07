CREATE TABLE [EDAA_STG].[FCT_PROMO_ADV_SMPL_DOC]
(
	[UpdatedDate] [datetime2](7) NULL,
	[UpdatedBy] [nvarchar](4000) NULL,
	[PromotionAdvertisementBlockId] [int] NULL,
	[PromotionAdvertisementProductGroupId] [int] NULL,
	[PromotionAdvertisementProductSubGroupId] [int] NULL,
	[ProductItemSKUDocumentId] [decimal](18, 0) NULL,
	[PromotionAdvertisementDocumentTreatmentId] [int] NULL,
	[PromotionAdvertisementSampleSourceId] [int] NULL,
	[PromotionAdvertisementSignRequiredIndicator] [nvarchar](4000) NULL,
	[PromotionAdvertisementCouponRequiredIndicator] [nvarchar](4000) NULL,
	[PromotionAdvertisementUnitAvailableQuantity] [int] NULL,
	[PromotionAdvertisementOnClearenceQuantity] [int] NULL
)
WITH
(
	DISTRIBUTION = ROUND_ROBIN,
	CLUSTERED COLUMNSTORE INDEX
)
