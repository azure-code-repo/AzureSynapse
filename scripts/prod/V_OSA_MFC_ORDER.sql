/******
Object:  View [COSMOS_STME_WRK].[V_OSA_MFC_ORDER]
Script Date: 9/6/2022
Script By: Manjula V
******/

use [edaa-stme]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create or ALTER  VIEW [COSMOS_STME_WRK].V_OSA_MFC_ORDER
AS
SELECT
    *
FROM
    OPENROWSET(
     PROVIDER = 'CosmosDB',
      CONNECTION = 'Account=cosmosdb-merch-mfc;Database=MFC',
      OBJECT = 'Orders',
      SERVER_CREDENTIAL = 'cosmosdb-merch-mfc'
    ) WITH (
		unitId varchar(10),
        orderId varchar(50) ,
        mfcUnitId varchar(10),
        customerUId varchar(50) '$.customer.uid',
        customerPhone varchar(20) '$.customer.phone',
        stagingStartTime varchar(50),
	    pickupStartTime varchar(50),
	    pickupslotID bigint,
	    orderStatus varchar(50),
	    StagingLocationCount smallint,
	    substitutionPreference varchar(100),
	    idVerificationRequired varchar(10),
	    idVerified varchar(100),
	    estimatedTotal float,
	    fulfillmentPartner varchar(100),
		orderStatusTimelinenew varchar(max),
		customer varchar (max ),
		lineItems varchar (max ),
		orderStatusTimeline varchar (max ),
		pickupDate varchar(20),
		_rid varchar (50 ),
		orderLateInfo varchar (250 ),
		_etag varchar (50 ),
		_ts varchar (20 ),
		etag varchar (50 ),
		id varchar (50 ),
		isOrderRunningLate smallint ,
		ebtSnapChargedAmount varchar (50 ),
		vehicleDescription varchar (500 ),
		IsMarkedOversized smallint ,
		deliveryMode varchar (20 ),
		additionalTexts varchar (100 ),
		startTime varchar (10 ),
		endTime varchar (10 ),
		ttl varchar (20 ),
		total varchar(20),
		taxTotal float ,
		createdOn varchar (30 ),
		timeline varchar (max ),
		ebtSnapRefundAmount varchar (20 ),
		orderConfirmWaveIdCount smallint ,
		BOHInformation varchar (100 ),
		subtotal varchar(20),
		deposit varchar(20),
		discountTotal varchar(20),
		pickerFee varchar(20),
		IsOrderStaged smallint ,
		logicAppRunDetails varchar (max ),
		isGMOrder smallint ,
		isValidationRequired smallint ,
		isTimeAdjusted smallint ,
		toteList varchar (max ),
		FlyBuyOrderId varchar (20 ),
		FlyBuyOrderdetailsData varchar (max ),
		IsMarkedOutOfStock smallint ,
		waveList varchar (max ),
		OrderConfirmationReceived smallint ,
		isValidationFailed smallint
        ) AS docs
GO
