
/******
Object:  View [COSMOS_STME_WRK].[V_OSA_MFC_ORDER_LINEITEM]
Script Date: 9/6/2022
Script By: Manjula V
******/
use [edaa-stme]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE or ALTER VIEW [COSMOS_STME_WRK].V_OSA_MFC_ORDER_LINEITEM
AS
SELECT
    *
FROM
    OPENROWSET(
     PROVIDER = 'CosmosDB',
      CONNECTION = 'Account=cosmosdb-np-merch-mfc-cert;Database=MFC',
      OBJECT = 'Orders',
      SERVER_CREDENTIAL = 'cosmosdb-np-merch-mfc-cert'
    ) WITH ( unitId varchar(10),
        orderId varchar(50) ,
        mfcUnitId varchar(10),
		pickupStartTime varchar(50),
        lineItems varchar(max),
		orderStatusTimelinenew varchar(max)
        ) AS docs
      CROSS APPLY OPENJSON ( lineItems )
                  WITH (
                        itemId varchar(20) ,
                        productId varchar(20) ,
                        upc varchar(20) ,
                        isFailed varchar(10)  ,
                        name varchar (256) ,
                        quantity varchar(20) ,
                        notes varchar (250) ,
                        avgWtPerItem varchar(20) ,
                        storageArea varchar (50) ,
                        isDeleted varchar(10) ,
                        isAlcohol varchar(10)  ,
                        price varchar(20) ,
                        pricePerUnit varchar(20) ,
                        productsubsitutedFor varchar(20) ,
                        itemLocationCode varchar (256) ,
                        prdSellUOM varchar (6) ,
                        ActualQuantityPicked varchar(20),
                        UPCSubstitutedFor varchar(20) ,
                        BOHTransfered Varchar (50)
                  ) AS a
GO
