
/******
Object:  View [COSMOS_STME_WRK].[V_OSA_MFC_ORDER_STATUS_TIMESTAMP]
Script Date: 9/6/2022
Script By: Manjula V
******/
use [edaa-stme]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE or ALTER VIEW [COSMOS_STME_WRK].V_OSA_MFC_ORDER_STATUS_TIMESTAMP
AS
SELECT
    *
FROM
    OPENROWSET(
     PROVIDER = 'CosmosDB',
      CONNECTION = 'Account=cosmosdb-merch-mfc;Database=MFC',
      OBJECT = 'Orders',
      SERVER_CREDENTIAL = 'cosmosdb-merch-mfc'
    ) WITH ( unitId varchar(10),
        orderId varchar(50) ,
        mfcUnitId varchar(10),
        orderStatusTimelinenew varchar(max)
        ) AS docs
      CROSS APPLY OPENJSON ( orderStatusTimelinenew )
                  WITH (
                        status varchar(100) ,
                        timestamp varchar(50)  ,
                        userId varchar(100)
                  ) AS a
GO
