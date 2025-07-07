/******
Object:  View [COSMOS_STME_WRK].[V_EDAA_STME_COSMOS_IMS_PRODUCT]
Script Date: 7/6/2022 11:10:27 AM
Script By: Prashant Tripathi
******/

use [edaa-stme]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE or ALTER VIEW [COSMOS_STME_WRK].[V_EDAA_STME_COSMOS_IMS_PRODUCT]
AS
select * from openrowset(
	Provider = 'CosmosDB',
	Connection = 'Account=cosmosdb-stme-ims;Database=IMS',
	Object = 'Product',
	Server_Credential = 'cosmosdb-stme-ims'
) with (
		upc varchar(20),
		scannedupc varchar(20),
		description varchar(max),
		containers varchar(max)
		) as table1


GO
