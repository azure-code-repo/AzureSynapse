/******
Object:  View [COSMOS_STME_WRK].[V_EDAA_STME_COSMOS_IMS_LOCATION]
Script Date: 7/6/2022 11:10:27 AM
Script By: Prashant Tripathi
******/

use [edaa-stme]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create or alter view [COSMOS_STME_WRK].[V_EDAA_STME_COSMOS_IMS_LOCATION]
AS
select * from openrowset(
	Provider = 'CosmosDB',
	Connection = 'Account=cosmosdb-stme-ims;Database=IMS',
	Object = 'Location',
	Server_Credential = 'cosmosdb-stme-ims'
) with (
		addressId varchar(20),
		store varchar(10),
		zoneName varchar(10),
		addressName varchar(10),
		addressNumber varchar(10),
		containers varchar(max)
		) as table1

GO
