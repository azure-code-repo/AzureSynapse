/****** Object:  View [dbo].[vw_edaa_stme_ims_action]    Script Date: 7/6/2022 11:10:27 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create   view [dbo].[vw_edaa_stme_ims_action]
AS
select * from openrowset(
	Provider = 'CosmosDB',
	Connection = 'Account=cosmosdb-np-stme-ims-int;Database=IMS',
	Object = 'Actions',
	Server_Credential = 'cosmosdb-np-stme-ims-int'
) as table1

GO
