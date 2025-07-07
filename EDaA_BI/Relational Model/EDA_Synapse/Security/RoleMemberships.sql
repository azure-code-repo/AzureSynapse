EXECUTE sp_addrolemember @rolename = N'db_owner', @membername = N'EDaASynapseLoaduser01';


GO
EXECUTE sp_addrolemember @rolename = N'db_owner', @membername = N'EDaASynapseDevOpsUser';


GO
EXECUTE sp_addrolemember @rolename = N'db_owner', @membername = N'Enterprise DaA - Azure Data Engineer';


GO
EXECUTE sp_addrolemember @rolename = N'db_ddladmin', @membername = N'Enterprise DaA - Data Modeler';


GO
EXECUTE sp_addrolemember @rolename = N'db_ddladmin', @membername = N'EDaASynapseLoaduser01';


GO
EXECUTE sp_addrolemember @rolename = N'db_ddladmin', @membername = N'Enterprise DaA - Azure Data Engineer';


GO
EXECUTE sp_addrolemember @rolename = N'db_ddladmin', @membername = N'Enterprise DaA - Azure Admin';


GO
EXECUTE sp_addrolemember @rolename = N'db_datawriter', @membername = N'Enterprise DaA - Azure Data Engineer';


GO
EXECUTE sp_addrolemember @rolename = N'db_datawriter', @membername = N'EDaASynapseLoaduser03';




GO
EXECUTE sp_addrolemember @rolename = N'db_datawriter', @membername = N'Enterprise DaA - Azure Admin';


GO
EXECUTE sp_addrolemember @rolename = N'db_datareader', @membername = N'Enterprise DaA - Azure Data Engineer';


GO
EXECUTE sp_addrolemember @rolename = N'db_datareader', @membername = N'EDAAADFDB.EDAAADFDB@meijer.com';


GO
EXECUTE sp_addrolemember @rolename = N'db_datareader', @membername = N'EDaASynapseLoaduser03';




GO
EXECUTE sp_addrolemember @rolename = N'db_datareader', @membername = N'Enterprise DaA - Azure Admin';
