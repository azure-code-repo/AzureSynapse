CREATE EXTERNAL DATA SOURCE [ext_datasource_with_abfss]
    WITH (
    TYPE = HADOOP,
    LOCATION = N'abfss://staged@saea2grsnpedaadlakeint.dfs.core.windows.net',
    CREDENTIAL = [sql-np-ea2-edaa-serve-01]
    );
