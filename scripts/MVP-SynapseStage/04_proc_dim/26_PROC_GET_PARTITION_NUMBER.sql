/****** Object:  StoredProcedure [ETL].[PROC_GET_PARTITION_NUMBER]    Script Date: 9/9/2020 7:35:18 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [ETL].[PROC_GET_PARTITION_NUMBER] @SCHEMA [NVARCHAR](255),@TABLENAME [NVARCHAR](255),@PARTITIONVALUE [INT],@PARTITIONNUMBER [INT] OUT AS
BEGIN
WITH CTE
AS
(
SELECT
 s.name                            AS [schema_name]
,t.name                            AS [table_name]
,p.partition_number                AS [ptn_nmbr]
,p.[rows]						   AS [ptn_rows]
,CAST(r.[value] AS INT)            AS [boundary_value]

FROM        sys.schemas						AS s
JOIN        sys.tables						AS t    ON		s.[schema_id]			= t.[schema_id]
JOIN        sys.indexes						AS i    ON		t.[object_id]			= i.[object_id]
JOIN        sys.partitions					AS p    ON		i.[object_id]			= p.[object_id]
													AND		i.[index_id]			= p.[index_id]
JOIN        sys.partition_schemes			AS h    ON		i.[data_space_id]		= h.[data_space_id]
JOIN        sys.partition_functions			AS f    ON		h.[function_id]			= f.[function_id]
LEFT JOIN    sys.partition_range_values		AS r    ON		f.[function_id]			= r.[function_id]
													AND		r.[boundary_id]			= p.[partition_number]
WHERE i.[index_id] <= 1
)

SELECT
@PARTITIONNUMBER = [ptn_nmbr] + 1
FROM    CTE
WHERE    [schema_name]        = @SCHEMA
AND        [table_name]        = @TABLENAME
AND        [boundary_value]    = @PARTITIONVALUE
OPTION (LABEL = 'GET Partition Number')
print(@PARTITIONNUMBER)

END
GO
