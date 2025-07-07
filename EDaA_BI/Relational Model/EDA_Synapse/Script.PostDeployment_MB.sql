/*
Post-Deployment Script Template
--------------------------------------------------------------------------------------
 This file contains SQL statements that will be appended to the build script.
 Use SQLCMD syntax to include a file in the post-deployment script.
 Example:      :r .\myfile.sql
 Use SQLCMD syntax to reference a variable in the post-deployment script.
 Example:      :setvar TableName MyTable
               SELECT * FROM [$(TableName)]
--------------------------------------------------------------------------------------
*/
IF OBJECT_ID('dbo.vCS_rg_physical_stats') IS NULL
BEGIN
    EXECUTE(
	'CREATE VIEW [dbo].[vCS_rg_physical_stats] AS with cte
	as
	(
	select tb.[name] AS [logical_table_name]
	,sm.name
	, rg.[row_group_id] AS [row_group_id]
	, rg.[state] AS [state]
	, rg.[state_desc] AS [state_desc]
	, rg.[total_rows] AS [total_rows]
	, rg.[trim_reason_desc] AS trim_reason_desc
	, mp.[physical_name] AS physical_name
	FROM sys.[schemas] sm
	JOIN sys.[tables] tb ON sm.[schema_id] = tb.[schema_id]
	JOIN sys.[pdw_table_mappings] mp ON tb.[object_id] = mp.[object_id]
	JOIN sys.[pdw_nodes_tables] nt ON nt.[name] = mp.[physical_name]
	JOIN sys.[dm_pdw_nodes_db_column_store_row_group_physical_stats] rg ON rg.[object_id] = nt.[object_id]
	AND rg.[pdw_node_id] = nt.[pdw_node_id]
	AND rg.[distribution_id] = nt.[distribution_id]
	where rg.[row_group_id] > 0
	)
	select *
	from cte;')
END
GO
IF OBJECT_ID('dbo.vDataDistributionSummary') IS NULL
BEGIN
    EXECUTE(
    'CREATE VIEW [dbo].[vDataDistributionSummary] AS
SELECT
schema_name, table_name,
distribution_policy_name, distribution_id,
sum(data_space_GB) as data_space_GB,
sum(index_space_GB) as index_space_GB,
sum(row_count) as row_count
FROM (
SELECT
   [execution_time]
,  [database_name]
,  [schema_name]
,  [table_name]
,  [two_part_name]
,  [node_table_name]
,  [node_table_name_seq]
,  [distribution_policy_name]
,  [distribution_column]
,  [distribution_id]
,  [index_type]
,  [index_type_desc]
,  [pdw_node_id]
,  [pdw_node_type]
,  [pdw_node_name]
,  [dist_name]
,  [dist_position]
,  [partition_nmbr]
,  [reserved_space_page_count]
,  [unused_space_page_count]
,  [data_space_page_count]
,  [index_space_page_count]
,  [row_count]
,  ([reserved_space_page_count] * 8.0)                                 AS [reserved_space_KB]
,  ([reserved_space_page_count] * 8.0)/1000                            AS [reserved_space_MB]
,  ([reserved_space_page_count] * 8.0)/1000000                         AS [reserved_space_GB]
,  ([reserved_space_page_count] * 8.0)/1000000000                      AS [reserved_space_TB]
,  ([unused_space_page_count]   * 8.0)                                 AS [unused_space_KB]
,  ([unused_space_page_count]   * 8.0)/1000                            AS [unused_space_MB]
,  ([unused_space_page_count]   * 8.0)/1000000                         AS [unused_space_GB]
,  ([unused_space_page_count]   * 8.0)/1000000000                      AS [unused_space_TB]
,  ([data_space_page_count]     * 8.0)                                 AS [data_space_KB]
,  ([data_space_page_count]     * 8.0)/1000                            AS [data_space_MB]
,  ([data_space_page_count]     * 8.0)/1000000                         AS [data_space_GB]
,  ([data_space_page_count]     * 8.0)/1000000000                      AS [data_space_TB]
,  ([index_space_page_count]  * 8.0)                                   AS [index_space_KB]
,  ([index_space_page_count]  * 8.0)/1000                              AS [index_space_MB]
,  ([index_space_page_count]  * 8.0)/1000000                           AS [index_space_GB]
,  ([index_space_page_count]  * 8.0)/1000000000                        AS [index_space_TB]
FROM
(

SELECT
 GETDATE()                                                             AS  [execution_time]
, DB_NAME()                                                            AS  [database_name]
, s.name                                                               AS  [schema_name]
, t.name                                                               AS  [table_name]
, QUOTENAME(s.name)+''.''+QUOTENAME(t.name)                              AS  [two_part_name]
, nt.[name]                                                            AS  [node_table_name]
, ROW_NUMBER() OVER(PARTITION BY nt.[name] ORDER BY (SELECT NULL))     AS  [node_table_name_seq]
, tp.[distribution_policy_desc]                                        AS  [distribution_policy_name]
, c.[name]                                                             AS  [distribution_column]
, nt.[distribution_id]                                                 AS  [distribution_id]
, i.[type]                                                             AS  [index_type]
, i.[type_desc]                                                        AS  [index_type_desc]
, nt.[pdw_node_id]                                                     AS  [pdw_node_id]
, pn.[type]                                                            AS  [pdw_node_type]
, pn.[name]                                                            AS  [pdw_node_name]
, di.name                                                              AS  [dist_name]
, di.position                                                          AS  [dist_position]
, nps.[partition_number]                                               AS  [partition_nmbr]
, nps.[reserved_page_count]                                            AS  [reserved_space_page_count]
, nps.[reserved_page_count] - nps.[used_page_count]                    AS  [unused_space_page_count]
, nps.[in_row_data_page_count]
    + nps.[row_overflow_used_page_count]
    + nps.[lob_used_page_count]                                        AS  [data_space_page_count]
, nps.[reserved_page_count]
 - (nps.[reserved_page_count] - nps.[used_page_count])
 - ([in_row_data_page_count]
         + [row_overflow_used_page_count]+[lob_used_page_count])       AS  [index_space_page_count]
, nps.[row_count]                                                      AS  [row_count]
from
    sys.schemas s
INNER JOIN sys.tables t
    ON s.[schema_id] = t.[schema_id]
INNER JOIN sys.indexes i
    ON  t.[object_id] = i.[object_id]
    AND i.[index_id] <= 1
INNER JOIN sys.pdw_table_distribution_properties tp
    ON t.[object_id] = tp.[object_id]
INNER JOIN sys.pdw_table_mappings tm
    ON t.[object_id] = tm.[object_id]
INNER JOIN sys.pdw_nodes_tables nt
    ON tm.[physical_name] = nt.[name]
INNER JOIN sys.dm_pdw_nodes pn
    ON  nt.[pdw_node_id] = pn.[pdw_node_id]
INNER JOIN sys.pdw_distributions di
    ON  nt.[distribution_id] = di.[distribution_id]
INNER JOIN sys.dm_pdw_nodes_db_partition_stats nps
    ON nt.[object_id] = nps.[object_id]
    AND nt.[pdw_node_id] = nps.[pdw_node_id]
    AND nt.[distribution_id] = nps.[distribution_id]
LEFT OUTER JOIN (select * from sys.pdw_column_distribution_properties where distribution_ordinal = 1) cdp
    ON t.[object_id] = cdp.[object_id]
LEFT OUTER JOIN sys.columns c
    ON cdp.[object_id] = c.[object_id]
    AND cdp.[column_id] = c.[column_id]
WHERE pn.[type] = ''COMPUTE''

)
base
) AS  Distribution_Summary
group by
schema_name, table_name,
distribution_policy_name, distribution_id;')
END;

GO

IF OBJECT_ID('dbo.vStatsDeviation') IS NULL
BEGIN
    EXECUTE(
'CREATE VIEW [dbo].[vStatsDeviation]
AS select
objIdsWithStats.[object_id],
actualRowCounts.[schema],
actualRowCounts.logical_table_name,
statsRowCounts.stats_row_count,
actualRowCounts.actual_row_count,
row_count_difference = CASE
    WHEN actualRowCounts.actual_row_count >= statsRowCounts.stats_row_count THEN actualRowCounts.actual_row_count - statsRowCounts.stats_row_count
    ELSE statsRowCounts.stats_row_count - actualRowCounts.actual_row_count
END,
percent_deviation_from_actual = CASE
    WHEN actualRowCounts.actual_row_count = 0 THEN statsRowCounts.stats_row_count
    WHEN statsRowCounts.stats_row_count = 0 THEN actualRowCounts.actual_row_count
    WHEN actualRowCounts.actual_row_count >= statsRowCounts.stats_row_count THEN CONVERT(NUMERIC(18, 0), CONVERT(NUMERIC(18, 2), (actualRowCounts.actual_row_count - statsRowCounts.stats_row_count)) / CONVERT(NUMERIC(18, 2), actualRowCounts.actual_row_count) * 100)
    ELSE CONVERT(NUMERIC(18, 0), CONVERT(NUMERIC(18, 2), (statsRowCounts.stats_row_count - actualRowCounts.actual_row_count)) / CONVERT(NUMERIC(18, 2), actualRowCounts.actual_row_count) * 100)
END
from
(
    select distinct object_id from sys.stats where stats_id > 1
) objIdsWithStats
left join
(
    select object_id, sum(rows) as stats_row_count from sys.partitions group by object_id
) statsRowCounts
on objIdsWithStats.object_id = statsRowCounts.object_id
left join
(
    SELECT sm.name [schema] ,
    tb.name logical_table_name ,
    tb.object_id object_id ,
    SUM(rg.row_count) actual_row_count
    FROM sys.schemas sm
    INNER JOIN sys.tables tb ON sm.schema_id = tb.schema_id
    INNER JOIN sys.pdw_table_mappings mp ON tb.object_id = mp.object_id
    INNER JOIN sys.pdw_nodes_tables nt ON nt.name = mp.physical_name
    INNER JOIN sys.dm_pdw_nodes_db_partition_stats rg
    ON rg.object_id = nt.object_id
    AND rg.pdw_node_id = nt.pdw_node_id
    AND rg.distribution_id = nt.distribution_id
    WHERE 1 = 1
    GROUP BY sm.name, tb.name, tb.object_id
) actualRowCounts
on objIdsWithStats.object_id = actualRowCounts.object_id;')
END;
GO
IF OBJECT_ID('dbo.vTableSizes') IS NULL
BEGIN
    EXECUTE(
    'CREATE VIEW [dbo].[vTableSizes]
AS SELECT
[execution_time]
, [database_name]
, [schema_name]
, [table_name]
, [two_part_name]
, [node_table_name]
, [node_table_name_seq]
, [distribution_policy_name]
, [distribution_column]
, [distribution_id]
, [index_type]
, [index_type_desc]
, [pdw_node_id]
, [pdw_node_type]
, [pdw_node_name]
, [dist_name]
, [dist_position]
, [partition_nmbr]
, [reserved_space_page_count]
, [unused_space_page_count]
, [data_space_page_count]
, [index_space_page_count]
, [row_count]
, ([reserved_space_page_count] * 8.0) AS [reserved_space_KB]
, ([reserved_space_page_count] * 8.0)/1000 AS [reserved_space_MB]
, ([reserved_space_page_count] * 8.0)/1000000 AS [reserved_space_GB]
, ([reserved_space_page_count] * 8.0)/1000000000 AS [reserved_space_TB]
, ([unused_space_page_count] * 8.0) AS [unused_space_KB]
, ([unused_space_page_count] * 8.0)/1000 AS [unused_space_MB]
, ([unused_space_page_count] * 8.0)/1000000 AS [unused_space_GB]
, ([unused_space_page_count] * 8.0)/1000000000 AS [unused_space_TB]
, ([data_space_page_count] * 8.0) AS [data_space_KB]
, ([data_space_page_count] * 8.0)/1000 AS [data_space_MB]
, ([data_space_page_count] * 8.0)/1000000 AS [data_space_GB]
, ([data_space_page_count] * 8.0)/1000000000 AS [data_space_TB]
, ([index_space_page_count] * 8.0) AS [index_space_KB]
, ([index_space_page_count] * 8.0)/1000 AS [index_space_MB]
, ([index_space_page_count] * 8.0)/1000000 AS [index_space_GB]
, ([index_space_page_count] * 8.0)/1000000000 AS [index_space_TB]
FROM
(
SELECT
GETDATE() AS [execution_time]
, DB_NAME() AS [database_name]
, s.name AS [schema_name]
, t.name AS [table_name]
, QUOTENAME(s.name)+''.''+QUOTENAME(t.name) AS [two_part_name]
, nt.[name] AS [node_table_name]
, ROW_NUMBER() OVER(PARTITION BY nt.[name] ORDER BY (SELECT NULL)) AS [node_table_name_seq]
, tp.[distribution_policy_desc] AS [distribution_policy_name]
, c.[name] AS [distribution_column]
, nt.[distribution_id] AS [distribution_id]
, i.[type] AS [index_type]
, i.[type_desc] AS [index_type_desc]
, nt.[pdw_node_id] AS [pdw_node_id]
, pn.[type] AS [pdw_node_type]
, pn.[name] AS [pdw_node_name]
, di.name AS [dist_name]
, di.position AS [dist_position]
, nps.[partition_number] AS [partition_nmbr]
, nps.[reserved_page_count] AS [reserved_space_page_count]
, nps.[reserved_page_count] - nps.[used_page_count] AS [unused_space_page_count]
, nps.[in_row_data_page_count]
+ nps.[row_overflow_used_page_count]
+ nps.[lob_used_page_count] AS [data_space_page_count]
, nps.[reserved_page_count]
- (nps.[reserved_page_count] - nps.[used_page_count])
- ([in_row_data_page_count]
+ [row_overflow_used_page_count]+[lob_used_page_count]) AS [index_space_page_count]
, nps.[row_count] AS [row_count]
from
sys.schemas s
INNER JOIN sys.tables t
ON s.[schema_id] = t.[schema_id]
INNER JOIN sys.indexes i
ON t.[object_id] = i.[object_id]
AND i.[index_id] <= 1
INNER JOIN sys.pdw_table_distribution_properties tp
ON t.[object_id] = tp.[object_id]
INNER JOIN sys.pdw_table_mappings tm
ON t.[object_id] = tm.[object_id]
INNER JOIN sys.pdw_nodes_tables nt
ON tm.[physical_name] = nt.[name]
INNER JOIN sys.dm_pdw_nodes pn
ON nt.[pdw_node_id] = pn.[pdw_node_id]
INNER JOIN sys.pdw_distributions di
ON nt.[distribution_id] = di.[distribution_id]
INNER JOIN sys.dm_pdw_nodes_db_partition_stats nps
ON nt.[object_id] = nps.[object_id]
AND nt.[pdw_node_id] = nps.[pdw_node_id]
AND nt.[distribution_id] = nps.[distribution_id]
LEFT OUTER JOIN (select * from sys.pdw_column_distribution_properties where distribution_ordinal = 1) cdp
ON t.[object_id] = cdp.[object_id]
LEFT OUTER JOIN sys.columns c
ON cdp.[object_id] = c.[object_id]
AND cdp.[column_id] = c.[column_id]
WHERE pn.[type] = ''COMPUTE''

)

base;')
END;
GO
IF OBJECT_ID('dbo.vTableSkewness') IS NULL
BEGIN
    EXECUTE(
    '
    CREATE VIEW [dbo].[vTableSkewness]
    AS with TotalCount as
    (
    Select sum(row_count) as row_count, table_name, schema_name
    FROM [dbo].[vTableSizes]
    where distribution_policy_name = ''Hash''
    group by table_name, schema_name
    ), cmp_details as (
    select sum(tbl.row_count) as row_count, sum(TotalCount.row_count) as Total,



    tbl.distribution_id, tbl.table_name, tbl.schema_name
    FROM [dbo].[vTableSizes]  AS tbl
    inner join TotalCount
    ON TotalCount.schema_name = tbl.schema_name
    and TotalCount.table_name = tbl.table_name
    where tbl.distribution_policy_name = ''Hash''
    group by tbl.distribution_id, tbl.table_name, tbl.schema_name
    )
    select table_name,  sum(row_count) as cmp_row_count
    , (max(row_count)-min(row_count)) as highest_skew_rows_difference
    , convert(decimal(10,2),((max(row_count) - min(row_count))*100.0 / nullif(sum(row_count),0))) as skew_percent
    ,schema_name
    from cmp_details
    group by  schema_name, table_name;
    ')
END;
GO
