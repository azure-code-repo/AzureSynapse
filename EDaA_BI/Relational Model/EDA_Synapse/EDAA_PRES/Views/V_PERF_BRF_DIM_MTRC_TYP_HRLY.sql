CREATE VIEW [EDAA_PRES].[V_PERF_BRF_DIM_MTRC_TYP_HRLY]
AS Select Cast('Sales Diff LY' as Varchar(20)) AS [MTRC_TYP]
,10 as MTRC_TYP_ORD
 from (select 'X' as DUMMY) as X

UNION ALL

Select 'Sales Diff AP' AS [MTRC_TYP]
,20 as MTRC_TYP_ORD
 from (select 'X' as DUMMY) as X

 UNION ALL

Select 'DM Diff LY' AS [MTRC_TYP]
,30 as MTRC_TYP_ORD
 from (select 'X' as DUMMY) as X;
