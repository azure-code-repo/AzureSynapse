CREATE VIEW [ETL].[V_SRC_DT_PRE_DEF_CALC_RPT]
AS SELECT
day_dt,
cldr_day_of_wk_nm,
cldr_day_of_wk_shrt_nm ,
CAST(DATEPART(MONTH, a.day_dt) as varchar(2)) + '/' +
	   CAST(DATEPART(DAY, a.day_dt) as varchar(2)) + ' (' + ltrim(rtrim(a.cldr_day_of_wk_shrt_nm)) + ')' AS DATE_NM ,
	   cldr_day_of_wk_id,
	   wk_end_dt,
	   fcl_wk_of_yr_id,
	   cldr_mth_of_yr_nm,
	   fcl_per_seq_id,
	   fcl_qtr_seq_id,
	   fcl_yr_id,
	   cldr_yr_id ,
	   FCL_QTR_ID,
       FCL_QTR_NM,
       FCL_PER_NM,
Cast('Yesterday' AS VARCHAR(25)) AS PRE_DEF_NM,
Cast(2 AS int) AS PRE_DEF_VAL,
Cast(10 AS int) AS SORT_ORD_ID
FROM [ETL].[DT_CALC_RPT_INF] A
WHERE A.DAY_DT = DATEADD(DAY, -1, CAST(GETDATE() as date))

UNION ALL

SELECT
day_dt,
cldr_day_of_wk_nm,
cldr_day_of_wk_shrt_nm ,
CAST(DATEPART(MONTH, a.day_dt) as varchar(2)) + '/' +
	   CAST(DATEPART(DAY, a.day_dt) as varchar(2)) + ' (' + ltrim(rtrim(a.cldr_day_of_wk_shrt_nm)) + ')' AS DATE_NM ,
	   cldr_day_of_wk_id,
	   wk_end_dt,
	   fcl_wk_of_yr_id,
	   cldr_mth_of_yr_nm,
	   fcl_per_seq_id,
	   fcl_qtr_seq_id,
	   fcl_yr_id,
	   cldr_yr_id ,
	  FCL_QTR_ID,
FCL_QTR_NM,
 FCL_PER_NM,
'Last Wk' AS PRE_DEF_NM,
Cast(4 AS INTEGER) AS PRE_DEF_VAL,
Cast(50 AS INTEGER) AS SORT_ORD_ID
FROM [ETL].[DT_CALC_RPT_INF] A
WHERE A.WK_END_DT = (SELECT WK_END_DT FROM dm_edw.DT_INF WHERE DAY_DT = DATEADD(DAY, -7,

CAST(GETDATE() as date)

))

UNION ALL

SELECT
day_dt,
cldr_day_of_wk_nm,
cldr_day_of_wk_shrt_nm ,
CAST(DATEPART(MONTH, a.day_dt) as varchar(2)) + '/' +
	   CAST(DATEPART(DAY, a.day_dt) as varchar(2)) + ' (' + ltrim(rtrim(a.cldr_day_of_wk_shrt_nm)) + ')' AS DATE_NM ,
	   cldr_day_of_wk_id,
	   wk_end_dt,
	   fcl_wk_of_yr_id,
	   cldr_mth_of_yr_nm,
	   fcl_per_seq_id,
	   fcl_qtr_seq_id,
	   fcl_yr_id,
	   cldr_yr_id ,
	   FCL_QTR_ID,
       FCL_QTR_NM,
       FCL_PER_NM,

Cast('WTD' AS VARCHAR(25)) AS PRE_DEF_NM,
Cast(3 AS INTEGER) AS PRE_DEF_VAL  ,
Cast(40 AS INTEGER) AS SORT_ORD_ID
FROM [ETL].[DT_CALC_RPT_INF] A
WHERE A.WK_END_DT = (SELECT WK_END_DT FROM dm_edw.DT_INF WHERE DAY_DT = DATEADD(DAY, -1,
CAST(GETDATE() as date)
))
AND A.DAY_DT <= DATEADD(DAY, -1, CAST(GETDATE() as date)

)

UNION ALL

SELECT
day_dt,
cldr_day_of_wk_nm,
cldr_day_of_wk_shrt_nm ,
CAST(DATEPART(MONTH, a.day_dt) as varchar(2)) + '/' +
	   CAST(DATEPART(DAY, a.day_dt) as varchar(2)) + ' (' + ltrim(rtrim(a.cldr_day_of_wk_shrt_nm)) + ')' AS DATE_NM ,
	   cldr_day_of_wk_id,
	   wk_end_dt,
	   fcl_wk_of_yr_id,
	   cldr_mth_of_yr_nm,
	   fcl_per_seq_id,
	   fcl_qtr_seq_id,
	   fcl_yr_id,
	   cldr_yr_id ,
	   FCL_QTR_ID,
       FCL_QTR_NM,
       FCL_PER_NM,

Cast('Current Wk' AS VARCHAR(25)) AS PRE_DEF_NM,
Cast(6 AS INTEGER) AS PRE_DEF_VAL,
Cast(71 AS INTEGER) AS SORT_ORD_ID
FROM [ETL].[DT_CALC_RPT_INF] A
WHERE A.WK_END_DT = (SELECT WK_END_DT FROM dm_edw.DT_INF WHERE DAY_DT = CAST(GETDATE() as date))

UNION ALL


SELECT
day_dt,
cldr_day_of_wk_nm,
cldr_day_of_wk_shrt_nm ,
CAST(DATEPART(MONTH, a.day_dt) as varchar(2)) + '/' +
	   CAST(DATEPART(DAY, a.day_dt) as varchar(2)) + ' (' + ltrim(rtrim(a.cldr_day_of_wk_shrt_nm)) + ')' AS DATE_NM ,
	   cldr_day_of_wk_id,
	   wk_end_dt,
	   fcl_wk_of_yr_id,
	   cldr_mth_of_yr_nm,
	   fcl_per_seq_id,
	   fcl_qtr_seq_id,
	   fcl_yr_id,
	   cldr_yr_id ,
        FCL_QTR_ID,
        FCL_QTR_NM,
        FCL_PER_NM,

'Last 14 Days' AS PRE_DEF_NM,
Cast(8 AS INTEGER) AS PRE_DEF_VAL,
Cast(100 AS INTEGER) AS SORT_ORD_ID
FROM [ETL].[DT_CALC_RPT_INF] A
WHERE  A.DAY_DT BETWEEN DATEADD(DAY, -14, CAST(GETDATE() as date))  AND
DATEADD(DAY, -1, CAST(GETDATE() as date))

UNION ALL

SELECT
day_dt,
cldr_day_of_wk_nm,
cldr_day_of_wk_shrt_nm ,
CAST(DATEPART(MONTH, a.day_dt) as varchar(2)) + '/' +
	   CAST(DATEPART(DAY, a.day_dt) as varchar(2)) + ' (' + ltrim(rtrim(a.cldr_day_of_wk_shrt_nm)) + ')' AS DATE_NM ,
	   cldr_day_of_wk_id,
	   wk_end_dt,
	   fcl_wk_of_yr_id,
	   cldr_mth_of_yr_nm,
	   fcl_per_seq_id,
	   fcl_qtr_seq_id,
	   fcl_yr_id,
	   cldr_yr_id ,
	   FCL_QTR_ID,
       FCL_QTR_NM,
       FCL_PER_NM,
'PTD' AS PRE_DEF_NM,
Cast(5 AS INTEGER) AS PRE_DEF_VAL,
Cast(170 AS INTEGER) AS SORT_ORD_ID
FROM [ETL].[DT_CALC_RPT_INF] A
WHERE A.FCL_PER_END_DT = (SELECT FCL_PER_END_DT FROM dm_edw.DT_INF
WHERE DAY_DT = DATEADD(DAY, -1, CAST(GETDATE() as date)))
AND A.DAY_DT <= DATEADD(DAY, -1, CAST(GETDATE() as date))

UNION ALL

SELECT
day_dt,
cldr_day_of_wk_nm,
cldr_day_of_wk_shrt_nm ,
CAST(DATEPART(MONTH, a.day_dt) as varchar(2)) + '/' +
	   CAST(DATEPART(DAY, a.day_dt) as varchar(2)) + ' (' + ltrim(rtrim(a.cldr_day_of_wk_shrt_nm)) + ')' AS DATE_NM ,
	   cldr_day_of_wk_id,
	   wk_end_dt,
	   fcl_wk_of_yr_id,
	   cldr_mth_of_yr_nm,
	   fcl_per_seq_id,
	   fcl_qtr_seq_id,
	   fcl_yr_id,
	   cldr_yr_id ,
	   FCL_QTR_ID,
       FCL_QTR_NM,
	   FCL_PER_NM,

Cast('QTD' AS VARCHAR(25)) AS PRE_DEF_NM,
Cast(12 AS INTEGER) AS PRE_DEF_VAL,
Cast(220 AS INTEGER) AS SORT_ORD_ID
FROM [ETL].[DT_CALC_RPT_INF] A
WHERE A.FCL_QTR_END_DT =
(SELECT FCL_QTR_END_DT FROM dm_edw.DT_INF WHERE DAY_DT =

DATEADD(DAY, -1, CAST(GETDATE() as date) )

) AND A.DAY_DT <= DATEADD(DAY, -1, CAST(GETDATE() as date) )

UNION ALL


SELECT
day_dt,
cldr_day_of_wk_nm,
cldr_day_of_wk_shrt_nm ,
CAST(DATEPART(MONTH, a.day_dt) as varchar(2)) + '/' +
	   CAST(DATEPART(DAY, a.day_dt) as varchar(2)) + ' (' + ltrim(rtrim(a.cldr_day_of_wk_shrt_nm)) + ')' AS DATE_NM ,
	   cldr_day_of_wk_id,
	   wk_end_dt,
	   fcl_wk_of_yr_id,
	   cldr_mth_of_yr_nm,
	   fcl_per_seq_id,
	   fcl_qtr_seq_id,
	   fcl_yr_id,
	   cldr_yr_id ,
	   FCL_QTR_ID,
       FCL_QTR_NM,
       FCL_PER_NM,

Cast('FYTD' AS VARCHAR(25)) AS PRE_DEF_NM,
Cast(13 AS INTEGER) AS PRE_DEF_VAL,
Cast(230 AS INTEGER) AS SORT_ORD_ID
FROM [ETL].[DT_CALC_RPT_INF] A
WHERE A.FCL_YR_ID =
(SELECT FCL_YR_ID FROM dm_edw.DT_INF WHERE DAY_DT = DATEADD(DAY, -1, CAST(GETDATE() as date) )) AND A.DAY_DT <= DATEADD(DAY, -1, CAST(GETDATE() as date) )

UNION ALL

SELECT
day_dt,
cldr_day_of_wk_nm,
cldr_day_of_wk_shrt_nm ,
CAST(DATEPART(MONTH, a.day_dt) as varchar(2)) + '/' +
	   CAST(DATEPART(DAY, a.day_dt) as varchar(2)) + ' (' + ltrim(rtrim(a.cldr_day_of_wk_shrt_nm)) + ')' AS DATE_NM ,
	   cldr_day_of_wk_id,
	   wk_end_dt,
	   fcl_wk_of_yr_id,
	   cldr_mth_of_yr_nm,
	   fcl_per_seq_id,
	   fcl_qtr_seq_id,
	   fcl_yr_id,
	   cldr_yr_id ,
	   FCL_QTR_ID,
       FCL_QTR_NM,
       FCL_PER_NM,

Cast('All Hist Days' AS VARCHAR(25)) AS PRE_DEF_NM,
Cast(22 AS INTEGER) AS PRE_DEF_VAL,
Cast(290 AS INTEGER) AS SORT_ORD_ID
FROM [ETL].[DT_CALC_RPT_INF] A
WHERE A.DAY_DT BETWEEN (SELECT Min(DAY_DT) FROM dm_edw.DT_INF WHERE FCL_YR_ID =
                       (SELECT FCL_YR_ID - 4 FROM dm_edw.DT_INF WHERE DAY_DT = DATEADD(DAY, -1, CAST(GETDATE() as date) )))
                   AND DATEADD(DAY, -1, CAST(GETDATE() as date) )

UNION ALL


SELECT
day_dt,
cldr_day_of_wk_nm,
cldr_day_of_wk_shrt_nm ,
CAST(DATEPART(MONTH, a.day_dt) as varchar(2)) + '/' +
	   CAST(DATEPART(DAY, a.day_dt) as varchar(2)) + ' (' + ltrim(rtrim(a.cldr_day_of_wk_shrt_nm)) + ')' AS DATE_NM ,
	   cldr_day_of_wk_id,
	   wk_end_dt,
	   fcl_wk_of_yr_id,
	   cldr_mth_of_yr_nm,
	   fcl_per_seq_id,
	   fcl_qtr_seq_id,
	   fcl_yr_id,
	   cldr_yr_id ,
	    FCL_QTR_ID,
         FCL_QTR_NM,
        FCL_PER_NM,
'Last 4 Wks' AS PRE_DEF_NM,
Cast(28 AS INTEGER) AS PRE_DEF_VAL,
Cast(110 AS INTEGER) AS SORT_ORD_ID
FROM [ETL].[DT_CALC_RPT_INF] A
WHERE  A.WK_END_DT BETWEEN (SELECT WK_END_DT FROM dm_edw.DT_INF
WHERE DAY_DT = (

DATEADD(DAY, -28, CAST(GETDATE() as date))

) )
        AND (SELECT WK_END_DT FROM dm_edw.DT_INF WHERE DAY_DT = (

		DATEADD(DAY, -7, CAST(GETDATE() as date))

		));
