/****** Object:  View [ETL].[V_SRC_DIM_TM_CALC]    Script Date: 5/24/2021 2:21:04 AM ******/
/*To have time data for 3 years value changed from -731 > -1096 to implement LLY*/

CREATE VIEW [ETL].[V_SRC_DIM_TM_CALC] AS select
CAST(convert(varchar,a.[DAY_DT],112) as int) AS [TM_SKY],
a.day_dt AS DAY_DT ,
a.day_dt_ly AS DAY_DT_LY,
a.[FCL_DAY_DT_LY],
a.[LY_HLDY_DAY_DT],
a.[WK_END_DT],
DATEADD(DAY, 1, a.[WK_END_DT]) AS [WK_STRT_DT],
a.[WK_END_DT_LY],
DATEADD(DAY, 1, a.[WK_END_DT_LY]) AS [WK_STRT_DT_LY],
a.[FCL_WK_END_DT_LY],
DATEADD(DAY, 1, a.[FCL_WK_END_DT_LY]) AS [FCL_WK_STRT_DT_LY],
a.cldr_typ_ct AS CLDR_TYP ,
a.CLDR_TYP_ORDR_ID ,
b.cldr_day_of_wk_nm AS CLDR_DAY_OF_WK_NM ,
b.cldr_day_of_wk_shrt_nm AS DAY_OF_WK ,
b.DATE_NM,
b.cldr_day_of_wk_id AS CLDR_DAY_OF_WK_ID ,
b.fcl_wk_of_yr_id AS FCL_WK_ID ,

b.CLDR_MTH_OF_YR_NM ,
b.FCL_PER_SEQ_ID ,
b.FCL_QTR_SEQ_ID ,
b.FCL_YR_ID ,
b.CLDR_YR_ID ,
b.FCL_QTR_NM ,
b.FCL_PER_NM  ,
b.PRE_DEF_NM AS PRE_DEF_DT_FLTR ,
b.sort_ord_id

FROM (
       SELECT a.day_dt ,
              a.day_dt_ly ,
              'Calendar'              AS cldr_typ_ct ,
              10                      AS cldr_typ_ordr_id,
			  a.[FCL_DAY_DT_LY],
         case when a.[LY_HLDY_DAY_DT] is null then  a.[FCL_DAY_DT_LY]
		 ELSE a.[LY_HLDY_DAY_DT] END [LY_HLDY_DAY_DT],
a.[WK_END_DT],
a.[WK_END_DT_LY],
a.[FCL_WK_END_DT_LY]
       FROM   DM.DIM_TM        AS a
	    where a.day_dt between DATEADD(DAY, -1096, GETDATE()) AND DATEADD(DAY, 7, GETDATE())

	   UNION ALL
       SELECT a.day_dt ,
              a.fcl_day_dt_ly         AS day_dt_ly ,
              'Fiscal'                AS cldr_typ_ct ,
              20                      AS cldr_typ_ordr_id,
			  a.[FCL_DAY_DT_LY],
 case when a.[LY_HLDY_DAY_DT] is null then  a.[FCL_DAY_DT_LY]
		 ELSE a.[LY_HLDY_DAY_DT] END [LY_HLDY_DAY_DT],
a.[WK_END_DT],
a.[WK_END_DT_LY],
a.[FCL_WK_END_DT_LY]
       FROM   DM.DIM_TM        AS a
	    where a.day_dt between DATEADD(DAY, -1096, GETDATE()) AND DATEADD(DAY, 7, GETDATE())
       UNION ALL
       SELECT    a.day_dt ,
                 c.ly_hldy_day_dt        AS day_dt_ly ,
                 'Holiday'               AS cldr_typ_ct ,
                 30                      AS cldr_typ_ordr_id,
				 a.[FCL_DAY_DT_LY],
 case when a.[LY_HLDY_DAY_DT] is null then  a.[FCL_DAY_DT_LY]
		 ELSE a.[LY_HLDY_DAY_DT] END [LY_HLDY_DAY_DT],
a.[WK_END_DT],
a.[WK_END_DT_LY],
a.[FCL_WK_END_DT_LY]
       FROM      DM.DIM_TM        AS a
       LEFT JOIN dm_edw.hldy_dt_xrf_inf AS c
       ON        c.day_dt=a.day_dt
	    where a.day_dt between DATEADD(DAY, -1096, GETDATE()) AND DATEADD(DAY, 7, GETDATE())
	   ) AS a
	   JOIN
	   (
	   select
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
	   Ltrim(rtrim(fcl_qtr_nm)) AS fcl_qtr_nm , ltrim(rtrim(fcl_per_nm)) AS fcl_per_nm ,
	   pre_def_nm, sort_ord_id, pre_def_val
	   FROM etl.dt_pre_def_calc_rpt AS a
) AS b ON a.day_dt = b.day_dt
WHERE NOT
(
  cldr_typ_ct = 'Holiday'
  AND
  pre_def_val IN (12,
                  13)
);
