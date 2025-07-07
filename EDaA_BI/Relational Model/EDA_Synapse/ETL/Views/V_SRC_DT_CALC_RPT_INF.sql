CREATE VIEW [ETL].[V_SRC_DT_CALC_RPT_INF]
AS SELECT

      D.[DAY_DT],
      D.[DAY_DT_LY],
      D.[FCL_DAY_DT_LY],
      D.[WK_END_DT],
      D.[WK_END_DT_LY],
      D.[FCL_WK_END_DT_LY],

      D.[CLDR_DAY_OF_WK_ID],
      D.[CLDR_DAY_OF_WK_NM],
      D.[CLDR_DAY_OF_WK_SHRT_NM],
      D.[CLDR_MTH_OF_YR_ID],
      D.[CLDR_MTH_OF_YR_NM],
      D.[CLDR_MTH_OF_YR_SHRT_NM],
      D.[CLDR_YR_ID],
      D.[FCL_YR_ID],
      D.[FCL_YR_BEGN_DT],
      D.[FCL_YR_END_DT],
      D.[FCL_QTR_BEGN_DT],
      D.[FCL_QTR_END_DT],
      D.[FCL_PER_BEGN_DT_LY],
      D.[FCL_PER_END_DT_LY],
      D.[fcl_wk_of_yr_id],
      D.[fcl_per_seq_id],
      D.[fcl_qtr_seq_id],
      	   (FCL_YR_ID * 10) +  FCL_QTR_OF_YR_ID AS FCL_QTR_ID,
' Q' + Cast(FCL_QTR_OF_YR_ID AS VARCHAR(4)) + '-' + Substring(Cast(FCL_YR_ID AS CHAR(4)),3,2)  AS FCL_QTR_NM,

RTRIM(LTrim('P' + Cast(FCL_PER_OF_YR_ID AS VARCHAR(4))) + '-' + Substring(Cast(FCL_YR_ID AS CHAR(4)),3,2))   AS FCL_PER_NM,
Max(CASE WHEN WK_END_DT < (select getdate()) THEN WK_END_DT END) Over (ORDER BY DAY_DT) AS LST_CPT_WK_END_DT,
datediff(dy,convert(date,getdate()),convert(date,DAY_DT)) AS DAY_AGO_DUR,
convert(datetime,DAY_DT) - (364*2) AS DAY_DT_2LY,
convert(datetime,WK_END_DT) - (364*2) AS WK_END_DT_2LY,
D.FCL_PER_END_DT
FROM dm_edw.DT_INF D
WHERE DAY_DT < CAST(DATEADD(DAY, 372, GETDATE()) as date)
    AND WK_END_DT >= CAST(DATEADD(DAY, -(209*7), GETDATE()) as date);
