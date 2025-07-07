CREATE VIEW [V_EDAA_PRES].[DIM_PRE_DEF_DATE]
AS WITH variables
AS (
   SELECT
       -- Daily Element
       Format(CAST(GETDATE() AS date), 'yyyyMMdd') AS CurrentDate,

       -- Weekly Elements
       Format(DATEADD(day, 1 - DATEPART(WEEKDAY, CAST(GETDATE() AS date)) % 7, CAST(GETDATE() AS date)), 'yyyyMMdd') AS WeekStart,
       Format(DATEADD(day, 7 - DATEPART(WEEKDAY, CAST(GETDATE() AS date)) % 7, CAST(GETDATE() AS date)), 'yyyyMMdd') AS WeekEnd,
       CASE
           WHEN DATEPART(WEEKDAY, CAST(GETDATE() AS date)) >= 4 THEN
               1
           ELSE
               0
       END AS isLaborWkClosed,

       -- Period and Quarter Elements
       Fsc_Qtr_Seq_Id AS QuarterId,
       Fsc_Prd_Seq_Id AS PeriodId,
       CONVERT(INT, Fsc_Yr) AS FiscalYear
   FROM EDAA_DW.DIM_DLY_CAL WITH (NOLOCK)
   WHERE Cal_Dt = CONVERT(varchar(10), CAST(GETDATE() AS date), 23)
   ),
     pd_lookup
AS (SELECT format(MIN(Cal_Dt), 'yyyyMMdd') AS PeriodBegin,
           format(MAX(Cal_Dt), 'yyyyMMdd') As PeriodEnd,
           Fsc_Prd_Seq_Id AS PdId
    FROM EDAA_DW.DIM_DLY_CAL WITH (NOLOCK)
    GROUP BY Fsc_Prd_Seq_Id
   ),
     qtr_lookup
AS (SELECT format(MIN(Fsc_Qtr_Bgn_Dt), 'yyyyMMdd') AS QuarterBegin,
           format(MAX(Fsc_Qtr_End_Dt), 'yyyyMMdd') AS QuarterEnd,
           Fsc_Qtr_Seq_Id AS QtrId
    FROM EDAA_DW.DIM_DLY_CAL WITH (NOLOCK)
    GROUP BY Fsc_Qtr_Seq_Id
   ),
     fsc_lookup
AS (SELECT format(MIN(Cal_Dt), 'yyyyMMdd') AS FscYrBegin,
           format(MAX(Cal_Dt), 'yyyyMMdd') AS FscYrEnd,
           CONVERT(INT, Fsc_Yr) AS FscYr
    FROM EDAA_DW.DIM_DLY_CAL WITH (NOLOCK)
    GROUP BY Fsc_Yr
   ),
     day_lookup
AS (
   -- Generate date table with last 42 days (inclusive of today, hence the -43 offset)
   -- The offset 43 is used because the CAST(GETDATE() AS date) function returns 12:00am, so we need to go an extra
   -- day back to include the last 42 days at this view's runtime.
   -- Max and Min values are the day itself, these are added to match the aggregations' schema for the union
   SELECT CONVERT(varchar(10), Cal_Dt) AS PRE_DEF_NM,
          CAST(DT_SK AS int) AS PRE_DEF_ID,
          'DLY' AS AGG_TYPE,
          format(CONVERT(datetime, CONVERT(varchar(8), Dt_Sk), 112), 'yyyyMMdd') AS MIN_DT_SK,
          format(CONVERT(datetime, CONVERT(varchar(8), Dt_Sk), 112), 'yyyyMMdd') AS MAX_DT_SK
   FROM EDAA_DW.DIM_DLY_CAL
   WHERE Cal_Dt
   BETWEEN DATEADD(day, -43, CAST(GETDATE() AS date)) AND CAST(GETDATE() AS date)
   )

-- Week Related
SELECT 'This Wk' AS PRE_DEF_NM,
       1 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       WeekStart AS MIN_DT_SK,
       WeekEnd AS MAX_DT_SK
FROM variables
UNION ALL
SELECT 'WTD' AS PRE_DEF_NM,
       2 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       WeekStart AS MIN_DT_SK,
       format(DATEADD(day, -1, CurrentDate), 'yyyyMMdd')
FROM variables
UNION ALL
SELECT 'Last Wk' AS PRE_DEF_NM,
       3 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       format(DATEADD(week, -1, WeekStart), 'yyyyMMdd') AS MIN_DT_SK,
       format(DATEADD(week, -1, WeekEnd), 'yyyyMMdd') AS MAX_DT_SK
FROM variables
UNION ALL
SELECT 'Last Lbr Wk' AS PRE_DEF_NM,
       4 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       CASE
           WHEN variables.isLaborWkClosed = 0 THEN
               format(DATEADD(week, -2, WeekStart), 'yyyyMMdd')
           ELSE
               format(DATEADD(week, -1, WeekStart), 'yyyyMMdd')
       END AS MIN_DT_SK,
       CASE
           WHEN variables.isLaborWkClosed = 0 THEN
               format(DATEADD(week, -2, WeekEnd), 'yyyyMMdd')
           ELSE
               format(DATEADD(week, -1, WeekEnd), 'yyyyMMdd')
       END AS MAX_DT_SK
FROM variables
UNION ALL
SELECT 'Last 4 Wks' AS PRE_DEF_NM,
       5 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       format(DATEADD(week, -4, WeekStart), 'yyyyMMdd') AS MIN_DT_SK,
       format(DATEADD(week, -1, WeekEnd), 'yyyyMMdd') AS MAX_DT_SK
FROM variables
UNION ALL
SELECT 'Last 8 Wks' AS PRE_DEF_NM,
       6 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       format(DATEADD(week, -8, WeekStart), 'yyyyMMdd') AS MIN_DT_SK,
       format(DATEADD(week, -1, WeekEnd), 'yyyyMMdd') AS MAX_DT_SK
FROM variables
UNION ALL
SELECT 'Last 8 Lbr Wks' AS PRE_DEF_NM,
       7 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       CASE
           WHEN variables.isLaborWkClosed = 0 THEN
               format(DATEADD(week, -9, WeekStart), 'yyyyMMdd')
           ELSE
               format(DATEADD(week, -8, WeekStart), 'yyyyMMdd')
       END AS MIN_DT_SK,
       CASE
           WHEN variables.isLaborWkClosed = 0 THEN
               format(DATEADD(week, -2, WeekEnd), 'yyyyMMdd')
           ELSE
               format(DATEADD(week, -1, WeekEnd), 'yyyyMMdd')
       END AS MAX_DT_SK
FROM variables
UNION ALL
SELECT 'Last 12 Wks' AS PRE_DEF_NM,
       8 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       format(DATEADD(week, -12, WeekStart), 'yyyyMMdd') AS MIN_DT_SK,
       format(DATEADD(week, -1, WeekEnd), 'yyyyMMdd') AS MAX_DT_SK
FROM variables
UNION ALL
SELECT 'Last 52 Wks' AS PRE_DEF_NM,
       9 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       format(DATEADD(week, -52, WeekStart), 'yyyyMMdd') AS MIN_DT_SK,
       format(DATEADD(week, -1, WeekEnd), 'yyyyMMdd') AS MAX_DT_SK
FROM variables
UNION ALL
SELECT 'Next Wk' AS PRE_DEF_NM,
       10 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       format(DATEADD(week, 1, WeekStart), 'yyyyMMdd') AS MIN_DT_SK,
       format(DATEADD(week, 1, WeekEnd), 'yyyyMMdd') AS MAX_DT_SK
FROM variables
UNION ALL
SELECT 'Next 4 Wks' AS PRE_DEF_NM,
       11 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       format(DATEADD(week, 1, WeekStart), 'yyyyMMdd') AS MIN_DT_SK,
       format(DATEADD(week, 4, WeekEnd), 'yyyyMMdd') AS MAX_DT_SK
FROM variables
UNION ALL
SELECT 'Next 8 Wks' AS PRE_DEF_NM,
       12 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       format(DATEADD(week, 1, WeekStart), 'yyyyMMdd') AS MIN_DT_SK,
       format(DATEADD(week, 8, WeekEnd), 'yyyyMMdd') AS MAX_DT_SK
FROM variables
UNION ALL
SELECT 'Rem Wk' AS PRE_DEF_NM,
       13 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       CurrentDate AS MIN_DT_SK,
       WeekEnd AS MAX_DT_SK
FROM variables
UNION ALL
SELECT '2 Wks Out' AS PRE_DEF_NM,
       14 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       format(DATEADD(week, 2, WeekStart), 'yyyyMMdd') AS MIN_DT_SK,
       format(DATEADD(week, 2, WeekEnd), 'yyyyMMdd') AS MAX_DT_SK
FROM variables
UNION ALL

--Day Related
SELECT 'Today' AS PRE_DEF_NM,
       15 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       CurrentDate AS MIN_DT_SK,
       CurrentDate AS MAX_DT_SK
FROM variables
UNION ALL
SELECT '2 days ago' AS PRE_DEF_NM,
       16 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       Format(DATEADD(day, -2, CurrentDate), 'yyyyMMdd') AS MIN_DT_SK,
       format(DATEADD(day, -2, CurrentDate), 'yyyyMMdd') AS MAX_DT_SK
FROM variables
UNION ALL
SELECT 'Last 14 days' AS PRE_DEF_NM,
       17 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       Format(DATEADD(day, -14, CurrentDate), 'yyyyMMdd') AS MIN_DT_SK,
       format(DATEADD(day, -1, CurrentDate), 'yyyyMMdd') AS MAX_DT_SK
FROM variables
UNION ALL
SELECT 'Tomorrow' AS PRE_DEF_NM,
       18 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       Format(DATEADD(day, 1, CurrentDate), 'yyyyMMdd') AS MIN_DT_SK,
       format(DATEADD(day, 1, CurrentDate), 'yyyyMMdd') AS MAX_DT_SK
FROM variables
UNION ALL

-- Period Related
SELECT 'This Pd' AS PRE_DEF_NM,
       19 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       PeriodBegin AS MIN_DT_SK,
       PeriodEnd AS MAX_DT_SK
FROM variables
    INNER JOIN pd_lookup
        ON (variables.PeriodId) = pd_lookup.PdId
UNION ALL
SELECT 'PTD' AS PRE_DEF_NM,
       20 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       PeriodBegin AS MIN_DT_SK,
       format(DATEADD(day, -1, CurrentDate), 'yyyyMMdd') AS MAX_DT_SK
FROM variables
    INNER JOIN pd_lookup
        ON (variables.PeriodId) = pd_lookup.PdId
UNION ALL
SELECT 'PTD Weeks' AS PRE_DEF_NM,
       21 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       PeriodBegin AS MIN_DT_SK,
       format(DATEADD(week, -1, WeekEnd), 'yyyyMMdd') AS MAX_DT_SK
FROM variables
    INNER JOIN pd_lookup
        ON (variables.PeriodId) = pd_lookup.PdId
UNION ALL
SELECT 'PTD Lbr Weeks' AS PRE_DEF_NM,
       22 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       CASE
           WHEN variables.isLaborWkClosed = 0
                AND DATEADD(week, -1, WeekStart) < MAX(PeriodBegin) THEN
               MIN(PeriodBegin)
           ELSE
               MAX(PeriodBegin)
       END AS MIN_DT_SK,
       CASE
           WHEN variables.isLaborWkClosed = 0
                AND DATEADD(week, -1, WeekStart) < MAX(PeriodBegin) THEN
               MIN(PeriodEnd)
           WHEN variables.isLaborWkClosed = 0
                AND DATEADD(week, -1, WeekStart) >= MAX(PeriodBegin) THEN
               format(MAX(DATEADD(week, -2, WeekEnd)), 'yyyyMMdd')
           ELSE
               format(MAX(DATEADD(week, -1, WeekEnd)), 'yyyyMMdd')
       END AS MAX_DT_SK
FROM variables
    INNER JOIN pd_lookup
        ON variables.PeriodId = pd_lookup.PdId
           OR (variables.PeriodId - 1) = pd_lookup.PdId
GROUP BY variables.isLaborWkClosed,
         WeekStart
UNION ALL
SELECT 'Last Pd' AS PRE_DEF_NM,
       23 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       PeriodBegin AS MIN_DT_SK,
       PeriodEnd AS MAX_DT_SK
FROM variables
    INNER JOIN pd_lookup
        ON (variables.PeriodId - 1) = pd_lookup.PdId
UNION ALL
SELECT 'Last 3 Pds' AS PRE_DEF_NM,
       24 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       MIN(PeriodBegin) AS MIN_DT_SK,
       MAX(PeriodEnd) AS MAX_DT_SK
FROM variables
    INNER JOIN pd_lookup
        ON pd_lookup.PdId >= (variables.PeriodId - 3)
           AND pd_lookup.PdId < variables.PeriodId
UNION ALL
SELECT 'Last 13 Pds' AS PRE_DEF_NM,
       25 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       MIN(PeriodBegin) AS MIN_DT_SK,
       MAX(PeriodEnd) AS MAX_DT_SK
FROM variables
    INNER JOIN pd_lookup
        ON pd_lookup.PdId >= (variables.PeriodId - 13)
           AND pd_lookup.PdId < variables.PeriodId
UNION ALL
SELECT 'Next Pd' AS PRE_DEF_NM,
       26 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       PeriodBegin AS MIN_DT_SK,
       PeriodEnd AS MAX_DT_SK
FROM variables
    INNER JOIN pd_lookup
        ON (variables.PeriodId + 1) = pd_lookup.PdId
UNION ALL

-- Qtr Related
SELECT 'This Qtr' AS PRE_DEF_NM,
       27 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       QuarterBegin AS MIN_DT_SK,
       QuarterEnd AS MAX_DT_SK
FROM variables
    INNER JOIN qtr_lookup
        ON (variables.QuarterId) = qtr_lookup.QtrId
UNION ALL
SELECT 'QTD' AS PRE_DEF_NM,
       28 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       QuarterBegin AS MIN_DT_SK,
       format(DATEADD(day, -1, CurrentDate), 'yyyyMMdd') AS MAX_DT_SK
FROM variables
    INNER JOIN qtr_lookup
        ON (variables.QuarterId) = qtr_lookup.QtrId
UNION ALL
SELECT 'QTD Weeks' AS PRE_DEF_NM,
       29 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       QuarterBegin AS MIN_DT_SK,
       format(DATEADD(week, -1, WeekEnd), 'yyyyMMdd') AS MAX_DT_SK
FROM variables
    INNER JOIN qtr_lookup
        ON (variables.QuarterId) = qtr_lookup.QtrId
UNION ALL
SELECT 'QTD Lbr Weeks' AS PRE_DEF_NM,
       30 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       CASE
           WHEN variables.isLaborWkClosed = 0
                AND DATEADD(week, -1, WeekStart) < MAX(QuarterBegin) THEN
               MIN(QuarterBegin)
           ELSE
               MAX(QuarterBegin)
       END AS MIN_DT_SK,
       CASE
           WHEN variables.isLaborWkClosed = 0
                AND DATEADD(week, -1, WeekStart) < MAX(QuarterBegin) THEN
               MIN(QuarterEnd)
           WHEN variables.isLaborWkClosed = 0
                AND DATEADD(week, -1, WeekStart) >= MAX(QuarterBegin) THEN
               format(MAX(DATEADD(week, -2, WeekEnd)), 'yyyyMMdd')
           ELSE
               format(MAX(DATEADD(week, -1, WeekEnd)), 'yyyyMMdd')
       END AS MAX_DT_SK
FROM variables
    INNER JOIN qtr_lookup
        ON variables.QuarterId = qtr_lookup.QtrId
           OR (variables.QuarterId - 1) = qtr_lookup.QtrId
GROUP BY variables.isLaborWkClosed,
         WeekStart
UNION ALL
SELECT 'Last Qtr' AS PRE_DEF_NM,
       31 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       QuarterBegin AS MIN_DT_SK,
       QuarterEnd AS MAX_DT_SK
FROM variables
    INNER JOIN qtr_lookup
        ON (variables.QuarterId - 1) = qtr_lookup.QtrId
UNION ALL

-- Fsc Year Related
SELECT 'This Fcl Yr' AS PRE_DEF_NM,
       32 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       FscYrBegin AS MIN_DT_SK,
       FscYrEnd AS MAX_DT_SK
FROM variables
    INNER JOIN fsc_lookup
        ON variables.FiscalYear = fsc_lookup.FscYr
UNION ALL
SELECT 'FYTD' AS PRE_DEF_NM,
       33 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       FscYrBegin AS MIN_DT_SK,
       format(DATEADD(day, -1, CurrentDate), 'yyyyMMdd')
FROM variables
    INNER JOIN fsc_lookup
        ON variables.FiscalYear = fsc_lookup.FscYr
UNION ALL
SELECT 'FYTD Lbr Weeks' AS PRE_DEF_NM,
       34 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       CASE
           WHEN variables.isLaborWkClosed = 0
                AND DATEADD(week, -1, WeekStart) < MAX(FscYrBegin) THEN
               MIN(FscYrBegin)
           ELSE
               MAX(FscYrBegin)
       END AS MIN_DT_SK,
       CASE
           WHEN variables.isLaborWkClosed = 0
                AND DATEADD(week, -1, WeekStart) < MAX(FscYrBegin) THEN
               MIN(FscYrEnd)
           WHEN variables.isLaborWkClosed = 0
                AND DATEADD(week, -1, WeekStart) >= MAX(FscYrBegin) THEN
               format(MAX(DATEADD(week, -2, WeekEnd)), 'yyyyMMdd')
           ELSE
               format(MAX(DATEADD(week, -1, WeekEnd)), 'yyyyMMdd')
       END AS MAX_DT_SK
FROM variables
    INNER JOIN fsc_lookup
        ON variables.FiscalYear = fsc_lookup.FscYr
           OR (variables.FiscalYear - 1) = fsc_lookup.FscYr
GROUP BY variables.isLaborWkClosed,
         WeekStart
UNION ALL
SELECT 'Last Fsc Year' AS PRE_DEF_NM,
       35 AS PRE_DEF_ID,
       'AGG' AS AGG_TYPE,
       FscYrBegin AS MIN_DT_SK,
       FscYrEnd AS MAX_DT_SK
FROM variables
    INNER JOIN fsc_lookup
        ON (variables.FiscalYear - 1) = fsc_lookup.FscYr
UNION ALL
SELECT *
FROM day_lookup;
