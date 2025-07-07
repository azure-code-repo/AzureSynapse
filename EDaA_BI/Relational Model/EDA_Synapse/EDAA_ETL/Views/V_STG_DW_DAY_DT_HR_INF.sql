
CREATE VIEW [EDAA_ETL].[V_STG_DW_DAY_DT_HR_INF] AS WITH CTE
AS
(
SELECT
	 A.DT_TM_HR
	,A.CAL_DT
	,A.DY_TM_INTRVL_ID
	,B.Cal_Dt_Nm
	,B.Dy_Of_Wk_Nm
FROM [EDAA_DW].[V_HOURLY_CAL] AS A
INNER JOIN [EDAA_DW].[DIM_DLY_CAL] AS B
    ON B.CAL_DT=A.CAL_DT
/*Checks to see if current time is between 0 and 1 (midnight and 1am), if yes returns yesterday, else today*/
WHERE A.CAL_DT = (CASE WHEN (SELECT DATEPART(HOUR,SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time')) = 0
					   THEN CONVERT(DATE, DATEADD(DAY,-1,SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time'))
					   ELSE CONVERT(DATE,SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time')
                  END)
				  ),

CTE_CLDR AS (
SELECT DISTINCT
 A.DT_TM_HR
,A.CAL_DT
,A.DY_TM_INTRVL_ID
FROM (SELECT A.*
    FROM [EDAA_DW].[V_HOURLY_CAL] AS A
    INNER JOIN [EDAA_DW].[DIM_DLY_CAL] AS B
    ON B.Lst_Yr_Cal_Dt=A.CAL_DT
	/*Checks to see if current time is between 0 and 1 (midnight and 1am), if yes returns yesterday, else today*/
    WHERE B.CAL_DT = (CASE WHEN (SELECT DATEPART(HOUR,SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time')) = 0
					   THEN CONVERT(DATE, DATEADD(DAY,-1,SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time'))
					   ELSE CONVERT(DATE,SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time')
                  END)) AS A
				  ),

CTE_FSC AS
(
SELECT DISTINCT
 A.DT_TM_HR
,A.CAL_DT
,A.DY_TM_INTRVL_ID
FROM (SELECT A.*
    FROM [EDAA_DW].[V_HOURLY_CAL] AS A
    INNER JOIN [EDAA_DW].[DIM_DLY_CAL] AS B
    ON B.Lst_Yr_Fsc_Dt=A.CAL_DT
	/*Checks to see if current time is between 0 and 1 (midnight and 1am), if yes returns yesterday, else today*/
    WHERE B.CAL_DT = (CASE WHEN (SELECT DATEPART(HOUR,SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time')) = 0
					   THEN CONVERT(DATE, DATEADD(DAY,-1,SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time'))
					   ELSE CONVERT(DATE,SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time')
                  END)) AS A
),

CTE_HLDY AS
(
SELECT DISTINCT
 A.DT_TM_HR
,A.CAL_DT
,A.DY_TM_INTRVL_ID
,A.Hldy_Nm
FROM (SELECT A.*, B.Hldy_Nm
    FROM [EDAA_DW].[V_HOURLY_CAL] AS A
    INNER JOIN [EDAA_DW].[DIM_DLY_CAL] AS B
    ON B.Lst_Yr_Hldy_Dt=A.CAL_DT
	/*Checks to see if current time is between 0 and 1 (midnight and 1am), if yes returns yesterday, else today*/
    WHERE B.CAL_DT = (CASE WHEN (SELECT DATEPART(HOUR,SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time')) = 0
					   THEN CONVERT(DATE, DATEADD(DAY,-1,SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time'))
					   ELSE CONVERT(DATE,SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time')
                  END)) AS A
)

SELECT A.DT_TM_HR

	,B.DT_TM_HR AS LST_YR_CAL_DT_TM_HR
	,C.DT_TM_HR AS LST_YR_FSC_DT_TM_HR
	,ISNULL(D.DT_TM_HR,B.DT_TM_HR) AS LST_YR_HLDY_DT_TM_HR
	,A.CAL_DT
	,A.DY_TM_INTRVL_ID
	,D.Hldy_NM AS HLDY_DT_DESC
	,A.CAL_DT_NM
	,A.DY_OF_WK_NM
	FROM CTE AS A
	INNER JOIN CTE_CLDR AS B
	ON A.DY_TM_INTRVL_ID = B.DY_TM_INTRVL_ID
	INNER JOIN CTE_FSC AS C
	ON A.DY_TM_INTRVL_ID = C.DY_TM_INTRVL_ID
	LEFT JOIN CTE_HLDY AS D
	ON A.DY_TM_INTRVL_ID = D.DY_TM_INTRVL_ID;
