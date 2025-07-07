/****** Object:  StoredProcedure [EDAA_CNTL].[SP_HRLY_SLS_EMAIL_NOTIFICATION_ALERT]    Script Date: 7/22/2022 2:32:56 AM ******/

CREATE PROC [EDAA_CNTL].[SP_HRLY_SLS_EMAIL_NOTIFICATION_ALERT] @input [INT],@CURRENT_HOUR [INT] OUT,@STORE_COUNT [INT] OUT,@LAST_HOUR_SALE [VARCHAR](30) OUT,@CURRENT_SALE [VARCHAR](30) OUT AS

/*
 =============================================
 Author:        Shrikant Sakpal
 Create date:   07-May-2021
 Description:   This SP developed for Hourly Email Notification. Once data updated in CY Sales table Todtal sales calculated by DTD.
				This SP will return the Current Hour, Store Count and Current Sale Output parameters.
 =============================================
 Change History
 07-May-2021   SS      Initial Version
 13-May-2021   SS	   Added Last Hour Sale CTE and '08Output Parameter

*/


BEGIN
DECLARE @Cur_Hr INT = @input;
--DECLARE @Cur_Hr INT = DATEPART(HOUR,CONVERT(DATETIME,SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time'));
DECLARE @Cur_Date DATE = CONVERT(DATE,SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time');


WITH CTE_STR_CNT
AS
(
SELECT
@Cur_Hr AS Current_Hour
,COUNT(DISTINCT CY.Str_Id) as Cnt_Str_Id
FROM [EDAA_DW].[FCT_HRLY_PROD_CURR_YR_SLS] AS CY
JOIN [EDAA_DW].[DIM_GEO] AS GEO
ON (CY.Str_Id  = CASE WHEN (GEO.Div_Id = 'CS' OR GEO.Div_Id = 'MS' OR GEO.Div_Id = 'ES')
							AND GEO.Is_Curr_Ind = 1 THEN GEO.Str_Id ELSE NULL END AND
							CY.Str_Id = GEO.Str_Id)
WHERE GEO.Div_Id IN ('MS') AND
GEO.Str_Opn_Dt <=  @Cur_Date
			AND (GEO.Str_Cls_Dt >=  @Cur_Date or GEO.Str_Cls_Dt IS NULL)
AND GEO.Str_Id NOT IN (7, 73)
AND CONVERT(DATE,CY.DT_TM_HR) = @Cur_Date
AND DATEPART(HOUR,CY.DT_TM_HR) = @Cur_Hr - 1
),


CTE_LST_HR_SLS
AS
(
SELECT
@Cur_Hr AS Current_Hour
,SUM(CY.[Sls_Amt]) AS Last_Hour_Sales
FROM [EDAA_DW].[FCT_HRLY_PROD_CURR_YR_SLS] AS CY
JOIN [EDAA_DW].[DIM_PROD] AS PRD
ON CY.Prod_Hist_Sk = PRD.Prod_Hist_Sk
JOIN [EDAA_DW].[DIM_GEO] AS GEO
ON (CY.Str_Id  = CASE WHEN (GEO.Div_Id = 'CS' OR GEO.Div_Id = 'MS' OR GEO.Div_Id = 'ES')
							AND GEO.Is_Curr_Ind = 1 THEN GEO.Str_Id ELSE NULL END AND
							CY.Str_Id = GEO.Str_Id)
WHERE GEO.Div_Id IN ('MS')
AND GEO.Str_Opn_Dt <= @Cur_Date
			and (GEO.Str_Cls_Dt >=  @Cur_Date or GEO.Str_Cls_Dt is null)
AND GEO.Str_Id not in (7, 73)
AND PRD.Fnc_lvl4_area_id NOT IN ('M','G')
AND CY.Fnc_Lvl3_Pky_Id NOT IN (12, 63, 772, 825, 637, 45, 46, 75, 82, 775, 815, 18, 67, 68, 87, 96, 882, 22, 23, 25, 31, 32, 33, 35, 37, 54, 56, 58, 84, 95, 545, 905, 48, 51, 476, 486, 89, 827, 65, 877, 962, 57, 93, 215, 245, 315, 548, 38, 841)
AND CONVERT(DATE,CY.DT_TM_HR) = @Cur_Date
AND DATEPART(HOUR,CY.DT_TM_HR) = @Cur_Hr - 1
),


 CTE_DTD_SLS
AS
(
SELECT
@Cur_Hr AS Current_Hour
,SUM(CY.[Sls_Amt]) AS Total_Sales
FROM [EDAA_DW].[FCT_HRLY_PROD_CURR_YR_SLS] AS CY
JOIN [EDAA_DW].[DIM_PROD] AS PRD
ON CY.Prod_Hist_Sk = PRD.Prod_Hist_Sk
JOIN [EDAA_DW].[DIM_GEO] AS GEO
ON (CY.Str_Id  = CASE WHEN (GEO.Div_Id = 'CS' OR GEO.Div_Id = 'MS' OR GEO.Div_Id = 'ES')
							AND GEO.Is_Curr_Ind = 1 THEN GEO.Str_Id ELSE NULL END AND
							CY.Str_Id = GEO.Str_Id)
WHERE GEO.Div_Id IN ('MS')
AND GEO.Str_Opn_Dt <= @Cur_Date
			and (GEO.Str_Cls_Dt >=  @Cur_Date or GEO.Str_Cls_Dt is null)
AND GEO.Str_Id not in (7, 73)
AND PRD.Fnc_lvl4_area_id NOT IN ('M','G')
AND CY.Fnc_Lvl3_Pky_Id NOT IN (12, 63, 772, 825, 637, 45, 46, 75, 82, 775, 815, 18, 67, 68, 87, 96, 882, 22, 23, 25, 31, 32, 33, 35, 37, 54, 56, 58, 84, 95, 545, 905, 48, 51, 476, 486, 89, 827, 65, 877, 962, 57, 93, 215, 245, 315, 548, 38, 841)
AND CONVERT(DATE,CY.DT_TM_HR) = @Cur_Date
--AND DATEPART(HOUR,CY.DT_TM_HR) = @Cur_Hr - 1
)

SELECT
 @CURRENT_HOUR = C1.Current_Hour
,@STORE_COUNT = C1.Cnt_Str_Id
,@LAST_HOUR_SALE = ISNULL('$' + CAST(FORMAT (C2.Last_Hour_Sales,'N','en-US') AS VARCHAR(20)),'No Sale')
,@CURRENT_SALE = ISNULL('$' + CAST(FORMAT (C3.Total_Sales,'N','en-US') AS VARCHAR(20)),'No Sale')
FROM CTE_STR_CNT AS C1
JOIN CTE_LST_HR_SLS AS C2 ON C1.Current_Hour = C2.Current_Hour
JOIN CTE_DTD_SLS AS C3 ON C1.Current_Hour = C3.Current_Hour


SELECT @CURRENT_HOUR AS Current_Hour ,@STORE_COUNT AS Store_Count, @LAST_HOUR_SALE AS Last_Hour_Sales, @CURRENT_SALE AS Current_Sales;

END
GO
