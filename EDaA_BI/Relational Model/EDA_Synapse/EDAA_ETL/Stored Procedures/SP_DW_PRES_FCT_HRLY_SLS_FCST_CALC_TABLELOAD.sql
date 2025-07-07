
CREATE PROC [EDAA_ETL].[SP_DW_PRES_FCT_HRLY_SLS_FCST_CALC_TABLELOAD] AS

/*
 =============================================
 Author:        Shrikant Sakpal
 Create date:   20-Mar-2021
 Description:   This SP Truncate and Load daily to trasform the  Forecast data on Store and Hour grain and load to PRES Fact table.
				and load to PRES Fact table. Data volumne ~70k
				It will update the stats of the table after insert
 =============================================
 Change History
 20-Mar-2021	SS      Initial Version
 15-Apr-2021	SS		Updated the type Id and Dept Id condition in CTE.
 28-Apr-2021	SS		Updated SP to remove Surrogate key condition form CTE Join

*/

BEGIN TRY

SET NOCOUNT ON
SET XACT_ABORT ON

/*Datamart Auditing variables*/
DECLARE @NEW_AUD_SKY BIGINT ;
DECLARE @NBR_OF_RW_ISRT INT ;
DECLARE @NBR_OF_RW_UDT INT ;
DECLARE @EXEC_LYR VARCHAR(255) ;
DECLARE @EXEC_JOB VARCHAR(500) ;
DECLARE @SRC_ENTY VARCHAR(500) ;
DECLARE @TGT_ENTY VARCHAR(500) ;

/*Audit Log Start*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START
 @EXEC_LYR  = 'EDAA_PRES'
,@EXEC_JOB  = 'SP_DW_PRES_FCT_HRLY_SLS_FCST_CALC'
,@SRC_ENTY  = 'EDAA_STG.FCT_HRLY_SLS_FCST_CALC'
,@TGT_ENTY = 'FCT_HRLY_SLS_FCST_CALC'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

BEGIN
------------ Truncate and Load the Table at every call of Stored Procedure-------

TRUNCATE TABLE EDAA_PRES.FCT_HRLY_SLS_FCST_CALC;

--------------CTE Start------------------------

WITH CTE_DlySales
AS
(
SELECT
DlySlsFct.Dt_Sk,
ISNULL(Geo.Geo_Hist_Sk, -1) AS Geo_Hist_Sk,
ISNULL(Geo.Geo_Sk, -1) AS Geo_Sk,
DlySlsFct.Str_Id,
SUM(DlySlsFct.Sls_Amt) AS Sls_Amt
FROM
[EDAA_DW].[FCT_DLY_LBR_SLS_FCST] AS DlySlsFct
INNER JOIN [EDAA_DW].[DIM_DLY_CAL] AS dt ON dt.Dt_Sk = DlySlsFct.Dt_Sk AND DlySlsFct.Dt_Sk = CONVERT(CHAR(8),(SELECT DISTINCT CAL_DT FROM [EDAA_ETL].[V_STG_DW_DAY_DT_HR_INF]),112)
LEFT JOIN EDAA_DW.DIM_GEO AS Geo
	ON  DlySlsFct.Str_Id = Geo.Str_id
	AND Geo.Is_Curr_Ind = 1
	WHERE  DlySlsFct.Typ_Id = 300
	AND DlySlsFct.Sch_Dpt_Id NOT IN (44,63,79,310,311,312,313,314,315,316,317,318,319,320,379)
GROUP BY
DlySlsFct.Dt_Sk,
Geo.Geo_Hist_Sk,
Geo.Geo_Sk,
DlySlsFct.Str_Id
),
CTE_HrlySales
AS
(
SELECT
HrlySales.Dt_Sk,
ISNULL(Geo.Geo_Hist_Sk, -1) AS Geo_Hist_Sk,
ISNULL(Geo.Geo_Sk, -1) AS Geo_Sk,
HrlySales.Str_Id,
CONVERT(DATETIME2(0),HrlySales.Dt_Tm_Hr) AS Dt_Tm_Hr,
SUM(HrlySales.Sls_Amt) AS Pln_Sls_Amt
FROM
[EDAA_DW].[FCT_HRLY_PKY_SLS_PLN] AS HrlySales
INNER JOIN [EDAA_DW].[DIM_DLY_CAL] AS dt ON dt.Dt_Sk = HrlySales.Dt_Sk AND HrlySales.Dt_Sk = CONVERT(CHAR(8),(SELECT DISTINCT CAL_DT FROM [EDAA_ETL].[V_STG_DW_DAY_DT_HR_INF]),112)
LEFT JOIN EDAA_DW.DIM_GEO Geo
	ON  HrlySales.Str_Id = Geo.Str_id
	AND Geo.Is_Curr_Ind = 1
GROUP BY
HrlySales.Dt_Sk,
Geo.Geo_Hist_Sk,
Geo.Geo_Sk,
HrlySales.Str_Id,
HrlySales.Dt_Tm_Hr
)
INSERT INTO EDAA_PRES.FCT_HRLY_SLS_FCST_CALC
(
	[Geo_Hist_Sk],
	[Dt_Tm_Hr],
	[Geo_Sk],
	[Str_Id],
	[Fcst_Sls_Amt],
	[Aud_Ins_Sk],
	[Aud_Upd_Sk]
)
SELECT
Ds.Geo_Hist_Sk,
Hs.Dt_Tm_Hr,
Ds.Geo_Sk,
Ds.Str_Id,
Ds.Sls_Amt * ((Hs.Pln_Sls_Amt * 100.00) /ISNULL((SUM(Hs.Pln_Sls_Amt) OVER(PARTITION BY HS.Dt_Sk, Hs.Str_Id)), 0)) / 100 AS Fcst_Sls_Amt,
@NEW_AUD_SKY AS Aud_Ins_Sk,
NULL AS Aud_Upd_Sk
FROM
CTE_DlySales AS Ds
	INNER JOIN CTE_HrlySales AS Hs
		ON Ds.Dt_Sk=HS.Dt_Sk  AND Ds.Str_Id=Hs.Str_Id
WHERE Hs.Pln_Sls_Amt>0

END ;

BEGIN
SELECT @NBR_OF_RW_ISRT = COUNT(1)  FROM EDAA_PRES.FCT_HRLY_SLS_FCST_CALC WHERE Aud_Ins_Sk = @NEW_AUD_SKY;
SELECT @NBR_OF_RW_UDT  = COUNT(1)  FROM EDAA_PRES.FCT_HRLY_SLS_FCST_CALC WHERE Aud_Upd_Sk = @NEW_AUD_SKY;

------------ UPDATE Statistics-------

UPDATE STATISTICS  EDAA_PRES.FCT_HRLY_SLS_FCST_CALC;

END

/*Audit Log End*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT  = @NBR_OF_RW_UDT

END TRY
BEGIN CATCH

DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(250) = 'EDAA_ETL.SP_STG_DW_FACT_FCT_WKND_PKY_MPRK_ADJ_TABLELOAD'
DECLARE @ERROR_LINE AS INT;
DECLARE @ERROR_MSG AS NVARCHAR(MAX);

 SELECT
      @ERROR_LINE =  ERROR_NUMBER()
     ,@ERROR_MSG = ERROR_MESSAGE();
----------- Log execution error ----------

EXEC EDAA_CNTL.SP_LOG_AUD_ERR
@AUD_SKY = @NEW_AUD_SKY,
@ERROR_PROCEDURE_NAME = @ERROR_PROCEDURE_NAME,
@ERROR_LINE = @ERROR_LINE,
@ERROR_MSG = @ERROR_MSG;

---- Detect the change

   THROW;
END CATCH
GO
