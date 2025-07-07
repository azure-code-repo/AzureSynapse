CREATE PROC [EDAA_ETL].[SP_DW_STG_FCT_HRLY_SLS_PLN] AS

/*
 =============================================
 Author:        Shrikant Sakpal
 Create date:   20-Apr-2021
 Description:   This SP Transform the data from DW to STG schema table on L3, Store and Hour Grain.
 =============================================
 Change History
 21-Apr-2021    SS      Updated the final CTE to replace View [EDAA_PRES].[V_PERF_BRF_DIM_PROD_L3_CURR] with table [EDAA_PRES].[DIM_PROD_L3]
 04-Jun-2021	SS		Updated the Final CTE to fix the Dim_PROD_L3 issue in Fact table.
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

DECLARE @CntrlCode1 VARCHAR(250)='L6';
DECLARE @CntrlValue1 VARCHAR(1000);

/*Audit Log Start*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START
 @EXEC_LYR  = 'EDAA_STG'
,@EXEC_JOB  = 'SP_DW_STG_FCT_HRLY_SLS_PLN'
,@SRC_ENTY  = 'EDAA_DW.FCT_HRLY_PROD_SUB_CTGRY_SLS_PLN'
,@TGT_ENTY = 'EDAA_STG.FCT_DW_STG_HRLY_SLS_PLN'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT


BEGIN

------------ Truncate and Load the Table at every call of Stored Procedure-------

TRUNCATE TABLE [EDAA_STG].[FCT_DW_STG_HRLY_SLS_PLN];

--------- SET [Lvl6_Mds_Area_Id] & [Lvl2_Prod_Clsfctn_Id] Value------------------


SELECT @CntrlValue1=CntrlValue FROM EDAA_CNTL.ETL_KEY_VAL_PAIRS WHERE CntrlCode=@CntrlCode1;

WITH CTE
AS
(
	SELECT	  /*Calculates MPRS hourly by taking % of MPRS within PKY_ID and multiplying to Hourly PKY_ID plan*/
		 AA.Dt_Tm_hr
		,AA.Fnc_Lvl3_Pky_Id
		,AA.Fnc_Lvl2_Mprs_Ctgry_Id
		,AA.Lvl3_Prod_Sub_Ctgry_Id
		,AA.MJR_P_SUB_CT_ID
		,c.Str_Id
		,CAST(c.Sls_Amt AS DECIMAL(20,12)) * COALESCE(AA.MPRS_PERCENT,0) AS MPRS_DAY_UT_PLN_AM   /*Breaks down hourly PKY_ID plan to MPRS level by multiplying by the % MPRS of the PKY_ID for the Daily plan*/
	FROM (
			SELECT	/*Brings in % of MPRS within each PKY_ID for plan day*/
			  CONVERT(DATETIME2(0),a11.Dt_Tm_hr) AS Dt_Tm_hr
			 ,a11.Fnc_Lvl3_Pky_Id
			 ,a11.Fnc_Lvl2_Mprs_Ctgry_Id
			 ,a11.Lvl3_Prod_Sub_Ctgry_Id
			 ,B.MJR_P_SUB_CT_ID
			 ,CASE WHEN SUM(Sls_Amt) OVER(PARTITION BY CONVERT(DATETIME2(0),a11.Dt_Tm_hr),Fnc_Lvl3_Pky_Id) = 0 THEN 0
				   ELSE CAST(Sls_Amt AS DECIMAL(20,12))/SUM(CAST(Sls_Amt AS DECIMAL(20,12))) OVER(PARTITION BY CONVERT(DATETIME2(0),a11.Dt_Tm_hr),Fnc_Lvl3_Pky_Id)
			  END AS MPRS_PERCENT /*Calculates the MPRS % of the PKY_ID*/
			 ,a11.Sls_Amt AS MPRS_TOTAL
			 ,SUM(Sls_Amt) OVER(PARTITION BY Fnc_Lvl3_Pky_Id) AS PKY_TOTAL
		    FROM	[EDAA_DW].[FCT_HRLY_PROD_SUB_CTGRY_SLS_PLN] a11
			INNER JOIN [EDAA_REF].[BYR_EMP_PKY_MPRS_CURRPREV_INF] AS B
			ON a11.Fnc_Lvl3_Pky_Id = B.PKY_ID AND a11.Fnc_Lvl2_Mprs_Ctgry_Id = B.MPRS_CT_ID
		    INNER JOIN [EDAA_ETL].[V_STG_DW_DAY_DT_HR_INF] AS CAL
		    ON CONVERT(DATETIME2(0),a11.Dt_Tm_hr) = CAL.DT_TM_HR
		    GROUP BY
			  CONVERT(DATETIME2(0),a11.Dt_Tm_hr)
			 ,a11.Fnc_Lvl3_Pky_Id
			 ,a11.Fnc_Lvl2_Mprs_Ctgry_Id
			 ,a11.Lvl3_Prod_Sub_Ctgry_Id
			 ,B.MJR_P_SUB_CT_ID
			 ,a11.Sls_Amt
		 ) AS AA
	INNER JOIN [EDAA_ETL].[V_STG_DW_DAY_DT_HR_INF] as b11
	ON b11.DT_TM_HR=AA.DT_TM_HR
	INNER JOIN [EDAA_DW].[FCT_HRLY_PKY_SLS_PLN] c
	ON AA.Fnc_Lvl3_Pky_Id = c.Fnc_Lvl3_Pky_Id AND  c.DT_TM_HR=AA.DT_TM_HR
),

CTE_FINAL
AS
(
	SELECT
	  CTE.Dt_Tm_hr,
	  CTE.Lvl3_Prod_Sub_Ctgry_Id,
	  CTE.MJR_P_SUB_CT_ID,
	  CTE.Str_Id,
	  Cast(Coalesce(Sum(CTE.MPRS_DAY_UT_PLN_AM),0) as DEC(16,2)) as Pln_Sls_Amt
	FROM CTE
	--[EDAA_PRES].[V_PERF_BRF_DIM_PROD_L3_CURR] P	-- Fix for SP performance
	INNER JOIN --[EDAA_PRES].[DIM_PROD_L3] AS P     -- Fix for Duplicate L3 issue in PROD_L3
	(SELECT
      DISTINCT
       [Lvl3_Prod_Sub_Ctgry_Id]
      ,[Lvl6_MDSArea_Id]
	  ,Is_Curr_Ind
	FROM [EDAA_PRES].[Dim_Prod_L3]
	WHERE [Is_Curr_Ind] = 1) P
	ON CTE.Lvl3_Prod_Sub_Ctgry_Id = P.Lvl3_Prod_Sub_Ctgry_Id  AND P.Is_Curr_Ind = 1
	WHERE P.[Lvl6_MDSArea_Id] IN (SELECT VALUE FROM STRING_SPLIT(@CntrlValue1, ','))
	AND CTE.MPRS_DAY_UT_PLN_AM <> 0
	GROUP BY
	  CTE.Dt_Tm_hr,
	  CTE.Lvl3_Prod_Sub_Ctgry_Id,
	  CTE.MJR_P_SUB_CT_ID,
	  CTE.Str_Id
)


-- Final Select Query

INSERT INTO [EDAA_STG].[FCT_DW_STG_HRLY_SLS_PLN](
	 [Dt_Tm_hr]
	,[Lvl3_Prod_Sub_Ctgry_Id]
	,[Str_Id]
	,[Pln_Sls_Amt]
	,Aud_Ins_Sk
	,Aud_Upd_Sk
	)

SELECT
	 [Dt_Tm_hr]
	--,[Lvl3_Prod_Sub_Ctgry_Id]
	,MJR_P_SUB_CT_ID
	,[Str_Id]
	,Pln_Sls_Amt
	,@NEW_AUD_SKY 	AS Aud_Ins_Sk
	,NULL 			AS Aud_Upd_Sk
FROM
	CTE_FINAL


END ;

BEGIN
SELECT @NBR_OF_RW_ISRT = COUNT(1)  FROM [EDAA_STG].[FCT_DW_STG_HRLY_SLS_PLN] WHERE Aud_Ins_Sk = @NEW_AUD_SKY
SELECT @NBR_OF_RW_UDT  = COUNT(1)  FROM [EDAA_STG].[FCT_DW_STG_HRLY_SLS_PLN] WHERE Aud_Upd_Sk = @NEW_AUD_SKY

------------ UPDATE Statistics-------

UPDATE STATISTICS  [EDAA_STG].[FCT_DW_STG_HRLY_SLS_PLN];

END

/*Audit Log End*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT  = @NBR_OF_RW_UDT

END TRY
BEGIN CATCH

DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(250) = 'EDAA_ETL.SP_DW_STG_FCT_HRLY_SLS_PLN'
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
