CREATE PROC [EDAA_ETL].[SP_STG_DW_FCT_HRLY_PROD_SUB_CTGRY_SLS_PLN_TABLELOAD] AS

/*
 =============================================
 Author:        Lubov
 Create date:   20-Apr-2021
 Description:   Stored proc truncate and load Hourly Product Sub-Category Sales Plan table.
 =============================================
 Change History
 20-Apr-2021   LB      Initial version

*/

BEGIN TRY

SET NOCOUNT ON
SET XACT_ABORT ON

/*Datamart Auditing variables*/
DECLARE @NEW_AUD_SKY BIGINT
DECLARE @NBR_OF_RW_ISRT INT
DECLARE @NBR_OF_RW_UDT INT
DECLARE @EXEC_LYR VARCHAR(255)
DECLARE @EXEC_JOB VARCHAR(500)
DECLARE @SRC_ENTY VARCHAR(500)
DECLARE @TGT_ENTY VARCHAR(500)

/*Audit Log Start*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START
 @EXEC_LYR  = 'EDAA_DW'
,@EXEC_JOB  = 'SP_STG_DW_FCT_HRLY_PROD_SUB_CTGRY_SLS_PLN_TABLELOAD'
,@SRC_ENTY  = 'EDAA_STG.FCT_HRLY_PROD_SUB_CTGRY_SLS_PLN'
,@TGT_ENTY = 'FCT_HRLY_PROD_SUB_CTGRY_SLS_PLN'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

INSERT INTO EDAA_DW.FCT_HRLY_PROD_SUB_CTGRY_SLS_PLN
(
[Dt_Tm_Hr]
      ,[Lvl3_Prod_Sub_Ctgry_Id]
      ,[Fnc_Lvl2_Mprs_Ctgry_Id]
      ,[Fnc_Lvl3_Pky_Id]



      ,[Sls_Amt]
      ,[Drct_Mgn_Amt]
      ,[Sls_Promo_Amt]
      ,[Same_Str_Amt]
      ,[Aud_Ins_Sk]
      ,[Aud_Upd_Sk]
)
SELECT

[Dt_Tm_Hr]
      ,[MJR_P_SUB_CT_ID] as [Lvl3_Prod_Sub_Ctgry_Id]
      ,[Fnc_Lvl2_Mprs_Ctgry_Id]
      ,[Fnc_Lvl3_Pky_Id]


      ,[Sls_Amt]
      ,[Drct_Mgn_Amt]
      ,[Sls_Promo_Amt]
      ,[Same_Str_Amt]

	 ,@NEW_AUD_SKY	AS Aud_Ins_Sk
	 ,NULL 			AS Aud_Upd_Sk
FROM  EDAA_STG.FCT_HRLY_PROD_SUB_CTGRY_SLS_PLN pln
INNER JOIN (

             SELECT [MJR_P_SUB_CT_ID], [MPRS_CT_ID], [PKY_ID]
             FROM [EDAA_REF].[BYR_EMP_PKY_MPRS_CURRPREV_INF]
             WHERE CAST(INS_DT AS DATE) = (SELECT CAST(MAX(INS_DT) AS DATE) AS INS_DT
                                                FROM [EDAA_REF].BYR_EMP_PKY_MPRS_CURRPREV_INF)

			 ) AS brdg
ON brdg.[PKY_ID] = pln.[Fnc_Lvl3_Pky_Id] AND brdg.[MPRS_CT_ID] = pln.[Fnc_Lvl2_Mprs_Ctgry_Id]





BEGIN
SELECT @NBR_OF_RW_ISRT = COUNT(1)  FROM EDAA_DW.FCT_HRLY_PROD_SUB_CTGRY_SLS_PLN WHERE Aud_Ins_Sk = @NEW_AUD_SKY
SELECT @NBR_OF_RW_UDT  = COUNT(1)  FROM EDAA_DW.FCT_HRLY_PROD_SUB_CTGRY_SLS_PLN WHERE Aud_Upd_Sk = @NEW_AUD_SKY

------------ UPDATE Statistics-------

UPDATE STATISTICS  EDAA_DW.FCT_HRLY_PROD_SUB_CTGRY_SLS_PLN;

END

/*Audit Log End*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT  = @NBR_OF_RW_UDT

END TRY

BEGIN CATCH
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = 'EDAA_ETL.SP_STG_DW_FCT_HRLY_PROD_SUB_CTGRY_SLS_PLN_TABLELOAD'
DECLARE @ERROR_LINE AS INT;
DECLARE @ERROR_MSG AS NVARCHAR(max);

 SELECT
      @ERROR_LINE =  ERROR_NUMBER()
       ,@ERROR_MSG = ERROR_MESSAGE();
--------- Log execution error ----------



EXEC EDAA_CNTL.SP_LOG_AUD_ERR
@AUD_SKY = @NEW_AUD_SKY,
@ERROR_PROCEDURE_NAME = @ERROR_PROCEDURE_NAME,
@ERROR_LINE = @ERROR_LINE,
@ERROR_MSG = @ERROR_MSG;

-- Detect the change


   THROW;




END CATCH
