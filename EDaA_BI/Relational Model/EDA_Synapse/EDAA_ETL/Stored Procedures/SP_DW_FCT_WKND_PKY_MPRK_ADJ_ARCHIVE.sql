CREATE PROC [EDAA_ETL].[SP_DW_FCT_WKND_PKY_MPRK_ADJ_ARCHIVE] AS
/*
 =============================================
 Author:        Arindam Pathak
 Create date:   15-Apr-2021
 Description:   This SP Archive the previous day data to Archive table before
				truncating the DW fact table for current day 1st load
 =============================================
 Change History
 15-Apr-2021	AP      Initial Version

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
,@EXEC_JOB  = 'SP_DW_FCT_WKND_PKY_MPRK_ADJ_ARCHIVE'
,@SRC_ENTY  = 'FCT_WKND_PKY_MPRK_ADJ'
,@TGT_ENTY = 'FCT_WKND_PKY_MPRK_ADJ_ARCHIVE'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT
;


INSERT INTO EDAA_DW.FCT_WKND_PKY_MPRK_ADJ_ARCHIVE(
		Fnc_Lvl3_Pky_Id
		,Dt_Sk
		,Mprk_Adj_Prcntg
		,Aud_Ins_Sk
        ,Aud_Upd_Sk
	    ,Row_Archv_ts)


	SELECT
		 Fnc_Lvl3_Pky_Id
		,Dt_Sk
		,Mprk_Adj_Prcntg
        ,Aud_Ins_Sk
        ,Aud_Upd_Sk
	   ,GETUTCDATE()
	FROM EDAA_DW.FCT_WKND_PKY_MPRK_ADJ;
  TRUNCATE TABLE EDAA_STG.FCT_WKND_PKY_MPRK_ADJ;







  /*Audit Log End*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = 0, @NBR_OF_RW_UDT  = 0

END TRY

BEGIN CATCH
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = 'EDAA_ETL.SP_DW_FCT_WKND_PKY_MPRK_ADJ_ARCHIVE'
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
GO
