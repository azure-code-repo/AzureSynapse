CREATE PROC [EDAA_ETL].[SP_DW_FCT_DLY_STR_FUEL_DLVY_DAILY_LOAD] AS

/*
 =============================================
 Author:        Manjula V
 Create date:   18-Oct-2021
 Description:   Stored proc to load the Fuel Delivery data from FuelQuest(kalibrate.csv)
				and OPIS(Custom_Meijer_Rack.csv) files to FCT_DLY_STR_FUEL_DLVY tble on EDAA_DW
 =============================================
 Change History
 18-Oct-2021   Manjula V      Initial version

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
,@EXEC_JOB  = 'SP_DW_FCT_DLY_STR_FUEL_DLVY_DAILY_LOAD'
,@SRC_ENTY  = 'DLY_FUEL_DLVY_HST'
,@TGT_ENTY = 'FCT_DLY_STR_FUEL_DLVY'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT


--------- Append data into Datawarehouse table -----
INSERT INTO [EDAA_DW].[FCT_DLY_STR_FUEL_DLVY]
(
		 [ENTY_NM]
		,[STR_ID]
		,[ITM_GRP_NM]
		,[FUEL_CST_TYP_NM]
		,[FUEL_CST_AMT]
		,[SRC_RW_GRTN_TMS]
		,[RW_PRCS_ID]
		,[GEO_HIST_SK]
		,[CAL_DT]
		,[DT_SK]
		,[AUD_INS_SK]
		,[AUD_UPD_SK]

)
SELECT [ENTY_NM]
      ,b.[STR_ID]
      ,[ITM_GRP_NM]
      ,[FUEL_CST_TYP_NM]
      ,[FUEL_CST_AMT]
      ,[SRC_RW_GRTN_TMS]
      ,[RW_PRCS_ID]
	  ,[GEO_HIST_SK]
	  ,CONVERT (date, GETDATE()) as cal_dt
	  ,[DT_SK]
	  ,@NEW_AUD_SKY [Aud_Ins_Sk]
      ,NULL AS [Aud_Upd_Sk]
  FROM [EDAA_STG].[DLY_FUEL_DLVY_HST] as a
  Inner join  [EDAA_DW].[DIM_GEO] as b on a.[STR_ID] = b.[STR_ID]
  and b.[vld_to]='2099-01-01'
  INNER JOIN [EDAA_DW].[DIM_DLY_CAL] c ON c.[CAL_DT]=CONVERT (date, GETDATE())


BEGIN
SELECT @NBR_OF_RW_ISRT = COUNT(1)  FROM [EDAA_DW].[FCT_DLY_STR_FUEL_DLVY] WHERE Aud_Ins_Sk = @NEW_AUD_SKY
SELECT @NBR_OF_RW_UDT  = COUNT(1)  FROM [EDAA_DW].[FCT_DLY_STR_FUEL_DLVY] WHERE Aud_Upd_Sk = @NEW_AUD_SKY

END

/*Audit Log End*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT  = @NBR_OF_RW_UDT

END TRY

BEGIN CATCH
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = 'SP_DW_FCT_DLY_STR_FUEL_DLVY_DAILY_LOAD'
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
------- stored proc execution should fail. The processing of the presentation layer

   THROW;




END CATCH
GO
