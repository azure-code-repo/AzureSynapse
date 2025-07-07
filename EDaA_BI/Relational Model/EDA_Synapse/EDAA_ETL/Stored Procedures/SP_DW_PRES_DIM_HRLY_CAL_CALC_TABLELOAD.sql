CREATE PROC [EDAA_ETL].[SP_DW_PRES_DIM_HRLY_CAL_CALC_TABLELOAD] AS

/*
 =============================================
 Author:        Shrikant Sakpal
 Create date:   20-Apr-2021
 Description:   Stored proc truncate and load Hourly Calendar presnetation dimension table
				with corresponding last year calendar, fiscal and holiday dates
				and Pre-defined filter which is calculated in EDAA_ETL.V_DIM_HRLY_CAL_CALC View
				based on Current Year Fact load.
 =============================================
 Change History
 20-Apr-2021   SS      Initial version

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
 @EXEC_LYR  = 'EDAA_PRES'
,@EXEC_JOB  = 'SP_DW_PRES_DIM_HRLY_CAL_CALC_TABLELOAD'
,@SRC_ENTY  = 'V_DIM_HRLY_CAL_CALC'
,@TGT_ENTY = 'DIM_HRLY_CAL_CALC'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT



TRUNCATE TABLE [EDAA_PRES].[DIM_HRLY_CAL_CALC]

--------- Insert data into Presentation table -----
INSERT INTO [EDAA_PRES].[DIM_HRLY_CAL_CALC]
(
       [Dt_Tm_Hr]
      ,[Cal_Typ_Id]
      ,[Pre_Def_Fltr_Id]
      ,[Cal_Dt]
      ,[Cal_Dt_Nm]
      ,[Lst_Yr_Fsc_Dt]
      ,[Lst_Yr_Hldy_Dt]
      ,[Lst_Yr_Cal_Dt]
      ,[Hldy_Dt_Desc]
      ,[Dy_Of_Wk_Nm]
      ,[Cal_Typ]
      ,[Dy_Tm_Intrvl_Id]
      ,[Pre_Def_Fltr]
      ,[Aud_Ins_Sk]
      ,[Aud_Upd_Sk]

)
SELECT
		[Dt_Tm_Hr]
      ,cldr_typ_ordr_id
      ,[Pre_Def_Fltr_Id]
      ,[Cal_Dt]
      ,[Cal_Dt_Nm]
      ,[Lst_Yr_Fsc_Dt_Tm_Hr]
      ,[Lst_Yr_Hldy_Dt_Tm_Hr]
      ,[Lst_Yr_Cal_Dt_Tm_Hr]
      ,[Hldy_Dt_Desc]
      ,[Dy_Of_Wk_Nm]
      ,cldr_typ_ct
      ,[Dy_Tm_Intrvl_Id]
      ,[Pre_Def_Fltr]
      ,@NEW_AUD_SKY [Aud_Ins_Sk]
      ,NULL AS [Aud_Upd_Sk]
FROM 	EDAA_ETL.V_DIM_HRLY_CAL_CALC

BEGIN
SELECT @NBR_OF_RW_ISRT = COUNT(1)  FROM [EDAA_PRES].[DIM_HRLY_CAL_CALC] WHERE Aud_Ins_Sk = @NEW_AUD_SKY
SELECT @NBR_OF_RW_UDT  = COUNT(1)  FROM [EDAA_PRES].[DIM_HRLY_CAL_CALC] WHERE Aud_Upd_Sk = @NEW_AUD_SKY

END

/*Audit Log End*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT  = @NBR_OF_RW_UDT

END TRY

BEGIN CATCH
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = 'SP_DW_PRES_DIM_HRLY_CAL_CALC_TABLELOAD'
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
