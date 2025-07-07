CREATE PROC [EDAA_ETL].[PROC_DIM_PROMO_VHL_DATA_LOAD] AS
BEGIN

  --EXEC  [EDAA_ETL].[PROC_FCT_PROMO_VHL_DATA_LOAD]

  BEGIN TRY
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON;

    /*DATAMART AUDITING VARIABLES*/
    DECLARE @NEW_AUD_SKY bigint;
    DECLARE @NBR_OF_RW_ISRT int;
    DECLARE @NBR_OF_RW_UDT int;
    DECLARE @EXEC_LYR varchar(255);
    DECLARE @EXEC_JOB varchar(500);
    DECLARE @SRC_ENTY varchar(500);
    DECLARE @TGT_ENTY varchar(500);
    DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = 'EDAA_ETL.PROC_DIM_PROMO_VHL_DATA_LOAD';
    DECLARE @ERROR_LINE AS int;
    DECLARE @ERROR_MSG AS nvarchar(max);
    DECLARE @DELETE_LAST_HOUR int;

    /*AUDIT LOG START*/
    EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR = 'EDAA_DW',
                                   @EXEC_JOB = 'EDAA_ETL.PROC_DIM_PROMO_VHL_DATA_LOAD',
                                   @SRC_ENTY = 'EDAA_STG.DIM_PROMO_VHL',
                                   @TGT_ENTY = 'EDAA_DW.DIM_PROMO_VHL',
                                   @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

    --Tuncate & Load in Delta table
    TRUNCATE TABLE [EDAA_DW].[DIM_PROMO_VHL]
    INSERT INTO [EDAA_DW].[DIM_PROMO_VHL] (
	   [Promo_Vhl_Id]
      ,[Promo_Vhl_Ct]
      ,[Promo_Vhl_Ct_Dsc]
      ,[Promo_Vhl_Struc_Id]
      ,[Promo_Vhl_Prt_Ind]
	)

      SELECT
        CAST([PromotionVehicleId] as INT) as [PromotionVehicleId],
        [PromotionVehicleCategory],
        [PromotionVehicleCategoryDescription],
        CAST([PromotionVehicleStructureId] as INT) as [PromotionVehicleStructureId],
        [PromotionVehiclePrintIndicator]
      FROM [EDAA_STG].[DIM_PROMO_VHL]

    PRINT 'Data Load completed..'


  END TRY

  BEGIN CATCH

    SELECT
      @ERROR_LINE = ERROR_NUMBER(),
      @ERROR_MSG = ERROR_MESSAGE();

    --------- Log execution error ----------
    EXEC EDAA_CNTL.SP_LOG_AUD_ERR @AUD_SKY = @NEW_AUD_SKY,
                                  @ERROR_PROCEDURE_NAME = @ERROR_PROCEDURE_NAME,
                                  @ERROR_LINE = @ERROR_LINE,
                                  @ERROR_MSG = @ERROR_MSG;

    -- Detect the change
    THROW;
  END CATCH

END
