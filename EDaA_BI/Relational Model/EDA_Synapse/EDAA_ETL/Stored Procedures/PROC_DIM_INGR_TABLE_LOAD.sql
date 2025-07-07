CREATE PROC [EDAA_ETL].[PROC_DIM_INGR_TABLE_LOAD] AS
BEGIN
  --EXEC [EDAA_ETL].[PROC_DIM_INGR_TABLE_LOAD]
  -- SET NOCOUNT ON added to prevent extra result sets from
  -- interfering with SELECT statements.
  SET NOCOUNT ON

  BEGIN TRY

    /*DATAMART AUDITING VARIABLES*/
    DECLARE @NEW_AUD_SKY bigint
    DECLARE @NBR_OF_RW_ISRT int
    DECLARE @NBR_OF_RW_UDT int
    DECLARE @EXEC_LYR varchar(255)
    DECLARE @EXEC_JOB varchar(500)
    DECLARE @SRC_ENTY varchar(500)
    DECLARE @TGT_ENTY varchar(500)

    /*AUDIT LOG START*/
    EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR = 'EDAA_ETL',
                                            @EXEC_JOB = 'EDAA_ETL.PROC_DIM_INGR_TABLE_LOAD',
                                            @SRC_ENTY = 'EDAA_REF.DIM_INGR',
                                            @TGT_ENTY = 'EDAA_DW.DIM_INGR',
                                            @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT


    /* Insert new records in DIM_Ingr*/

    BEGIN

      INSERT INTO [EDAA_DW].[DIM_INGR] ([INGR_ID], [INGR_NM], [INGR_DSC], [AUD_INS_SK])
      SELECT
                [INGR_ID],
                [INGR_NM],
                [INGR_DSC],
                @NEW_AUD_SKY
      FROM edaa_ref.dim_ingr
      WHERE NOT EXISTS (Select ingr_id From edaa_dw.dim_ingr WHERE edaa_dw.dim_ingr.ingr_id = edaa_ref.dim_ingr.ingr_id)

    END

    BEGIN

      /*Update the DIM_Ingr data */
      UPDATE a
      SET a.[INGR_NM] = b.[INGR_NM],
          a.[INGR_DSC] = b.[INGR_DSC],
          a.[AUD_UPD_SK] = @NEW_AUD_SKY
      FROM [EDAA_DW].[DIM_INGR] a
      INNER JOIN [EDAA_REF].[DIM_INGR] b
        ON a.[INGR_ID] = b.[INGR_ID]
    END

  END TRY

  BEGIN CATCH
    DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = 'EDAA_ETL.PROC_DIM_INGR_TABLE_LOAD'
    DECLARE @ERROR_LINE AS int;
    DECLARE @ERROR_MSG AS nvarchar(max);

    SELECT
      @ERROR_LINE = ERROR_NUMBER(),
      @ERROR_MSG = ERROR_MESSAGE();

    --------- Log execution error ----------

    EXEC EDAA_CNTL.SP_LOG_AUD_ERR @AUD_SKY = @NEW_AUD_SKY,
                                  @ERROR_PROCEDURE_NAME = @ERROR_PROCEDURE_NAME,
                                  @ERROR_LINE = @ERROR_LINE,
                                  @ERROR_MSG = @ERROR_MSG;

    THROW;

  END CATCH

END
