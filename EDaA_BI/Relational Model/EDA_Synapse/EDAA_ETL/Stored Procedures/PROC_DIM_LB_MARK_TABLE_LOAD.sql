CREATE PROC [EDAA_ETL].[PROC_DIM_LB_MARK_TABLE_LOAD] AS
BEGIN
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
                                            @EXEC_JOB = 'EDAA_ETL.PROC_DIM_LB_MARK_TABLE_LOAD',
                                            @SRC_ENTY = 'EDAA_REF.DIM_LB_MARK',
                                            @TGT_ENTY = 'EDAA_DW.DIM_LB_MARK',
                                            @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

/* Insert new rows to DIM_LB_MARK */
    BEGIN
      INSERT INTO [EDAA_DW].[DIM_LB_MARK] ([LB_MARK_ID], [LB_MARK_NM], [LB_MARK_DSC],[LB_MARK_IMG], [AUD_INS_SK])
        SELECT
          [LB_MARK_ID],
          [LB_MARK_NM],
          [LB_MARK_DSC],
		  [LB_MARK_IMG],
		  @NEW_AUD_SKY
        FROM [EDAA_REF].[DIM_LB_MARK]
		WHERE [LB_MARK_ID] NOT IN (SELECT [LB_MARK_ID] FROM [EDAA_DW].[DIM_LB_MARK])
    END

    BEGIN

      /*Update the DIM_LB_MARK data */
      UPDATE a
      SET a.[LB_MARK_NM] = b.[LB_MARK_NM],
          a.[LB_MARK_DSC] = b.[LB_MARK_DSC],
		  a.[LB_MARK_IMG] = b.[LB_MARK_IMG],
          a.[AUD_UPD_SK] = @NEW_AUD_SKY
      FROM [EDAA_DW].[DIM_LB_MARK] a
      INNER JOIN [EDAA_REF].[DIM_LB_MARK] b
        ON a.[LB_MARK_ID] = b.[LB_MARK_ID]
    END


  END TRY

  BEGIN CATCH
    DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = 'EDAA_ETL.PROC_DIM_LB_MARK_TABLE_LOAD'
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
