CREATE PROC [EDAA_ETL].[PROC_REF_FBRC_TABLE_LOAD] AS
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
                                            @EXEC_JOB = 'EDAA_ETL.PROC_REF_FBRC_TABLE_LOAD',
                                            @SRC_ENTY = 'EDAA_STG.REF_FBRC',
                                            @TGT_ENTY = 'EDAA_REF.REF_FBRC',
                                            @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

/* Insert new rows to REF_FBRC */
    BEGIN
      INSERT INTO [EDAA_REF].[REF_FBRC] ([FBRC_ID], [FBRC_NM], [FBRC_DESC], [UDT_TMS], [UDT_BY], [AUD_INS_SK])
        SELECT
          [FBRC_ID],
          [FBRC_NM],
          [FBRC_DESC],
		  [UDT_TMS],
                  [UDT_BY],
		  @NEW_AUD_SKY
        FROM [EDAA_STG].[REF_FBRC]
		WHERE [FBRC_ID] NOT IN (SELECT [FBRC_ID] FROM [EDAA_REF].[REF_FBRC])
    END

    BEGIN

      /*Update the REF_FBRC data */
      UPDATE a
      SET a.[FBRC_NM] = b.[FBRC_NM],
		  a.[FBRC_DESC] = b.[FBRC_DESC],
                  a.[UDT_TMS] = b.[UDT_TMS],
                  a.[UDT_BY] = b.[UDT_BY],
          a.[AUD_UPD_SK] = @NEW_AUD_SKY
      FROM [EDAA_REF].[REF_FBRC] a
      INNER JOIN [EDAA_STG].[REF_FBRC] b
        ON a.[FBRC_ID] = b.[FBRC_ID]
    END


  END TRY

  BEGIN CATCH
    DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = 'EDAA_ETL.PROC_REF_FBRC_TABLE_LOAD'
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
