CREATE PROC [EDAA_ETL].[PROC_REF_FTWR_PROD_STYLE_TABLE_LOAD] AS
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
                                            @EXEC_JOB = 'EDAA_ETL.PROC_REF_FTWR_PROD_STYLE_TABLE_LOAD',
                                            @SRC_ENTY = 'EDAA_STG.REF_FTWR_PROD_STYLE',
                                            @TGT_ENTY = 'EDAA_REF.REF_FTWR_PROD_STYLE',
                                            @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

/* Insert new rows to REF_FTWR_PROD_STYLE */
    BEGIN
      INSERT INTO [EDAA_REF].[REF_FTWR_PROD_STYLE] ([FTWR_PROD_STYLE_ID], [FTWR_PROD_STYLE_NM], [FTWR_PROD_STYLE_DESC],[MJR_ATTR_TYP_ID],[MJR_ATTR_SUB_TYP_ID],[UDT_TMS],[UDT_BY], [AUD_INS_SK])
        SELECT
          [FTWR_PROD_STYLE_ID],
          [FTWR_PROD_STYLE_NM],
          [FTWR_PROD_STYLE_DESC],
		  [MJR_ATTR_TYP_ID],
		  [MJR_ATTR_SUB_TYP_ID],
		  [UDT_TMS],
		  [UDT_BY],
		  @NEW_AUD_SKY
        FROM [EDAA_STG].[REF_FTWR_PROD_STYLE]
		WHERE [FTWR_PROD_STYLE_ID] NOT IN (SELECT [FTWR_PROD_STYLE_ID] FROM [EDAA_REF].[REF_FTWR_PROD_STYLE])
    END

    BEGIN

      /*Update the REF_FTWR_PROD_STYLE data */
      UPDATE a
      SET a.[FTWR_PROD_STYLE_NM] = b.[FTWR_PROD_STYLE_NM],
          a.[FTWR_PROD_STYLE_DESC] = b.[FTWR_PROD_STYLE_DESC],
		  a.[MJR_ATTR_TYP_ID] = b.[MJR_ATTR_TYP_ID],
		  a.[MJR_ATTR_SUB_TYP_ID] = b.[MJR_ATTR_SUB_TYP_ID],
		  a.[UDT_TMS]=b.[UDT_TMS],
		  a.[UDT_BY]=b.[UDT_BY],
          a.[AUD_UPD_SK] = @NEW_AUD_SKY
      FROM [EDAA_REF].[REF_FTWR_PROD_STYLE] a
      INNER JOIN [EDAA_STG].[REF_FTWR_PROD_STYLE] b
        ON a.[FTWR_PROD_STYLE_ID] = b.[FTWR_PROD_STYLE_ID]
    END


  END TRY

  BEGIN CATCH
    DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = 'EDAA_ETL.PROC_REF_FTWR_PROD_STYLE_TABLE_LOAD'
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
