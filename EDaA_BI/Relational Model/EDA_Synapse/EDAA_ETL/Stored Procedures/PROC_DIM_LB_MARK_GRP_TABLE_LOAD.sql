CREATE PROC [EDAA_ETL].[PROC_DIM_LB_MARK_GRP_TABLE_LOAD] AS
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
                                            @EXEC_JOB = 'EDAA_ETL.PROC_DIM_LB_MARK_GRP_TABLE_LOAD',
                                            @SRC_ENTY = 'EDAA_REF.DIM_LB_MARK_GRP',
                                            @TGT_ENTY = 'EDAA_DW.DIM_LB_MARK_GRP',
                                            @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

   /*Updating LB_SK in the stage table*/

	UPDATE
		a
    SET
		a.LB_SK = ISNULL(b.LB_SK, '-1')
    FROM
		EDAA_REF.DIM_LB_MARK_GRP a
		left join EDAA_DW.DIM_LB_MARK b on a.LB_MARK_ID = b.LB_MARK_ID ;

   /*Inserting data in the target table*/

    BEGIN
      INSERT INTO
        [EDAA_DW].[DIM_LB_MARK_GRP] (
          [LB_MARK_GRP_ID],
          [LB_MARK_ID],
          [LB_MARK_GRP_NM],
          [AUD_INS_SK]
        )
		(
      SELECT
        a.[LB_MARK_GRP_ID],
        a.[LB_MARK_ID],
        a.[LB_MARK_GRP_NM],
        @NEW_AUD_SKY
      FROM
        [EDAA_REF].[DIM_LB_MARK_GRP] a
        LEFT JOIN [EDAA_DW].[DIM_LB_MARK] b on a.[LB_SK] = b.[LB_SK]
        left join [EDAA_DW].[DIM_LB_MARK_GRP] c on a.LB_MARK_GRP_ID = c.LB_MARK_GRP_ID
        and a.LB_MARK_ID = c.LB_MARK_ID
      where
        c.LB_MARK_GRP_ID is null
        and c.LB_MARK_ID is null
		)
    END

    BEGIN

      /*Update the DIM_LB_MARK_GRP data */
      UPDATE a
      SET
          a.[LB_MARK_GRP_NM] = b.[LB_MARK_GRP_NM],
          a.[AUD_UPD_SK] = @NEW_AUD_SKY
      FROM [EDAA_DW].[DIM_LB_MARK_GRP] a
      INNER JOIN [EDAA_REF].[DIM_LB_MARK_GRP] b
        ON a.[LB_MARK_GRP_ID] = b.[LB_MARK_GRP_ID]
		and a.[LB_MARK_ID] = b.[LB_MARK_ID]

    END

  END TRY

  BEGIN CATCH
    DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = 'EDAA_ETL.PROC_DIM_LB_MARK_GRP_TABLE_LOAD'
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
