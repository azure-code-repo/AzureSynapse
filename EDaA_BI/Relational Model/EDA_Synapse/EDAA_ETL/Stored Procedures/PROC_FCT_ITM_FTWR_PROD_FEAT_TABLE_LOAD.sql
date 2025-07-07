CREATE PROC [EDAA_ETL].[PROC_FCT_ITM_FTWR_PROD_FEAT_TABLE_LOAD] AS
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
                                            @EXEC_JOB = 'EDAA_ETL.PROC_FCT_ITM_FTWR_PROD_FEAT_TABLE_LOAD',
                                            @SRC_ENTY = 'EDAA_STG.FCT_ITM_FTWR_PROD_FEAT',
                                            @TGT_ENTY = 'EDAA_DW.FCT_ITM_FTWR_PROD_FEAT',
                                            @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT
/*Updating PROD_SK in the stage table*/

	UPDATE
		a
    SET
		a.Prod_Hist_Sk = b.Prod_Hist_Sk
    FROM
		[EDAA_STG].[FCT_ITM_FTWR_PROD_FEAT] a
		left join [EDAA_DW].[dim_prod] b on a.ITM_SKU_ID = b.itm_sku and a.PROD_ID = b. Lvl1_Prod_Id and b.Is_Curr_Ind = 1;

/* Insert new rows to FCT_ITM_FTWR_PROD_FEAT */
    BEGIN
      INSERT INTO [EDAA_DW].[FCT_ITM_FTWR_PROD_FEAT] ([Prod_Hist_Sk], [ITM_SKU_ID], [FTWR_PROD_FEAT_ID],[PROD_ID],[GOLD_RCD_ID],[UDT_TMS],[UDT_BY], [AUD_INS_SK])
        (
		SELECT
          a.[Prod_Hist_Sk],
          a.[ITM_SKU_ID],
		  a.[FTWR_PROD_FEAT_ID],
          a.[PROD_ID],
		  a.[GOLD_RCD_ID],
		  a.[UDT_TMS],
		  a.[UDT_BY],
		  @NEW_AUD_SKY
        FROM [EDAA_STG].[FCT_ITM_FTWR_PROD_FEAT] a
		left join [EDAA_DW].[FCT_ITM_FTWR_PROD_FEAT] b on a.ITM_SKU_ID = b.ITM_SKU_ID and a.PROD_ID = b.PROD_ID
      where
        b.[Prod_Hist_Sk] is null and a.[Prod_Hist_Sk] is not null)
    END

    BEGIN

      /*Update the FCT_ITM_FTWR_PROD_FEAT data */
      UPDATE a
      SET a.[ITM_SKU_ID] = b.[ITM_SKU_ID],
	      a.[FTWR_PROD_FEAT_ID] = b.[FTWR_PROD_FEAT_ID],
          a.[PROD_ID] = b.[PROD_ID],
		  a.[GOLD_RCD_ID] = b.[GOLD_RCD_ID],
		  a.[UDT_TMS]=b.[UDT_TMS],
		  a.[UDT_BY]=b.[UDT_BY],
          a.[AUD_UPD_SK] = @NEW_AUD_SKY
      FROM [EDAA_DW].[FCT_ITM_FTWR_PROD_FEAT] a
      INNER JOIN [EDAA_STG].[FCT_ITM_FTWR_PROD_FEAT] b
	  ON a.[ITM_SKU_ID] = b.[ITM_SKU_ID]
		and a.[PROD_ID] = b.[PROD_ID]
		and a.[Prod_Hist_Sk] = b.[Prod_Hist_Sk]
    END


  END TRY

  BEGIN CATCH
    DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = 'EDAA_ETL.PROC_FCT_ITM_FTWR_PROD_FEAT_TABLE_LOAD'
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
