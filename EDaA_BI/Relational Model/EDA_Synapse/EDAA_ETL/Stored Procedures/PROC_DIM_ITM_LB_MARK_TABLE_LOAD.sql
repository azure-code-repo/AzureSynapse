CREATE PROC [EDAA_ETL].[PROC_DIM_ITM_LB_MARK_TABLE_LOAD] AS
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
                                            @EXEC_JOB = 'EDAA_ETL.PROC_DIM_ITM_LB_MARK_TABLE_LOAD',
                                            @SRC_ENTY = 'EDAA_REF.DIM_ITM_LB_MARK',
                                            @TGT_ENTY = 'EDAA_DW.DIM_ITM_LB_MARK',
                                            @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT


	/*Updating PROD_SK in the stage table*/

	UPDATE
		a
    SET
		a.PROD_SK = ISNULL(b.PROD_SK, '-1')
    FROM
		EDAA_REF.DIM_ITM_LB_MARK a
		left join EDAA_DW.dim_prod b on a.itm_sku = b.itm_sku and b.Is_Curr_Ind = 1;


    /* Insert new records in the DIM_ITM_LB_MARK*/

    BEGIN
      INSERT INTO
        [EDAA_DW].[DIM_ITM_LB_MARK] (
          [LB_MARK_ID],
          [PROD_SK],
          [ITM_SKU],
          [UPC_TYP_NM],
          [P_LB_VRS_ID],
          [LB_MARK_NT],
          [AUD_INS_SK]
        )
		(
      SELECT
        ISNULL(b.LB_MARK_ID, '-1') as LB_MARK_ID,
        a.[PROD_SK],
        a.[ITM_SKU],
        a.[UPC_TYP_NM],
        a.[P_LB_VRS_ID],
        a.[LB_MARK_NT],
        @NEW_AUD_SKY
      FROM
        [EDAA_REF].[DIM_ITM_LB_MARK] a
        LEFT JOIN [EDAA_DW].[DIM_LB_MARK] b ON a.[LB_MARK_ID] = b.[LB_MARK_ID]
        left join [EDAA_DW].[DIM_ITM_LB_MARK] d on d.LB_MARK_ID = a.LB_MARK_ID
        and d.prod_sk = a.prod_sk and d.upc_typ_nm = a.upc_typ_nm and d.ITM_SKU = a.ITM_SKU
      where
        d.prod_sk is null
        and d.LB_MARK_ID is null
		and d.upc_typ_nm is null
		)

    END

    BEGIN

      /*Update the DIM_ITM_LB_MARK data */
      UPDATE a
      SET
		  a.[P_LB_VRS_ID] = b.[P_LB_VRS_ID],
		  a.[LB_MARK_NT] = b.[LB_MARK_NT],
          a.[AUD_UPD_SK] = @NEW_AUD_SKY
      FROM [EDAA_DW].[DIM_ITM_LB_MARK] a
      INNER JOIN [EDAA_REF].[DIM_ITM_LB_MARK] b
        ON a.[LB_MARK_ID] = b.[LB_MARK_ID]
		and a.[PROD_SK] = b.[PROD_SK]
		and a.[UPC_TYP_NM] = b.[UPC_TYP_NM]
    END

  END TRY

  BEGIN CATCH
    DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = 'EDAA_ETL.PROC_DIM_ITM_LB_MARK_TABLE_LOAD'
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
