CREATE PROC [EDAA_ETL].[PROC_DIM_ITM_INGR_TABLE_LOAD] AS
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
                                            @EXEC_JOB = 'EDAA_ETL.PROC_DIM_ITM_INGR_TABLE_LOAD',
                                            @SRC_ENTY = 'EDAA_REF.DIM_ITM_INGR',
                                            @TGT_ENTY = 'EDAA_DW.DIM_ITM_INGR',
                                            @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

    BEGIN

	/* Updating ITM_SK for the existing records in the stage table*/

	UPDATE
		a
    SET
		a.ITM_SK = b.ITM_SK
    FROM
		EDAA_REF.DIM_ITM_INGR a
		inner join EDAA_DW.DIM_ITM_INGR b on a.ingr_id = b.ingr_id and a.itm_sku = b.itm_sku and a.upc_typ_nm = b.upc_typ_nm



	/*Updating INGR_SK in the stage table*/

	UPDATE
		a
    SET
		a.INGR_SK = ISNULL(b.INGR_SK, '-1')
    FROM
		EDAA_REF.DIM_ITM_INGR a
		left join EDAA_DW.dim_ingr b on a.ingr_id = b.ingr_id;

	/*Updating PROD_SK in the stage table*/

	UPDATE
		a
    SET
		a.PROD_SK = ISNULL(b.PROD_SK, '-1')
    FROM
		EDAA_REF.DIM_ITM_INGR a
		left join EDAA_DW.dim_prod b on a.itm_sku = b.itm_sku and b.Is_Curr_Ind = 1;

	--SCD type 2 change
    -- close current row in Target

	UPDATE
		EDAA_DW.DIM_ITM_INGR
	SET
		P_INGR_PER_END_DT = GETDATE(),
		Aud_Upd_sk = @NEW_AUD_SKY
	FROM
		EDAA_REF.DIM_ITM_INGR stg
		INNER JOIN EDAA_DW.DIM_ITM_INGR tgt ON stg.itm_sk = tgt.itm_sk
		and (
		  stg.[PROD_SK] <> tgt.[PROD_SK]
		  or stg.[UPC_TYP_NM] <> tgt.[UPC_TYP_NM]
		  or stg.[INGR_ID] <> tgt.[INGR_ID]
		)

    /*insert new records from the stage table*/
	IF EXISTS(select [PROD_SK],[ITM_SKU],[UPC_TYP_NM],max('k') as misc_col from EDAA_REF.DIM_ITM_INGR group by [PROD_SK],[ITM_SKU],[UPC_TYP_NM]
    except
    select [PROD_SK],[ITM_SKU],[UPC_TYP_NM],max('k') as misc_col from EDAA_DW.DIM_ITM_INGR group by [PROD_SK],[ITM_SKU],[UPC_TYP_NM])
	BEGIN
	INSERT INTO
		EDAA_DW.DIM_ITM_INGR (
			 [INGR_SK],
			 [PROD_SK],
			 [INGR_ID],
			 [ITM_SKU],
			 [UPC_TYP_NM],
			 [P_INGR_PER_STRT_DT],
			 [P_INGR_PER_END_DT],
			 [AUD_INS_SK]
        )
	SELECT
		a.[INGR_SK],
		a.[PROD_SK],
		a.[INGR_ID],
		a.[ITM_SKU],
		a.[UPC_TYP_NM],
		a.[P_INGR_PER_STRT_DT],
		a.[P_INGR_PER_END_DT],
		@NEW_AUD_SKY
	from
	  EDAA_REF.DIM_ITM_INGR a
	  left join EDAA_DW.DIM_INGR b on a.ingr_sk = b.ingr_sk
	  left join EDAA_DW.DIM_PROD c on c.prod_sk = a.prod_sk
	  and c.Is_Curr_Ind = 1
      END

	  /*updating surrogate key for late arriving dimension */
	  update
	    a
	  set
	    a.ingr_sk = b.ingr_sk,
	    Aud_Upd_sk = @NEW_AUD_SKY
	  FROM
	    EDAA_DW.DIM_ITM_INGR a
	    INNER JOIN EDAA_DW.DIM_INGR b ON a.ingr_id = b.ingr_id
		and a.ingr_sk='-1';

	 END


 END TRY

  BEGIN CATCH
    DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = 'EDAA_ETL.PROC_DIM_ITM_INGR_TABLE_LOAD'
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
