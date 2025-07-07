CREATE PROC [EDAA_ETL].[PROC_FCT_ITM_GOLD_RCD_TABLE_LOAD] AS
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
                                            @EXEC_JOB = 'EDAA_ETL.PROC_FCT_ITM_GOLD_RCD_TABLE_LOAD',
                                            @SRC_ENTY = 'EDAA_STG.FCT_ITM_GOLD_RCD',
                                            @TGT_ENTY = 'EDAA_DW.FCT_ITM_GOLD_RCD',
                                            @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

    /* Validate Late arriving dimension: DIM_PROD */
	PRINT('LATEARRIVINGDIMENSION: DIM_PROD')

	IF OBJECT_ID('TEMPDB..#LATEARRIVINGPRODDIMENSIONFACT_ITM_GOLD_RECORDS') IS NOT NULL
	DROP TABLE #LATEARRIVINGPRODDIMENSIONFACT_ITM_GOLD_RECORDS

	CREATE TABLE #LATEARRIVINGPRODDIMENSIONFACT_ITM_GOLD_RECORDS
	WITH (DISTRIBUTION = ROUND_ROBIN)
	AS
	SELECT DISTINCT FACT_DATA_FEED.ITM_SKU_ID as Itm_Sku, FACT_DATA_FEED.PROD_ID as Lvl1_Prod_Id
	, CASE WHEN PROD.Itm_Sku IS NULL THEN 1 ELSE 0 END AS IS_PROD_MISSING
	FROM
	(
	SELECT DISTINCT ITM_SKU_ID, PROD_ID
	FROM  EDAA_STG.FCT_ITM_GOLD_RCD
	) AS FACT_DATA_FEED

	LEFT  JOIN	(SELECT DISTINCT Itm_Sku, Lvl1_Prod_Id
				FROM EDAA_DW.DIM_PROD
			---	WHERE IS_CURR_IND<>0
					) AS PROD
	ON PROD.Itm_Sku = FACT_DATA_FEED.ITM_SKU_ID
     AND PROD.Lvl1_Prod_Id = FACT_DATA_FEED.PROD_ID
     -- AND PROD.Fnc_Lvl3_Pky_Id = FACT_DATA_FEED.Fnc_Lvl3_Pky_Id
	WHERE  PROD.Itm_Sku IS NULL

	DECLARE @NEXT_PROD_HIST_SK INT
	SELECT @NEXT_PROD_HIST_SK =ISNULL(MAX(PROD_HIST_SK),0)   FROM EDAA_DW.DIM_PROD

	PRINT('LATEARRIVINGDIMENSION: DIM_PROD LOAD')

	IF(@NEXT_PROD_HIST_SK IS NOT NULL)
	BEGIN

	INSERT INTO EDAA_DW.DIM_PROD

    SELECT
            (ROW_NUMBER() OVER (ORDER BY Itm_Sku)) + @NEXT_PROD_HIST_SK 	AS PROD_HIST_SK
            ,(ROW_NUMBER() OVER (ORDER BY Itm_Sku)) + @NEXT_PROD_HIST_SK  	AS PROD_SK
            ,CONVERT(VARCHAR(150),MISSING_PROD.Itm_Sku)   					AS Itm_Sku
            ,'n/a'        AS      Itm_Nm
            ,MISSING_PROD.Lvl1_Prod_Id        AS      Lvl1_Prod_Id
            ,'n/a'        AS      Lvl1_Prod_Desc
            ,'n/a'        AS      Lvl2_Prod_Clsfctn_Id
            ,'n/a'        AS      Lvl2_Prod_Clsfctn_Desc
            ,'n/a'        AS      Lvl3_Prod_Sub_Ctgry_Id
            ,'n/a'        AS      Lvl3_Prod_Sub_Ctgry_Desc
            ,'n/a'        AS      Lvl4_Prod_Ctgry_Id
            ,'n/a'        AS      Lvl4_Prod_Ctgry_Desc
            ,'n/a'        AS      Lvl5_Bsns_Sgmt_Id
            ,'n/a'        AS      Lvl5_Bsns_Sgmt_Desc
            ,'n/a'        AS      Lvl6_MDS_Area_Id
            ,'n/a'        AS      Lvl6_MDS_Area_Desc
            ,'n/a'        AS      Fnc_Lvl1_Prod_Ctgry_Id
            ,'n/a'        AS      Fnc_Lvl1_Prod_Ctgry_Desc
            ,NULL        AS      Fnc_Lvl2_Mprs_Ctgry_Id
            ,'n/a'        AS      Fnc_Lvl2_Mprs_Ctgry_Desc
            ,NULL         AS      Fnc_Lvl3_Pky_Id
            ,'n/a'        AS      Fnc_Lvl3_Pky_Desc
            ,'n/a'        AS      Fnc_Lvl4_Area_Id
            ,'n/a'        AS      Fnc_Lvl4_Area_Desc
            ,NULL   AS Str_Area_Dtl_Id
            ,'n/a'        AS      Str_Area_Dtl_Desc
            ,NULL   AS Str_Area_Summ_Id
            ,'n/a'        AS      Str_Area_Summ_Desc
            ,NULL   AS Str_Dept_Dtl_Id
            ,'n/a'        AS      Str_Dept_Dtl_Desc
            ,NULL   AS Str_Dept_Summ_Id
            ,'n/a'        AS      Str_Dept_Summ_Desc
            ,CAST('2000-01-01' AS DATE) AS Vld_Frm
            ,CAST('2099-01-01' AS DATE) AS Vld_To
            ,CAST(1 AS BIT)					Is_Curr_Ind
            ,CAST(0 AS BIT)					Is_Dmy_Ind
            ,CAST(1 AS BIT)					Is_Emb_Ind
            ,'New embryo'					Etl_Actn
            ,@NEW_AUD_SKY				    Aud_Ins_sk
            ,NULL					        Aud_Upd_sk
--      Added for new attributes
            ,NULL                           Itm_Prim_Ind
            ,NULL                           Prod_Size_Desc
            ,NULL                           Brnd_Nm
            ,NULL                           Brnd_Desc
            ,NULL                           Brnd_Ctgry
            ,NULL                           Prod_Desc
            ,NULL                           Prod_Desc_Note
            ,NULL                           Outdate
            ,NULL                           Byr_Id
            ,NULL                           Prod_Sts
            ,NULL                           Prod_Sts_Note
            ,NULL                           Vdr_Id
            ,NULL                           Sku_Ct
	FROM
	(
	SELECT
			DISTINCT Itm_Sku, Lvl1_Prod_Id
	FROM #LATEARRIVINGPRODDIMENSIONFACT_ITM_GOLD_RECORDS
	WHERE 	IS_PROD_MISSING = 1
	) AS MISSING_PROD
	WHERE 	NOT EXISTS
	(
	SELECT 1 FROM EDAA_DW.DIM_PROD DIM_PROD
	WHERE
	      MISSING_PROD.Itm_Sku = DIM_PROD.Itm_Sku
          AND MISSING_PROD.Lvl1_Prod_Id = DIM_PROD.Lvl1_Prod_Id
      --AND MISSING_PROD.Fnc_Lvl3_Pky_Id = DIM_PROD.Fnc_Lvl3_Pky_Id
	)


END


	/*Updating PROD_Hist_Sk in the stage table*/

	UPDATE
		a
    SET
		a.Prod_Hist_Sk = b.Prod_Hist_Sk
    FROM
		[EDAA_STG].[FCT_ITM_GOLD_RCD] a
		left join [EDAA_DW].[dim_prod] b on a.ITM_SKU_ID = b.itm_sku and a.PROD_ID = b. Lvl1_Prod_Id and b.Is_Curr_Ind = 1;


	/* Insert new rows to FCT_ITM_GOLD_RCD */
		BEGIN
        INSERT INTO [EDAA_DW].[FCT_ITM_GOLD_RCD] ([Prod_Hist_Sk], [ITM_SKU_ID], [PROD_ID],[GOLD_RCD_ID],[CNTN_LED_IND],[SUST_CERT_ID],[PROD_SIZ_RNG_SUB_TYP_ID],[PROD_SIZ_RNG_CD_ID],[SUST_CERT],[THD_CNT],[NBR_OF_PCE_CNT],[FTWR_PROD_TGT_GENDR_ID],[UDT_TMS],[UDT_BY], [AUD_INS_SK])
		(
      SELECT
        a.[Prod_Hist_Sk],
        a.[ITM_SKU_ID],
        a.[PROD_ID],
		a.[GOLD_RCD_ID],
		a.[CNTN_LED_IND],
		a.[SUST_CERT_ID],
		a.[PROD_SIZ_RNG_SUB_TYP_ID],
		a.[PROD_SIZ_RNG_CD_ID],
		a.[SUST_CERT],
		a.[THD_CNT],
		a.[NBR_OF_PCE_CNT],
		a.[FTWR_PROD_TGT_GENDR_ID],
		a.[UDT_TMS],
		a.[UDT_BY],
		@NEW_AUD_SKY
      FROM
        [EDAA_STG].[FCT_ITM_GOLD_RCD] a
        left join [EDAA_DW].[FCT_ITM_GOLD_RCD] b on a.ITM_SKU_ID = b.ITM_SKU_ID and a.PROD_ID = b.PROD_ID
      where
        b.[Prod_Hist_Sk] is null and a.[Prod_Hist_Sk] is not null
		)


	END

    BEGIN

      /*Update the [EDAA_DW].[FCT_ITM_GOLD_RCD] data */
      UPDATE a
      SET
		a.[GOLD_RCD_ID] = b.[GOLD_RCD_ID],
		a.[CNTN_LED_IND] = b.[CNTN_LED_IND],
		a.[SUST_CERT_ID] = b.[SUST_CERT_ID],
		a.[PROD_SIZ_RNG_SUB_TYP_ID] = b.[PROD_SIZ_RNG_SUB_TYP_ID],
		a.[PROD_SIZ_RNG_CD_ID] = b.[PROD_SIZ_RNG_CD_ID],
		a.[SUST_CERT] = b.[SUST_CERT],
		a.[THD_CNT] = b.[THD_CNT],
		a.[NBR_OF_PCE_CNT] = b.[NBR_OF_PCE_CNT],
		a.[FTWR_PROD_TGT_GENDR_ID] = b.[FTWR_PROD_TGT_GENDR_ID],
		a.[UDT_TMS] = b.[UDT_TMS],
		a.[UDT_BY] = b.[UDT_BY],
        a.[AUD_INS_SK] = @NEW_AUD_SKY
      FROM [EDAA_DW].[FCT_ITM_GOLD_RCD] a
      INNER JOIN [EDAA_STG].[FCT_ITM_GOLD_RCD] b
        ON a.[ITM_SKU_ID] = b.[ITM_SKU_ID]
		and a.[PROD_ID] = b.[PROD_ID]
		and a.[Prod_Hist_Sk] = b.[Prod_Hist_Sk]
    END

/*/* Insert new rows to FCT_ITM_GOLD_RCD */
    BEGIN
      INSERT INTO [EDAA_DW].[FCT_ITM_GOLD_RCD] ([Prod_Hist_Sk], [ITM_SKU_ID], [PROD_ID],[GOLD_RCD_ID],[CNTN_LED_IND],[SUST_CERT_ID],[PROD_SIZ_RNG_SUB_TYP_ID],[PROD_SIZ_RNG_CD_ID],[SUST_CERT],[THD_CNT],[NBR_OF_PCE_CNT],[FTWR_PROD_TGT_GENDR_ID],[UDT_TMS],[UDT_BY], [AUD_INS_SK])
        SELECT
          [Prod_Hist_Sk],
          [ITM_SKU_ID],
          [PROD_ID],
		  [GOLD_RCD_ID]],
		  [CNTN_LED_IND],
		  [SUST_CERT_ID],
		  [PROD_SIZ_RNG_SUB_TYP_ID],
		  [PROD_SIZ_RNG_CD_ID],
		  [SUST_CERT],
		  [THD_CNT],
		  [NBR_OF_PCE_CNT],
		  [FTWR_PROD_TGT_GENDR_ID],
		  [UDT_TMS],
		  [UDT_BY],
		  @NEW_AUD_SKY
        FROM [EDAA_STG].[FCT_ITM_GOLD_RCD]
		WHERE [Prod_Hist_Sk] NOT IN (SELECT [Prod_Hist_Sk] FROM [EDAA_DW].[FCT_ITM_GOLD_RCD])
    END

    BEGIN

      /*Update the FCT_ITM_GOLD_RCD data */
      UPDATE a
      SET a.[ITM_SKU_ID] = b.[ITM_SKU_ID],
          a.[PROD_ID] = b.[PROD_ID],
		  a.[GOLD_RCD_ID] = b.[GOLD_RCD_ID],
		  a.[CNTN_LED_IND] = b.[CNTN_LED_IND],
		  a.[SUST_CERT_ID] = b.[SUST_CERT_ID],
		  a.[PROD_SIZ_RNG_SUB_TYP_ID] = b.[PROD_SIZ_RNG_SUB_TYP_ID],
		  a.[PROD_SIZ_RNG_CD_ID] = b.[PROD_SIZ_RNG_CD_ID],
		  a.[SUST_CERT] = b.[SUST_CERT],
		  a.[THD_CNT] = b.[THD_CNT],
		  a.[NBR_OF_PCE_CNT]=b.[NBR_OF_PCE_CNT],
		  a.[FTWR_PROD_TGT_GENDR_ID] = b.[FTWR_PROD_TGT_GENDR_ID],
		  a.[UDT_TMS]=b.[UDT_TMS],
		  a.[UDT_BY]=b.[UDT_BY],
          a.[AUD_UPD_SK] = @NEW_AUD_SKY
      FROM [EDAA_DW].[FCT_ITM_GOLD_RCD] a
      INNER JOIN [EDAA_STG].[FCT_ITM_GOLD_RCD] b
        ON a.[Prod_Hist_Sk] = b.[Prod_Hist_Sk]
    END*/


  END TRY

  BEGIN CATCH
    DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = 'EDAA_ETL.PROC_FCT_ITM_GOLD_RCD_TABLE_LOAD'
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
