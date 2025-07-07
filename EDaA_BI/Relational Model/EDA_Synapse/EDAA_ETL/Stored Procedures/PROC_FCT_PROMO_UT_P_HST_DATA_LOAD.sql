CREATE PROC [EDAA_ETL].[PROC_FCT_PROMO_UT_P_HST_DATA_LOAD] AS
BEGIN

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
    DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = '[EDAA_ETL].[PROC_FCT_PROMO_UT_P_HST_DATA_LOAD]';
    DECLARE @ERROR_LINE AS int;
    DECLARE @ERROR_MSG AS nvarchar(max);
    DECLARE @DELETE_LAST_HOUR int;



    /*AUDIT LOG START*/
    EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR = 'EDAA_DW',
                                            @EXEC_JOB = '[EDAA_ETL].[PROC_FCT_PROMO_UT_P_HST_DATA_LOAD]',
                                            @SRC_ENTY = '[EDAA_STG].[FCT_PROMO_UT_P_HST]',
                                            @TGT_ENTY = 'EDAA_DW.FCT_PROMO_UT_P_HST',
                                            @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

	PRINT('LATEARRIVINGDIMENSION: DIM_GEO')

IF OBJECT_ID('TEMPDB..#LATEARRIVINGGEODIMENSIONFACTRECORDS1') IS NOT NULL
DROP TABLE #LATEARRIVINGGEODIMENSIONFACTRECORDS1

CREATE TABLE #LATEARRIVINGGEODIMENSIONFACTRECORDS1
WITH (DISTRIBUTION = ROUND_ROBIN)
AS
SELECT DISTINCT
    FACT_DATA_FEED_SB.StoreId,
CASE WHEN GEO_SB.Str_Id
IS NULL THEN 1
ELSE 0
END
AS IS_GEO_MISSING
FROM
(
SELECT DISTINCT StoreId
FROM  EDAA_STG.FCT_PROMO_UT_P_HST
) AS FACT_DATA_FEED_SB

LEFT  JOIN	(SELECT DISTINCT Str_Id
				FROM EDAA_DW.DIM_GEO
				---WHERE IS_CURR_IND<>0
					) AS GEO_SB
	ON GEO_SB.Str_Id = FACT_DATA_FEED_SB.StoreId
	WHERE  GEO_SB.Str_Id IS NULL


DECLARE @NEXT_GEO_HIST_SK INT
SELECT @NEXT_GEO_HIST_SK =ISNULL(MAX(GEO_HIST_SK),0)   FROM EDAA_DW.DIM_GEO

PRINT('LATEARRIVINGDIMENSION: DIM_GEO LOAD')

IF(@NEXT_GEO_HIST_SK IS NOT NULL)
BEGIN


INSERT INTO EDAA_DW.DIM_GEO

SELECT
		   (ROW_NUMBER() OVER (ORDER BY StoreId)) + @NEXT_GEO_HIST_SK 		AS GEO_HIST_SK
		  ,(ROW_NUMBER() OVER (ORDER BY StoreId)) + @NEXT_GEO_HIST_SK  		AS GEO_SK
		  ,CONVERT(INTEGER,StoreId)   								 		AS STR_ID
		  ,CONVERT(VARCHAR(5),'n/a')		STR_NM
		  ,CONVERT(INTEGER,0)				GROSS_FLR_AREA
		  ,CONVERT(VARCHAR(5),'n/a')		RGN_NM
		  ,CONVERT(VARCHAR(5),'n/a')		MKT_NM
		  ,NULL                    AS		LOC_LNGTD
		  ,NULL                    AS		LOC_LTTD
		  ,CONVERT(INTEGER,-1)				STR_CLS_ID
		  ,CONVERT(VARCHAR(5),'n/a')		STR_CLS_NM
		  ,CONVERT(VARCHAR(5),'n/a')		LOC_ST_ID
		  ,CONVERT(VARCHAR(5),'n/a')		LOC_CNTY_ID
		  ,CONVERT(VARCHAR(5),'n/a')		LOC_CTY
		  ,CONVERT(VARCHAR(5),'n/a')		LOC_ZP_CODE
		  ,CONVERT(VARCHAR(5),'n/a')		DIV_ID
		  ,CONVERT(VARCHAR(5),'n/a')		DIV_NM
		  ,NULL                    AS		CURR_YR_NEW_STR_IND
		  ,NULL                    AS		LST_YR_NEW_STR_IND
		  ,NULL                    AS		TWO_YRS_AGO_NEW_STR_IND
		  ,NULL                    AS		RGN_ID
		  ,NULL                    AS		MKT_ID
		  ,NULL                    AS		STR_OPN_DT
		  ,NULL                    AS		STR_CLS_DT
		  ,CONVERT(VARCHAR(5),'n/a')		STR_CTGRY_NM
		  ,NULL                    AS		STR_ZN_ID
		  ,CONVERT(VARCHAR(5),'n/a')		STR_ZN_NM
		  ,NULL                    AS		STR_CMP_CTGRY_ID
		  ,CONVERT(VARCHAR(5),'n/a')		STR_CMP_CTGRY_DESC
		  ,CAST('2000-01-01' AS DATE)	    VLD_FROM
		  ,CAST('2099-01-01' AS DATE)		VLD_TO
		  ,CAST(1 AS BIT)					IS_CURR_IND
		  ,CAST(0 AS BIT)					IS_DMY_IND
		  ,CAST(1 AS BIT)					IS_EMB_IND
		  ,'New embryo'					    ETL_ACTN
		  ,@NEW_AUD_SKY						AUD_INS_SK
		  ,NULL								AUD_UPD_SK

FROM
	(
	SELECT
			DISTINCT StoreId
	FROM #LATEARRIVINGGEODIMENSIONFACTRECORDS1
	WHERE 	IS_GEO_MISSING = 1
	) AS MISSING_GEO_SB
	WHERE 	NOT EXISTS
	(
	SELECT 1 FROM EDAA_DW.DIM_GEO DIM_SB
	WHERE
	MISSING_GEO_SB.StoreId = DIM_SB.STR_ID
	)


END
---------------------------------------------------------------------------
/* Validate Late arriving dimension: DIM_PROD */
PRINT('LATEARRIVINGDIMENSION: DIM_PROD_SB')

IF OBJECT_ID('TEMPDB..#LATEARRIVINGPRODDIMENSIONFACTRECORDS2') IS NOT NULL
DROP TABLE #LATEARRIVINGPRODDIMENSIONFACTRECORDS2

CREATE TABLE #LATEARRIVINGPRODDIMENSIONFACTRECORDS2
WITH (DISTRIBUTION = ROUND_ROBIN)
AS

SELECT DISTINCT

	FACT_DATA_FEED_SB.ProductItemId,

CASE WHEN PROD_SB.Itm_Sku
IS NULL THEN 1
ELSE 0
END
AS IS_PROD_MISSING

FROM
(
SELECT DISTINCT ProductItemId
FROM  EDAA_STG.FCT_PROMO_UT_P_HST
) AS FACT_DATA_FEED_SB

LEFT  JOIN
(SELECT DISTINCT Itm_Sku
				FROM EDAA_DW.DIM_PROD
			---	WHERE IS_CURR_IND<>0
) AS PROD_SB
	ON PROD_SB.Itm_Sku = FACT_DATA_FEED_SB.ProductItemId

	WHERE  PROD_SB.Itm_Sku IS NULL


DECLARE @NEXT_PROD_HIST_SK INT
SELECT @NEXT_PROD_HIST_SK =ISNULL(MAX(PROD_HIST_SK),0)   FROM EDAA_DW.DIM_PROD

PRINT('LATEARRIVINGDIMENSION: DIM_PROD_SB LOAD')

IF(@NEXT_PROD_HIST_SK IS NOT NULL)
BEGIN


INSERT INTO EDAA_DW.DIM_PROD
(PROD_HIST_SK,PROD_SK,Itm_Sku,Itm_Nm,Lvl1_Prod_Id,Lvl1_Prod_Desc,Lvl2_Prod_Clsfctn_Id,Lvl2_Prod_Clsfctn_Desc,
Lvl3_Prod_Sub_Ctgry_Id,Lvl3_Prod_Sub_Ctgry_Desc,Lvl4_Prod_Ctgry_Id,Lvl4_Prod_Ctgry_Desc,Lvl5_Bsns_Sgmt_Id,
Lvl5_Bsns_Sgmt_Desc,Lvl6_MDS_Area_Id,Lvl6_MDS_Area_Desc,Fnc_Lvl1_Prod_Ctgry_Id,Fnc_Lvl1_Prod_Ctgry_Desc,
Fnc_Lvl2_Mprs_Ctgry_Id,Fnc_Lvl2_Mprs_Ctgry_Desc,Fnc_Lvl3_Pky_Id,Fnc_Lvl3_Pky_Desc,Fnc_Lvl4_Area_Id,
Fnc_Lvl4_Area_Desc,Str_Area_Dtl_Id,Str_Area_Dtl_Desc,Str_Area_Summ_Id,Str_Area_Summ_Desc,Str_Dept_Dtl_Id
,Str_Dept_Dtl_Desc,Str_Dept_Summ_Id,Str_Dept_Summ_Desc,Vld_Frm,Vld_To,Is_Curr_Ind,Is_Dmy_Ind,Is_Emb_Ind,Etl_Actn,
Aud_Ins_sk,Aud_Upd_sk)

SELECT
            (ROW_NUMBER() OVER (ORDER BY ProductItemId)) + @NEXT_PROD_HIST_SK 	AS PROD_HIST_SK
            ,(ROW_NUMBER() OVER (ORDER BY ProductItemId)) + @NEXT_PROD_HIST_SK  	AS PROD_SK
            ,CONVERT(VARCHAR(150),MISSING_PROD_SB.ProductItemId)   		        AS Itm_Sku
            ,'n/a'        AS      Itm_Nm
            , NULL        AS      Lvl1_Prod_Id
            ,'n/a'        AS      Lvl1_Prod_Desc
            , NULL        AS      Lvl2_Prod_Clsfctn_Id
            ,'n/a'        AS      Lvl2_Prod_Clsfctn_Desc
            , NULL        AS      Lvl3_Prod_Sub_Ctgry_Id
            ,'n/a'        AS      Lvl3_Prod_Sub_Ctgry_Desc
            , NULL        AS      Lvl4_Prod_Ctgry_Id
            ,'n/a'        AS      Lvl4_Prod_Ctgry_Desc
            , NULL        AS      Lvl5_Bsns_Sgmt_Id
            ,'n/a'        AS      Lvl5_Bsns_Sgmt_Desc
            , NULL        AS      Lvl6_MDS_Area_Id
            ,'n/a'        AS      Lvl6_MDS_Area_Desc
            , NULL        AS      Fnc_Lvl1_Prod_Ctgry_Id
            ,'n/a'        AS      Fnc_Lvl1_Prod_Ctgry_Desc
            , NULL        AS      Fnc_Lvl2_Mprs_Ctgry_Id
            ,'n/a'        AS      Fnc_Lvl2_Mprs_Ctgry_Desc
            , NULL        AS      Fnc_Lvl3_Pky_Id
            ,'n/a'        AS      Fnc_Lvl3_Pky_Desc
            , NULL        AS      Fnc_Lvl4_Area_Id
            ,'n/a'        AS      Fnc_Lvl4_Area_Desc
            , NULL   AS Str_Area_Dtl_Id
            ,'n/a'        AS      Str_Area_Dtl_Desc
            , NULL   AS Str_Area_Summ_Id
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
FROM
	(
	SELECT
			DISTINCT ProductItemId

	FROM #LATEARRIVINGPRODDIMENSIONFACTRECORDS2
	WHERE 	IS_PROD_MISSING = 1
	) AS MISSING_PROD_SB
	WHERE 	NOT EXISTS
	(
	SELECT 1 FROM EDAA_DW.DIM_PROD AS DIM
	WHERE
	      MISSING_PROD_SB.ProductItemId = DIM.Itm_Sku

	)

END


    -- Delete last 30 days of data
    --DELETE FROM [EDAA_DW].[FCT_PROMO_UT_P_HST]
    --WHERE Promo_Adv_Blk_Str_End_Dt IN (SELECT
    --    Promo_Adv_Blk_Str_End_Dt
    --  FROM [EDAA_DW].[FCT_PROMO_UT_P_HST]
    --  WHERE Promo_Adv_Blk_Str_End_Dt >= (SELECT
    --    DATEADD(DAY, -30, CAST(GETDATE() AS date))))

	 --DELETE A
  --    from  [EDAA_DW].[FCT_PROMO_UT_P_HST] A inner join [EDAA_STG].[FCT_PROMO_UT_P_HST] B

  --    On A.ProductItemId = B.ProductItemId
	 -- AND   A.StoreId = B.StoreId
	 -- AND  A. Promo_Adv_Blk_Id = B.PromotionAdvertisementBlockId
	 -- AND  A. Promo_P_Grp_Id = B.PromotionVehicleProductGroupId
	 -- AND A.Promo_P_Sub_Grp_Id = B.PromotionVehicleProductSubGroupId
	 -- AND  A. Promo_Adv_Blk_Str_End_Dt = B.PromotionAdvertisementBlockStoreEndDate



    INSERT INTO [EDAA_DW].[FCT_PROMO_UT_P_HST] ([ProductItemId],
    [StoreId],
    [Prod_Hist_Sk],
    [Geo_Hist_Sk],
    [Promo_Adv_Blk_Id],
    [Promo_P_Grp_Id],
    [Promo_P_Sub_Grp_Id],
    [Promo_Adv_Blk_Str_Strt_Dt],
    [Dt_Sk],
    [Promo_Adv_Blk_Str_End_Dt],
    [Promo_T_Sl_Qty],
    [Promo_T_Sl_Amt],
    [Promo_T_Dmgn_Amt],
    [Promo_Avg_Rtl_Amt],
    [Avg_Reg_Rtl_Amt],
    [Avg_Cst_Amt],
    [Rwb_Fcst_T_Sl_Qty],
    [Rwb_Fcst_Bsln_Sl_Qty],
    [Tg_Lost_Sl_Qty],
    [Tg_Begn_Inv_Qty],
    [Tg_End_Inv_Qty],
    [Incr_T_Sl_Qty],
    [Incr_T_Sl_Amt],
    [Incr_T_Dmgn_Amt],
    [Tg_Pts_Sl_Qty],
    [Tg_Pts_Sl_Amt],
    [Tg_Pts_Dmgn_Amt],
    [Tg_Clrn_Sl_Qty],
    [Tg_Clrn_Sl_Amt],
    [Tg_Clrn_Dmgn_Amt],
    [UpdatedDate])

      SELECT

        P.[ProductItemId],
        P.[StoreId],
        D.[Prod_Hist_Sk],
        G.[Geo_Hist_Sk],
        P.[PromotionAdvertisementBlockId],
        P.[PromotionVehicleProductGroupId],
        P.[PromotionVehicleProductSubGroupId],
        P.[PromotionAdvertisementBlockStoreStartDate],
        CAST(format(PromotionAdvertisementBlockStoreEndDate, N'yyyyMMdd') AS int) AS Dt_Sk,
        P.[PromotionAdvertisementBlockStoreEndDate],
        P.[PromotionTotalSalesQuantity],
        P.[PromotionTotalSalesAmount],
        P.[PromotionTotalDirectMarginAmount],
        P.[PromotionAverageRetailAmount],
        P.[AverageRegisterRetailAmount],
        P.[AverageCostAmount],
        P.[ReplenishmentWorkbenchForecastTotalSalesQuantity],
        P.[ReplenishmentWorkbenchForecastBaselineSalesQuantity],
        P.[TotalAggregateLostSalesQuantity],
        P.[TotalAggregateBeginInventoryQuantity],
        P.[TotalAggregateEndInventoryQuantity],
        P.[IncrementalTotalSalesQuantity],
        P.[IncrementalTotalSalesAmount],
        P.[IncrementalTotalDirectMarginAmount],
        P.[TotalAggregatePermToSaleQuantity],
        P.[TotalAggregatePermToSaleAmount],
        P.[TotalAggregatePermToSaleDirectMarginAmount],
        P.[TotalAggregateClearanceQuantity],
        P.[TotalAggregateClearanceAmount],
        P.[TotalAggregateClearanceDirectMarginAmount],
        P.[UpdatedDate]

      FROM EDAA_STG.FCT_PROMO_UT_P_HST AS P
	  LEFT OUTER JOIN EDAA_DW.DIM_PROD AS D
        ON D.Itm_Sku = P.ProductItemId
        AND D.Is_Curr_Ind = 1
      LEFT OUTER JOIN EDAA_DW.DIM_GEO AS G
        ON G.Str_Id = P.StoreId
        AND G.Is_Curr_Ind = 1
	  where P.[PromotionAdvertisementBlockStoreStartDate]>= DATEADD(year, -3 , CAST(getdate() AS DATE))

---- To delete last 3yrs Data from DW Table

     DELETE FROM [EDAA_DW].[FCT_PROMO_UT_P_HST]
     WHERE [Promo_Adv_Blk_Str_Strt_Dt] < DATEADD(YEAR, -3, CAST(GETDATE() AS date))

----delete duplicate records for full load and Incr table from DW table

   ;WITH CTE
        AS (SELECT *,
                   row_number() OVER (PARTITION BY ProductItemId,
				                                   StoreId,
												   Promo_Adv_Blk_Id,
												   Promo_P_Grp_Id,
												   Promo_P_Sub_Grp_Id

                                      ORDER BY UpdatedDate desc
                                     ) as RN
            FROM [EDAA_DW].[FCT_PROMO_UT_P_HST]
           )
        DELETE FROM CTE WHERE RN > 1

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
