CREATE PROC [EDAA_ETL].[PROC_FCT_PROMO_PROD_HIST_DATA_LOAD] AS
BEGIN
  BEGIN TRY

	SET NOCOUNT ON;

    /*DATAMART AUDITING VARIABLES*/
    DECLARE @NEW_AUD_SKY bigint;
    DECLARE @NBR_OF_RW_ISRT int;
    DECLARE @NBR_OF_RW_UDT int;
    DECLARE @EXEC_LYR varchar(255);
    DECLARE @EXEC_JOB varchar(500);
    DECLARE @SRC_ENTY varchar(500);
    DECLARE @TGT_ENTY varchar(500);
    DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) ='EDAA_ETL.PROC_FCT_PROMO_PROD_HIST_DATA_LOAD';
    DECLARE @ERROR_LINE AS int;
    DECLARE @ERROR_MSG AS nvarchar(max);
    DECLARE @DELETE_LAST_HOUR int;

    /*AUDIT LOG START*/
    EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR ='EDAA_DW',
                                   @EXEC_JOB ='EDAA_ETL.PROC_FCT_PROMO_PROD_HIST_DATA_LOAD',
                                   @SRC_ENTY ='EDAA_STG.FCT_PROMO_PROD_HIST',
                                   @TGT_ENTY ='EDAA_DW.FCT_PROMO_PROD_HIST',
                                   @NEW_AUD_SKY=@NEW_AUD_SKY OUTPUT

/* Validate Late arriving dimension: DIM_PROD */
PRINT('LATEARRIVINGDIMENSION: DIM_PROD_SB')

IF OBJECT_ID('TEMPDB..#LATEARRIVINGPRODDIMENSIONFACTRECORDS_PROD_HIST') IS NOT NULL
DROP TABLE #LATEARRIVINGPRODDIMENSIONFACTRECORDS_PROD_HIST

CREATE TABLE #LATEARRIVINGPRODDIMENSIONFACTRECORDS_PROD_HIST
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
FROM  EDAA_STG.FCT_PROMO_PROD_HIST
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

	FROM #LATEARRIVINGPRODDIMENSIONFACTRECORDS_PROD_HIST
	WHERE 	IS_PROD_MISSING = 1
	) AS MISSING_PROD_SB
	WHERE 	NOT EXISTS
	(
	SELECT 1 FROM EDAA_DW.DIM_PROD AS DIM
	WHERE
	      MISSING_PROD_SB.ProductItemId = DIM.Itm_Sku

	)

END
      --DELETE FROM [EDAA_DW].[FCT_PROMO_PROD_HIST]
      --WHERE Promo_Blk_Max_End_Dt IN (SELECT
      --Promo_Blk_Max_End_Dt
      --FROM [EDAA_DW].[FCT_PROMO_PROD_HIST]
      --WHERE Promo_Blk_Max_End_Dt >= (SELECT
      --DATEADD(DAY, -30, CAST(GETDATE() AS date))))

--- To prevent duplicate records from inserting into DW table

	  --DELETE A
   --   from  [EDAA_DW].[FCT_PROMO_PROD_HIST] A inner join [EDAA_STG].[FCT_PROMO_PROD_HIST] B
   --   On A.ProductItemId = B.ProductItemId
	  --AND A.Promo_Adv_Blk_Id= B.PromotionAdvertisementBlockId
	  --AND A.Promo_P_Grp_Id= B.PromotionVehicleProductGroupId
	  --AND A.Promo_P_Sub_Grp_Id= B.PromotionVehicleProductSubGroupId


    --INSERT into [EDAA_DW].[FCT_PROMO_PROD_HIST]
    INSERT INTO [EDAA_DW].[FCT_PROMO_PROD_HIST]
	(
		[ProductItemId] ,
		[Prod_Hist_Sk] ,
		[Promo_Adv_Blk_Id] ,
		[Promo_P_Grp_Id] ,
		[Promo_P_Sub_Grp_Id] ,
		[Dt_Sk]  ,
		[Promo_Blk_Min_Strt_Dt] ,
		[Promo_Blk_Max_End_Dt] ,
		[Ut_Qty] ,
		[Promo_Rck_Id]  ,
		[Promo_T_Sl_Qty] ,
		[Promo_T_Sl_Amt] ,
		[Promo_T_Dmgn_Amt] ,
		[Promo_Avg_Rtl_Amt] ,
		[Promo_Min_Rtl_Amt] ,
		[Promo_Max_Rtl_Amt] ,
		[Avg_Reg_Rtl_Amt] ,
		[Avg_Cst_Amt] ,
		[Rwb_Fcst_T_Sl_Qty] ,
		[Rwb_Fcst_Bsln_Sl_Qty] ,
		[Tg_Lost_Sl_Qty],
		[Tg_Begn_Inv_Qty] ,
		[Tg_End_Inv_Qty] ,
		[Incr_T_Sl_Qty] ,
		[Incr_T_Sl_Amt] ,
		[Incr_T_Dmgn_Amt] ,
		[Tg_Pts_Sl_Qty] ,
		[Tg_Pts_Sl_Amt] ,
		[Tg_Pts_Dmgn_Amt] ,
		[Tg_Clrn_Sl_Qty] ,
		[Tg_Clrn_Sl_Amt] ,
		[Tg_Clrn_Dmgn_Amt],
		[UpdatedDate]
	)

      SELECT
		A.[ProductItemId] ,
		C.[Prod_Hist_Sk],
		A.[PromotionAdvertisementBlockId],
		A.[PromotionVehicleProductGroupId] ,
		A.[PromotionVehicleProductSubGroupId] ,
		CAST(format(A.PromotionBlockMinimumStartDate, N'yyyyMMdd') as int) as Dt_Sk,
		A.[PromotionBlockMinimumStartDate] ,
		A.[PromotionBlockMaximumEndDate] ,
		A.[UnitQuantity],
		A.[PromotionRainCheckId],
		A.[PromotionTotalSalesQuantity] ,
		A.[PromotionTotalSalesAmount],
		A.[PromotionTotalDirectMarginAmount],
		A.[PromotionAverageRetailAmount] ,
		A.[PromotionMinimumRetailAmount] ,
		A.[PromotionMaximumRetailAmount] ,
		A.[AverageRegisterRetailAmount] ,
		A.[AverageCostAmount],
		A.[ReplenishmentWorkbenchForecastTotalSalesQuantity] ,
		A.[ReplenishmentWorkbenchForecastBaselineSalesQuantity],
		A.[TotalAggregateLostSalesQuantity] ,
		A.[TotalAggregateBeginInventoryQuantity],
		A.[TotalAggregateEndInventoryQuantity] ,
		A.[IncrementTotalSalesQuantity] ,
		A.[IncrementTotalSalesAmount] ,
		A.[IncrementTotalDirectMarginAmount],
		A.[TotalAggregatePermToSaleQuantity],
		A.[TotalAggregatePermToSaleAmount] ,
		A.[TotalAggregatePermToSaleDirectMarginAmount],
		A.[TotalAggregateClearanceQuantity],
		A.[TotalAggregateClearanceAmount] ,
		A.[TotalAggregateClearanceDirectMarginAmount],
		A.[Updateddate]

       FROM [EDAA_STG].[FCT_PROMO_PROD_HIST] A
	   left join EDAA_DW.DIM_PROD C
	   on cast(C.Itm_Sku as bigint) = cast(A.ProductItemId as bigint)
	   and c.Is_Curr_Ind = 1
	   where A.[PromotionBlockMinimumStartDate] >= DATEADD(year, -3 , CAST(getdate() AS DATE))

	   ---- To delete last 3yrs Data from DW Table

	   DELETE FROM [EDAA_DW].[FCT_PROMO_PROD_HIST]
       where [Promo_Blk_Min_Strt_Dt] < DATEADD(year, -3, CAST(getdate() AS DATE))

	   ----delete duplicate records for full load and Incr table from DW table

	  ; WITH CTE
        AS (SELECT *,
                   row_number() OVER (PARTITION BY ProductItemId,
				                                   Promo_Adv_Blk_Id,
												   Promo_P_Grp_Id,
												   Promo_P_Sub_Grp_Id
                                      ORDER BY UpdatedDate desc
                                     ) as RN
            FROM [EDAA_DW].[FCT_PROMO_PROD_HIST]
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
