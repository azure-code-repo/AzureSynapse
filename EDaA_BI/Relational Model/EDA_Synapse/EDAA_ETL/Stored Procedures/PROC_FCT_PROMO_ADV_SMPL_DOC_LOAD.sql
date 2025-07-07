CREATE PROC [EDAA_ETL].[PROC_FCT_PROMO_ADV_SMPL_DOC_LOAD] AS
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
    DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = '[EDAA_ETL].[PROC_FCT_PROMO_ADV_SMPL_DOC_LOAD]';
    DECLARE @ERROR_LINE AS int;
    DECLARE @ERROR_MSG AS nvarchar(max);
    DECLARE @Date date;


    /*AUDIT LOG START*/
    EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR = 'EDAA_DW',
                                            @EXEC_JOB = '[EDAA_ETL].[PROC_FCT_PROMO_ADV_SMPL_DOC_LOAD]',
                                            @SRC_ENTY = '[EDAA_STG].[PROMO_ADV_SMPL_DOC_FCT]',
                                            @TGT_ENTY = '[EDAA_DW].[PROMO_ADV_SMPL_DOC_FCT]',
                                            @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT



	--- To prevent duplicate recors from inserting into DW table
	--DELETE A
 --     from  [EDAA_DW].[FCT_PROMO_ADV_SMPL_DOC] A inner join [EDAA_STG].[FCT_PROMO_ADV_SMPL_DOC] B
 --     On  A.Promo_Adv_Blk_Id= B.PromotionAdvertisementBlockId
 --     AND A.Promo_Adv_P_Grp_Id= B.PromotionAdvertisementProductGroupId
	--  AND A.Promo_Adv_P_Sub_Grp_Id= B.PromotionAdvertisementProductSubGroupId
	--  AND A.P_Item_Sku_Doc_Id= B.ProductItemSKUDocumentId


    --INSERT into [EDAA_DW].[FCT_PROMO_ADV_SMPL_DOC]
    INSERT INTO [EDAA_DW].[FCT_PROMO_ADV_SMPL_DOC] ([Promo_Adv_Blk_Id]
    , [Promo_Adv_P_Grp_Id]
    , [Promo_Adv_P_Sub_Grp_Id]
    , [P_Item_Sku_Doc_Id]
    , [Promo_Adv_Doc_Trtmt_Id]
    , [Promo_Adv_Smpl_Src_Id]
    , [Promo_Adv_Cpn_Req_Ind]
    , [Promo_Adv_Sgn_Req_Ind]
    , [Promo_Adv_Ut_Avl_Qty]
    , [Promo_Adv_On_Clrn_Qty]
	,[UpdatedDate])
      SELECT
        [PromotionAdvertisementBlockId],
        [PromotionAdvertisementProductGroupId],
        [PromotionAdvertisementProductSubGroupId],
        [ProductItemSKUDocumentId],
        [PromotionAdvertisementDocumentTreatmentId],
        [PromotionAdvertisementSampleSourceId],
        [PromotionAdvertisementSignRequiredIndicator],
        [PromotionAdvertisementCouponRequiredIndicator],
        [PromotionAdvertisementUnitAvailableQuantity],
        [PromotionAdvertisementOnClearenceQuantity],
		[UpdatedDate]
      FROM [EDAA_STG].[FCT_PROMO_ADV_SMPL_DOC] A
	  WHERE A.PromotionAdvertisementBlockId IN (SELECT Promo_Adv_Blk_Id
      FROM [EDAA_DW].[FCT_PROMO_ADV_BLK]
      WHERE Promo_Adv_Blk_Strt_Dt >= DATEADD(YEAR, -3, CAST(GETDATE() AS date)))

    ---- To delete last 3yrs Data from DW Table

    DELETE FROM [EDAA_DW].[FCT_PROMO_ADV_SMPL_DOC]
    WHERE Promo_Adv_Blk_Id IN (
                              SELECT Promo_Adv_Blk_Id
                              FROM [EDAA_DW].[FCT_PROMO_ADV_BLK]
                              WHERE Promo_Adv_Blk_Strt_Dt < DATEADD(YEAR, -3, CAST(GETDATE() AS date))
                          )

	--- To delete duplicates records in the DW table if exists
	;WITH CTE
	AS
	(
	SELECT *,
	row_number() OVER(PARTITION BY Promo_Adv_Blk_Id,Promo_Adv_P_Grp_Id,Promo_Adv_P_Sub_Grp_Id,P_Item_Sku_Doc_Id
	ORDER BY UpdatedDate desc
	) as RN
	FROM [EDAA_DW].[FCT_PROMO_ADV_SMPL_DOC]
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
