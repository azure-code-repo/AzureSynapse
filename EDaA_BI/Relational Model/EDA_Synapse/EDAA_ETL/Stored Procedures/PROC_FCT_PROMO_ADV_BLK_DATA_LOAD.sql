CREATE PROC [EDAA_ETL].[PROC_FCT_PROMO_ADV_BLK_DATA_LOAD] AS
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
        DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = 'EDAA_ETL.PROC_FCT_PROMO_ADV_BLK_DATA_LOAD';
        DECLARE @ERROR_LINE AS int;
        DECLARE @ERROR_MSG AS nvarchar(max);


        /*AUDIT LOG START*/
        EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR = 'EDAA_DW',
                                                @EXEC_JOB = 'EDAA_ETL.PROC_FCT_PROMO_ADV_BLK_DATA_LOAD',
                                                @SRC_ENTY = 'EDAA_STG.FCT_PROMO_ADV_BLK',
                                                @TGT_ENTY = 'EDAA_DW.FCT_PROMO_ADV_BLK',
                                                @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

        --DELETE FROM [EDAA_DW].[FCT_PROMO_ADV_BLK]
        --   WHERE Promo_Adv_Blk_End_Dt IN (SELECT
        --   Promo_Adv_Blk_End_Dt
        --   FROM [EDAA_DW].[FCT_PROMO_ADV_BLK]
        --   WHERE Promo_Adv_Blk_End_Dt >= (SELECT
        --   DATEADD(DAY, -30, CAST(GETDATE() AS date))))


        --- To prevent duplicate recors from inserting into DW table
        --DELETE A
        --from [EDAA_DW].[FCT_PROMO_ADV_BLK] A
        --    inner join [EDAA_STG].[FCT_PROMO_ADV_BLK] B
        --        On A.Promo_Adv_Blk_Id = B.PromotionAdvertisementBlockId
        --           AND A.Promo_Adv_Sn_Vhl_Id = B.PromotionAdvertisementSeasonVehicleId

        --INSERT into [EDAA_DW].[FCT_PROMOTION_ADVERTISEMENT_BLOCK]
        INSERT INTO [EDAA_DW].[FCT_PROMO_ADV_BLK]
        (
            [Promo_Adv_Blk_Id],
            [Promo_Adv_Sn_Vhl_Id],
            [Promo_Vhl_Id],
            [Promo_Adv_Pg_Id],
            [Promo_Adv_Blk_Nbr_Id],
            [Promo_Adv_Blk_Strt_Dt],
            [Promo_Adv_Blk_End_Dt],
            [Promo_Adv_Blk_Nm],
            [Promo_Adv_Blk_Dsc],
            [Promo_Adv_Sz_Qty],
            [Promo_Adv_Asgn_Byr_Id],
            [Promo_Adv_Blk_Thm],
            [Promo_Adv_Blk_Sts_Ct],
            [Promo_Adv_Prop_Vhl_Ct],
            [Promo_Adv_Prop_Pg_Id],
            [Promo_Adv_As_Buy_At_Least_Ofr_Ind],
            [UpdatedDate]
        )
        SELECT A.[PromotionAdvertisementBlockId],
               A.[PromotionAdvertisementSeasonVehicleId],
               A.[PromotionVehicleId],
               A.[PromotionAdvertisementPageId],
               A.[PromotionAdvertisementBlockNumberId],
               A.[PromotionAdvertisementBlockStartDate],
               A.[PromotionAdvertisementBlockEndDate],
               A.[PromotionAdvertisementBlockName],
               A.[PromotionAdvertisementBlockDescription],
               A.[PromotionAdvertisementSizeQuantity],
               A.[PromotionAdvertisementAssignBuyerId],
               A.[PromotionAdvertisementBlockTheme],
               A.[PromotionAdvertisementBlockStatusCategory],
               A.[PromotionAdvertisementProposedVehicleCategory],
               A.[PromotionAdvertisementProposedPageId],
               A.[PromotionAdvertisementAsBuyAtLeastOfferIndicator],
               A.[UpdatedDate]

        FROM [EDAA_STG].[FCT_PROMO_ADV_BLK] A
        WHERE
		A.[PromotionAdvertisementBlockStartDate] >= DATEADD(YEAR, -3, CAST(GETDATE() AS date))

        ---- To delete last 3yrs Data from DW Table

        DELETE FROM [EDAA_DW].[FCT_PROMO_ADV_BLK]
        WHERE [Promo_Adv_Blk_Strt_Dt] < DATEADD(YEAR, -3, CAST(GETDATE() AS date))

		 --- To delete duplicates records in the DW table during full load
        ;
        WITH CTE
        AS (SELECT *,
                   row_number() OVER (PARTITION BY Promo_Adv_Blk_Id
                                      ORDER BY UpdatedDate desc
                                     ) as RN
            FROM [EDAA_DW].[FCT_PROMO_ADV_BLK]
           )
        DELETE FROM CTE WHERE RN>1

        PRINT 'Data Load completed..'


    END TRY
    BEGIN CATCH

        SELECT @ERROR_LINE = ERROR_NUMBER(),
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
