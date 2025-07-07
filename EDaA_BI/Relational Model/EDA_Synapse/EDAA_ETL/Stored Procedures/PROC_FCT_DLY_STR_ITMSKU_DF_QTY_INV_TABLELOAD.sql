CREATE PROC [EDAA_ETL].[PROC_FCT_DLY_STR_ITMSKU_DF_QTY_INV_TABLELOAD] AS
-- exec  [EDAA_ETL].[PROC_FCT_DLY_STR_ITMSKU_DF_QTY_INV_TABLELOAD]
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

	BEGIN TRY
		/*DATAMART AUDITING VARIABLES*/
		DECLARE @NEW_AUD_SKY BIGINT
		DECLARE @NBR_OF_RW_ISRT BIGINT
		DECLARE @NBR_OF_RW_UDT BIGINT
		DECLARE @EXEC_LYR VARCHAR(255)
		DECLARE @EXEC_JOB VARCHAR(500)
		DECLARE @SRC_ENTY VARCHAR(500)
		DECLARE @TGT_ENTY VARCHAR(500)
		DECLARE @UPDATEDTIMESTAMP DATETIME = GETDATE()

		/*AUDIT LOG START*/
		EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR = 'EDAA_DW'
			,@EXEC_JOB = '[EDAA_ETL].[PROC_FCT_DLY_STR_ITMSKU_DF_QTY_INV_TABLELOAD]'
			,@SRC_ENTY = '[EDAA_STG].[FCT_DSTB_CTR_INV_QTY] '
			,@TGT_ENTY = '[EDAA_DW].[FCT_DSTB_CTR_INV_QTY]'
			,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT;

		WITH incl_data
		AS (
			SELECT
			     FORMAT([UpdatedTimestamp], 'yyyyMMdd') AS DT_SK
				,ISNULL(CAST([StoreId] as INT),-1) as STR_ID
				,ISNULL(CAST([ProductId] as INT),-1) as P_ID
				,[DistributionCenterDirectStoreDeliveryCategory] as [DSTB_CTR_DRC_STO_DLVY_CT]
				,[DistributionCenterPalletCategory]as [DSTB_CTR_PLT_CT]
				,[DistributionCenterFinanceChargeAmount] as [DSTB_CTR_FNC_CHRG_AM]
				,[DistributionCenterFinanceCreditAmount] as [DSTB_CTR_FNC_CR_AM]
				,[DistributionCenterStorageChargeAmount] as [DSTB_CTR_STG_CHRG_AM]
				,[EstimatedDeliveryCostAmount] as [EST_DLVY_CST_AM]
				,[TransferQuantity]as [TRNSF_QT]
				,[WeeklyOutofStockQuantity]as [WK_OUT_OF_STK_QT]
				,[WeeklyOutofStockSubstituteQuantity]as [WK_OUT_OF_STK_SBS_QT]
				,[WeeklyOverrideSubstituteQuantity]as [WK_OVRD_SBS_QT]
				,[WeeklyAdjustedQuantity]as [WK_ADJ_QT]
				,[WeeklyDistributionCenterMovementQuantity]as [WK_DSTB_CTR_MVMT_QT]
				,[WeeklyMovementCostAmount] as [WK_MVMT_CST_AM]
				,[WeeklyMovementRetailAmount]as [WK_MVMT_RTL_AM]
				,[WeeklyReceivedQuantity] as [WK_RCV_QT]
				,[DistributionCenterBalanceOnHandQuantity]  as [DSTB_CTR_BAL_ON_HND_QT]
				,[RetailServiceCenterBalanceOnHandQuantity] as [RTL_SVC_CTR_BAL_ON_HND_QT]
				,[DistributionCenterCostAmount] as [DSTB_CTR_CST_AM]
				,@NEW_AUD_SKY as AUD_INS_SK
				,@NEW_AUD_SKY as AUD_UPD_SK
			FROM [EDAA_STG].[FCT_DSTB_CTR_INV_QTY] AS A
			LEFT OUTER JOIN (
				SELECT GEO_HIST_SK
					,STR_ID
					,VLD_FRM
					,VLD_TO
				FROM EDAA_DW.DIM_GEO
				WHERE IS_CURR_IND = 1
				) AS GEO ON GEO.STR_ID = A.[StoreId]
				AND CAST(A.[UpdatedTimestamp] AS DATE) BETWEEN GEO.VLD_FRM
					AND ISNULL(GEO.VLD_TO, CAST('2099-01-01' AS DATE))
			LEFT OUTER JOIN (
				SELECT PROD_HIST_SK
					,ITM_SKU
					,PROD_SK
					,VLD_FRM
					,VLD_TO
				FROM EDAA_DW.DIM_PROD
				WHERE IS_CURR_IND = 1
				) AS P ON P.ITM_SKU = A.[ProductId]
				AND CAST(A.[UpdatedTimestamp] AS DATE) BETWEEN P.VLD_FRM
					AND ISNULL(P.VLD_TO, CAST('2099-01-01' AS DATE))
				--WHERE [ProductId] IS NOT NULL AND [LocationId] IS NOT NULL
			)
		INSERT INTO [EDAA_DW].[FCT_DSTB_CTR_INV_QTY]
		SELECT
		 [DT_SK]
        ,[STR_ID]
        ,[P_ID]
        ,[DSTB_CTR_DRC_STO_DLVY_CT]
        ,[DSTB_CTR_PLT_CT]
        ,[DSTB_CTR_FNC_CHRG_AM]
        ,[DSTB_CTR_FNC_CR_AM]
        ,[DSTB_CTR_STG_CHRG_AM]
        ,[EST_DLVY_CST_AM]
        ,[TRNSF_QT]
        ,[WK_OUT_OF_STK_QT]
        ,[WK_OUT_OF_STK_SBS_QT]
        ,[WK_OVRD_SBS_QT]
        ,[WK_ADJ_QT]
        ,[WK_DSTB_CTR_MVMT_QT]
        ,[WK_MVMT_CST_AM]
        ,[WK_MVMT_RTL_AM]
        ,[WK_RCV_QT]
        ,[DSTB_CTR_BAL_ON_HND_QT]
        ,[RTL_SVC_CTR_BAL_ON_HND_QT]
        ,[DSTB_CTR_CST_AM]
        ,@UPDATEDTIMESTAMP
        ,[AUD_INS_SK]
        ,[AUD_UPD_SK]
		FROM incl_data

		PRINT ('---DW load complete---');
	END TRY

	BEGIN CATCH
		DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = 'EDAA_ETL.PROC_FCT_DLY_STR_ITMSKU_DF_QTY_INV_TABLELOAD'
		DECLARE @ERROR_LINE AS INT;
		DECLARE @ERROR_MSG AS NVARCHAR(max);

		SELECT @ERROR_LINE = ERROR_NUMBER()
			,@ERROR_MSG = ERROR_MESSAGE();

		--------- Log execution error ----------
		EXEC EDAA_CNTL.SP_LOG_AUD_ERR @AUD_SKY = @NEW_AUD_SKY
			,@ERROR_PROCEDURE_NAME = @ERROR_PROCEDURE_NAME
			,@ERROR_LINE = @ERROR_LINE
			,@ERROR_MSG = @ERROR_MSG;


		-- Detect the change
		THROW;
	END CATCH
END
