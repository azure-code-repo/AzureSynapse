CREATE PROC [EDAA_ETL].[PROC_FCT_PROMO_BLK_P_SGRP_HST_DATA_LOAD] AS
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
    DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = '[EDAA_ETL].[PROC_FCT_PROMO_BLK_P_SGRP_HST_DATA_LOAD]';
    DECLARE @ERROR_LINE AS int;
    DECLARE @ERROR_MSG AS nvarchar(max);
    DECLARE @DELETE_LAST_HOUR int;



    /*AUDIT LOG START*/
    EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR = 'EDAA_DW',
                                   @EXEC_JOB = '[EDAA_ETL].[PROC_FCT_PROMO_BLK_P_SGRP_HST_DATA_LOAD]',
                                   @SRC_ENTY = 'EDAA_STG.FCT_PROMO_BLK_P_SGRP_HST',
                                   @TGT_ENTY = 'EDAA_DW.FCT_PROMO_BLK_P_SGRP_HST',
                                   @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT



	--Truncate and Load Data into Datamart Table

	TRUNCATE TABLE [EDAA_DW].[FCT_PROMO_BLK_P_SGRP_HST]

	INSERT INTO
		[EDAA_DW].[FCT_PROMO_BLK_P_SGRP_HST] (
		[Promo_Adv_Blk_Id],
		[Promo_Vhl_P_Grp_Id],
		[Promo_Vhl_P_Sub_Grp_Id],
		[Promo_Vhl_P_Sub_Grp_Dsc],
		[Promo_Vhl_P_Sub_Grp_Nte],
		[Fcst_Vdr_Lump_Sum_Amt],
		[Fcst_Promo_Vdr_Alw_Amt],
		[Fcst_T_Sl_Qty],
		[Fcst_T_Sl_Amt],
		[Fcst_T_Dmgn_Amt],
		[Fcst_Avg_Cst_Amt],
		[Fcst_Avg_Sl_Qty],
		[Fcst_Ut_Qty],
		[Fcst_Itm_Sku_Qty],
		[Ut_Qty],
		[Itm_Sku_Qty],
		[Promo_T_Sl_Qty],
		[Promo_T_Sl_Amt],
		[Promo_T_Dmgn_Amt],
		[Promo_Avg_Rtl_Amt],
		[Promo_Min_Rtl_Amt],
		[Promo_Max_Rtl_Amt],
		[Avg_Rgl_Rtl_Amt],
		[Avg_Cst_Amt],
		[Rwb_Fcst_T_Sl_Qty],
		[Rwb_Fcst_Bsln_Sl_Qty],
		[Tg_Lost_Sl_Qty],
		[Tg_Begn_Inv_Qty],
		[Tg_End_Inv_Qty],
		[Incr_T_Sl_Qty],
		[Incr_T_Sl_Amt],
		[Incr_T_Dmgn_Amt],
		[Tg_Pts_Sl_Qty] ,
		[Tg_Pts_Sl_Amt],
		[Tg_Pts_Dmgn_Amt],
		[Tg_Clrn_Sl_Qty],
		[Tg_Clrn_Sl_Amt],
		[Tg_Clrn_Dmgn_Amt]
	    )
	SELECT
		[PROMO_BLK_ID],
		[P_GRP_ID],
		[P_SGRP_ID],
		[P_SGRP_SHRT_DSC_TX],
		[P_SGRP_NTE_TX],
		[FCST_VDR_LUMP_SUM_AM],
		[FCST_PROMO_VDR_ALW_AM],
		[FCST_T_SL_QT],
		[FCST_T_SL_AM],
		[FCST_T_DMGN_AM],
		[FCST_AVG_CST_AM],
		[FCST_AVG_SL_QT],
		[FCST_UT_QT],
		[FCST_P_UPC_QT],
		[UT_QT],
		[P_UPC_QT],
		[PROMO_T_SL_QT],
		[PROMO_T_SL_AM],
		[PROMO_T_DMGN_AM],
		[PROMO_AVG_RTL_AM],
		[PROMO_MIN_RTL_AM],
		[PROMO_MAX_RTL_AM],
		[AVG_REG_RTL_AM],
		[AVG_CST_AM],
		[RWB_FCST_T_SL_QT],
		[RWB_FCST_BSLN_SL_QT],
		[TG_LOST_SL_QT],
		[TG_BGN_INV_QT],
		[TG_END_INV_QT],
		[INCRMNTL_T_SL_QT],
		[INCRMNTL_T_SL_AM],
		[INCRMNTL_T_DMGN_AM],
		[TG_PTS_QT] ,
		[TG_PTS_AM],
		[TG_PTS_DMGN_AM],
		[TG_CLRN_QT],
		[TG_CLRN_AM],
		[TG_CLRN_DMGN_AM]
    FROM
		EDAA_STG.FCT_PROMO_BLK_P_SGRP_HST A
		WHERE A.Promo_Blk_Id IN (SELECT Promo_Adv_Blk_Id
  FROM [EDAA_DW].[FCT_PROMO_ADV_BLK]
  WHERE Promo_Adv_Blk_Strt_Dt >= DATEADD(YEAR, -3, CAST(GETDATE() AS date)))

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
