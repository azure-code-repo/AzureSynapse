CREATE PROC [EDAA_ETL].[PROC_FCT_DSTB_CTR_INV_QTY_DM_PRES_INCL_LOAD] AS
BEGIN

    --exec [EDAA_ETL].[PROC_FCT_DSTB_CTR_INV_QTY_DM_PRES_INCL_LOAD]
    BEGIN TRY
        -- SET NOCOUNT ON added to prevent extra result sets from
        -- interfering with SELECT statements.
        SET NOCOUNT ON

        /*DATAMART AUDITING VARIABLES*/
        DECLARE @NEW_AUD_SKY bigint
        DECLARE @NBR_OF_RW_ISRT bigint
        DECLARE @NBR_OF_RW_UDT bigint
        DECLARE @EXEC_LYR varchar(255)
        DECLARE @EXEC_JOB varchar(500)
        DECLARE @SRC_ENTY varchar(500)
        DECLARE @TGT_ENTY varchar(500)
        DECLARE @UPDATEDTIMESTAMP datetime = GETDATE()

        /*AUDIT LOG START*/
        EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR = 'EDAA_PRES',
                                                @EXEC_JOB = '[EDAA_ETL].[PROC_FCT_DSTB_CTR_INV_QTY_DM_PRES_INCL_LOAD]',
                                                @SRC_ENTY = '[EDAA_DW].[FCT_DSTB_CTR_INV_QTY]',
                                                @TGT_ENTY = '[EDAA_PRES].[FCT_DSTB_CTR_INV_QTY_TY_LY_FSC]',
                                                @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

        --DROP TABLE IF EXISTS
        IF OBJECT_ID('tempdb..#DIM_DLY_CAL_DSTB_INV') IS NOT NULL
        BEGIN
            DROP TABLE #DIM_DLY_CAL_DSTB_INV
        END

        --SELECT ONLY THIS YEAR DT_SK and Last YEAR DT_SK
        SELECT DISTINCT
            a.DT_SK,
            FORMAT(a.Lst_Yr_Fsc_Dt, 'yyyyMMdd') AS LY_DT_SK,
            FORMAT(a.Lst_Yr_Hldy_Dt, 'yyyyMMdd') AS LY_DT_SK_HLDY,
            FORMAT(Lst_Yr_Cal_Dt, 'yyyyMMdd') as LY_DT_SK_CAL
        INTO #DIM_DLY_CAL_DSTB_INV
        FROM edaa_dw.dim_dly_cal a
            INNER JOIN EDAA_STG.FCT_DSTB_CTR_INV_QTY b
                ON A.DT_SK = FORMAT(b.[UpdatedTimestamp], 'yyyyMMdd')

        ---- select * from #DIM_DLY_CAL_DSTB_INV

        ---- Get Incremental timestamp
        DECLARE @INCR_TIMESTAMP datetime
        SET @INCR_TIMESTAMP =
        (
            SELECT UPDATEDTIMESTAMP
            FROM [EDAA_STG].[FCT_SL_WTRMARK]
            WHERE TABLE_NME = 'FCT_DSTB_CTR_INV_QTY_TY_LY_FSC'
        )
        PRINT (@INCR_TIMESTAMP)
        if @INCR_TIMESTAMP = '1900-01-01'
        begin
            Truncate table [EDAA_PRES].[FCT_DSTB_CTR_INV_QTY_TY_LY_FSC]
        end
        PRINT ' Data load started for presentation table'

        --Insert LY & TY
        ;
        WITH TY_Distribution_Center_Inventory_Load
        AS (SELECT a.[DT_SK],
                   b.[LY_DT_SK],
                   b.[LY_DT_SK_HLDY],
                   b.[LY_DT_SK_CAL],
                   a.[STR_ID],
                   a.[P_ID],
                   a.[DSTB_CTR_DRC_STO_DLVY_CT],
                   a.[DSTB_CTR_PLT_CT],
                   a.[DSTB_CTR_FNC_CHRG_AM] AS [DSTB_CTR_FNC_CHRG_AM_TY_FSC],
                   0 AS [DSTB_CTR_FNC_CHRG_AM_LY_FSC],
                   a.[DSTB_CTR_FNC_CR_AM] AS [DSTB_CTR_FNC_CR_AM_TY_FSC],
                   0 AS [DSTB_CTR_FNC_CR_AM_LY_FSC],
                   a.[DSTB_CTR_STG_CHRG_AM] AS [DSTB_CTR_STG_CHRG_AM_TY_FSC],
                   0 AS [DSTB_CTR_STG_CHRG_AM_LY_FSC],
                   a.[EST_DLVY_CST_AM] As [EST_DLVY_CST_AM_TY_FSC],
                   0 AS [EST_DLVY_CST_AM_LY_FSC],
                   a.[TRNSF_QT] AS [TRNSF_QT_TY_FSC],
                   0 AS [TRNSF_QT_LY_FSC],
                   a.[WK_OUT_OF_STK_QT] AS [WK_OUT_OF_STK_QT_TY_FSC],
                   0 AS [WK_OUT_OF_STK_QT_LY_FSC],
                   a.[WK_OUT_OF_STK_SBS_QT] AS [WK_OUT_OF_STK_SBS_QT_TY_FSC],
                   0 AS [WK_OUT_OF_STK_SBS_QT_LY_FSC],
                   a.[WK_OVRD_SBS_QT] AS [WK_OVRD_SBS_QT_TY_FSC],
                   0 AS [WK_OVRD_SBS_QT_LY_FSC],
                   a.[WK_ADJ_QT] AS [WK_ADJ_QT_TY_FSC],
                   0 AS [WK_ADJ_QT_LY_FSC],
                   a.[WK_DSTB_CTR_MVMT_QT] AS [WK_DSTB_CTR_MVMT_QT_TY_FSC],
                   0 AS [WK_DSTB_CTR_MVMT_QT_LY_FSC],
                   a.[WK_MVMT_CST_AM] AS [WK_MVMT_CST_AM_TY_FSC],
                   0 AS [WK_MVMT_CST_AM_LY_FSC],
                   a.[WK_MVMT_RTL_AM] AS [WK_MVMT_RTL_AM_TY_FSC],
                   0 AS [WK_MVMT_RTL_AM_LY_FSC],
                   a.[WK_RCV_QT] AS [WK_RCV_QT_TY_FSC],
                   0 AS [WK_RCV_QT_LY_FSC],
                   a.[DSTB_CTR_BAL_ON_HND_QT] AS [DSTB_CTR_BAL_ON_HND_QT_TY_FSC],
                   0 AS [DSTB_CTR_BAL_ON_HND_QT_LY_FSC],
                   a.[RTL_SVC_CTR_BAL_ON_HND_QT] AS [RTL_SVC_CTR_BAL_ON_HND_QT_TY_FSC],
                   0 AS [RTL_SVC_CTR_BAL_ON_HND_QT_LY_FSC],
                   a.[DSTB_CTR_CST_AM] AS [DSTB_CTR_CST_AM_TY_FSC],
                   0 AS [DSTB_CTR_CST_AM_LY_FSC],
                   0 AS [DSTB_CTR_FNC_CHRG_AM_LY_HLDY],
                   0 AS [DSTB_CTR_FNC_CR_AM_LY_HLDY],
                   0 AS [DSTB_CTR_STG_CHRG_AM_LY_HLDY],
                   0 AS [EST_DLVY_CST_AM_LY_HLDY],
                   0 AS [TRNSF_QT_LY_HLDY],
                   0 AS [WK_OUT_OF_STK_QT_LY_HLDY],
                   0 AS [WK_OUT_OF_STK_SBS_QT_LY_HLDY],
                   0 AS [WK_OVRD_SBS_QT_LY_HLDY],
                   0 AS [WK_ADJ_QT_LY_HLDY],
                   0 AS [WK_DSTB_CTR_MVMT_QT_LY_HLDY],
                   0 AS [WK_MVMT_CST_AM_LY_HLDY],
                   0 AS [WK_MVMT_RTL_AM_LY_HLDY],
                   0 AS [WK_RCV_QT_LY_HLDY],
                   0 AS [DSTB_CTR_BAL_ON_HND_QT_LY_HLDY],
                   0 AS [RTL_SVC_CTR_BAL_ON_HND_QT_LY_HLDY],
                   0 AS [DSTB_CTR_CST_AM_LY_HLDY],
                   0 AS [DSTB_CTR_FNC_CHRG_AM_LY_CAL],
                   0 AS [DSTB_CTR_FNC_CR_AM_LY_CAL],
                   0 AS [DSTB_CTR_STG_CHRG_AM_LY_CAL],
                   0 AS [EST_DLVY_CST_AM_LY_CAL],
                   0 AS [TRNSF_QT_LY_CAL],
                   0 AS [WK_OUT_OF_STK_QT_LY_CAL],
                   0 AS [WK_OUT_OF_STK_SBS_QT_LY_CAL],
                   0 AS [WK_OVRD_SBS_QT_LY_CAL],
                   0 AS [WK_ADJ_QT_LY_CAL],
                   0 AS [WK_DSTB_CTR_MVMT_QT_LY_CAL],
                   0 AS [WK_MVMT_CST_AM_LY_CAL],
                   0 AS [WK_MVMT_RTL_AM_LY_CAL],
                   0 AS [WK_RCV_QT_LY_CAL],
                   0 AS [DSTB_CTR_BAL_ON_HND_QT_LY_CAL],
                   0 AS [RTL_SVC_CTR_BAL_ON_HND_QT_LY_CAL],
                   0 AS [DSTB_CTR_CST_AM_LY_CAL]
            FROM [V_EDAA_DW].[FCT_DSTB_CTR_INV_QTY] as A
                inner join #DIM_DLY_CAL_DSTB_INV AS B
                    on A.DT_SK = B.[DT_SK]
                       and A.[UDT_TS] >= @INCR_TIMESTAMP
           ),
             --select * from TY_Distribution_Center_Inventory_Load
             LY_Distribution_Center_Inventory_Load
        AS (SELECT a.[DT_SK],
                   b.[LY_DT_SK],
                   b.[LY_DT_SK_HLDY],
                   b.[LY_DT_SK_CAL],
                   a.[STR_ID],
                   a.[P_ID],
                   a.[DSTB_CTR_DRC_STO_DLVY_CT],
                   a.[DSTB_CTR_PLT_CT],
                   0 AS [DSTB_CTR_FNC_CHRG_AM_TY_FSC],
                   a.[DSTB_CTR_FNC_CHRG_AM] AS [DSTB_CTR_FNC_CHRG_AM_LY_FSC],
                   0 AS [DSTB_CTR_FNC_CR_AM_TY_FSC],
                   a.[DSTB_CTR_FNC_CR_AM] AS [DSTB_CTR_FNC_CR_AM_LY_FSC],
                   0 AS [DSTB_CTR_STG_CHRG_AM_TY_FSC],
                   a.[DSTB_CTR_STG_CHRG_AM] AS [DSTB_CTR_STG_CHRG_AM_LY_FSC],
                   0 As [EST_DLVY_CST_AM_TY_FSC],
                   a.[EST_DLVY_CST_AM] AS [EST_DLVY_CST_AM_LY_FSC],
                   0 AS [TRNSF_QT_TY_FSC],
                   a.[TRNSF_QT] AS [TRNSF_QT_LY_FSC],
                   0 AS [WK_OUT_OF_STK_QT_TY_FSC],
                   a.[WK_OUT_OF_STK_QT] AS [WK_OUT_OF_STK_QT_LY_FSC],
                   0 AS [WK_OUT_OF_STK_SBS_QT_TY_FSC],
                   a.[WK_OUT_OF_STK_SBS_QT] AS [WK_OUT_OF_STK_SBS_QT_LY_FSC],
                   0 AS [WK_OVRD_SBS_QT_TY_FSC],
                   a.[WK_OVRD_SBS_QT] AS [WK_OVRD_SBS_QT_LY_FSC],
                   0 AS [WK_ADJ_QT_TY_FSC],
                   a.[WK_ADJ_QT] AS [WK_ADJ_QT_LY_FSC],
                   0 AS [WK_DSTB_CTR_MVMT_QT_TY_FSC],
                   a.[WK_DSTB_CTR_MVMT_QT] AS [WK_DSTB_CTR_MVMT_QT_LY_FSC],
                   0 AS [WK_MVMT_CST_AM_TY_FSC],
                   a.[WK_MVMT_CST_AM] AS [WK_MVMT_CST_AM_LY_FSC],
                   0 AS [WK_MVMT_RTL_AM_TY_FSC],
                   a.[WK_MVMT_RTL_AM] AS [WK_MVMT_RTL_AM_LY_FSC],
                   0 AS [WK_RCV_QT_TY_FSC],
                   a.[WK_RCV_QT] AS [WK_RCV_QT_LY_FSC],
                   0 AS [DSTB_CTR_BAL_ON_HND_QT_TY_FSC],
                   a.[DSTB_CTR_BAL_ON_HND_QT] AS [DSTB_CTR_BAL_ON_HND_QT_LY_FSC],
                   0 AS [RTL_SVC_CTR_BAL_ON_HND_QT_TY_FSC],
                   a.[RTL_SVC_CTR_BAL_ON_HND_QT] AS [RTL_SVC_CTR_BAL_ON_HND_QT_LY_FSC],
                   0 AS [DSTB_CTR_CST_AM_TY_FSC],
                   a.[DSTB_CTR_CST_AM] AS [DSTB_CTR_CST_AM_LY_FSC],
                   0 AS [DSTB_CTR_FNC_CHRG_AM_LY_HLDY],
                   0 AS [DSTB_CTR_FNC_CR_AM_LY_HLDY],
                   0 AS [DSTB_CTR_STG_CHRG_AM_LY_HLDY],
                   0 AS [EST_DLVY_CST_AM_LY_HLDY],
                   0 AS [TRNSF_QT_LY_HLDY],
                   0 AS [WK_OUT_OF_STK_QT_LY_HLDY],
                   0 AS [WK_OUT_OF_STK_SBS_QT_LY_HLDY],
                   0 AS [WK_OVRD_SBS_QT_LY_HLDY],
                   0 AS [WK_ADJ_QT_LY_HLDY],
                   0 AS [WK_DSTB_CTR_MVMT_QT_LY_HLDY],
                   0 AS [WK_MVMT_CST_AM_LY_HLDY],
                   0 AS [WK_MVMT_RTL_AM_LY_HLDY],
                   0 AS [WK_RCV_QT_LY_HLDY],
                   0 AS [DSTB_CTR_BAL_ON_HND_QT_LY_HLDY],
                   0 AS [RTL_SVC_CTR_BAL_ON_HND_QT_LY_HLDY],
                   0 AS [DSTB_CTR_CST_AM_LY_HLDY],
                   0 AS [DSTB_CTR_FNC_CHRG_AM_LY_CAL],
                   0 AS [DSTB_CTR_FNC_CR_AM_LY_CAL],
                   0 AS [DSTB_CTR_STG_CHRG_AM_LY_CAL],
                   0 AS [EST_DLVY_CST_AM_LY_CAL],
                   0 AS [TRNSF_QT_LY_CAL],
                   0 AS [WK_OUT_OF_STK_QT_LY_CAL],
                   0 AS [WK_OUT_OF_STK_SBS_QT_LY_CAL],
                   0 AS [WK_OVRD_SBS_QT_LY_CAL],
                   0 AS [WK_ADJ_QT_LY_CAL],
                   0 AS [WK_DSTB_CTR_MVMT_QT_LY_CAL],
                   0 AS [WK_MVMT_CST_AM_LY_CAL],
                   0 AS [WK_MVMT_RTL_AM_LY_CAL],
                   0 AS [WK_RCV_QT_LY_CAL],
                   0 AS [DSTB_CTR_BAL_ON_HND_QT_LY_CAL],
                   0 AS [RTL_SVC_CTR_BAL_ON_HND_QT_LY_CAL],
                   0 AS [DSTB_CTR_CST_AM_LY_CAL]
            FROM [V_EDAA_DW].[FCT_DSTB_CTR_INV_QTY] as A
                inner join #DIM_DLY_CAL_DSTB_INV AS B
                    on A.DT_SK = B.LY_DT_SK
					   and A.[UDT_TS] >= @INCR_TIMESTAMP
           ),
             --select * from LY_Distribution_Center_Inventory_Load
             Union_Distribution_Center_Inventory_Load
        AS (SELECT *
            FROM TY_Distribution_Center_Inventory_Load
            UNION ALL
            SELECT *
            FROM LY_Distribution_Center_Inventory_Load
           ),
             Distribution_Center_Inv_Load
        AS (SELECT [DT_SK],
                   [LY_DT_SK],
                   [LY_DT_SK_HLDY],
                   [LY_DT_SK_CAL],
                   [STR_ID],
                   [P_ID],
                   [DSTB_CTR_DRC_STO_DLVY_CT],
                   [DSTB_CTR_PLT_CT],
                   sum([DSTB_CTR_FNC_CHRG_AM_TY_FSC]) AS [DSTB_CTR_FNC_CHRG_AM_TY_FSC],
                   sum([DSTB_CTR_FNC_CHRG_AM_LY_FSC]) AS [DSTB_CTR_FNC_CHRG_AM_LY_FSC],
                   sum([DSTB_CTR_FNC_CR_AM_TY_FSC]) AS [DSTB_CTR_FNC_CR_AM_TY_FSC],
                   sum([DSTB_CTR_FNC_CR_AM_LY_FSC]) AS [DSTB_CTR_FNC_CR_AM_LY_FSC],
                   sum([DSTB_CTR_STG_CHRG_AM_TY_FSC]) AS [DSTB_CTR_STG_CHRG_AM_TY_FSC],
                   sum([DSTB_CTR_STG_CHRG_AM_LY_FSC]) AS [DSTB_CTR_STG_CHRG_AM_LY_FSC],
                   sum([EST_DLVY_CST_AM_TY_FSC]) AS [EST_DLVY_CST_AM_TY_FSC],
                   sum([EST_DLVY_CST_AM_LY_FSC]) AS [EST_DLVY_CST_AM_LY_FSC],
                   sum([TRNSF_QT_TY_FSC]) AS [TRNSF_QT_TY_FSC],
                   sum([TRNSF_QT_LY_FSC]) AS [TRNSF_QT_LY_FSC],
                   sum([WK_OUT_OF_STK_QT_TY_FSC]) AS [WK_OUT_OF_STK_QT_TY_FSC],
                   sum([WK_OUT_OF_STK_QT_LY_FSC]) AS [WK_OUT_OF_STK_QT_LY_FSC],
                   sum([WK_OUT_OF_STK_SBS_QT_TY_FSC]) AS [WK_OUT_OF_STK_SBS_QT_TY_FSC],
                   sum([WK_OUT_OF_STK_SBS_QT_LY_FSC]) AS [WK_OUT_OF_STK_SBS_QT_LY_FSC],
                   sum([WK_OVRD_SBS_QT_TY_FSC]) AS [WK_OVRD_SBS_QT_TY_FSC],
                   sum([WK_OVRD_SBS_QT_LY_FSC]) AS [WK_OVRD_SBS_QT_LY_FSC],
                   sum([WK_ADJ_QT_TY_FSC]) AS [WK_ADJ_QT_TY_FSC],
                   sum([WK_ADJ_QT_LY_FSC]) AS [WK_ADJ_QT_LY_FSC],
                   sum([WK_DSTB_CTR_MVMT_QT_TY_FSC]) AS [WK_DSTB_CTR_MVMT_QT_TY_FSC],
                   sum([WK_DSTB_CTR_MVMT_QT_LY_FSC]) AS [WK_DSTB_CTR_MVMT_QT_LY_FSC],
                   sum([WK_MVMT_CST_AM_TY_FSC]) AS [WK_MVMT_CST_AM_TY_FSC],
                   sum([WK_MVMT_CST_AM_LY_FSC]) AS [WK_MVMT_CST_AM_LY_FSC],
                   sum([WK_MVMT_RTL_AM_TY_FSC]) AS [WK_MVMT_RTL_AM_TY_FSC],
                   sum([WK_MVMT_RTL_AM_LY_FSC]) AS [WK_MVMT_RTL_AM_LY_FSC],
                   sum([WK_RCV_QT_TY_FSC]) AS [WK_RCV_QT_TY_FSC],
                   sum([WK_RCV_QT_LY_FSC]) AS [WK_RCV_QT_LY_FSC],
                   sum([DSTB_CTR_BAL_ON_HND_QT_TY_FSC]) AS [DSTB_CTR_BAL_ON_HND_QT_TY_FSC],
                   sum([DSTB_CTR_BAL_ON_HND_QT_LY_FSC]) AS [DSTB_CTR_BAL_ON_HND_QT_LY_FSC],
                   sum([RTL_SVC_CTR_BAL_ON_HND_QT_TY_FSC]) AS [RTL_SVC_CTR_BAL_ON_HND_QT_TY_FSC],
                   sum([RTL_SVC_CTR_BAL_ON_HND_QT_LY_FSC]) AS [RTL_SVC_CTR_BAL_ON_HND_QT_LY_FSC],
                   sum([DSTB_CTR_CST_AM_TY_FSC]) AS [DSTB_CTR_CST_AM_TY_FSC],
                   sum([DSTB_CTR_CST_AM_LY_FSC]) AS [DSTB_CTR_CST_AM_LY_FSC],
                   sum([DSTB_CTR_FNC_CHRG_AM_LY_HLDY]) AS [DSTB_CTR_FNC_CHRG_AM_LY_HLDY],
                   sum([DSTB_CTR_FNC_CR_AM_LY_HLDY]) AS [DSTB_CTR_FNC_CR_AM_LY_HLDY],
                   sum([DSTB_CTR_STG_CHRG_AM_LY_HLDY]) AS [DSTB_CTR_STG_CHRG_AM_LY_HLDY],
                   sum([EST_DLVY_CST_AM_LY_HLDY]) AS [EST_DLVY_CST_AM_LY_HLDY],
                   sum([TRNSF_QT_LY_HLDY]) AS [TRNSF_QT_LY_HLDY],
                   sum([WK_OUT_OF_STK_QT_LY_HLDY]) AS [WK_OUT_OF_STK_QT_LY_HLDY],
                   sum([WK_OUT_OF_STK_SBS_QT_LY_HLDY]) AS [WK_OUT_OF_STK_SBS_QT_LY_HLDY],
                   sum([WK_OVRD_SBS_QT_LY_HLDY]) AS [WK_OVRD_SBS_QT_LY_HLDY],
                   sum([WK_ADJ_QT_LY_HLDY]) AS [WK_ADJ_QT_LY_HLDY],
                   sum([WK_DSTB_CTR_MVMT_QT_LY_HLDY]) AS [WK_DSTB_CTR_MVMT_QT_LY_HLDY],
                   sum([WK_MVMT_CST_AM_LY_HLDY]) AS [WK_MVMT_CST_AM_LY_HLDY],
                   sum([WK_MVMT_RTL_AM_LY_HLDY]) AS [WK_MVMT_RTL_AM_LY_HLDY],
                   sum([WK_RCV_QT_LY_HLDY]) AS [WK_RCV_QT_LY_HLDY],
                   sum([DSTB_CTR_BAL_ON_HND_QT_LY_HLDY]) AS [DSTB_CTR_BAL_ON_HND_QT_LY_HLDY],
                   sum([RTL_SVC_CTR_BAL_ON_HND_QT_LY_HLDY]) AS [RTL_SVC_CTR_BAL_ON_HND_QT_LY_HLDY],
                   sum([DSTB_CTR_CST_AM_LY_HLDY]) AS [(DSTB_CTR_CST_AM_LY_HLDY],
                   sum([DSTB_CTR_FNC_CHRG_AM_LY_CAL]) AS [DSTB_CTR_FNC_CHRG_AM_LY_CAL],
                   sum([DSTB_CTR_FNC_CR_AM_LY_CAL]) AS [DSTB_CTR_FNC_CR_AM_LY_CAL],
                   sum([DSTB_CTR_STG_CHRG_AM_LY_CAL]) AS [DSTB_CTR_STG_CHRG_AM_LY_CAL],
                   sum([EST_DLVY_CST_AM_LY_CAL]) AS [EST_DLVY_CST_AM_LY_CAL],
                   sum([TRNSF_QT_LY_CAL]) AS [TRNSF_QT_LY_CAL],
                   sum([WK_OUT_OF_STK_QT_LY_CAL]) AS [WK_OUT_OF_STK_QT_LY_CAL],
                   sum([WK_OUT_OF_STK_SBS_QT_LY_CAL]) AS [WK_OUT_OF_STK_SBS_QT_LY_CAL],
                   sum([WK_OVRD_SBS_QT_LY_CAL]) AS [WK_OVRD_SBS_QT_LY_CAL],
                   sum([WK_ADJ_QT_LY_CAL]) AS [WK_ADJ_QT_LY_CAL],
                   sum([WK_DSTB_CTR_MVMT_QT_LY_CAL]) AS [WK_DSTB_CTR_MVMT_QT_LY_CAL],
                   sum([WK_MVMT_CST_AM_LY_CAL]) AS [WK_MVMT_CST_AM_LY_CAL],
                   sum([WK_MVMT_RTL_AM_LY_CAL]) AS [WK_MVMT_RTL_AM_LY_CAL],
                   sum([WK_RCV_QT_LY_CAL]) AS [WK_RCV_QT_LY_CAL],
                   sum([DSTB_CTR_BAL_ON_HND_QT_LY_CAL]) AS [DSTB_CTR_BAL_ON_HND_QT_LY_CAL],
                   sum([RTL_SVC_CTR_BAL_ON_HND_QT_LY_CAL]) AS [RTL_SVC_CTR_BAL_ON_HND_QT_LY_CAL],
                   sum([DSTB_CTR_CST_AM_LY_CAL]) AS [DSTB_CTR_CST_AM_LY_CAL]
            FROM Union_Distribution_Center_Inventory_Load
            GROUP BY [DT_SK],
                     [LY_DT_SK],
                     [LY_DT_SK_HLDY],
                     [LY_DT_SK_CAL],
                     [STR_ID],
                     [P_ID],
                     [DSTB_CTR_DRC_STO_DLVY_CT],
                     [DSTB_CTR_PLT_CT]
           )
        --select top(5)* from Distribution_Center_Inv_Load
        INSERT INTO [EDAA_PRES].[FCT_DSTB_CTR_INV_QTY_TY_LY_FSC]
        (
            [DT_SK],
            [LY_DT_SK],
            [LY_DT_SK_HLDY],
            [LY_DT_SK_CAL],
            [STR_ID],
            [P_ID],
            [DSTB_CTR_DRC_STO_DLVY_CT],
            [DSTB_CTR_PLT_CT],
            [DSTB_CTR_FNC_CHRG_AM_TY_FSC],
            [DSTB_CTR_FNC_CHRG_AM_LY_FSC],
            [DSTB_CTR_FNC_CR_AM_TY_FSC],
            [DSTB_CTR_FNC_CR_AM_LY_FSC],
            [DSTB_CTR_STG_CHRG_AM_TY_FSC],
            [DSTB_CTR_STG_CHRG_AM_LY_FSC],
            [EST_DLVY_CST_AM_TY_FSC],
            [EST_DLVY_CST_AM_LY_FSC],
            [TRNSF_QT_TY_FSC],
            [TRNSF_QT_LY_FSC],
            [WK_OUT_OF_STK_QT_TY_FSC],
            [WK_OUT_OF_STK_QT_LY_FSC],
            [WK_OUT_OF_STK_SBS_QT_TY_FSC],
            [WK_OUT_OF_STK_SBS_QT_LY_FSC],
            [WK_OVRD_SBS_QT_TY_FSC],
            [WK_OVRD_SBS_QT_LY_FSC],
            [WK_ADJ_QT_TY_FSC],
            [WK_ADJ_QT_LY_FSC],
            [WK_DSTB_CTR_MVMT_QT_TY_FSC],
            [WK_DSTB_CTR_MVMT_QT_LY_FSC],
            [WK_MVMT_CST_AM_TY_FSC],
            [WK_MVMT_CST_AM_LY_FSC],
            [WK_MVMT_RTL_AM_TY_FSC],
            [WK_MVMT_RTL_AM_LY_FSC],
            [WK_RCV_QT_TY_FSC],
            [WK_RCV_QT_LY_FSC],
            [DSTB_CTR_BAL_ON_HND_QT_TY_FSC],
            [DSTB_CTR_BAL_ON_HND_QT_LY_FSC],
            [RTL_SVC_CTR_BAL_ON_HND_QT_TY_FSC],
            [RTL_SVC_CTR_BAL_ON_HND_QT_LY_FSC],
            [DSTB_CTR_CST_AM_TY_FSC],
            [DSTB_CTR_CST_AM_LY_FSC],
            [DSTB_CTR_FNC_CHRG_AM_LY_HLDY],
            [DSTB_CTR_FNC_CR_AM_LY_HLDY],
            [DSTB_CTR_STG_CHRG_AM_LY_HLDY],
            [EST_DLVY_CST_AM_LY_HLDY],
            [TRNSF_QT_LY_HLDY],
            [WK_OUT_OF_STK_QT_LY_HLDY],
            [WK_OUT_OF_STK_SBS_QT_LY_HLDY],
            [WK_OVRD_SBS_QT_LY_HLDY],
            [WK_ADJ_QT_LY_HLDY],
            [WK_DSTB_CTR_MVMT_QT_LY_HLDY],
            [WK_MVMT_CST_AM_LY_HLDY],
            [WK_MVMT_RTL_AM_LY_HLDY],
            [WK_RCV_QT_LY_HLDY],
            [DSTB_CTR_BAL_ON_HND_QT_LY_HLDY],
            [RTL_SVC_CTR_BAL_ON_HND_QT_LY_HLDY],
            [DSTB_CTR_CST_AM_LY_HLDY],
            [DSTB_CTR_FNC_CHRG_AM_LY_CAL],
            [DSTB_CTR_FNC_CR_AM_LY_CAL],
            [DSTB_CTR_STG_CHRG_AM_LY_CAL],
            [EST_DLVY_CST_AM_LY_CAL],
            [TRNSF_QT_LY_CAL],
            [WK_OUT_OF_STK_QT_LY_CAL],
            [WK_OUT_OF_STK_SBS_QT_LY_CAL],
            [WK_OVRD_SBS_QT_LY_CAL],
            [WK_ADJ_QT_LY_CAL],
            [WK_DSTB_CTR_MVMT_QT_LY_CAL],
            [WK_MVMT_CST_AM_LY_CAL],
            [WK_MVMT_RTL_AM_LY_CAL],
            [WK_RCV_QT_LY_CAL],
            [DSTB_CTR_BAL_ON_HND_QT_LY_CAL],
            [RTL_SVC_CTR_BAL_ON_HND_QT_LY_CAL],
            [DSTB_CTR_CST_AM_LY_CAL],
            [UPDATEDTIMESTAMP]
        )
        SELECT *,
               @UPDATEDTIMESTAMP as [UPDATEDTIMESTAMP]
        FROM Distribution_Center_Inv_Load

        --update calendar values
        UPDATE T
        SET T.[DSTB_CTR_FNC_CHRG_AM_LY_CAL] = T.[DSTB_CTR_FNC_CHRG_AM_LY_FSC],
            T.[DSTB_CTR_FNC_CR_AM_LY_CAL] = T.[DSTB_CTR_FNC_CR_AM_LY_FSC],
            T.[DSTB_CTR_STG_CHRG_AM_LY_CAL] = T.[DSTB_CTR_STG_CHRG_AM_LY_FSC],
            T.[EST_DLVY_CST_AM_LY_CAL] = T.[EST_DLVY_CST_AM_LY_FSC],
            T.[TRNSF_QT_LY_CAL] = T.[TRNSF_QT_LY_FSC],
            T.[WK_OUT_OF_STK_QT_LY_CAL] = T.[WK_OUT_OF_STK_QT_LY_FSC],
            T.[WK_OUT_OF_STK_SBS_QT_LY_CAL] = T.[WK_OUT_OF_STK_SBS_QT_LY_FSC],
            T.[WK_OVRD_SBS_QT_LY_CAL] = T.[WK_OVRD_SBS_QT_LY_FSC],
            T.[WK_ADJ_QT_LY_CAL] = T.[WK_ADJ_QT_LY_FSC],
            T.[WK_DSTB_CTR_MVMT_QT_LY_CAL] = T.[WK_DSTB_CTR_MVMT_QT_LY_FSC],
            T.[WK_MVMT_CST_AM_LY_CAL] = T.[WK_MVMT_CST_AM_LY_FSC],
            T.[WK_MVMT_RTL_AM_LY_CAL] = T.[WK_MVMT_RTL_AM_LY_FSC],
            T.[WK_RCV_QT_LY_CAL] = T.[WK_RCV_QT_LY_FSC],
            T.[DSTB_CTR_BAL_ON_HND_QT_LY_CAL] = T.[DSTB_CTR_BAL_ON_HND_QT_LY_FSC],
            T.[RTL_SVC_CTR_BAL_ON_HND_QT_LY_CAL] = T.[RTL_SVC_CTR_BAL_ON_HND_QT_LY_FSC],
            T.[DSTB_CTR_CST_AM_LY_CAL] = T.[DSTB_CTR_CST_AM_LY_FSC]
        FROM [EDAA_PRES].[FCT_DSTB_CTR_INV_QTY_TY_LY_FSC] T
            inner join [EDAA_STG].[FCT_DSTB_CTR_INV_QTY] S
                on T.[LY_DT_SK] = T.[LY_DT_SK_CAL]
                   and T.[STR_ID] = S.[StoreId]
                   and T.[P_ID] = S.[productId]

        --update holiday values
        UPDATE T
        SET T.[DSTB_CTR_FNC_CHRG_AM_LY_HLDY] = T.[DSTB_CTR_FNC_CHRG_AM_LY_FSC],
            T.[DSTB_CTR_FNC_CR_AM_LY_HLDY] = T.[DSTB_CTR_FNC_CR_AM_LY_FSC],
            T.[DSTB_CTR_STG_CHRG_AM_LY_HLDY] = T.[DSTB_CTR_STG_CHRG_AM_LY_FSC],
            T.[EST_DLVY_CST_AM_LY_HLDY] = T.[EST_DLVY_CST_AM_LY_FSC],
            T.[TRNSF_QT_LY_HLDY] = T.[TRNSF_QT_LY_FSC],
            T.[WK_OUT_OF_STK_QT_LY_HLDY] = T.[WK_OUT_OF_STK_QT_LY_FSC],
            T.[WK_OUT_OF_STK_SBS_QT_LY_HLDY] = T.[WK_OUT_OF_STK_SBS_QT_LY_FSC],
            T.[WK_OVRD_SBS_QT_LY_HLDY] = T.[WK_OVRD_SBS_QT_LY_FSC],
            T.[WK_ADJ_QT_LY_HLDY] = T.[WK_ADJ_QT_LY_FSC],
            T.[WK_DSTB_CTR_MVMT_QT_LY_HLDY] = T.[WK_DSTB_CTR_MVMT_QT_LY_FSC],
            T.[WK_MVMT_CST_AM_LY_HLDY] = T.[WK_MVMT_CST_AM_LY_FSC],
            T.[WK_MVMT_RTL_AM_LY_HLDY] = T.[WK_MVMT_RTL_AM_LY_FSC],
            T.[WK_RCV_QT_LY_HLDY] = T.[WK_RCV_QT_LY_FSC],
            T.[DSTB_CTR_BAL_ON_HND_QT_LY_HLDY] = T.[DSTB_CTR_BAL_ON_HND_QT_LY_FSC],
            T.[RTL_SVC_CTR_BAL_ON_HND_QT_LY_HLDY] = T.[RTL_SVC_CTR_BAL_ON_HND_QT_LY_FSC],
            T.[DSTB_CTR_CST_AM_LY_HLDY] = T.[DSTB_CTR_CST_AM_LY_FSC]
        FROM [EDAA_PRES].[FCT_DSTB_CTR_INV_QTY_TY_LY_FSC] T
            inner join [EDAA_STG].[FCT_DSTB_CTR_INV_QTY] S
                on T.[LY_DT_SK] = T.[LY_DT_SK_HLDY]
                   and T.[STR_ID] = S.[StoreId]
                   and T.[P_ID] = S.[ProductId]

        PRINT 'Daily Data Load Completed'

    END TRY
    BEGIN CATCH
        DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = '[EDAA_ETL].[PROC_FCT_DSTB_CTR_INV_QTY_DM_PRES_INCL_LOAD]'
        DECLARE @ERROR_LINE AS int;
        DECLARE @ERROR_MSG AS nvarchar(max);
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
