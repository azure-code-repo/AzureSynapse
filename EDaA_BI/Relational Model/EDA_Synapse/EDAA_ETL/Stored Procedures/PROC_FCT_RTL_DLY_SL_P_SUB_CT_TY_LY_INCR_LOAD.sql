CREATE PROC [EDAA_ETL].[PROC_FCT_RTL_DLY_SL_P_SUB_CT_TY_LY_INCR_LOAD]
AS
BEGIN

    --exec [EDAA_ETL].[PROC_FCT_RTL_DLY_SL_P_SUB_CT_TY_LY_INCR_LOAD]

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
        DECLARE @SYSDATETIME datetime = GETDATE()


        /*AUDIT LOG START*/
        EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR = 'EDAA_PRES',
                                                @EXEC_JOB = 'EDAA_ETL.PROC_FCT_RTL_DLY_SL_P_SUB_CT_TY_LY_INCR_LOAD',
                                                @SRC_ENTY = 'PRES.FCT_DLY_SL_P_SUB_CT_CALC',
                                                @TGT_ENTY = 'EDAA_PRES.FCT_RTL_DLY_SL_P_SUB_CT_TY_LY',
                                                @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

        --- TRUNCATE STAGE TABLE
        TRUNCATE TABLE [EDAA_STG].[FCT_RTL_DLY_SL_P_SUB_CT_TY_LY_TEMP]

        ---- Get Incremental timestamp
        DECLARE @INCR_TIMESTAMP datetime
        SET @INCR_TIMESTAMP =
        (
            SELECT UPDATEDTIMESTAMP
            FROM [EDAA_STG].[FCT_SL_WTRMARK]
            WHERE TABLE_NME = 'FCT_RTL_DLY_SL_P_SUB_CT_TY_LY'
        )

        PRINT (@INCR_TIMESTAMP)


        ---- Insert Incremental data into stage table
        INSERT INTO [EDAA_STG].[FCT_RTL_DLY_SL_P_SUB_CT_TY_LY_TEMP]
        SELECT [GEO_HST_SKY],
               [GEO_SKY],
               [TM_SKY],
               [P_HST_SKY],
               [P_SKY],
               [BYR_HST_SKY],
               [BYR_SKY],
               [UT_ID],
               [SLS_FNC_AM],
               [SLS_FNC_QTY],
               [DM_FNC_AM],
               [SLS_FNC_SCN_QTY],
               [SLS_FNC_AM_LY_CLDR],
               [SLS_FNC_QTY_LY_CLDR],
               [DM_FNC_AM_LY_CLDR],
               [SLS_FNC_SCN_QTY_LY_CLDR],
               [SLS_FNC_AM_LY_FSC],
               [SLS_FNC_QTY_LY_FSC],
               [DM_FNC_AM_LY_FSC],
               [SLS_FNC_SCN_QTY_LY_FSC],
               [SLS_FNC_AM_LY_HLDY],
               [SLS_FNC_QTY_LY_HLDY],
               [DM_FNC_AM_LY_HLDY],
               [SLS_FNC_SCN_QTY_LY_HLDY]
        FROM PRES.FCT_DLY_SL_P_SUB_CT_CALC AS a
        WHERE a.fst_isrt_dt >= @INCR_TIMESTAMP
              or a.lst_udt_dt >= @INCR_TIMESTAMP

        ---Delete if exists in pres table
        DELETE a
        FROM [EDAA_PRES].[FCT_RTL_DLY_SL_P_SUB_CT_TY_LY] AS a
            INNER JOIN [EDAA_STG].[FCT_RTL_DLY_SL_P_SUB_CT_TY_LY_TEMP] AS b
                ON a.ut_id = b.ut_id
                   AND a.p_sky = b.p_sky
                   AND a.dt_sk = b.tm_sky
                   AND a.byr_sky = b.byr_sky

        ----Insert Incremental data into presentation layer
        INSERT INTO [EDAA_PRES].[FCT_RTL_DLY_SL_P_SUB_CT_TY_LY]
        (
            GEO_HST_SKY,
            GEO_SKY,
            DT_SK,
            P_HST_SKY,
            P_SKY,
            BYR_HST_SKY,
            BYR_SKY,
            UT_ID,
            SLS_FNC_AM,
            SLS_FNC_QTY,
            DM_FNC_AM,
            SLS_FNC_SCN_QTY,
            SLS_FNC_AM_LY_CAL,
            SLS_FNC_QTY_LY_CAL,
            DM_FNC_AM_LY_CAL,
            SLS_FNC_SCN_QTY_LY_CAL,
            SLS_FNC_AM_LY_FSC,
            SLS_FNC_QTY_LY_FSC,
            DM_FNC_AM_LY_FSC,
            SLS_FNC_SCN_QTY_LY_FSC,
            SLS_FNC_AM_LY_HLDY,
            SLS_FNC_QTY_LY_HLDY,
            DM_FNC_AM_LY_HLDY,
            SLS_FNC_SCN_QTY_LY_HLDY,
            UPDATEDTIMESTAMP
        )
        SELECT a.GEO_HST_SKY,
               a.GEO_SKY,
               a.TM_SKY,
               a.P_HST_SKY,
               a.P_SKY,
               a.BYR_HST_SKY,
               a.BYR_SKY,
               a.UT_ID,
               a.SLS_FNC_AM,
               a.SLS_FNC_QTY,
               a.DM_FNC_AM,
               a.SLS_FNC_SCN_QTY,
               a.SLS_FNC_AM_LY_CLDR,
               a.SLS_FNC_QTY_LY_CLDR,
               a.DM_FNC_AM_LY_CLDR,
               a.SLS_FNC_SCN_QTY_LY_CLDR,
               a.SLS_FNC_AM_LY_FSC,
               a.SLS_FNC_QTY_LY_FSC,
               a.DM_FNC_AM_LY_FSC,
               a.SLS_FNC_SCN_QTY_LY_FSC,
               a.SLS_FNC_AM_LY_HLDY,
               a.SLS_FNC_QTY_LY_HLDY,
               a.DM_FNC_AM_LY_HLDY,
               a.SLS_FNC_SCN_QTY_LY_HLDY,
               @SYSDATETIME
        FROM [EDAA_STG].[FCT_RTL_DLY_SL_P_SUB_CT_TY_LY_TEMP] AS a


        PRINT ('Insert completed')

        --UPDATE Watermark table
        UPDATE [EDAA_STG].[FCT_SL_WTRMARK]
        SET UPDATEDTIMESTAMP = GETDATE()
        WHERE TABLE_NME = 'FCT_RTL_DLY_SL_P_SUB_CT_TY_LY'

        --UPDATE STATISTICS
        UPDATE STATISTICS [EDAA_PRES].[FCT_RTL_DLY_SL_P_SUB_CT_TY_LY]


    END TRY
    BEGIN CATCH
        DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = '[EDAA_ETL].[PROC_FCT_RTL_DLY_SL_P_SUB_CT_TY_LY_INCR_LOAD]'
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
GO
