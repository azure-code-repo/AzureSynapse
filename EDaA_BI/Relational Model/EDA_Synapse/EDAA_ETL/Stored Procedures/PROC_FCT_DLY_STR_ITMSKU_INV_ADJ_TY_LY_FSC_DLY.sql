CREATE PROC [EDAA_ETL].[PROC_FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_DLY] AS
BEGIN

    --EXEC [EDAA_ETL].[PROC_FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_DLY]

    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

    BEGIN TRY


        /*DATAMART AUDITING VARIABLES*/
        DECLARE @NEW_AUD_SKY bigint
        DECLARE @NBR_OF_RW_ISRT bigint
        DECLARE @NBR_OF_RW_UDT bigint
        DECLARE @EXEC_LYR varchar(255)
        DECLARE @EXEC_JOB varchar(500)
        DECLARE @SRC_ENTY varchar(500)
        DECLARE @TGT_ENTY varchar(500)

        /*AUDIT LOG START*/
        EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR = 'EDAA_DW',
                                                @EXEC_JOB = 'EDAA_ETL.FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_DLY',
                                                @SRC_ENTY = 'FCT_DLY_STR_ITMSKU_INV_ADJ',
                                                @TGT_ENTY = 'FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_DLY',
                                                @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

        DECLARE @DATETIME DATETIME
        SET @DATETIME =
        (
            SELECT MAX(UPDATEDTIMESTAMP)
            FROM [EDAA_STG].[FCT_SL_WTRMARK]
            WHERE TABLE_NME = 'FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC'
        );

        --TRUNCATE TABLE IF EXISTS
        TRUNCATE TABLE EDAA_STG.FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_DLY_TEMP

        --LOAD ALL DATA  TO TEMP TABLE
        INSERT INTO EDAA_STG.FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_DLY_TEMP
        (
            [ROW_ID_VAL],
            [PRE_DEF_ID],
            [DT_VAL],
            [ITM_SKU],
            [STR_ID],
            [INV_ADJ_SK],
            [INV_ADJSTD_AMT_TY_FSC],
            [INV_ADJSTD_QTY_TY_FSC],
            [INV_ADJSTD_AMT_LY_FSC],
            [INV_ADJSTD_QTY_LY_FSC],
            [INV_ADJSTD_AMT_LY_HLDY],
            [INV_ADJSTD_QTY_LY_HLDY],
            [INV_ADJSTD_AMT_LY_CAL],
            [INV_ADJSTD_QTY_LY_CAL],
            [UPDATEDTIMESTAMP]
        )
        SELECT dtsel.ROW_ID_VAL,
               dtsel.PRE_DEF_ID,
               CONVERT(date, CONVERT(varchar(10), dtsel.MAX_DT, 120)) AS DT_VAL,
               A.[ITM_SKU],
               A.[STR_ID],
               A.[INV_ADJ_SK],
               A.[INV_ADJSTD_AMT_TY_FSC],
               A.[INV_ADJSTD_QTY_TY_FSC],
               A.[INV_ADJSTD_AMT_LY_FSC],
               A.[INV_ADJSTD_QTY_LY_FSC],
               A.[INV_ADJSTD_AMT_LY_HLDY],
               A.[INV_ADJSTD_QTY_LY_HLDY],
               A.[INV_ADJSTD_AMT_LY_CAL],
               A.[INV_ADJSTD_QTY_LY_CAL],
               [UPDATEDTIMESTAMP]
        FROM EDAA_PRES.FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC A,
        (
            SELECT *,
                   ROW_NUMBER() OVER (ORDER BY PRE_DEF_ID) AS ROW_ID_VAL
            FROM EDAA_PRES.V_DIM_PRE_DEF_CAL_TYP
            WHERE AGG_TYP = 'DLY'
        ) dtsel
        WHERE A.DT_SK >= dtsel.MIN_DT
              AND A.DT_SK <= dtsel.MAX_DT
              AND A.DT_SK IN (
                                 SELECT DISTINCT
                                     DT_SK
                                 FROM EDAA_PRES.FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC
                                 WHERE UPDATEDTIMESTAMP >= @DATETIME
                             )


        --DELETE IF EXISTS IN DLY TABLE
        DELETE FROM EDAA_PRES.FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_DLY
        WHERE PRE_DEF_ID IN (
                                SELECT DISTINCT
                                    DT_SK
                                FROM EDAA_PRES.FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC
                                WHERE UPDATEDTIMESTAMP >= @DATETIME
                            )


        --INSERT DATA IF NOT EXISTS IN PRES TABLE
        INSERT INTO EDAA_PRES.FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_DLY
        (
            [ROW_ID_VAL],
            [PRE_DEF_ID],
            [DT_VAL],
            [ITM_SKU],
            [STR_ID],
            [INV_ADJ_SK],
            [INV_ADJSTD_AMT_TY_FSC],
            [INV_ADJSTD_QTY_TY_FSC],
            [INV_ADJSTD_AMT_LY_FSC],
            [INV_ADJSTD_QTY_LY_FSC],
            [INV_ADJSTD_AMT_LY_HLDY],
            [INV_ADJSTD_QTY_LY_HLDY],
            [INV_ADJSTD_AMT_LY_CAL],
            [INV_ADJSTD_QTY_LY_CAL],
            [UPDATEDTIMESTAMP]
        )
        SELECT [ROW_ID_VAL],
               [PRE_DEF_ID],
               [DT_VAL],
               [ITM_SKU],
               [STR_ID],
               [INV_ADJ_SK],
               [INV_ADJSTD_AMT_TY_FSC],
               [INV_ADJSTD_QTY_TY_FSC],
               [INV_ADJSTD_AMT_LY_FSC],
               [INV_ADJSTD_QTY_LY_FSC],
               [INV_ADJSTD_AMT_LY_HLDY],
               [INV_ADJSTD_QTY_LY_HLDY],
               [INV_ADJSTD_AMT_LY_CAL],
               [INV_ADJSTD_QTY_LY_CAL],
               [UPDATEDTIMESTAMP]
        FROM EDAA_STG.FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_DLY_TEMP


        --DELETE MORE THAN 42 DAYS DATA FROM DLY TABLE
        DELETE FROM EDAA_PRES.FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_DLY
        WHERE DT_VAL < GETDATE() - 43

        --Update watermark table
        UPDATE [EDAA_STG].[FCT_SL_WTRMARK]
        SET UPDATEDTIMESTAMP =
            (
                SELECT MAX(UPDATEDTIMESTAMP)
                FROM EDAA_PRES.FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC
            )
        WHERE TABLE_NME = 'FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC'

        --UPDATE STATISTICS
        UPDATE STATISTICS EDAA_PRES.FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_DLY;

    END TRY
    BEGIN CATCH
        DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = 'EDAA_ETL.FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_DLY'
        DECLARE @ERROR_LINE AS int;
        DECLARE @ERROR_MSG AS nvarchar(max);

        SELECT @ERROR_LINE = ERROR_NUMBER(),
               @ERROR_MSG = ERROR_MESSAGE();
        --------- Log execution error ----------
        EXEC EDAA_CNTL.SP_LOG_AUD_ERR @AUD_SKY = @NEW_AUD_SKY,
                                      @ERROR_PROCEDURE_NAME = @ERROR_PROCEDURE_NAME,
                                      @ERROR_LINE = @ERROR_LINE,
                                      @ERROR_MSG = @ERROR_MSG;

        -- DETECT THE CHANGE

        THROW;


    END CATCH
END
