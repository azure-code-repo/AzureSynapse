CREATE PROC [EDAA_ETL].[PROC_FCT_DLY_STR_ITMSKU_INV_ADJ_DM_PRES_LOAD] AS
BEGIN
    --exec [EDAA_ETL].[PROC_FCT_DLY_STR_ITMSKU_INV_ADJ_DM_PRES_LOAD]

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
        DECLARE @GET_DATE datetime = GETDATE()


        /*AUDIT LOG START*/
        EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR = 'EDAA_DW',
                                                @EXEC_JOB = 'EDAA_ETL.PROC_FCT_DLY_STR_ITMSKU_INV_ADJ_DM_PRES_LOAD',
                                                @SRC_ENTY = 'EDAA_DW.FCT_DLY_STR_ITMSKU_INV_ADJ',
                                                @TGT_ENTY = 'EDAA_PRES.FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC',
                                                @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

        --DROP TABLE IF EXISTS
        IF OBJECT_ID('tempdb..#DIM_DLY_CAL') IS NOT NULL
        BEGIN
            DROP TABLE #DIM_DLY_CAL
        END

        --SELECT ONLY THIS YEAR DT_SK and Last YEAR DT_SK
        SELECT DT_SK,
               FORMAT(Lst_Yr_Fsc_Dt, 'yyyyMMdd') as LY_DT_SK,
               FORMAT(Lst_Yr_Hldy_Dt, 'yyyyMMdd') as LY_DT_SK_HLDY,
               FORMAT(Lst_Yr_Cal_Dt, 'yyyyMMdd') AS LY_DT_SK_CAL
        INTO #DIM_DLY_CAL
        FROM edaa_dw.dim_dly_cal d
        WHERE Fsc_Yr = (YEAR(GETDATE()))
              --AND DT_SK < format(GETDATE(), 'yyyyMMdd')

        --- TRUNCATE TEMP TABLE
        TRUNCATE TABLE [EDAA_STG].[FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_TEMP]

        --TRUNCATE TABLE EDAA_PRES.FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC
        TRUNCATE TABLE [EDAA_PRES].[FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC]
        TRUNCATE TABLE [EDAA_PRES].[FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_DLY]

        --TY FSC Data Load
        INSERT INTO [EDAA_STG].[FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_TEMP]
        (
            [DT_SK],
            [LY_DT_SK],
            [LY_DT_SK_HLDY],
            [LY_DT_SK_CAL],
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
            [INV_ADJSTD_QTY_LY_CAL]
        )
        SELECT B.[DT_SK],
               B.[LY_DT_SK],
               B.[LY_DT_SK_HLDY],
               B.[LY_DT_SK_CAL],
               [ITM_SKU],
               [STR_ID],
               [INV_ADJ_SK],
               INV_ADJSTD_AMT as [INV_ADJSTD_AMT_TY_FSC],
               INV_ADJSTD_QTY as [INV_ADJSTD_QTY_TY_FSC],
               0 as [INV_ADJSTD_AMT_LY_FSC],
               0 as [INV_ADJSTD_QTY_LY_FSC],
               0 as [INV_ADJSTD_AMT_LY_HLDY],
               0 as [INV_ADJSTD_QTY_LY_HLDY],
               0 as [INV_ADJSTD_AMT_LY_CAL],
               0 as [INV_ADJSTD_QTY_LY_CAL]
        FROM EDAA_DW.FCT_DLY_STR_ITMSKU_INV_ADJ AS A,
             #DIM_DLY_CAL AS B
        WHERE A.DT_SK = B.DT_SK
              AND INV_ADJ_SK IN ( 50, 51, 52, 701, 702, 633, 900, 904, 905, 906, 670, 912, 331, 340, 341, 348, 914 )

        --LY FSC  Data Load
        INSERT INTO [EDAA_STG].[FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_TEMP]
        (
            [DT_SK],
            [LY_DT_SK],
            [LY_DT_SK_HLDY],
            [LY_DT_SK_CAL],
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
            [INV_ADJSTD_QTY_LY_CAL]
        )
        SELECT B.[DT_SK],
               B.[LY_DT_SK],
               B.[LY_DT_SK_HLDY],
               B.[LY_DT_SK_CAL],
               [ITM_SKU],
               [STR_ID],
               [INV_ADJ_SK],
               0 as [INV_ADJSTD_AMT_TY_FSC],
               0 as [INV_ADJSTD_QTY_TY_FSC],
               INV_ADJSTD_AMT as [INV_ADJSTD_AMT_LY_FSC],
               INV_ADJSTD_QTY as [INV_ADJSTD_QTY_LY_FSC],
               0 as [INV_ADJSTD_AMT_LY_HLDY],
               0 as [INV_ADJSTD_QTY_LY_HLDY],
               0 as [INV_ADJSTD_AMT_LY_CAL],
               0 as [INV_ADJSTD_QTY_LY_CAL]
        FROM EDAA_DW.FCT_DLY_STR_ITMSKU_INV_ADJ AS A,
             #DIM_DLY_CAL AS B
        WHERE A.DT_SK = B.[LY_DT_SK]
              AND INV_ADJ_SK IN ( 50, 51, 52, 701, 702, 633, 900, 904, 905, 906, 670, 912, 331, 340, 341, 348, 914 )


		--LY HLDY Load
		INSERT INTO [EDAA_STG].[FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_TEMP]
        (
            [DT_SK],
            [LY_DT_SK],
            [LY_DT_SK_HLDY],
            [LY_DT_SK_CAL],
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
            [INV_ADJSTD_QTY_LY_CAL]
        )
        SELECT B.[DT_SK],
               B.[LY_DT_SK],
               B.[LY_DT_SK_HLDY],
               B.[LY_DT_SK_CAL],
               [ITM_SKU],
               [STR_ID],
               [INV_ADJ_SK],
               0 as [INV_ADJSTD_AMT_TY_FSC],
               0 as [INV_ADJSTD_QTY_TY_FSC],
               0 as [INV_ADJSTD_AMT_LY_FSC],
               0 as [INV_ADJSTD_QTY_LY_FSC],
               INV_ADJSTD_AMT as [INV_ADJSTD_AMT_LY_HLDY],
               INV_ADJSTD_QTY as [INV_ADJSTD_QTY_LY_HLDY],
               0 as [INV_ADJSTD_AMT_LY_CAL],
               0 as [INV_ADJSTD_QTY_LY_CAL]
        FROM EDAA_DW.FCT_DLY_STR_ITMSKU_INV_ADJ AS A,
             #DIM_DLY_CAL AS B
        WHERE A.DT_SK = B.[LY_DT_SK_HLDY]
              AND INV_ADJ_SK IN ( 50, 51, 52, 701, 702, 633, 900, 904, 905, 906, 670, 912, 331, 340, 341, 348, 914 )


        --TY LY Data Load
        INSERT INTO [EDAA_PRES].[FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC]
        (
            [DT_SK],
            [LY_DT_SK],
            [LY_DT_SK_HLDY],
            [LY_DT_SK_CAL],
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
        SELECT A.[DT_SK],
               A.[LY_DT_SK],
               A.[LY_DT_SK_HLDY],
               A.[LY_DT_SK_CAL],
               A.[ITM_SKU],
               A.[STR_ID],
               A.[INV_ADJ_SK],
               SUM(A.[INV_ADJSTD_AMT_TY_FSC]) as [INV_ADJSTD_AMT_TY_FSC],
               SUM(A.[INV_ADJSTD_QTY_TY_FSC]) as [INV_ADJSTD_QTY_TY_FSC],
               SUM(A.[INV_ADJSTD_AMT_LY_FSC]) as [INV_ADJSTD_AMT_LY_FSC],
               SUM(A.[INV_ADJSTD_QTY_LY_FSC]) as [INV_ADJSTD_QTY_LY_FSC],
               SUM(A.[INV_ADJSTD_AMT_LY_HLDY]) as [INV_ADJSTD_AMT_LY_HLDY],
               SUM(A.[INV_ADJSTD_QTY_LY_HLDY]) as [INV_ADJSTD_QTY_LY_HLDY],
               SUM(A.[INV_ADJSTD_AMT_LY_CAL]) as [INV_ADJSTD_AMT_LY_CAL],
               SUM(A.[INV_ADJSTD_QTY_LY_CAL]) as [INV_ADJSTD_QTY_LY_CAL],
               @GET_DATE
        FROM [EDAA_STG].[FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_TEMP] A
        GROUP BY A.[DT_SK],
                 A.[LY_DT_SK],
                 A.[LY_DT_SK_HLDY],
                 A.[LY_DT_SK_CAL],
                 A.[ITM_SKU],
                 A.[STR_ID],
                 A.[INV_ADJ_SK]


        --UPDATE HLDY & CALENDAR
        ----HLDY
        --UPDATE T
        --SET T.INV_ADJSTD_AMT_LY_HLDY = T.INV_ADJSTD_AMT_LY_FSC,
        --    T.INV_ADJSTD_QTY_LY_HLDY = T.INV_ADJSTD_QTY_LY_FSC
        --FROM [EDAA_PRES].[FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC] T
        --WHERE T.LY_DT_SK = T.LY_DT_SK_HLDY
        --      AND T.STR_ID = T.STR_ID
        --      AND T.ITM_SKU = T.ITM_SKU

        --CAL
        UPDATE T
        SET T.INV_ADJSTD_AMT_LY_CAL = T.INV_ADJSTD_AMT_LY_FSC,
            T.INV_ADJSTD_QTY_LY_CAL = T.INV_ADJSTD_QTY_LY_FSC
        FROM [EDAA_PRES].[FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC] T
        WHERE T.LY_DT_SK = T.LY_DT_SK_CAL
              AND T.STR_ID = T.STR_ID
              AND T.ITM_SKU = T.ITM_SKU

        ---exec Daily Load
        EXEC [EDAA_ETL].[PROC_FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC_DLY]

    END TRY
    BEGIN CATCH
        DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = 'EDAA_ETL.PROC_FCT_DLY_STR_ITMSKU_INV_ADJ_DM_PRES_LOAD'
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
