CREATE PROC [EDAA_ETL].[PROC_FCT_DLY_STR_ITMSKU_INV_ADJ_TABLELOAD] AS
--exec [EDAA_ETL].[PROC_FCT_DLY_STR_ITMSKU_INV_ADJ_TABLELOAD]
BEGIN
    BEGIN TRY

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
        EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR = 'EDAA_DW',
                                                @EXEC_JOB = 'PROC_FCT_DLY_STR_ITMSKU_INV_ADJ_TABLELOAD',
                                                @SRC_ENTY = 'FCT_DLY_STR_ITMSKU_INV_ADJ',
                                                @TGT_ENTY = 'FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC',
                                                @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT


        DECLARE @NUMBEROFPARTITIONS int,
                @NEXTPROCESSINGSTARTDATE date,
                @MAXPROCESSINGDATE date,
                @PROCESSINGENDDATE date
        DECLARE @NBR_OF_RW_ISRT_TOTAL int = 0,
                @NBR_OF_RW_UDT_TOTAL int = 0


        PRINT ('----starting DW load----')

        --TRUNCATE TEMP TABLES
        TRUNCATE TABLE [EDAA_STG].[UT_UPC_DLY_INV_TN_DTL_HST_TEMP];
        TRUNCATE TABLE [EDAA_STG].[FCT_DLY_STR_ITMSKU_INV_ADJ_TY_FSC_TEMP];

        WITH Incl_data
        AS (SELECT ISNULL(PROD_HIST_SK, -1) AS PROD_HIST_SK,
                   ISNULL(GEO_HIST_SK, -1) AS GEO_HIST_SK,
                   A.MIRINV_UPC_ID AS ITM_SKU,
                   CAST(A.MIRINV_DAY_DT AS int) AS DT_SK,
                   ISNULL(INV_ADJ_SK, -1) AS INV_ADJ_SK,
                   A.MIRINV_UT_ID AS STR_ID,
                   A.MIRINV_ADJ_QTY AS INV_ADJSTD_QTY,
                   A.MIRINV_TOT_INV_ADJ_AM AS INV_ADJSTD_AMT
            FROM EDAA_STG.UT_UPC_DLY_INV_TN_DTL_HST AS A
                LEFT OUTER JOIN
                (SELECT GEO_HIST_SK, STR_ID, VLD_FRM, VLD_TO FROM EDAA_DW.DIM_GEO) AS GEO
                    ON GEO.STR_ID = A.MIRINV_UT_ID
                       AND CAST(A.MIRINV_DAY_DT AS date)
                       BETWEEN GEO.VLD_FRM AND ISNULL(GEO.VLD_TO, CAST('2099-01-01' AS date))
                LEFT OUTER JOIN
                (
                    SELECT PROD_HIST_SK,
                           ITM_SKU,
                           PROD_SK,
                           VLD_FRM,
                           VLD_TO
                    FROM EDAA_DW.DIM_PROD
                ) AS P
                    ON P.ITM_SKU = A.MIRINV_UPC_ID
                       AND CAST(A.MIRINV_DAY_DT AS date)
                       BETWEEN P.VLD_FRM AND ISNULL(P.VLD_TO, CAST('2099-01-01' AS date))
                LEFT OUTER JOIN
                (SELECT INV_ADJ_SK, TXN_TYP, ADJ_TYP FROM EDAA_DW.DIM_INV_ADJ) AS INV_ADJ
                    ON INV_ADJ.TXN_TYP = A.MIRINV_TRAN_TYPE
                       AND INV_ADJ.ADJ_TYP = A.MIRINV_ADJ_TYPE
           )


        --INSERT INTO EDAA_DW.FCT_DLY_STR_ITMSKU_INV_ADJ
        INSERT INTO EDAA_STG.UT_UPC_DLY_INV_TN_DTL_HST_TEMP
        (
            [PROD_HIST_SK],
            [GEO_HIST_SK],
            [ITM_SKU],
            [DT_SK],
            [INV_ADJ_SK],
            [STR_ID],
            [INV_ADJSTD_QTY],
            [INV_ADJSTD_AMT],
            [AUD_INS_SK],
            [AUD_UPD_SK]
        )
        SELECT [PROD_HIST_SK],
               [GEO_HIST_SK],
               [ITM_SKU],
               [DT_SK],
               [INV_ADJ_SK],
               [STR_ID],
               [INV_ADJSTD_QTY],
               [INV_ADJSTD_AMT],
               @NEW_AUD_SKY AS AUD_INS_SK,
               NULL AS AUD_UPD_SK
        FROM Incl_data;


        INSERT INTO EDAA_DW.FCT_DLY_STR_ITMSKU_INV_ADJ
        (
            [PROD_HIST_SK],
            [GEO_HIST_SK],
            [ITM_SKU],
            [DT_SK],
            [INV_ADJ_SK],
            [STR_ID],
            [INV_ADJSTD_AMT],
            [INV_ADJSTD_QTY],
            [AUD_INS_SK],
            [AUD_UPD_SK]
        )
        SELECT PROD_HIST_SK,
               GEO_HIST_SK,
               ITM_SKU,
               DT_SK,
               INV_ADJ_SK,
               STR_ID,
               INV_ADJSTD_AMT,
               INV_ADJSTD_QTY,
               AUD_INS_SK,
               AUD_UPD_SK
        FROM EDAA_STG.UT_UPC_DLY_INV_TN_DTL_HST_TEMP;


        SELECT @NBR_OF_RW_ISRT = COUNT(1)
        FROM EDAA_DW.FCT_DLY_STR_ITMSKU_INV_ADJ
        WHERE AUD_INS_SK = @NEW_AUD_SKY
        SELECT @NBR_OF_RW_UDT = COUNT(1)
        FROM EDAA_DW.FCT_DLY_STR_ITMSKU_INV_ADJ
        WHERE AUD_UPD_SK = @NEW_AUD_SKY

        SET @NBR_OF_RW_UDT_TOTAL = @NBR_OF_RW_UDT_TOTAL + @NBR_OF_RW_UDT
        SET @NBR_OF_RW_ISRT_TOTAL = @NBR_OF_RW_ISRT_TOTAL + @NBR_OF_RW_ISRT

        PRINT ('---DW load complete---');

        BEGIN
          ---------------------------------------------------------------------------------------------------------
          ------- Load TY values into stage table
          --------------------------------------------------------------------------------------------------------

			INSERT INTO [EDAA_STG].[FCT_DLY_STR_ITMSKU_INV_ADJ_TY_FSC_TEMP]
            (
                [ITM_SKU],
                [DT_SK],
                [LY_DT_SK],
                [LY_DT_SK_HLDY],
                [LY_DT_SK_CAL],
                [INV_ADJ_SK],
                [STR_ID],
                [INV_ADJSTD_TY_AMT],
                [INV_ADJSTD_TY_QTY]
            )
            SELECT S.ITM_SKU,
                   S.DT_SK,
                   S.LY_DT_SK,
                   S.LY_DT_SK_HLDY,
                   S.LY_DT_SK_CAL,
                   S.INV_ADJ_SK,
                   S.STR_ID,
                   A.INV_ADJSTD_AMT AS INV_ADJSTD_TY_AMT,
                   A.INV_ADJSTD_QTY AS INV_ADJSTD_TY_QTY
            FROM EDAA_DW.FCT_DLY_STR_ITMSKU_INV_ADJ AS A
                INNER JOIN
                (
                    SELECT DISTINCT
                        CAST(REPLACE(c.lst_yr_fsc_dt, '-', '') AS int) AS LY_DT_SK,
                        CAST(REPLACE(c.Lst_Yr_Hldy_Dt, '-', '') AS int) AS LY_DT_SK_HLDY,
                        CAST(REPLACE(c.Lst_Yr_Cal_Dt, '-', '') AS int) AS LY_DT_SK_CAL,
                        s.DT_SK,
                        s.ITM_SKU,
                        s.INV_ADJ_SK,
                        s.STR_ID
                    FROM EDAA_STG.UT_UPC_DLY_INV_TN_DTL_HST_TEMP s
                        inner join edaa_dw.dim_dly_cal c
                            on s.dt_sk = c.dt_sk
                    WHERE INV_ADJ_SK IN ( 50, 51, 52, 701, 702, 633, 900, 904, 905, 906, 670, 912, 331, 340, 341, 348,
                                          914
                                        )
                ) S
                    ON CAST(A.ITM_SKU AS decimal) = CAST(S.ITM_SKU AS decimal)
                       AND CAST(A.STR_ID AS int) = CAST(S.STR_ID AS int)
                       AND CAST(A.INV_ADJ_SK AS int) = CAST(S.INV_ADJ_SK AS int)
                       AND CAST(S.DT_SK AS int) = CAST(A.DT_SK AS int)
				LEFT OUTER JOIN EDAA_DW.DIM_PROD P
                    ON A.PROD_HIST_SK = P.PROD_HIST_SK
                       AND P.[Is_Curr_Ind] = 1
                LEFT OUTER JOIN EDAA_DW.DIM_GEO G
                    ON A.GEO_HIST_SK = G.GEO_HIST_SK
                       AND G.[Is_Curr_Ind] = 1


            --Load TY Data into PRES Layer
            MERGE [EDAA_PRES].[FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC] AS tgt
            USING
            (
                SELECT [DT_SK],
                       [LY_DT_SK],
                       [LY_DT_SK_HLDY],
                       [LY_DT_SK_CAL],
                       [ITM_SKU],
                       [STR_ID],
                       [INV_ADJ_SK],
                       SUM([INV_ADJSTD_TY_AMT]) as [INV_ADJSTD_AMT_TY_FSC],
                       SUM([INV_ADJSTD_TY_QTY]) as [INV_ADJSTD_QTY_TY_FSC],
					   @UPDATEDTIMESTAMP as [UPDATEDTIMESTAMP]
                FROM [EDAA_STG].[FCT_DLY_STR_ITMSKU_INV_ADJ_TY_FSC_TEMP]
                GROUP BY [DT_SK],
                         [LY_DT_SK],
                         [LY_DT_SK_HLDY],
                         [LY_DT_SK_CAL],
                         [ITM_SKU],
                         [STR_ID],
                         [INV_ADJ_SK]
            ) AS src
            ON tgt.[DT_SK] = src.[DT_SK]
               and tgt.[ITM_SKU] = src.[ITM_SKU]
               and tgt.[STR_ID] = src.[STR_ID]
               and tgt.[INV_ADJ_SK] = src.[INV_ADJ_SK]
               and tgt.[LY_DT_SK] = src.[LY_DT_SK]
               and tgt.[LY_DT_SK_HLDY] = src.[LY_DT_SK_HLDY]
               and tgt.[LY_DT_SK_CAL] = src.[LY_DT_SK_CAL]
            -- For Inserts
            WHEN NOT MATCHED BY Target THEN
                INSERT
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
                    [UPDATEDTIMESTAMP]
                )
                Values
                (src.[DT_SK],
                 src.[LY_DT_SK],
                 src.[LY_DT_SK_HLDY],
                 src.[LY_DT_SK_CAL],
                 src.[ITM_SKU],
                 src.[STR_ID],
                 src.[INV_ADJ_SK],
                 src.[INV_ADJSTD_AMT_TY_FSC],
                 src.[INV_ADJSTD_QTY_TY_FSC],
                 src.[UPDATEDTIMESTAMP]
                )
            -- For Updates
            WHEN MATCHED THEN
                UPDATE SET tgt.[INV_ADJSTD_AMT_TY_FSC] = src.[INV_ADJSTD_AMT_TY_FSC],
                           tgt.[INV_ADJSTD_QTY_TY_FSC] = src.[INV_ADJSTD_QTY_TY_FSC],
                           tgt.[UPDATEDTIMESTAMP] = src.[UPDATEDTIMESTAMP];

            --UPDATE STATISTICS
            UPDATE STATISTICS [EDAA_PRES].[FCT_DLY_STR_ITMSKU_INV_ADJ_TY_LY_FSC]
            UPDATE STATISTICS [EDAA_DW].[FCT_DLY_STR_ITMSKU_INV_ADJ]
            PRINT ('---TY Pres load Completed---');

        END

        /*AUDIT LOG END*/
        EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY,
                                              @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT_TOTAL,
                                              @NBR_OF_RW_UDT = @NBR_OF_RW_UDT_TOTAL

    END TRY
    BEGIN CATCH
        DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = '[EDAA_ETL].[PROC_FCT_DLY_STR_ITMSKU_INV_ADJ_TABLELOAD]'
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
