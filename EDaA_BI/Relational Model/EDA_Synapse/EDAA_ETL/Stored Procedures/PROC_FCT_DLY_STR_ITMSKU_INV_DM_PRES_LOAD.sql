CREATE PROC [EDAA_ETL].[PROC_FCT_DLY_STR_ITMSKU_INV_DM_PRES_LOAD] AS
BEGIN

  --exec [EDAA_ETL].[PROC_FCT_DLY_STR_ITMSKU_INV_DM_PRES_LOAD]

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
    EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR = 'EDAA_DW',
                                            @EXEC_JOB = 'EDAA_ETL.PROC_FCT_DLY_STR_ITMSKU_INV_DM_PRES_LOAD',
                                            @SRC_ENTY = 'EDAA_DW.FCT_DLY_STR_ITMSKU_INV_HST',
                                            @TGT_ENTY = 'EDAA_PRES.FCT_DLY_STR_ITMSKU_INV_TY_LY_FSC',
                                            @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT


	--TRUNCATE & Load 2 Years data
    TRUNCATE TABLE [EDAA_PRES].[FCT_DLY_STR_ITMSKU_INV_TY_LY_FSC];


    --DROP TABLE IF EXISTS
    IF OBJECT_ID('tempdb..#DIM_DLY_CAL') IS NOT NULL
    BEGIN
      DROP TABLE #DIM_DLY_CAL
    END

    --SELECT ONLY THIS YEAR DT_SK and Last YEAR DT_SK
    SELECT
      CAST(CAST(DT_SK AS VARCHAR(10)) AS DATE) DT_SK ,
		Lst_Yr_Fsc_Dt AS LY_DT_SK,
		Lst_Yr_Hldy_Dt AS LY_DT_SK_HLDY,
		Lst_Yr_Cal_Dt AS LY_DT_SK_CAL
	  INTO #DIM_DLY_CAL
    FROM edaa_dw.dim_dly_cal d
    WHERE Fsc_Yr = (YEAR(GETDATE())) AND DT_SK < FORMAT(GETDATE(),'yyyyMMdd')

    --Insert LY & TY
    ;WITH TY_INV_DATA_LOAD
    AS (
	SELECT
      [STR_ID],
      [ITM_SKU],
      CAST(CONVERT(VARCHAR(10),b.[DT_SK],112) AS INT) [DT_SK],
      CAST(CONVERT(VARCHAR(10),b.[LY_DT_SK],112) AS INT) [LY_DT_SK],
      CAST(CONVERT(VARCHAR(10),b.[LY_DT_SK_HLDY],112) AS INT) [LY_DT_SK_HLDY],
      CAST(CONVERT(VARCHAR(10),b.[LY_DT_SK_CAL],112) AS INT) [LY_DT_SK_CAL],
      [INV_AMT] AS [INV_AMT_TY_FSC],
      [INV_QTY] AS [INV_QTY_TY_FSC],
      0 AS [INV_AMT_LY_FSC],
      0 AS [INV_QTY_LY_FSC]
	--SELECT COUNT_BIG(1)
    FROM [EDAA_DW].[FCT_DLY_STR_ITMSKU_INV] AS A,
         #DIM_DLY_CAL AS B
    WHERE B.DT_SK BETWEEN A.[INV_ROW_START_DT] AND A.INV_ROW_END_DT
    --AND STR_ID = 19
	),
    LY_INV_DATA_LOAD
    AS (
	SELECT
      [STR_ID],
      [ITM_SKU],
      CAST(CONVERT(VARCHAR(10),b.[DT_SK],112) AS INT) [DT_SK],
      CAST(CONVERT(VARCHAR(10),b.[LY_DT_SK],112) AS INT) [LY_DT_SK],
      CAST(CONVERT(VARCHAR(10),b.[LY_DT_SK_HLDY],112) AS INT) [LY_DT_SK_HLDY],
      CAST(CONVERT(VARCHAR(10),b.[LY_DT_SK_CAL],112) AS INT) [LY_DT_SK_CAL],
      0 AS [INV_AMT_TY_FSC],
      0 AS [INV_QTY_TY_FSC],
      [INV_AMT] AS [INV_AMT_LY_FSC],
      [INV_QTY] AS [INV_QTY_LY_FSC]
    FROM [EDAA_DW].[FCT_DLY_STR_ITMSKU_INV] AS A,
         #DIM_DLY_CAL AS B
    WHERE B.[LY_DT_SK] BETWEEN A.[INV_ROW_START_DT] AND A.INV_ROW_END_DT
    --AND STR_ID = 19
	),
    INV_TY_LY_UNION_DATA
    AS (
	SELECT
	  *
	FROM TY_INV_DATA_LOAD
	UNION ALL
	SELECT
	  *
	FROM LY_INV_DATA_LOAD
	),

    INV_TY_LY_SUM_DATA
    AS (
	SELECT
      A.[STR_ID],
      A.[ITM_SKU],
      A.[DT_SK],
      A.[LY_DT_SK],
      A.LY_DT_SK_HLDY,
      A.[LY_DT_SK_CAL],
      SUM(A.[INV_AMT_TY_FSC]) AS [INV_AMT_TY_FSC],
      SUM(A.[INV_QTY_TY_FSC]) AS [INV_QTY_TY_FSC],
      SUM(A.[INV_AMT_LY_FSC]) AS [INV_AMT_LY_FSC],
      SUM(A.[INV_QTY_LY_FSC]) AS [INV_QTY_LY_FSC]
    FROM INV_TY_LY_UNION_DATA A
    GROUP BY A.[STR_ID],
             A.[ITM_SKU],
             A.[DT_SK],
             A.[LY_DT_SK],
             A.LY_DT_SK_HLDY,
             A.LY_DT_SK_CAL
			 )


    INSERT INTO [EDAA_PRES].[FCT_DLY_STR_ITMSKU_INV_TY_LY_FSC]
	(
	   [DT_SK]
      ,[LY_DT_SK]
      ,[LY_DT_SK_HLDY]
      ,[LY_DT_SK_CAL]
      ,[STR_ID]
      ,[ITM_SKU]
      ,[INV_AMT_TY_FSC]
      ,[INV_QTY_TY_FSC]
      ,[INV_AMT_LY_FSC]
      ,[INV_QTY_LY_FSC]
      ,[INV_AMT_LY_HLDY]
      ,[INV_QTY_LY_HLDY]
      ,[INV_AMT_LY_CAL]
      ,[INV_QTY_LY_CAL]
      ,[UPDATEDTIMESTAMP]
	)

      SELECT
        [DT_SK],
        [LY_DT_SK],
        [LY_DT_SK_HLDY],
        [LY_DT_SK_CAL],
        [STR_ID],
        [ITM_SKU],
        [INV_AMT_TY_FSC],
        [INV_QTY_TY_FSC],
        [INV_AMT_LY_FSC],
        [INV_QTY_LY_FSC],
	    0 as [INV_AMT_LY_HLDY],
        0 as [INV_QTY_LY_HLDY],
        0 as [INV_AMT_LY_CAL],
        0 as [INV_QTY_LY_CAL],
        @UPDATEDTIMESTAMP AS [UPDATEDTIMESTAMP]
      FROM INV_TY_LY_SUM_DATA
      WHERE [INV_AMT_TY_FSC] <> 0
      OR [INV_QTY_TY_FSC] <> 0
      OR [INV_AMT_LY_FSC] <> 0
      OR [INV_QTY_LY_FSC] <> 0
    PRINT ('Insert completed')

    --delete empty rows
    DELETE FROM [EDAA_PRES].[FCT_DLY_STR_ITMSKU_INV_TY_LY_FSC]
    WHERE [INV_AMT_TY_FSC] = 0
      AND [INV_QTY_TY_FSC] = 0
      AND [INV_AMT_LY_FSC] = 0
      AND [INV_QTY_LY_FSC] = 0

	  UPDATE T
      SET T.INV_AMT_LY_HLDY = T.INV_AMT_LY_FSC,
          T.INV_QTY_LY_HLDY = T.INV_QTY_LY_FSC
	  FROM [EDAA_PRES].[FCT_DLY_STR_ITMSKU_INV_TY_LY_FSC] T
      WHERE T.LY_DT_SK = T.LY_DT_SK_HLDY
	  AND T.STR_ID = T.STR_ID
	  AND T.ITM_SKU = T.ITM_SKU

	  --CAL
	  UPDATE T
      SET T.INV_AMT_LY_CAL = T.INV_AMT_LY_CAL,
          T.INV_QTY_LY_CAL = T.INV_QTY_LY_CAL
	  FROM [EDAA_PRES].[FCT_DLY_STR_ITMSKU_INV_TY_LY_FSC] T
      WHERE T.LY_DT_SK = T.LY_DT_SK_HLDY
	  AND T.STR_ID = T.STR_ID
	  AND T.ITM_SKU = T.ITM_SKU

  --Daily Incremental Load
    -- EXEC [EDAA_ETL].[PROC_FCT_DLY_STR_ITMSKU_INV_TY_LY_FSC_DLY]
  --PRINT 'Daily  Data Load Completed'

  END TRY

  BEGIN CATCH
    DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = 'EDAA_ETL.PROC_FCT_DLY_STR_ITMSKU_INV_DM_PRES_LOAD'
    DECLARE @ERROR_LINE AS int;
    DECLARE @ERROR_MSG AS nvarchar(max);

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
