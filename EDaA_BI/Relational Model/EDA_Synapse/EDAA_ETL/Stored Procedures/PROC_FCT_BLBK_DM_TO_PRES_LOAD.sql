/****** Object:  StoredProcedure [EDAA_ETL].[PROC_FCT_BLBK_DM_TO_PRES_LOAD]    Script Date: 9/19/2022 12:49:25 AM ******/

CREATE PROC [EDAA_ETL].[PROC_FCT_BLBK_DM_TO_PRES_LOAD] AS


BEGIN TRY

  /*DATAMART AUDITING VARIABLES*/
  DECLARE @NEW_AUD_SKY bigint
  DECLARE @NBR_OF_RW_ISRT int
  DECLARE @NBR_OF_RW_UDT int
  DECLARE @EXEC_LYR varchar(255)
  DECLARE @EXEC_JOB varchar(500)
  DECLARE @SRC_ENTY varchar(500)
  DECLARE @TGT_ENTY varchar(500)
  DECLARE @GETDATETIME datetime = GETDATE()

  /* -------- insert into TY subset table --------  */
  EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR = 'EDAA_DW',
                                          @EXEC_JOB = 'PROC_FCT_BLBK_DM_TO_PRES_LOAD',
                                          @SRC_ENTY = 'FCT_BLBK_SUMMARY',
                                          @TGT_ENTY = 'FCT_BLBK_TY_LY_FSC',
                                          @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

  IF OBJECT_ID('tempdb..#BLBK_DATES') IS NOT NULL
  BEGIN
    DROP TABLE #BLBK_DATES
  END

  SELECT
    dt_sk AS TY_DT_SK,
    REPLACE(LST_YR_FSC_DT, '-', '') AS LY_DT_SK INTO #BLBK_DATES
  FROM edaa_dw.dim_dly_cal d
  WHERE  DT_SK <= (SELECT MAX(DT_SK) FROM [EDAA_DW].[FCT_BLBK_SUMMARY])

  ;WITH INITIAL_DATA_TY
	AS
	(
	SELECT
		CAST(b.TY_DT_SK AS int) AS TY_DT_SK,
		CAST(b.LY_DT_SK AS int) AS LY_DT_SK,
		a.Geo_Hist_Sk,
		a.Prod_Hist_Sk,
		SUM(CostOfGoodsReductionAmount) AS CostOfGoodsReductionAmount,
		a.Aud_Ins_Sk
	FROM [EDAA_DW].[FCT_BLBK_SUMMARY] AS a
	INNER JOIN #BLBK_DATES b ON a.DT_SK = b.TY_DT_SK
	GROUP BY
		CAST(b.TY_DT_SK AS int),
		CAST(b.LY_DT_SK AS int),
		a.Geo_Hist_Sk,
		a.Prod_Hist_Sk,
		a.Aud_Ins_Sk
	)
	MERGE [EDAA_PRES].[FCT_BLBK_TY_LY_FSC] AS T1
	USING INITIAL_DATA_TY AS STY
	ON (STY.TY_DT_SK = T1.Dt_Sk
	and STY.LY_DT_SK = T1.Dt_Sk_LY_FSC
	and STY.Prod_Hist_Sk = T1.Prod_Hist_Sk
	and STY.Geo_Hist_Sk = T1.Geo_Hist_Sk)
	WHEN NOT MATCHED THEN
	INSERT (Dt_Sk,Dt_Sk_LY_FSC,Geo_Hist_Sk,Prod_Hist_Sk,CostOfGoodsReductionAmount_TY_FSC,LoadKey,Create_Date,Update_Date)
	VALUES (STY.TY_DT_SK,STY.LY_DT_SK,STY.Geo_Hist_Sk,STY.Prod_Hist_Sk,STY.CostOfGoodsReductionAmount,STY.Aud_Ins_Sk,@GETDATETIME,@GETDATETIME);

  -- END OF TY DATA POPULATION--
  ;WITH INITIAL_DATA_LY
  AS(
  SELECT
	CAST(b.TY_DT_SK AS int) AS TY_DT_SK,
    CAST(b.LY_DT_SK AS int) AS LY_DT_SK,
	a.Geo_Hist_Sk ,
	a.Prod_Hist_Sk ,
	SUM(CostOfGoodsReductionAmount) AS CostOfGoodsReductionAmount,
	a.Aud_Ins_Sk
  FROM [EDAA_DW].[FCT_BLBK_SUMMARY] AS a
  INNER JOIN #BLBK_DATES b ON a.DT_SK = b.LY_DT_SK
  GROUP BY
		CAST(b.TY_DT_SK AS int),
		CAST(b.LY_DT_SK AS int),
		a.Geo_Hist_Sk,
		a.Prod_Hist_Sk,
		a.Aud_Ins_Sk
	)

   MERGE [EDAA_PRES].[FCT_BLBK_TY_LY_FSC] AS T2
		USING INITIAL_DATA_LY AS SLY
		ON (SLY.TY_DT_SK = T2.Dt_Sk
		and SLY.LY_DT_SK = T2.Dt_Sk_LY_FSC
		and SLY.Prod_Hist_Sk = T2.Prod_Hist_Sk
		and SLY.Geo_Hist_Sk = T2.Geo_Hist_Sk)
  WHEN MATCHED AND CAST(T2.Update_Date AS DATE) = CAST(GETDATE() AS DATE) THEN
  UPDATE SET
		T2.CostOfGoodsReductionAmount_LY_FSC=SLY.CostOfGoodsReductionAmount,
		T2.Create_Date = @GETDATETIME,
		T2.Update_Date = @GETDATETIME;

  -- END OF LY_FSC DATA POPULATION--
  UPDATE STATISTICS [EDAA_PRES].[FCT_BLBK_TY_LY_FSC];
  UPDATE STATISTICS [EDAA_DW].[FCT_BLBK_SUMMARY];

  EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY,
                                        @NBR_OF_RW_ISRT = 0,
                                        @NBR_OF_RW_UDT = 0


END TRY

BEGIN CATCH
  DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = '[EDAA_ETL].[PROC_FCT_BLBK_DM_TO_PRES_LOAD]'
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
