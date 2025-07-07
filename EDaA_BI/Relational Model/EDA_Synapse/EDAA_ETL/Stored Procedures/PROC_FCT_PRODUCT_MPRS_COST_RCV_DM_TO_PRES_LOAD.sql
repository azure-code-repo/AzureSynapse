/****** Object:  StoredProcedure [EDAA_ETL].[PROC_FCT_PRODUCT_MPRS_COST_RCV_DM_TO_PRES_LOAD]    Script Date: 10/25/2022 7:57:58 AM ******/

CREATE PROC [EDAA_ETL].[PROC_FCT_PRODUCT_MPRS_COST_RCV_DM_TO_PRES_LOAD] AS

BEGIN TRY

  /*DATAMART AUDITING VARIABLES*/
  DECLARE @Inc_Row_Count bigint
  DECLARE @NEW_AUD_SKY bigint
  DECLARE @NBR_OF_RW_ISRT int
  DECLARE @NBR_OF_RW_UDT int
  DECLARE @EXEC_LYR varchar(255)
  DECLARE @EXEC_JOB varchar(500)
  DECLARE @SRC_ENTY varchar(500)
  DECLARE @TGT_ENTY varchar(500)
  DECLARE @GETDATETIME datetime = GETDATE()
  DECLARE @DM_Last_Processed_Time AS DATETIME
  DECLARE @From_DT_SK int
  DECLARE @TO_DT_SK int
  /* -------- insert into TY subset table --------  */
  EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR = 'EDAA_DW',
                                          @EXEC_JOB = 'PROC_FCT_PRODUCT_MPRS_COST_RCV_DM_TO_PRES_LOAD',
                                          @SRC_ENTY = 'FCT_MPRS_PRODUCT_RCV_SUMMARY',
                                          @TGT_ENTY = 'FCT_PRODUCT_MPRS_COST_RCV_TY_LY_FSC',
                                          @NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

  /* Set Variable Values */
  SET @Inc_Row_Count = (SELECT COUNT_BIG(*) AS Row_Count FROM EDAA_PRES.FCT_PRODUCT_MPRS_COST_RCV_TY_LY_FSC);
  SET @DM_Last_Processed_Time = (SELECT MAX(Update_Date) AS Create_Date_Product FROM EDAA_DW.FCT_MPRS_PRODUCT_RCV_SUMMARY);
  SET @From_DT_SK=(SELECT MAX(DT_SK) FROM [EDAA_PRES].[FCT_PRODUCT_MPRS_COST_RCV_TY_LY_FSC]);
  SET @TO_DT_SK=(SELECT MAX(DT_SK) FROM [EDAA_DW].[FCT_MPRS_PRODUCT_RCV_SUMMARY]);


  -- For incrimental
  WITH MPRS_COST_RCV_DATES_INC AS (
    SELECT dt_sk AS TY_DT_SK,
    REPLACE(LST_YR_FSC_DT, '-', '') AS LY_DT_SK
  FROM edaa_dw.dim_dly_cal d
  WHERE  DT_SK <= (SELECT MAX(DT_SK) FROM [EDAA_DW].[FCT_MPRS_PRODUCT_RCV_SUMMARY]) and DT_SK >= (SELECT MAX(DT_SK) FROM [EDAA_PRES].[FCT_PRODUCT_MPRS_COST_RCV_TY_LY_FSC])
  and  @Inc_Row_Count>0) ,

  -- insert INC fresh records
 INITIAL_DATA_TY
  AS (SELECT
    a.Prod_Hist_Sk,
	a.Geo_Hist_Sk,
	a.Str_Id,
	a.Itm_Sku,
    CAST(b.TY_DT_SK AS int) AS TY_DT_SK,
    CAST(b.LY_DT_SK AS int) AS LY_DT_SK,
	Mjr_Pkg_Ut_Qty,
	Itm_Grss_Cst_Amt,
	Itm_Net_Cst_Amt,
	Itm_Chrg_Ea_Amt,
	Itm_Alw_Ea_Amt,
	Po_Chrg_Ea_Amt,
	Po_Alw_Ea_Amt,
	Csh_Dct_Ea_Amt,
	Frt_Chrg_Ea_Amt,
	Frt_Alw_Ea_Amt,
	Frt_Unld_Chrg_Amt,
	Ipt_Ld_Chrg_Amt,
	Bkh_Frt_Chrg_Amt,
	Extd_Bkh_Frt_Chrg_Amt,
	update_date,
	a.Aud_Ins_Sk
  FROM [EDAA_DW].[FCT_MPRS_PRODUCT_RCV_SUMMARY] AS a
  INNER JOIN MPRS_COST_RCV_DATES_INC b
    ON a.DT_SK = b.TY_DT_SK)

  MERGE [EDAA_PRES].[FCT_PRODUCT_MPRS_COST_RCV_TY_LY_FSC] AS T1
  USING INITIAL_DATA_TY AS STY
  ON (STY.TY_DT_SK = T1.Dt_Sk
  and STY.LY_DT_SK = T1.Dt_Sk_LY_FSC
  and STY.Itm_Sku = T1.Itm_Sku
  and STY.Str_Id = T1.Str_Id)
  WHEN NOT MATCHED and @Inc_Row_Count > 0 THEN
  INSERT (PROD_Hist_Sk,Geo_Hist_Sk,Str_Id,Itm_Sku,Dt_Sk,Dt_Sk_LY_FSC,Mjr_Pkg_Ut_Qty_TY_FSC,Itm_Grss_Cst_Amt_TY_FSC,Itm_Net_Cst_Amt_TY_FSC,Itm_Chrg_Ea_Amt_TY_FSC,Itm_Alw_Ea_Amt_TY_FSC,Po_Chrg_Ea_Amt_TY_FSC,Po_Alw_Ea_Amt_TY_FSC,Csh_Dct_Ea_Amt_TY_FSC,Frt_Chrg_Ea_Amt_TY_FSC,Frt_Alw_Ea_Amt_TY_FSC,Frt_Unld_Chrg_Amt_TY_FSC,Ipt_Ld_Chrg_Amt_TY_FSC,Bkh_Frt_Chrg_Amt_TY_FSC,Extd_Bkh_Frt_Chrg_Amt_TY_FSC,LoadKey,Create_Date,Update_Date)
  VALUES (STY.Prod_Hist_Sk,STY.Geo_Hist_Sk,STY.Str_Id,STY.Itm_Sku,STY.TY_DT_SK,STY.LY_DT_SK,STY.Mjr_Pkg_Ut_Qty,STY.Itm_Grss_Cst_Amt,STY.Itm_Net_Cst_Amt,STY.Itm_Chrg_Ea_Amt,STY.Itm_Alw_Ea_Amt,STY.Po_Chrg_Ea_Amt,STY.Po_Alw_Ea_Amt,STY.Csh_Dct_Ea_Amt,STY.Frt_Chrg_Ea_Amt,STY.Frt_Alw_Ea_Amt,STY.Frt_Unld_Chrg_Amt,STY.Ipt_Ld_Chrg_Amt,STY.Bkh_Frt_Chrg_Amt,STY.Extd_Bkh_Frt_Chrg_Amt,STY.Aud_Ins_Sk,@GETDATETIME,@GETDATETIME);



  -- update duplicate records
WITH MPRS_COST_RCV_DATES_INC_UDT AS (
    SELECT dt_sk AS TY_DT_SK,
    REPLACE(LST_YR_FSC_DT, '-', '') AS LY_DT_SK
  FROM edaa_dw.dim_dly_cal d
  WHERE  DT_SK <= (SELECT MAX(DT_SK) FROM [EDAA_DW].[FCT_MPRS_PRODUCT_RCV_SUMMARY]) and DT_SK >= (SELECT MAX(DT_SK) FROM [EDAA_PRES].[FCT_PRODUCT_MPRS_COST_RCV_TY_LY_FSC])
  and  @Inc_Row_Count>0) ,

INITIAL_DATA_TY
  AS (SELECT
    a.Prod_Hist_Sk,
	a.Geo_Hist_Sk,
	a.Str_Id,
	a.Itm_Sku,
    CAST(b.TY_DT_SK AS int) AS TY_DT_SK,
    CAST(b.LY_DT_SK AS int) AS LY_DT_SK,
	Mjr_Pkg_Ut_Qty,
	Itm_Grss_Cst_Amt,
	Itm_Net_Cst_Amt,
	Itm_Chrg_Ea_Amt,
	Itm_Alw_Ea_Amt,
	Po_Chrg_Ea_Amt,
	Po_Alw_Ea_Amt,
	Csh_Dct_Ea_Amt,
	Frt_Chrg_Ea_Amt,
	Frt_Alw_Ea_Amt,
	Frt_Unld_Chrg_Amt,
	Ipt_Ld_Chrg_Amt,
	Bkh_Frt_Chrg_Amt,
	Extd_Bkh_Frt_Chrg_Amt,
	update_date,
	a.Aud_Ins_Sk
  FROM [EDAA_DW].[FCT_MPRS_PRODUCT_RCV_SUMMARY] AS a
  INNER JOIN MPRS_COST_RCV_DATES_INC_UDT b
    ON a.DT_SK = b.TY_DT_SK)

  UPDATE T1
	SET T1.Mjr_Pkg_Ut_Qty_TY_FSC = STY.Mjr_Pkg_Ut_Qty,
	T1.Itm_Grss_Cst_Amt_TY_FSC = STY.Itm_Grss_Cst_Amt,
	T1.Itm_Net_Cst_Amt_TY_FSC = STY.Itm_Net_Cst_Amt,
	T1.Itm_Chrg_Ea_Amt_TY_FSC = STY.Itm_Chrg_Ea_Amt,
	T1.Itm_Alw_Ea_Amt_TY_FSC = STY.Itm_Alw_Ea_Amt,
	T1.Po_Chrg_Ea_Amt_TY_FSC = STY.Po_Chrg_Ea_Amt,
	T1.Po_Alw_Ea_Amt_TY_FSC = STY.Po_Alw_Ea_Amt,
	T1.Csh_Dct_Ea_Amt_TY_FSC = STY.Csh_Dct_Ea_Amt,
	T1.Frt_Chrg_Ea_Amt_TY_FSC = STY.Frt_Chrg_Ea_Amt,
	T1.Frt_Alw_Ea_Amt_TY_FSC = STY.Frt_Alw_Ea_Amt,
	T1.Frt_Unld_Chrg_Amt_TY_FSC = STY.Frt_Unld_Chrg_Amt,
	T1.Ipt_Ld_Chrg_Amt_TY_FSC = STY.Ipt_Ld_Chrg_Amt,
	T1.Bkh_Frt_Chrg_Amt_TY_FSC = STY.Bkh_Frt_Chrg_Amt,
	T1.Extd_Bkh_Frt_Chrg_Amt_TY_FSC = STY.Extd_Bkh_Frt_Chrg_Amt,
	T1.Update_Date = @GETDATETIME
  FROM
	[EDAA_PRES].[FCT_PRODUCT_MPRS_COST_RCV_TY_LY_FSC] AS T1
	INNER JOIN INITIAL_DATA_TY AS STY
					ON (STY.TY_DT_SK = T1.Dt_Sk
					AND STY.LY_DT_SK = T1.Dt_Sk_LY_FSC
					AND STY.Itm_Sku = T1.Itm_Sku
					AND STY.Str_Id = T1.Str_Id)
					where  STY.update_date>(SELECT MAX(Update_Date) FROM EDAA_PRES.FCT_PRODUCT_MPRS_COST_RCV_TY_LY_FSC)
					and @Inc_Row_Count > 0;
-- TY END

-- LY Start

WITH MPRS_COST_RCV_DATES AS (
  SELECT dt_sk AS TY_DT_SK,
    REPLACE(LST_YR_FSC_DT, '-', '') AS LY_DT_SK
  FROM edaa_dw.dim_dly_cal d
  WHERE DT_SK >= @From_DT_SK AND DT_SK <=@TO_DT_SK ),

 INITIAL_DATA_LY
  AS (SELECT
	a.Prod_Hist_Sk,
	a.Geo_Hist_Sk,
	a.Str_Id,
	a.Itm_Sku,
    CAST(b.TY_DT_SK AS int) AS TY_DT_SK,
    CAST(b.LY_DT_SK AS int) AS LY_DT_SK,
	Mjr_Pkg_Ut_Qty,
	Itm_Grss_Cst_Amt,
	Itm_Net_Cst_Amt,
	Itm_Chrg_Ea_Amt,
	Itm_Alw_Ea_Amt,
	Po_Chrg_Ea_Amt,
	Po_Alw_Ea_Amt,
	Csh_Dct_Ea_Amt,
	Frt_Chrg_Ea_Amt,
	Frt_Alw_Ea_Amt,
	Frt_Unld_Chrg_Amt,
	Ipt_Ld_Chrg_Amt,
	Bkh_Frt_Chrg_Amt,
	Extd_Bkh_Frt_Chrg_Amt,
	a.Aud_Ins_Sk

  FROM [EDAA_DW].[FCT_MPRS_PRODUCT_RCV_SUMMARY] AS a
  INNER JOIN MPRS_COST_RCV_DATES b
    ON a.DT_SK = b.LY_DT_SK)

  MERGE [EDAA_PRES].[FCT_PRODUCT_MPRS_COST_RCV_TY_LY_FSC] AS T2
  USING INITIAL_DATA_LY AS SLY
  ON (SLY.TY_DT_SK = T2.Dt_Sk
  and SLY.LY_DT_SK = T2.Dt_Sk_LY_FSC
  and SLY.Itm_Sku = T2.Itm_Sku
  and SLY.Str_Id = T2.Str_Id
  and T2.Update_Date  = @GETDATETIME)
  WHEN MATCHED THEN
  UPDATE SET
	T2.Mjr_Pkg_Ut_Qty_LY_FSC = SLY.Mjr_Pkg_Ut_Qty,
	T2.Itm_Grss_Cst_Amt_LY_FSC = SLY.Itm_Grss_Cst_Amt,
	T2.Itm_Net_Cst_Amt_LY_FSC = SLY.Itm_Net_Cst_Amt,
	T2.Itm_Chrg_Ea_Amt_LY_FSC = SLY.Itm_Chrg_Ea_Amt,
	T2.Itm_Alw_Ea_Amt_LY_FSC = SLY.Itm_Alw_Ea_Amt,
	T2.Po_Chrg_Ea_Amt_LY_FSC = SLY.Po_Chrg_Ea_Amt,
	T2.Po_Alw_Ea_Amt_LY_FSC = SLY.Po_Alw_Ea_Amt,
	T2.Csh_Dct_Ea_Amt_LY_FSC = SLY.Csh_Dct_Ea_Amt,
	T2.Frt_Chrg_Ea_Amt_LY_FSC = SLY.Frt_Chrg_Ea_Amt,
	T2.Frt_Alw_Ea_Amt_LY_FSC = SLY.Frt_Alw_Ea_Amt,
	T2.Frt_Unld_Chrg_Amt_LY_FSC = SLY.Frt_Unld_Chrg_Amt,
	T2.Ipt_Ld_Chrg_Amt_LY_FSC = SLY.Ipt_Ld_Chrg_Amt,
	T2.Bkh_Frt_Chrg_Amt_LY_FSC = SLY.Bkh_Frt_Chrg_Amt,
	T2.Extd_Bkh_Frt_Chrg_Amt_LY_FSC = SLY.Extd_Bkh_Frt_Chrg_Amt,
--	T2.Create_Date = @GETDATETIME,
	T2.Update_Date = @GETDATETIME;

-- LY END

/* Delete the Duplicate Records If Same Key Combination Records Inserted Multiple times w.r.t Same Days*/

  BEGIN
	WITH DELETE_DUPLICATE_CTE
	AS
	(
	SELECT
		Str_Id,
		ITM_SKU,
		Dt_Sk,
		ROW_NUMBER() OVER (PARTITION BY Str_Id,ITM_SKU,Dt_Sk ORDER BY UPDATE_DATE DESC) AS RNK
	FROM EDAA_PRES.FCT_PRODUCT_MPRS_COST_RCV_TY_LY_FSC WHERE DT_SK >= @From_DT_SK AND DT_SK <=@TO_DT_SK
	)
	DELETE FROM DELETE_DUPLICATE_CTE WHERE RNK >1
  END

  /*Update STATISTICS and Control Table*/

  UPDATE STATISTICS [EDAA_PRES].[FCT_PRODUCT_MPRS_COST_RCV_TY_LY_FSC];
  UPDATE STATISTICS [EDAA_DW].[FCT_MPRS_PRODUCT_RCV_SUMMARY];

  EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY,
                                        @NBR_OF_RW_ISRT = 0,
                                        @NBR_OF_RW_UDT = 0


END TRY

BEGIN CATCH
  DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) = '[EDAA_ETL].[PROC_FCT_PRODUCT_MPRS_COST_RCV_DM_TO_PRES_LOAD]'
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
