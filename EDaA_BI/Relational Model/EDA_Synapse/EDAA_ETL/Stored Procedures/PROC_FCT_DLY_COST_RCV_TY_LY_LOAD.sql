/****** Object:  StoredProcedure [EDAA_ETL].[PROC_FCT_DLY_COST_RCV_TY_LY_LOAD]    Script Date: 10/4/2022 3:10:29 AM ******/

CREATE PROC [EDAA_ETL].[PROC_FCT_DLY_COST_RCV_TY_LY_LOAD] AS
BEGIN TRY

/*Datamart Auditing variables*/
DECLARE @Inc_Row_Count BIGINT
DECLARE @NEW_AUD_SKY BIGINT
DECLARE @NBR_OF_RW_ISRT INT
DECLARE @NBR_OF_RW_UDT INT
DECLARE @EXEC_LYR VARCHAR(255)
DECLARE @EXEC_JOB VARCHAR(500)
DECLARE @SRC_ENTY VARCHAR(500)
DECLARE @TGT_ENTY VARCHAR(500)
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = 'EDAA_ETL.PROC_FCT_DLY_COST_RCV_TY_LY_LOAD'
DECLARE @ERROR_LINE AS int
DECLARE @ERROR_MSG AS NVARCHAR(max)
DECLARE @Create_Date_Product AS DATETIME
DECLARE @Update_Date_Product AS DATETIME
DECLARE @From_DT_SK AS INT
/*Audit Log Start*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START
 @EXEC_LYR  = 'EDAA_PRES'
,@EXEC_JOB  = 'PROC_FCT_DLY_COST_RCV_TY_LY_LOAD'
,@SRC_ENTY  = 'FCT_PRODUCT_MPRS_COST_RCV_TY_LY_FSC'
,@TGT_ENTY = 'FCT_PRODUCT_COST_RCV_TY_LY_FSC'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT;

/* Set Variable Values */
SET @Inc_Row_Count = (SELECT COUNT_BIG(1) AS Row_Count FROM EDAA_PRES.FCT_PRODUCT_COST_RCV_TY_LY_FSC);
Set @Create_Date_Product = (SELECT MAX(Create_Date) AS Create_Date_Product FROM EDAA_PRES.FCT_PRODUCT_COST_RCV_TY_LY_FSC);/* added to get incremental load*/
SET @Update_Date_Product = (SELECT MAX(Update_Date) AS Update_Date_Product FROM EDAA_PRES.FCT_PRODUCT_COST_RCV_TY_LY_FSC);
SET @From_DT_SK=(SELECT MAX(DT_SK) FROM EDAA_PRES.FCT_PRODUCT_COST_RCV_TY_LY_FSC);

/*Incremental Data Loading Process Script*/
IF @Inc_Row_Count>0
BEGIN
;WITH INC_DATA AS
( SELECT
T.Prod_Hist_Sk,
T.Geo_Hist_Sk,
T.Itm_Sku,
T.Dt_Sk,
T.Dt_Sk_LY_FSC,
T.Mjr_Pkg_Ut_Qty_TY_FSC,
T.Mjr_Pkg_Ut_Qty_LY_FSC,
T.Itm_Grss_Cst_Amt_TY_FSC,
T.Itm_Grss_Cst_Amt_LY_FSC,
T.Itm_Net_Cst_Amt_TY_FSC,
T.Itm_Net_Cst_Amt_LY_FSC,
T.Itm_Chrg_Ea_Amt_TY_FSC,
T.Itm_Chrg_Ea_Amt_LY_FSC,
T.Itm_Alw_Ea_Amt_TY_FSC,
T.Itm_Alw_Ea_Amt_LY_FSC,
T.Po_Chrg_Ea_Amt_TY_FSC,
T.Po_Chrg_Ea_Amt_LY_FSC,
T.Po_Alw_Ea_Amt_TY_FSC,
T.Po_Alw_Ea_Amt_LY_FSC,
T.Csh_Dct_Ea_Amt_TY_FSC,
T.Csh_Dct_Ea_Amt_LY_FSC,
T.Frt_Chrg_Ea_Amt_TY_FSC,
T.Frt_Chrg_Ea_Amt_LY_FSC,
T.Frt_Alw_Ea_Amt_TY_FSC,
T.Frt_Alw_Ea_Amt_LY_FSC,
T.Frt_Unld_Chrg_Amt_TY_FSC,
T.Frt_Unld_Chrg_Amt_LY_FSC,
T.Ipt_Ld_Chrg_Amt_TY_FSC,
T.Ipt_Ld_Chrg_Amt_LY_FSC,
T.Bkh_Frt_Chrg_Amt_TY_FSC,
T.Bkh_Frt_Chrg_Amt_LY_FSC,
T.Extd_Bkh_Frt_Chrg_Amt_TY_FSC,
T.Extd_Bkh_Frt_Chrg_Amt_LY_FSC
FROM [EDAA_PRES].[FCT_PRODUCT_MPRS_COST_RCV_TY_LY_FSC] AS T
WHERE (T.Create_Date > @Create_Date_Product) or (T.Update_Date > @Update_Date_Product)
),
combined_tables
AS (
SELECT
a.Prod_Hist_Sk,
a.Geo_Hist_Sk,
a.Itm_Sku,
a.Dt_Sk,
a.Dt_Sk_LY_FSC,
a.Mjr_Pkg_Ut_Qty_TY_FSC,
a.Mjr_Pkg_Ut_Qty_LY_FSC,
a.Itm_Grss_Cst_Amt_TY_FSC,
a.Itm_Grss_Cst_Amt_LY_FSC,
a.Itm_Net_Cst_Amt_TY_FSC,
a.Itm_Net_Cst_Amt_LY_FSC,
a.Itm_Chrg_Ea_Amt_TY_FSC,
a.Itm_Chrg_Ea_Amt_LY_FSC,
a.Itm_Alw_Ea_Amt_TY_FSC,
a.Itm_Alw_Ea_Amt_LY_FSC,
a.Po_Chrg_Ea_Amt_TY_FSC,
a.Po_Chrg_Ea_Amt_LY_FSC,
a.Po_Alw_Ea_Amt_TY_FSC,
a.Po_Alw_Ea_Amt_LY_FSC,
a.Csh_Dct_Ea_Amt_TY_FSC,
a.Csh_Dct_Ea_Amt_LY_FSC,
a.Frt_Chrg_Ea_Amt_TY_FSC,
a.Frt_Chrg_Ea_Amt_LY_FSC,
a.Frt_Alw_Ea_Amt_TY_FSC,
a.Frt_Alw_Ea_Amt_LY_FSC,
a.Frt_Unld_Chrg_Amt_TY_FSC,
a.Frt_Unld_Chrg_Amt_LY_FSC,
a.Ipt_Ld_Chrg_Amt_TY_FSC,
a.Ipt_Ld_Chrg_Amt_LY_FSC,
a.Bkh_Frt_Chrg_Amt_TY_FSC,
a.Bkh_Frt_Chrg_Amt_LY_FSC,
a.Extd_Bkh_Frt_Chrg_Amt_TY_FSC,
a.Extd_Bkh_Frt_Chrg_Amt_LY_FSC,
b.CostOfGoodsReductionAmount_TY_FSC,
b.CostOfGoodsReductionAmount_LY_FSC,
c.SwellAndDefectiveAlw_TY_FSC,
c.SwellAndDefectiveAlw_LY_FSC
FROM INC_DATA AS a
left join [EDAA_PRES].[FCT_BLBK_TY_LY_FSC] b
on a.DT_Sk = b.Dt_Sk
and a.Prod_Hist_Sk = b.Prod_Hist_Sk
and a.Geo_Hist_Sk = b.Geo_Hist_Sk
left join[EDAA_PRES].[FCT_POCAA_HST_TY_LY_FSC] C
on a.DT_Sk = c.Dt_Sk
and a.Prod_Hist_Sk = c.Prod_Hist_Sk
and a.Geo_Hist_Sk = c.Geo_Hist_Sk
)
INSERT INTO EDAA_PRES.FCT_PRODUCT_COST_RCV_TY_LY_FSC (Prod_Hist_Sk,Geo_Hist_Sk,Itm_Sku,Dt_Sk,Dt_Sk_LY_FSC,Mjr_Pkg_Ut_Qty_TY_FSC,Mjr_Pkg_Ut_Qty_LY_FSC,Itm_Grss_Cst_Amt_TY_FSC,Itm_Grss_Cst_Amt_LY_FSC,Itm_Net_Cst_Amt_TY_FSC,Itm_Net_Cst_Amt_LY_FSC,Itm_Chrg_Ea_Amt_TY_FSC,Itm_Chrg_Ea_Amt_LY_FSC,Itm_Alw_Ea_Amt_TY_FSC,Itm_Alw_Ea_Amt_LY_FSC,Po_Chrg_Ea_Amt_TY_FSC,Po_Chrg_Ea_Amt_LY_FSC,Po_Alw_Ea_Amt_TY_FSC,Po_Alw_Ea_Amt_LY_FSC,Csh_Dct_Ea_Amt_TY_FSC,Csh_Dct_Ea_Amt_LY_FSC,Frt_Chrg_Ea_Amt_TY_FSC,Frt_Chrg_Ea_Amt_LY_FSC,Frt_Alw_Ea_Amt_TY_FSC,Frt_Alw_Ea_Amt_LY_FSC,Frt_Unld_Chrg_Amt_TY_FSC,Frt_Unld_Chrg_Amt_LY_FSC,Ipt_Ld_Chrg_Amt_TY_FSC,Ipt_Ld_Chrg_Amt_LY_FSC,Bkh_Frt_Chrg_Amt_TY_FSC,Bkh_Frt_Chrg_Amt_LY_FSC,Extd_Bkh_Frt_Chrg_Amt_TY_FSC,Extd_Bkh_Frt_Chrg_Amt_LY_FSC,CostOfGoodsReductionAmount_TY_FSC,CostOfGoodsReductionAmount_LY_FSC,SwellAndDefectiveAlw_TY_FSC,SwellAndDefectiveAlw_LY_FSC,Create_Date,Update_Date)
SELECT Prod_Hist_Sk,Geo_Hist_Sk,Itm_Sku,Dt_Sk,Dt_Sk_LY_FSC,Mjr_Pkg_Ut_Qty_TY_FSC,Mjr_Pkg_Ut_Qty_LY_FSC,Itm_Grss_Cst_Amt_TY_FSC,Itm_Grss_Cst_Amt_LY_FSC,Itm_Net_Cst_Amt_TY_FSC,Itm_Net_Cst_Amt_LY_FSC,Itm_Chrg_Ea_Amt_TY_FSC,Itm_Chrg_Ea_Amt_LY_FSC,Itm_Alw_Ea_Amt_TY_FSC,Itm_Alw_Ea_Amt_LY_FSC,Po_Chrg_Ea_Amt_TY_FSC,Po_Chrg_Ea_Amt_LY_FSC,Po_Alw_Ea_Amt_TY_FSC,Po_Alw_Ea_Amt_LY_FSC,Csh_Dct_Ea_Amt_TY_FSC,Csh_Dct_Ea_Amt_LY_FSC,Frt_Chrg_Ea_Amt_TY_FSC,Frt_Chrg_Ea_Amt_LY_FSC,Frt_Alw_Ea_Amt_TY_FSC,Frt_Alw_Ea_Amt_LY_FSC,Frt_Unld_Chrg_Amt_TY_FSC,Frt_Unld_Chrg_Amt_LY_FSC,Ipt_Ld_Chrg_Amt_TY_FSC,Ipt_Ld_Chrg_Amt_LY_FSC,Bkh_Frt_Chrg_Amt_TY_FSC,Bkh_Frt_Chrg_Amt_LY_FSC,Extd_Bkh_Frt_Chrg_Amt_TY_FSC,Extd_Bkh_Frt_Chrg_Amt_LY_FSC,CostOfGoodsReductionAmount_TY_FSC,CostOfGoodsReductionAmount_LY_FSC,SwellAndDefectiveAlw_TY_FSC,SwellAndDefectiveAlw_LY_FSC,GETDATE(),GETDATE()
FROM combined_tables;

END

/* Delete the Duplicate Records If Same Key Combination Records Inserted Multiple times w.r.t Same Days*/

BEGIN
WITH DELETE_DUPLICATE_CTE
AS
(
SELECT
	PROD_HIST_SK,
	GEO_HIST_SK,
	ITM_SKU,
	Dt_Sk,
    ROW_NUMBER() OVER (PARTITION BY PROD_HIST_SK,GEO_HIST_SK,ITM_SKU,Dt_Sk ORDER BY UPDATE_DATE DESC) AS RNK
FROM EDAA_PRES.FCT_PRODUCT_COST_RCV_TY_LY_FSC WHERE DT_SK>= @From_DT_SK
)
DELETE FROM DELETE_DUPLICATE_CTE WHERE RNK >1
END

 --UPDATE STATISTICS
 BEGIN
 UPDATE STATISTICS [EDAA_PRES].[FCT_PRODUCT_COST_RCV_TY_LY_FSC];
 END

END TRY

BEGIN CATCH
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = 0, @NBR_OF_RW_UDT  = 0
SET @ERROR_PROCEDURE_NAME = 'EDAA_ETL.PROC_FCT_DLY_COST_RCV_TY_LY_LOAD'
--SET @ERROR_LINE
--SET @ERROR_MSG

 SELECT
      @ERROR_LINE =  ERROR_NUMBER()
       ,@ERROR_MSG = ERROR_MESSAGE();

--------- Log execution error ----------
EXEC EDAA_CNTL.SP_LOG_AUD_ERR
@AUD_SKY = @NEW_AUD_SKY,
@ERROR_PROCEDURE_NAME = @ERROR_PROCEDURE_NAME,
@ERROR_LINE = @ERROR_LINE,
@ERROR_MSG = @ERROR_MSG;

   THROW;

END CATCH
