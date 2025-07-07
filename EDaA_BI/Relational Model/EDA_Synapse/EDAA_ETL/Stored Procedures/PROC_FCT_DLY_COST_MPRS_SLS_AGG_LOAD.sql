/****** Object:  StoredProcedure [EDAA_ETL].[PROC_FCT_DLY_COST_MPRS_SLS_AGG_LOAD_UAT]    Script Date: 3/1/2023 5:11:41 AM ******/

CREATE PROC [EDAA_ETL].[PROC_FCT_DLY_COST_MPRS_SLS_AGG_LOAD] AS
BEGIN TRY

/*Datamart Auditing variables*/
DECLARE @NEW_AUD_SKY BIGINT
DECLARE @NBR_OF_RW_ISRT INT
DECLARE @NBR_OF_RW_UDT INT
DECLARE @EXEC_LYR VARCHAR(255)
DECLARE @EXEC_JOB VARCHAR(500)
DECLARE @SRC_ENTY VARCHAR(500)
DECLARE @TGT_ENTY VARCHAR(500)
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = 'EDAA_ETL.PROC_FCT_DLY_COST_MPRS_SLS_AGG_LOAD'
DECLARE @ERROR_LINE AS int
DECLARE @ERROR_MSG AS NVARCHAR(max)

/*Audit Log Start*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START
 @EXEC_LYR  = 'EDAA_PRES'
,@EXEC_JOB  = 'PROC_FCT_DLY_COST_MPRS_SLS_AGG_LOAD'
,@SRC_ENTY  = 'FCT_COST_MPRS_ITMSKU_AGG'
,@TGT_ENTY = 'FCT_DLY_STR_ITMSKU_MPRS_COSTRCV_SLS_AGG'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT;


TRUNCATE TABLE [EDAA_PRES].[FCT_DLY_STR_ITMSKU_MPRS_COSTRCV_SLS_AGG]

/* Stores which is Having Cost Data */

INSERT INTO [EDAA_PRES].[FCT_DLY_STR_ITMSKU_MPRS_COSTRCV_SLS_AGG]
(ROW_ID_VAL,CLN_TYP_ID,PRE_DEF_ID,DT_VAL,GEO_HIST_SK,PROD_HIST_SK,ITM_SKU,STR_ID,Mjr_Pkg_Ut_Qty_TY_FSC,Mjr_Pkg_Ut_Qty_LY_FSC,Itm_Grss_Cst_Amt_TY_FSC,Itm_Grss_Cst_Amt_LY_FSC,Itm_Net_Cst_Amt_TY_FSC,Itm_Net_Cst_Amt_LY_FSC,Itm_Chrg_Ea_Amt_TY_FSC,Itm_Chrg_Ea_Amt_LY_FSC,Itm_Alw_Ea_Amt_TY_FSC,Itm_Alw_Ea_Amt_LY_FSC,Po_Chrg_Ea_Amt_TY_FSC,Po_Chrg_Ea_Amt_LY_FSC,Po_Alw_Ea_Amt_TY_FSC,Po_Alw_Ea_Amt_LY_FSC,Csh_Dct_Ea_Amt_TY_FSC,Csh_Dct_Ea_Amt_LY_FSC,Frt_Chrg_Ea_Amt_TY_FSC,Frt_Chrg_Ea_Amt_LY_FSC,Frt_Alw_Ea_Amt_TY_FSC,Frt_Alw_Ea_Amt_LY_FSC,Frt_Unld_Chrg_Amt_TY_FSC,Frt_Unld_Chrg_Amt_LY_FSC,Ipt_Ld_Chrg_Amt_TY_FSC,Ipt_Ld_Chrg_Amt_LY_FSC,Bkh_Frt_Chrg_Amt_TY_FSC,Bkh_Frt_Chrg_Amt_LY_FSC,Extd_Bkh_Frt_Chrg_Amt_TY_FSC,Extd_Bkh_Frt_Chrg_Amt_LY_FSC,Cst_Gds_Rdc_Amt_Ty_Fsc,Cst_Gds_Rdc_Amt_Ly_Fsc,Swl_Dft_Alw_Ty_Fsc,Swl_Dft_Alw_Ly_Fsc,SLS_AMT_TY_FSC,SLS_AMT_LY_FSC,SLS_QTY_TY_FSC,SLS_QTY_LY_FSC,SLS_WGT_TY_FSC,SLS_WGT_LY_FSC,SLS_UOM_QTY_TY,SLS_UOM_QTY_LY,PRM_TO_SLS_AMT_TY_FSC,PRM_TO_SLS_AMT_LY_FSC,PRM_TO_SLS_QTY_TY_FSC,PRM_TO_SLS_QTY_LY_FSC,PRM_TO_SLS_WGT_TY_FSC,PRM_TO_SLS_WGT_LY_FSC,PRM_TO_SLS_MKDN_AMT_TY_FSC,PRM_TO_SLS_MKDN_AMT_LY_FSC,CLRNC_SLS_AMT_TY_FSC,CLRNC_SLS_AMT_LY_FSC,CLRNC_QTY_TY_FSC,CLRNC_QTY_LY_FSC,CLRNC_WGT_TY_FSC,CLRNC_WGT_LY_FSC,CLRNC_MKDN_AMT_TY_FSC,CLRNC_MKDN_AMT_LY_FSC,COGS_AMT_TY_FSC,COGS_AMT_LY_FSC,COGS_FNC_CHRG_AMT_TY_FSC,COGS_FNC_CHRG_AMT_LY_FSC,COGS_FNC_CR_AMT_TY_FSC,COGS_FNC_CR_AMT_LY_FSC,COGS_STRGE_CHRG_AMT_TY_FSC,COGS_STRGE_CHRG_AMT_LY_FSC,DRCT_MGN_AMT_TY_FSC,DRCT_MGN_AMT_LY_FSC,PRM_TO_SLS_DRCT_MGN_AMT_TY_FSC,PRM_TO_SLS_DRCT_MGN_AMT_LY_FSC,CLRNC_DRCT_MGN_AMT_TY_FSC,CLRNC_DRCT_MGN_AMT_LY_FSC,COGS_FRT_AMT_TY_FSC,COGS_FRT_AMT_LY_FSC,COGS_DIST_CNTR_HNDL_AMT_TY_FSC,COGS_DIST_CNTR_HNDL_AMT_LY_FSC)
SELECT
b.ROW_ID_VAL,
--b.CLN_TYP_ID,
1,
b.PRE_DEF_ID,
b.DT_VAL,
c.GEO_HIST_SK,
--b.PROD_SK,
-1,
b.ITM_SKU,
b.STR_ID,
ISNULL(a.Mjr_Pkg_Ut_Qty_TY_FSC,0) AS Mjr_Pkg_Ut_Qty_TY_FSC,
ISNULL(a.Mjr_Pkg_Ut_Qty_LY_FSC,0) AS Mjr_Pkg_Ut_Qty_LY_FSC,
ISNULL(a.Itm_Grss_Cst_Amt_TY_FSC,0) AS Itm_Grss_Cst_Amt_TY_FSC,
ISNULL(a.Itm_Grss_Cst_Amt_LY_FSC,0) AS Itm_Grss_Cst_Amt_LY_FSC,
ISNULL(a.Itm_Net_Cst_Amt_TY_FSC,0) AS Itm_Net_Cst_Amt_TY_FSC,
ISNULL(a.Itm_Net_Cst_Amt_LY_FSC,0) AS Itm_Net_Cst_Amt_LY_FSC,
ISNULL(a.Itm_Chrg_Ea_Amt_TY_FSC,0) AS Itm_Chrg_Ea_Amt_TY_FSC,
ISNULL(a.Itm_Chrg_Ea_Amt_LY_FSC,0) AS Itm_Chrg_Ea_Amt_LY_FSC,
ISNULL(a.Itm_Alw_Ea_Amt_TY_FSC,0) AS Itm_Alw_Ea_Amt_TY_FSC,
ISNULL(a.Itm_Alw_Ea_Amt_LY_FSC,0) AS Itm_Alw_Ea_Amt_LY_FSC,
ISNULL(a.Po_Chrg_Ea_Amt_TY_FSC,0) AS Po_Chrg_Ea_Amt_TY_FSC,
ISNULL(a.Po_Chrg_Ea_Amt_LY_FSC,0) AS Po_Chrg_Ea_Amt_LY_FSC,
ISNULL(a.Po_Alw_Ea_Amt_TY_FSC,0) AS Po_Alw_Ea_Amt_TY_FSC,
ISNULL(a.Po_Alw_Ea_Amt_LY_FSC,0) AS Po_Alw_Ea_Amt_LY_FSC,
ISNULL(a.Csh_Dct_Ea_Amt_TY_FSC,0) AS Csh_Dct_Ea_Amt_TY_FSC,
ISNULL(a.Csh_Dct_Ea_Amt_LY_FSC,0) AS Csh_Dct_Ea_Amt_LY_FSC,
ISNULL(a.Frt_Chrg_Ea_Amt_TY_FSC,0) AS Frt_Chrg_Ea_Amt_TY_FSC,
ISNULL(a.Frt_Chrg_Ea_Amt_LY_FSC,0) AS Frt_Chrg_Ea_Amt_LY_FSC,
ISNULL(a.Frt_Alw_Ea_Amt_TY_FSC,0) AS Frt_Alw_Ea_Amt_TY_FSC,
ISNULL(a.Frt_Alw_Ea_Amt_LY_FSC,0) AS Frt_Alw_Ea_Amt_LY_FSC,
ISNULL(a.Frt_Unld_Chrg_Amt_TY_FSC,0) AS Frt_Unld_Chrg_Amt_TY_FSC,
ISNULL(a.Frt_Unld_Chrg_Amt_LY_FSC,0) AS Frt_Unld_Chrg_Amt_LY_FSC,
ISNULL(a.Ipt_Ld_Chrg_Amt_TY_FSC,0) AS Ipt_Ld_Chrg_Amt_TY_FSC,
ISNULL(a.Ipt_Ld_Chrg_Amt_LY_FSC,0) AS Ipt_Ld_Chrg_Amt_LY_FSC,
ISNULL(a.Bkh_Frt_Chrg_Amt_TY_FSC,0) AS Bkh_Frt_Chrg_Amt_TY_FSC,
ISNULL(a.Bkh_Frt_Chrg_Amt_LY_FSC,0) AS Bkh_Frt_Chrg_Amt_LY_FSC,
ISNULL(a.Extd_Bkh_Frt_Chrg_Amt_TY_FSC,0) AS Extd_Bkh_Frt_Chrg_Amt_TY_FSC,
ISNULL(a.Extd_Bkh_Frt_Chrg_Amt_LY_FSC,0) AS Extd_Bkh_Frt_Chrg_Amt_LY_FSC,
ISNULL(a.CostOfGoodsReductionAmount_TY_FSC,0) AS Cst_Gds_Rdc_Amt_Ty_Fsc,
ISNULL(a.CostOfGoodsReductionAmount_LY_FSC,0) AS Cst_Gds_Rdc_Amt_Ly_Fsc,
ISNULL(a.SwellAndDefectiveAlw_TY_FSC,0) AS Swl_Dft_Alw_Ty_Fsc,
ISNULL(a.SwellAndDefectiveAlw_LY_FSC,0) AS Swl_Dft_Alw_Ly_Fsc,
b.SLS_AMT_TY_FSC,
b.SLS_AMT_LY_FSC,
b.SLS_QTY_TY_FSC,
b.SLS_QTY_LY_FSC,
b.SLS_WGT_TY_FSC,
b.SLS_WGT_LY_FSC,
COALESCE(NULLIF(b.SLS_WGT_TY_FSC,0),b.SLS_QTY_TY_FSC) AS SLS_UOM_QTY_TY,
COALESCE(NULLIF(b.SLS_WGT_LY_FSC,0),b.SLS_QTY_LY_FSC) AS SLS_UOM_QTY_LY,
b.PRM_TO_SLS_AMT_TY_FSC,
b.PRM_TO_SLS_AMT_LY_FSC,
b.PRM_TO_SLS_QTY_TY_FSC,
b.PRM_TO_SLS_QTY_LY_FSC,
b.PRM_TO_SLS_WGT_TY_FSC,
b.PRM_TO_SLS_WGT_LY_FSC,
b.PRM_TO_SLS_MKDN_AMT_TY_FSC,
b.PRM_TO_SLS_MKDN_AMT_LY_FSC,
b.CLRNC_SLS_AMT_TY_FSC,
b.CLRNC_SLS_AMT_LY_FSC,
b.CLRNC_QTY_TY_FSC,
b.CLRNC_QTY_LY_FSC,
b.CLRNC_WGT_TY_FSC,
b.CLRNC_WGT_LY_FSC,
b.CLRNC_MKDN_AMT_TY_FSC,
b.CLRNC_MKDN_AMT_LY_FSC,
b.COGS_AMT_TY_FSC,
b.COGS_AMT_LY_FSC,
b.COGS_FNC_CHRG_AMT_TY_FSC,
b.COGS_FNC_CHRG_AMT_LY_FSC,
b.COGS_FNC_CR_AMT_TY_FSC,
b.COGS_FNC_CR_AMT_LY_FSC,
b.COGS_STRGE_CHRG_AMT_TY_FSC,
b.COGS_STRGE_CHRG_AMT_LY_FSC,
b.DRCT_MGN_AMT_TY_FSC,
b.DRCT_MGN_AMT_LY_FSC,
b.PRM_TO_SLS_DRCT_MGN_AMT_TY_FSC,
b.PRM_TO_SLS_DRCT_MGN_AMT_LY_FSC,
b.CLRNC_DRCT_MGN_AMT_TY_FSC,
b.CLRNC_DRCT_MGN_AMT_LY_FSC,
b.COGS_FRT_AMT_TY_FSC,
b.COGS_FRT_AMT_LY_FSC,
b.COGS_DIST_CNTR_HNDL_AMT_TY_FSC,
b.COGS_DIST_CNTR_HNDL_AMT_LY_FSC
FROM [EDAA_PRES].[FCT_DLY_STR_ITMSKU_SLS_AGG] b
INNER JOIN EDAA_DW.DIM_GEO c
       ON b.STR_ID = c.STR_ID and c.Is_Curr_ind = 1
INNER JOIN [EDAA_PRES].[FCT_COST_MPRS_ITMSKU_AGG] a
                      ON a.PRE_DEF_ID = b.PRE_DEF_ID
                      AND a.DT_VAL = b.DT_VAL
                      AND a.ITM_SKU = b.ITM_SKU
					  AND a.GEO_HIST_SK = c.GEO_HIST_SK


/* Average Cost for Stores Which Don't have Cost Receiving Data eventhough We have Sales Record for that particular store*/
;WITH CTE_AVG AS (
select t.Itm_Sku,t.pre_Def_id,
avg(t.Mjr_Pkg_Ut_Qty_TY_FSC) AS Mjr_Pkg_Ut_Qty_TY_FSC,
avg(t.Mjr_Pkg_Ut_Qty_LY_FSC) AS Mjr_Pkg_Ut_Qty_LY_FSC,
avg(t.Itm_Grss_Cst_Amt_TY_FSC) AS Itm_Grss_Cst_Amt_TY_FSC,
avg(t.Itm_Grss_Cst_Amt_LY_FSC) AS Itm_Grss_Cst_Amt_LY_FSC,
avg(t.Itm_Net_Cst_Amt_TY_FSC) AS Itm_Net_Cst_Amt_TY_FSC,
avg(t.Itm_Net_Cst_Amt_LY_FSC) AS Itm_Net_Cst_Amt_LY_FSC,
avg(t.Itm_Chrg_Ea_Amt_TY_FSC) AS Itm_Chrg_Ea_Amt_TY_FSC,
avg(t.Itm_Chrg_Ea_Amt_LY_FSC) AS Itm_Chrg_Ea_Amt_LY_FSC,
avg(t.Itm_Alw_Ea_Amt_TY_FSC) AS Itm_Alw_Ea_Amt_TY_FSC,
avg(t.Itm_Alw_Ea_Amt_LY_FSC) AS Itm_Alw_Ea_Amt_LY_FSC,
avg(t.Po_Chrg_Ea_Amt_TY_FSC) AS Po_Chrg_Ea_Amt_TY_FSC,
avg(t.Po_Chrg_Ea_Amt_LY_FSC) AS Po_Chrg_Ea_Amt_LY_FSC,
avg(t.Po_Alw_Ea_Amt_TY_FSC) AS Po_Alw_Ea_Amt_TY_FSC,
avg(t.Po_Alw_Ea_Amt_LY_FSC) AS Po_Alw_Ea_Amt_LY_FSC,
avg(t.Csh_Dct_Ea_Amt_TY_FSC) AS Csh_Dct_Ea_Amt_TY_FSC,
avg(t.Csh_Dct_Ea_Amt_LY_FSC) AS Csh_Dct_Ea_Amt_LY_FSC,
avg(t.Frt_Chrg_Ea_Amt_TY_FSC) AS Frt_Chrg_Ea_Amt_TY_FSC,
avg(t.Frt_Chrg_Ea_Amt_LY_FSC) AS Frt_Chrg_Ea_Amt_LY_FSC,
avg(t.Frt_Alw_Ea_Amt_TY_FSC) AS Frt_Alw_Ea_Amt_TY_FSC,
avg(t.Frt_Alw_Ea_Amt_LY_FSC) AS Frt_Alw_Ea_Amt_LY_FSC,
avg(t.Frt_Unld_Chrg_Amt_TY_FSC) AS Frt_Unld_Chrg_Amt_TY_FSC,
avg(t.Frt_Unld_Chrg_Amt_LY_FSC) AS Frt_Unld_Chrg_Amt_LY_FSC,
avg(t.Ipt_Ld_Chrg_Amt_TY_FSC) AS Ipt_Ld_Chrg_Amt_TY_FSC,
avg(t.Ipt_Ld_Chrg_Amt_LY_FSC) AS Ipt_Ld_Chrg_Amt_LY_FSC,
avg(t.Bkh_Frt_Chrg_Amt_TY_FSC) AS Bkh_Frt_Chrg_Amt_TY_FSC,
avg(t.Bkh_Frt_Chrg_Amt_LY_FSC) AS Bkh_Frt_Chrg_Amt_LY_FSC,
avg(t.Extd_Bkh_Frt_Chrg_Amt_TY_FSC) AS Extd_Bkh_Frt_Chrg_Amt_TY_FSC,
avg(t.Extd_Bkh_Frt_Chrg_Amt_LY_FSC) AS Extd_Bkh_Frt_Chrg_Amt_LY_FSC,
avg(t.CostOfGoodsReductionAmount_TY_FSC) AS Cst_Gds_Rdc_Amt_Ty_Fsc,
avg(t.CostOfGoodsReductionAmount_LY_FSC) AS Cst_Gds_Rdc_Amt_Ly_Fsc,
avg(t.SwellAndDefectiveAlw_TY_FSC) AS Swl_Dft_Alw_Ty_Fsc,
avg(t.SwellAndDefectiveAlw_LY_FSC) AS Swl_Dft_Alw_Ly_Fsc
from [EDAA_PRES].[FCT_COST_MPRS_ITMSKU_AGG] as t
INNER JOIN [EDAA_PRES].[FCT_DLY_STR_ITMSKU_SLS_AGG] AS b
ON t.Itm_Sku=b.Itm_Sku and t.pre_Def_id=b.pre_def_id
INNER JOIN EDAA_DW.DIM_GEO c
ON b.STR_ID = c.STR_ID and c.Is_Curr_ind = 1 and c.Div_Id in ('MS','MG')
--where  t.Mjr_Pkg_Ut_Qty_TY_FSC<>0
GROUP BY t.Itm_Sku,t.pre_Def_id
)

INSERT INTO [EDAA_PRES].[FCT_DLY_STR_ITMSKU_MPRS_COSTRCV_SLS_AGG]
(ROW_ID_VAL,CLN_TYP_ID,PRE_DEF_ID,DT_VAL,GEO_HIST_SK,PROD_HIST_SK,ITM_SKU,STR_ID,Mjr_Pkg_Ut_Qty_TY_FSC,Mjr_Pkg_Ut_Qty_LY_FSC,Itm_Grss_Cst_Amt_TY_FSC,Itm_Grss_Cst_Amt_LY_FSC,Itm_Net_Cst_Amt_TY_FSC,Itm_Net_Cst_Amt_LY_FSC,Itm_Chrg_Ea_Amt_TY_FSC,Itm_Chrg_Ea_Amt_LY_FSC,Itm_Alw_Ea_Amt_TY_FSC,Itm_Alw_Ea_Amt_LY_FSC,Po_Chrg_Ea_Amt_TY_FSC,Po_Chrg_Ea_Amt_LY_FSC,Po_Alw_Ea_Amt_TY_FSC,Po_Alw_Ea_Amt_LY_FSC,Csh_Dct_Ea_Amt_TY_FSC,Csh_Dct_Ea_Amt_LY_FSC,Frt_Chrg_Ea_Amt_TY_FSC,Frt_Chrg_Ea_Amt_LY_FSC,Frt_Alw_Ea_Amt_TY_FSC,Frt_Alw_Ea_Amt_LY_FSC,Frt_Unld_Chrg_Amt_TY_FSC,Frt_Unld_Chrg_Amt_LY_FSC,Ipt_Ld_Chrg_Amt_TY_FSC,Ipt_Ld_Chrg_Amt_LY_FSC,Bkh_Frt_Chrg_Amt_TY_FSC,Bkh_Frt_Chrg_Amt_LY_FSC,Extd_Bkh_Frt_Chrg_Amt_TY_FSC,Extd_Bkh_Frt_Chrg_Amt_LY_FSC,Cst_Gds_Rdc_Amt_Ty_Fsc,Cst_Gds_Rdc_Amt_Ly_Fsc,Swl_Dft_Alw_Ty_Fsc,Swl_Dft_Alw_Ly_Fsc,SLS_AMT_TY_FSC,SLS_AMT_LY_FSC,SLS_QTY_TY_FSC,SLS_QTY_LY_FSC,SLS_WGT_TY_FSC,SLS_WGT_LY_FSC,SLS_UOM_QTY_TY,SLS_UOM_QTY_LY,PRM_TO_SLS_AMT_TY_FSC,PRM_TO_SLS_AMT_LY_FSC,PRM_TO_SLS_QTY_TY_FSC,PRM_TO_SLS_QTY_LY_FSC,PRM_TO_SLS_WGT_TY_FSC,PRM_TO_SLS_WGT_LY_FSC,PRM_TO_SLS_MKDN_AMT_TY_FSC,PRM_TO_SLS_MKDN_AMT_LY_FSC,CLRNC_SLS_AMT_TY_FSC,CLRNC_SLS_AMT_LY_FSC,CLRNC_QTY_TY_FSC,CLRNC_QTY_LY_FSC,CLRNC_WGT_TY_FSC,CLRNC_WGT_LY_FSC,CLRNC_MKDN_AMT_TY_FSC,CLRNC_MKDN_AMT_LY_FSC,COGS_AMT_TY_FSC,COGS_AMT_LY_FSC,COGS_FNC_CHRG_AMT_TY_FSC,COGS_FNC_CHRG_AMT_LY_FSC,COGS_FNC_CR_AMT_TY_FSC,COGS_FNC_CR_AMT_LY_FSC,COGS_STRGE_CHRG_AMT_TY_FSC,COGS_STRGE_CHRG_AMT_LY_FSC,DRCT_MGN_AMT_TY_FSC,DRCT_MGN_AMT_LY_FSC,PRM_TO_SLS_DRCT_MGN_AMT_TY_FSC,PRM_TO_SLS_DRCT_MGN_AMT_LY_FSC,CLRNC_DRCT_MGN_AMT_TY_FSC,CLRNC_DRCT_MGN_AMT_LY_FSC,COGS_FRT_AMT_TY_FSC,COGS_FRT_AMT_LY_FSC,COGS_DIST_CNTR_HNDL_AMT_TY_FSC,COGS_DIST_CNTR_HNDL_AMT_LY_FSC)
SELECT
b.ROW_ID_VAL,
--b.CLN_TYP_ID,
1,
b.PRE_DEF_ID,
b.DT_VAL,
c.GEO_HIST_SK,
--b.PROD_SK,
-1,
b.ITM_SKU,
b.STR_ID,
case when a.Mjr_Pkg_Ut_Qty_TY_FSC is null then G.Mjr_Pkg_Ut_Qty_TY_FSC
else a.Mjr_Pkg_Ut_Qty_TY_FSC end as  Mjr_Pkg_Ut_Qty_TY_FSC ,
case when a.Mjr_Pkg_Ut_Qty_LY_FSC is null then G.Mjr_Pkg_Ut_Qty_LY_FSC
else a.Mjr_Pkg_Ut_Qty_LY_FSC end as  Mjr_Pkg_Ut_Qty_LY_FSC ,
case when a.Itm_Grss_Cst_Amt_TY_FSC is null then G.Itm_Grss_Cst_Amt_TY_FSC
else a.Itm_Grss_Cst_Amt_TY_FSC end as  Itm_Grss_Cst_Amt_TY_FSC ,
case when a.Itm_Grss_Cst_Amt_LY_FSC is null then G.Itm_Grss_Cst_Amt_LY_FSC
else a.Itm_Grss_Cst_Amt_LY_FSC end as  Itm_Grss_Cst_Amt_LY_FSC ,
case when a.Itm_Net_Cst_Amt_TY_FSC is null then G.Itm_Net_Cst_Amt_TY_FSC
else a.Itm_Net_Cst_Amt_TY_FSC end as  Itm_Net_Cst_Amt_TY_FSC ,
case when a.Itm_Net_Cst_Amt_LY_FSC is null then G.Itm_Net_Cst_Amt_LY_FSC
else a.Itm_Net_Cst_Amt_LY_FSC end as  Itm_Net_Cst_Amt_LY_FSC ,
case when a.Itm_Chrg_Ea_Amt_TY_FSC is null then G.Itm_Chrg_Ea_Amt_TY_FSC
else a.Itm_Chrg_Ea_Amt_TY_FSC end as  Itm_Chrg_Ea_Amt_TY_FSC ,
case when a.Itm_Chrg_Ea_Amt_LY_FSC is null then G.Itm_Chrg_Ea_Amt_LY_FSC
else a.Itm_Chrg_Ea_Amt_LY_FSC end as  Itm_Chrg_Ea_Amt_LY_FSC ,
case when a.Itm_Alw_Ea_Amt_TY_FSC is null then G.Itm_Alw_Ea_Amt_TY_FSC
else a.Itm_Alw_Ea_Amt_TY_FSC end as  Itm_Alw_Ea_Amt_TY_FSC ,
case when a.Itm_Alw_Ea_Amt_LY_FSC is null then G.Itm_Alw_Ea_Amt_LY_FSC
else a.Itm_Alw_Ea_Amt_LY_FSC end as  Itm_Alw_Ea_Amt_LY_FSC ,
case when a.Po_Chrg_Ea_Amt_TY_FSC is null then G.Po_Chrg_Ea_Amt_TY_FSC
else a.Po_Chrg_Ea_Amt_TY_FSC end as  Po_Chrg_Ea_Amt_TY_FSC ,
case when a.Po_Chrg_Ea_Amt_LY_FSC is null then G.Po_Chrg_Ea_Amt_LY_FSC
else a.Po_Chrg_Ea_Amt_LY_FSC end as  Po_Chrg_Ea_Amt_LY_FSC ,
case when a.Po_Alw_Ea_Amt_TY_FSC is null then G.Po_Alw_Ea_Amt_TY_FSC
else a.Po_Alw_Ea_Amt_TY_FSC end as  Po_Alw_Ea_Amt_TY_FSC ,
case when a.Po_Alw_Ea_Amt_LY_FSC is null then G.Po_Alw_Ea_Amt_LY_FSC
else a.Po_Alw_Ea_Amt_LY_FSC end as  Po_Alw_Ea_Amt_LY_FSC ,
case when a.Csh_Dct_Ea_Amt_TY_FSC is null then G.Csh_Dct_Ea_Amt_TY_FSC
else a.Csh_Dct_Ea_Amt_TY_FSC end as  Csh_Dct_Ea_Amt_TY_FSC ,
case when a.Csh_Dct_Ea_Amt_LY_FSC is null then G.Csh_Dct_Ea_Amt_LY_FSC
else a.Csh_Dct_Ea_Amt_LY_FSC end as  Csh_Dct_Ea_Amt_LY_FSC ,
case when a.Frt_Chrg_Ea_Amt_TY_FSC is null then G.Frt_Chrg_Ea_Amt_TY_FSC
else a.Frt_Chrg_Ea_Amt_TY_FSC end as  Frt_Chrg_Ea_Amt_TY_FSC ,
case when a.Frt_Chrg_Ea_Amt_LY_FSC is null then G.Frt_Chrg_Ea_Amt_LY_FSC
else a.Frt_Chrg_Ea_Amt_LY_FSC end as  Frt_Chrg_Ea_Amt_LY_FSC ,
case when a.Frt_Alw_Ea_Amt_TY_FSC is null then G.Frt_Alw_Ea_Amt_TY_FSC
else a.Frt_Alw_Ea_Amt_TY_FSC end as  Frt_Alw_Ea_Amt_TY_FSC ,
case when a.Frt_Alw_Ea_Amt_LY_FSC is null then G.Frt_Alw_Ea_Amt_LY_FSC
else a.Frt_Alw_Ea_Amt_LY_FSC end as  Frt_Alw_Ea_Amt_LY_FSC ,
case when a.Frt_Unld_Chrg_Amt_TY_FSC is null then G.Frt_Unld_Chrg_Amt_TY_FSC
else a.Frt_Unld_Chrg_Amt_TY_FSC end as  Frt_Unld_Chrg_Amt_TY_FSC ,
case when a.Frt_Unld_Chrg_Amt_LY_FSC is null then G.Frt_Unld_Chrg_Amt_LY_FSC
else a.Frt_Unld_Chrg_Amt_LY_FSC end as  Frt_Unld_Chrg_Amt_LY_FSC ,
case when a.Ipt_Ld_Chrg_Amt_TY_FSC is null then G.Ipt_Ld_Chrg_Amt_TY_FSC
else a.Ipt_Ld_Chrg_Amt_TY_FSC end as  Ipt_Ld_Chrg_Amt_TY_FSC ,
case when a.Ipt_Ld_Chrg_Amt_LY_FSC is null then G.Ipt_Ld_Chrg_Amt_LY_FSC
else a.Ipt_Ld_Chrg_Amt_LY_FSC end as  Ipt_Ld_Chrg_Amt_LY_FSC ,
case when a.Bkh_Frt_Chrg_Amt_TY_FSC is null then G.Bkh_Frt_Chrg_Amt_TY_FSC
else a.Bkh_Frt_Chrg_Amt_TY_FSC end as  Bkh_Frt_Chrg_Amt_TY_FSC ,
case when a.Bkh_Frt_Chrg_Amt_LY_FSC is null then G.Bkh_Frt_Chrg_Amt_LY_FSC
else a.Bkh_Frt_Chrg_Amt_LY_FSC end as  Bkh_Frt_Chrg_Amt_LY_FSC ,
case when a.Extd_Bkh_Frt_Chrg_Amt_TY_FSC is null then G.Extd_Bkh_Frt_Chrg_Amt_TY_FSC
else a.Extd_Bkh_Frt_Chrg_Amt_TY_FSC end as  Extd_Bkh_Frt_Chrg_Amt_TY_FSC ,
case when a.Extd_Bkh_Frt_Chrg_Amt_LY_FSC is null then G.Extd_Bkh_Frt_Chrg_Amt_LY_FSC
else a.Extd_Bkh_Frt_Chrg_Amt_LY_FSC end as  Extd_Bkh_Frt_Chrg_Amt_LY_FSC ,
case when a.CostOfGoodsReductionAmount_TY_FSC is null then G.Cst_Gds_Rdc_Amt_Ty_Fsc
else a.CostOfGoodsReductionAmount_TY_FSC end as  Cst_Gds_Rdc_Amt_Ty_Fsc ,
case when a.CostOfGoodsReductionAmount_LY_FSC is null then G.Cst_Gds_Rdc_Amt_Ly_Fsc
else a.CostOfGoodsReductionAmount_LY_FSC end as  Cst_Gds_Rdc_Amt_Ly_Fsc ,
case when a.SwellAndDefectiveAlw_TY_FSC is null then G.Swl_Dft_Alw_Ty_Fsc
else a.SwellAndDefectiveAlw_TY_FSC end as  Swl_Dft_Alw_Ty_Fsc ,
case when a.SwellAndDefectiveAlw_LY_FSC is null then G.Swl_Dft_Alw_Ly_Fsc
else a.SwellAndDefectiveAlw_LY_FSC end as  Swl_Dft_Alw_Ly_Fsc,
b.SLS_AMT_TY_FSC,
b.SLS_AMT_LY_FSC,
b.SLS_QTY_TY_FSC,
b.SLS_QTY_LY_FSC,
b.SLS_WGT_TY_FSC,
b.SLS_WGT_LY_FSC,
COALESCE(NULLIF(b.SLS_WGT_TY_FSC,0),b.SLS_QTY_TY_FSC) AS SLS_UOM_QTY_TY,
COALESCE(NULLIF(b.SLS_WGT_LY_FSC,0),b.SLS_QTY_LY_FSC) AS SLS_UOM_QTY_LY,
b.PRM_TO_SLS_AMT_TY_FSC,
b.PRM_TO_SLS_AMT_LY_FSC,
b.PRM_TO_SLS_QTY_TY_FSC,
b.PRM_TO_SLS_QTY_LY_FSC,
b.PRM_TO_SLS_WGT_TY_FSC,
b.PRM_TO_SLS_WGT_LY_FSC,
b.PRM_TO_SLS_MKDN_AMT_TY_FSC,
b.PRM_TO_SLS_MKDN_AMT_LY_FSC,
b.CLRNC_SLS_AMT_TY_FSC,
b.CLRNC_SLS_AMT_LY_FSC,
b.CLRNC_QTY_TY_FSC,
b.CLRNC_QTY_LY_FSC,
b.CLRNC_WGT_TY_FSC,
b.CLRNC_WGT_LY_FSC,
b.CLRNC_MKDN_AMT_TY_FSC,
b.CLRNC_MKDN_AMT_LY_FSC,
b.COGS_AMT_TY_FSC,
b.COGS_AMT_LY_FSC,
b.COGS_FNC_CHRG_AMT_TY_FSC,
b.COGS_FNC_CHRG_AMT_LY_FSC,
b.COGS_FNC_CR_AMT_TY_FSC,
b.COGS_FNC_CR_AMT_LY_FSC,
b.COGS_STRGE_CHRG_AMT_TY_FSC,
b.COGS_STRGE_CHRG_AMT_LY_FSC,
b.DRCT_MGN_AMT_TY_FSC,
b.DRCT_MGN_AMT_LY_FSC,
b.PRM_TO_SLS_DRCT_MGN_AMT_TY_FSC,
b.PRM_TO_SLS_DRCT_MGN_AMT_LY_FSC,
b.CLRNC_DRCT_MGN_AMT_TY_FSC,
b.CLRNC_DRCT_MGN_AMT_LY_FSC,
b.COGS_FRT_AMT_TY_FSC,
b.COGS_FRT_AMT_LY_FSC,
b.COGS_DIST_CNTR_HNDL_AMT_TY_FSC,
b.COGS_DIST_CNTR_HNDL_AMT_LY_FSC
FROM [EDAA_PRES].[FCT_DLY_STR_ITMSKU_SLS_AGG] b
INNER JOIN EDAA_DW.DIM_GEO c
       ON b.STR_ID = c.STR_ID and c.Is_Curr_ind = 1
LEFT JOIN [EDAA_PRES].[FCT_COST_MPRS_ITMSKU_AGG] a
                      ON a.PRE_DEF_ID = b.PRE_DEF_ID
                      AND a.DT_VAL = b.DT_VAL
                      AND a.ITM_SKU = b.ITM_SKU
					  AND a.GEO_HIST_SK = c.GEO_HIST_SK
LEFT JOIN CTE_AVG AS G ON B.ITM_SKU=G.ITM_SKU AND B.PRE_DEF_ID=G.PRE_DEF_ID
WHERE A.GEO_HIST_SK IS NULL AND a.DT_VAL IS NULL AND a.ITM_SKU IS NULL AND a.PRE_DEF_ID IS NULL




 --UPDATE STATISTICS
 UPDATE STATISTICS [EDAA_PRES].[FCT_DLY_STR_ITMSKU_MPRS_COSTRCV_SLS_AGG];


END TRY

BEGIN CATCH
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = 0, @NBR_OF_RW_UDT  = 0
SET @ERROR_PROCEDURE_NAME = 'EDAA_ETL.PROC_FCT_DLY_COST_MPRS_SLS_AGG_LOAD'
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
