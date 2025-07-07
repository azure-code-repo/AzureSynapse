CREATE PROC [EDAA_ETL].[SP_REF_DW_DIM_PROD_TABLELOAD] AS

/*
 =============================================
 Author:        Krishna Kumar Shaw
 Create date:   20-Apr-2021
 Description:   Stored proc truncate and load Hourly Product dimension table.
 =============================================
 Change History
 20-Apr-2021   KKS      Initial version
 27-Jan-2021   AK       SKU_CT column added

*/


BEGIN TRY

SET NOCOUNT ON
SET XACT_ABORT ON

/*Declare Auditing variables*/
DECLARE @NEW_AUD_SKY BIGINT
DECLARE @NBR_OF_RW_ISRT INT
DECLARE @NBR_OF_RW_UDT INT
DECLARE @EXEC_LYR VARCHAR(255)
DECLARE @EXEC_JOB VARCHAR(500)
DECLARE @SRC_ENTY VARCHAR(500)
DECLARE @TGT_ENTY VARCHAR(500)

/*Audit Log Start*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START
 @EXEC_LYR  = 'EDAA_DW'
,@EXEC_JOB  = 'SP_REF_DW_DIM_PROD_TABLELOAD'
,@SRC_ENTY  = 'DIM_PROD'
,@TGT_ENTY =  'DIM_PROD'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT


--SCD Type 1 change in Target table
 UPDATE EDAA_DW.DIM_PROD
  SET
		 Prod_Hist_Sk = stg.Prod_Hist_Sk
		,Prod_Sk = stg.Prod_Sk
		,Itm_Sku = stg.Itm_Sku
		,Itm_Nm = stg.Itm_Nm
		,Lvl1_Prod_Id = stg.Lvl1_Prod_Id
		,Lvl1_Prod_Desc = stg.Lvl1_Prod_Desc
		,Lvl2_Prod_Clsfctn_Id = stg.Lvl2_Prod_Clsfctn_Id
		,Lvl2_Prod_Clsfctn_Desc = stg.Lvl2_Prod_Clsfctn_Desc
		,Lvl3_Prod_Sub_Ctgry_Id = stg.Lvl3_Prod_Sub_Ctgry_Id
		,Lvl3_Prod_Sub_Ctgry_Desc = stg.Lvl3_Prod_Sub_Ctgry_Desc
		,Lvl4_Prod_Ctgry_Id = stg.Lvl4_Prod_Ctgry_Id
		,Lvl4_Prod_Ctgry_Desc = stg.Lvl4_Prod_Ctgry_Desc
		,Lvl5_Bsns_Sgmt_Id = stg.Lvl5_Bsns_Sgmt_Id
		,Lvl5_Bsns_Sgmt_Desc = stg.Lvl5_Bsns_Sgmt_Desc
		,Lvl6_MDS_Area_Id = stg.Lvl6_MDS_Area_Id
		,Lvl6_MDS_Area_Desc = stg.Lvl6_MDS_Area_Desc
		,Fnc_Lvl1_Prod_Ctgry_Id = stg.Fnc_Lvl1_Prod_Ctgry_Id
		,Fnc_Lvl1_Prod_Ctgry_Desc = stg.Fnc_Lvl1_Prod_Ctgry_Desc
		,Fnc_Lvl2_Mprs_Ctgry_Id = stg.Fnc_Lvl2_Mprs_Ctgry_Id
		,Fnc_Lvl2_Mprs_Ctgry_Desc = stg.Fnc_Lvl2_Mprs_Ctgry_Desc
		,Fnc_Lvl3_Pky_Id = stg.Fnc_Lvl3_Pky_Id
		,Fnc_Lvl3_Pky_Desc = stg.Fnc_Lvl3_Pky_Desc
		,Fnc_Lvl4_Area_Id = stg.Fnc_Lvl4_Area_Id
		,Fnc_Lvl4_Area_Desc = stg.Fnc_Lvl4_Area_Desc
		,Str_Area_Dtl_Id = stg.Str_Area_Dtl_Id
		,Str_Area_Dtl_Desc = stg.Str_Area_Dtl_Desc
		,Str_Area_Summ_Id = stg.Str_Area_Summ_Id
		,Str_Area_Summ_Desc = stg.Str_Area_Summ_Desc
		,Str_Dept_Dtl_Id = stg.Str_Dept_Dtl_Id
		,Str_Dept_Dtl_Desc = stg.Str_Dept_Dtl_Desc
		,Str_Dept_Summ_Id = stg.Str_Dept_Summ_Id
		,Str_Dept_Summ_Desc = stg.Str_Dept_Summ_Desc
		,Vld_Frm = stg.Vld_Frm
		,Vld_To = stg.Vld_To
		,Is_Curr_Ind = stg.Is_Curr_Ind
		,Is_Dmy_Ind = stg.Is_Dmy_Ind
		,Is_Emb_Ind = stg.Is_Emb_Ind
		,Etl_Actn = stg.Etl_Actn
		,Aud_Upd_sk = @NEW_AUD_SKY

		--SKU_CT added - Missing attribute
		,Sku_Ct = stg.Sku_Ct

FROM EDAA_REF.DIM_PROD stg
INNER JOIN EDAA_DW.DIM_PROD tgt
on  stg.Prod_Hist_Sk =tgt.Prod_Hist_Sk
and stg.Prod_Sk = tgt.Prod_Sk
where  stg.row_stat_cd = 'SCD1'

--SCD type 2 change
-- close current row in Target
 UPDATE EDAA_DW.DIM_PROD
  SET
		Vld_To = DATEADD(day,-2,cast((SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time') as date))
		,Is_Curr_Ind = 0
		,Etl_Actn = 'SCD 2 update Record'
		,Aud_Upd_sk = @NEW_AUD_SKY
FROM EDAA_REF.DIM_PROD stg
INNER JOIN EDAA_DW.DIM_PROD tgt
on    stg.Prod_Sk = tgt.Prod_Sk
and   tgt.Is_Curr_Ind = 1
where  stg.row_stat_cd = 'SCD2'

--Insert updated SCD type 2 row in Target

INSERT INTO EDAA_DW.DIM_PROD
SELECT
		 Prod_Hist_Sk
		,Prod_Sk
		,Itm_Sku
		,Itm_Nm
		,Lvl1_Prod_Id
		,Lvl1_Prod_Desc
		,Lvl2_Prod_Clsfctn_Id
		,Lvl2_Prod_Clsfctn_Desc
		,Lvl3_Prod_Sub_Ctgry_Id
		,Lvl3_Prod_Sub_Ctgry_Desc
		,Lvl4_Prod_Ctgry_Id
		,Lvl4_Prod_Ctgry_Desc
		,Lvl5_Bsns_Sgmt_Id
		,Lvl5_Bsns_Sgmt_Desc
		,Lvl6_MDS_Area_Id
		,Lvl6_MDS_Area_Desc
		,Fnc_Lvl1_Prod_Ctgry_Id
		,Fnc_Lvl1_Prod_Ctgry_Desc
		,Fnc_Lvl2_Mprs_Ctgry_Id
		,Fnc_Lvl2_Mprs_Ctgry_Desc
		,Fnc_Lvl3_Pky_Id
		,Fnc_Lvl3_Pky_Desc
		,Fnc_Lvl4_Area_Id
		,Fnc_Lvl4_Area_Desc
		,Str_Area_Dtl_Id
		,Str_Area_Dtl_Desc
		,Str_Area_Summ_Id
		,Str_Area_Summ_Desc
		,Str_Dept_Dtl_Id
		,Str_Dept_Dtl_Desc
		,Str_Dept_Summ_Id
		,Str_Dept_Summ_Desc
		,DATEADD(day,-1,cast((SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time') as date)) as Vld_Frm
		,Vld_To
		,Is_Curr_Ind
		,Is_Dmy_Ind
		,Is_Emb_Ind
		,Etl_Actn
		,@NEW_AUD_SKY as Aud_Ins_sk
		,NULL as Aud_Upd_sk
		-- Added for new attributes
		,Itm_Prim_Ind
        ,Prod_Size_Desc
        ,Brnd_Nm
        ,Brnd_Desc
        ,Brnd_Ctgry
        ,Prod_Desc
        ,Prod_Desc_Note
        ,Outdate
        ,Byr_Id
        ,Prod_Sts
        ,Prod_Sts_Note
        ,Vdr_Id
		-- SKU_CT added - Missing attribute
		,Sku_Ct

	FROM EDAA_REF.DIM_PROD a where a.row_stat_cd = 'SCD2'

--insert new rows in Target
INSERT INTO EDAA_DW.DIM_PROD
SELECT
		 Prod_Hist_Sk
		,Prod_Sk
		,Itm_Sku
		,Itm_Nm
		,Lvl1_Prod_Id
		,Lvl1_Prod_Desc
		,Lvl2_Prod_Clsfctn_Id
		,Lvl2_Prod_Clsfctn_Desc
		,Lvl3_Prod_Sub_Ctgry_Id
		,Lvl3_Prod_Sub_Ctgry_Desc
		,Lvl4_Prod_Ctgry_Id
		,Lvl4_Prod_Ctgry_Desc
		,Lvl5_Bsns_Sgmt_Id
		,Lvl5_Bsns_Sgmt_Desc
		,Lvl6_MDS_Area_Id
		,Lvl6_MDS_Area_Desc
		,Fnc_Lvl1_Prod_Ctgry_Id
		,Fnc_Lvl1_Prod_Ctgry_Desc
		,Fnc_Lvl2_Mprs_Ctgry_Id
		,Fnc_Lvl2_Mprs_Ctgry_Desc
		,Fnc_Lvl3_Pky_Id
		,Fnc_Lvl3_Pky_Desc
		,Fnc_Lvl4_Area_Id
		,Fnc_Lvl4_Area_Desc
		,Str_Area_Dtl_Id
		,Str_Area_Dtl_Desc
		,Str_Area_Summ_Id
		,Str_Area_Summ_Desc
		,Str_Dept_Dtl_Id
		,Str_Dept_Dtl_Desc
		,Str_Dept_Summ_Id
		,Str_Dept_Summ_Desc
		,Vld_Frm
		,Vld_To
		,Is_Curr_Ind
		,Is_Dmy_Ind
		,Is_Emb_Ind
		,Etl_Actn
		,@NEW_AUD_SKY as Aud_Ins_sk
		,NULL as Aud_Upd_sk
		,Itm_Prim_Ind
        ,Prod_Size_Desc
        ,Brnd_Nm
        ,Brnd_Desc
        ,Brnd_Ctgry
        ,Prod_Desc
        ,Prod_Desc_Note
        ,Outdate
        ,Byr_Id
        ,Prod_Sts
        ,Prod_Sts_Note
        ,Vdr_Id
		-- SKU_CT added - Missing attribute
		,Sku_Ct

	FROM EDAA_REF.DIM_PROD a where a.row_stat_cd = 'INS'

BEGIN
SELECT @NBR_OF_RW_ISRT = COUNT(1)  FROM EDAA_DW.DIM_PROD WHERE Aud_Ins_Sk = @NEW_AUD_SKY
SELECT @NBR_OF_RW_UDT  = COUNT(1)  FROM EDAA_DW.DIM_PROD WHERE Aud_Upd_Sk = @NEW_AUD_SKY

------------ UPDATE Statistics-------

UPDATE STATISTICS  EDAA_DW.DIM_PROD;

END

/*Audit Log End*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT  = @NBR_OF_RW_UDT

END TRY

BEGIN CATCH
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = 'EDAA_ETL.SP_REF_DW_DIM_PROD_TABLELOAD'
DECLARE @ERROR_LINE AS INT;
DECLARE @ERROR_MSG AS NVARCHAR(max);

 SELECT
      @ERROR_LINE =  ERROR_NUMBER()
       ,@ERROR_MSG = ERROR_MESSAGE();
--------- Log execution error ----------



EXEC EDAA_CNTL.SP_LOG_AUD_ERR
@AUD_SKY = @NEW_AUD_SKY,
@ERROR_PROCEDURE_NAME = @ERROR_PROCEDURE_NAME,
@ERROR_LINE = @ERROR_LINE,
@ERROR_MSG = @ERROR_MSG;

-- Detect the change


   THROW;




END CATCH
