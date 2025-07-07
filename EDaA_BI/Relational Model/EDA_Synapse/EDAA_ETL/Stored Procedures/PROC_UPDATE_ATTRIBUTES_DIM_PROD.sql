/****** Object:  StoredProcedure [EDAA_ETL].[PROC_UPDATE_ATTRIBUTES_DIM_PROD]    Script Date: 10/25/2021 9:17:29 AM ******/
CREATE PROC [EDAA_ETL].[PROC_UPDATE_ATTRIBUTES_DIM_PROD] AS
begin try
DECLARE @NEW_AUD_SKY BIGINT
DECLARE @NBR_OF_RW_ISRT INT
DECLARE @NBR_OF_RW_UDT INT
DECLARE @EXEC_LYR VARCHAR(255)
DECLARE @EXEC_JOB VARCHAR(500)
DECLARE @SRC_ENTY VARCHAR(500)
DECLARE @TGT_ENTY VARCHAR(500)

EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START
 @EXEC_LYR  = 'EDAA_DW'
,@EXEC_JOB  = 'PROC_UPDATE_ATTRIBUTES_DIM_PROD'
,@SRC_ENTY  = 'DIM_PROD_ATTRIBUTE'
,@TGT_ENTY = 'DIM_PROD'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT
;
update  [EDAA_DW].[DIM_PROD] set

	Itm_Prim_Ind = pa.Itm_Prim_Ind,
	Prod_Size_Desc = pa.Prod_Size_Desc,
	Brnd_Nm = pa.Brnd_Nm,
	Brnd_Desc = pa.Brnd_Desc,
	Brnd_Ctgry = pa.Brnd_Ctgry,
	Prod_Desc = pa.Prod_Desc,
	Prod_Desc_Note = pa.Prod_Desc_Note,
	Outdate = pa.Outdate,
	Byr_Id = pa.Byr_Id,
	Prod_Sts = pa.Prod_Sts,
	Prod_Sts_Note = pa.Prod_Sts_Note,
	Vdr_Id = pa.Vdr_Id,
	AUD_UPD_SK = 	@NEW_AUD_SKY
from   [EDAA_DW].[DIM_PROD] p, [EDAA_STG].[DIM_PROD_ATTRIBUTES] pa
where p.itm_sku = pa.Itm_sku ;

update  [EDAA_REF].[DIM_PROD] set

	Itm_Prim_Ind = pa.Itm_Prim_Ind,
	Prod_Size_Desc = pa.Prod_Size_Desc,
	Brnd_Nm = pa.Brnd_Nm,
	Brnd_Desc = pa.Brnd_Desc,
	Brnd_Ctgry = pa.Brnd_Ctgry,
	Prod_Desc = pa.Prod_Desc,
	Prod_Desc_Note = pa.Prod_Desc_Note,
	Outdate = pa.Outdate,
	Byr_Id = pa.Byr_Id,
	Prod_Sts = pa.Prod_Sts,
	Prod_Sts_Note = pa.Prod_Sts_Note,
	Vdr_Id = pa.Vdr_Id
from   [EDAA_REF].[DIM_PROD] p, [EDAA_STG].[DIM_PROD_ATTRIBUTES] pa
where p.itm_sku = pa.Itm_sku ;


BEGIN

SELECT @NBR_OF_RW_ISRT = COUNT(1)  FROM EDAA_DW.DIM_PROD WHERE AUD_INS_SK = @NEW_AUD_SKY
SELECT @NBR_OF_RW_UDT = COUNT(1)  FROM EDAA_DW.DIM_PROD WHERE AUD_UPD_SK = @NEW_AUD_SKY
print(@NBR_OF_RW_ISRT)
print(@NBR_OF_RW_UDT)

END


/*AUDIT LOG END*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT = @NBR_OF_RW_UDT

END TRY

BEGIN CATCH
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = '[EDAA_ETL].[PROC_UPDATE_ATTRIBUTES_DIM_PROD]'
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
