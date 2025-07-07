CREATE PROC [EDAA_ETL].[SP_REF_PRES_DIM_BYR_TABLELOAD] AS

/*
 =============================================
 Author:        Arindam Pathak
 Create date:   20-Apr-2021
 Description:   Stored proc truncate and load Hourly Buyer Dimesnion table Load in PRES Layer.
 =============================================
 Change History
 20-Apr-2021   AP      Initial version

*/


BEGIN TRY

SET NOCOUNT ON
SET XACT_ABORT ON

/*Datamart Auditing variables*/
DECLARE @NEW_AUD_SKY BIGINT
DECLARE @NBR_OF_RW_ISRT INT
DECLARE @NBR_OF_RW_UDT INT
DECLARE @EXEC_LYR VARCHAR(255)
DECLARE @EXEC_JOB VARCHAR(500)
DECLARE @SRC_ENTY VARCHAR(500)
DECLARE @TGT_ENTY VARCHAR(500)

/*Audit Log Start*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START
 @EXEC_LYR  = 'EDAA_PRES'
,@EXEC_JOB  = 'SP_REF_PRES_DIM_BYR_TABLELOAD'
,@SRC_ENTY  = 'EDAA_REF.DIM_BYR'
,@TGT_ENTY = 'DIM_BYR'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT
;




--SCD Type 1 change in Target table
 UPDATE EDAA_PRES.DIM_BYR
  SET

		 Byr_Id = stg.Byr_Id
		,lvl3_Prod_Sub_Ctgry_Id = stg.lvl3_Prod_Sub_Ctgry_Id
		,lvl3_Prod_Sub_Ctgry_Desc = stg.lvl3_Prod_Sub_Ctgry_Desc
		,Grp_Vp_Nm = stg.Grp_Vp_Nm
		,Mngr_Nm = stg.Mngr_Nm
		,Vp_Nm = stg.Vp_Nm
		,Byr_Nm= ISNULL(stg.Byr_Nm,'n/a')
		,Vld_Frm = stg.Vld_Frm
		,Vld_To = stg.Vld_To
		,Is_Curr_Ind = stg.Is_Curr_Ind
		,Is_Dmy_Ind = stg.Is_Dmy_Ind
		,Is_Emb_Ind = stg.Is_Emb_Ind
		,Etl_Actn = stg.Etl_Actn
		,Aud_Upd_sk = @NEW_AUD_SKY

		FROM EDAA_REF.DIM_BYR stg
		INNER JOIN EDAA_PRES.DIM_BYR tgt
		on  stg.Byr_L3_Hist_Sk =tgt.Byr_L3_Hist_Sk
		and stg.Byr_L3_Sk = tgt.Byr_L3_Sk
		where  stg.row_stat_cd = 'SCD1'

--SCD type 2 change
-- close current row in Target
 UPDATE EDAA_PRES.DIM_BYR
  SET
		Vld_To = DATEADD(day,-2,cast((SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time') as date))
		,Is_Curr_Ind = 0
		,Etl_Actn = 'SCD 2 update Record'
		,Aud_Upd_sk = @NEW_AUD_SKY
		FROM EDAA_REF.DIM_BYR stg
		INNER JOIN EDAA_PRES.DIM_BYR tgt
		ON stg.Byr_L3_Sk = tgt.Byr_L3_Sk
and   tgt.Is_Curr_Ind = 1
where  stg.row_stat_cd = 'SCD2'


--Insert updated SCD type 2 row in Target
INSERT INTO EDAA_PRES.DIM_BYR
SELECT
		Byr_L3_Hist_Sk
		,Byr_L3_Sk
		,Byr_Id
		,lvl3_Prod_Sub_Ctgry_Id
		,lvl3_Prod_Sub_Ctgry_Desc
		,Grp_Vp_Nm
		,Mngr_Nm
		,Vp_Nm
		,ISNULL(Byr_Nm,'n/a') as Byr_Nm
		,DATEADD(day,-1,cast((SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time') as date)) as Vld_Frm
		,Vld_To
		,Is_Curr_Ind
		,Is_Dmy_Ind
		,Is_Emb_Ind
		,Etl_Actn
		,@NEW_AUD_SKY as Aud_Ins_sk
		,NULL as Aud_Upd_sk
	FROM EDAA_REF.DIM_BYR a where a.row_stat_cd = 'SCD2'


--Insert  new rows in Target
INSERT INTO EDAA_PRES.DIM_BYR
SELECT
		Byr_L3_Hist_Sk
		,Byr_L3_Sk
		,Byr_Id
		,lvl3_Prod_Sub_Ctgry_Id
		,lvl3_Prod_Sub_Ctgry_Desc
		,Grp_Vp_Nm
		,Mngr_Nm
		,Vp_Nm
		,ISNULL(Byr_Nm,'n/a') as Byr_Nm
		,Vld_Frm
		,Vld_To
		,Is_Curr_Ind
		,Is_Dmy_Ind
		,Is_Emb_Ind
		,Etl_Actn
		,@NEW_AUD_SKY as Aud_Ins_sk
		,NULL as Aud_Upd_sk
	FROM EDAA_REF.DIM_BYR a where a.row_stat_cd = 'INS'



BEGIN
SELECT @NBR_OF_RW_ISRT = COUNT(1)  FROM EDAA_PRES.DIM_BYR WHERE Aud_Ins_Sk = @NEW_AUD_SKY
SELECT @NBR_OF_RW_UDT  = COUNT(1)  FROM EDAA_PRES.DIM_BYR WHERE Aud_Upd_Sk = @NEW_AUD_SKY

------------ UPDATE Statistics-------

UPDATE STATISTICS EDAA_PRES.DIM_BYR;

END

/*Audit Log End*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT  = @NBR_OF_RW_UDT

END TRY

BEGIN CATCH
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = 'EDAA_ETL.SP_REF_PRES_DIM_BYR_TABLELOAD'
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
