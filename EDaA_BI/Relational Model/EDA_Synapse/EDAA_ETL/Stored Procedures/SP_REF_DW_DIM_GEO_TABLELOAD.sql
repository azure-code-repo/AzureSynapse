CREATE PROC [EDAA_ETL].[SP_REF_DW_DIM_GEO_TABLELOAD] AS

/*
 =============================================
 Author:        Mohammad Shaik
 Create date:   20-Mar-2021
 Description:   This SP loads the Geographical data into "EDAA_DW.DIM_GEO" Dimension table
                in which holding the corresponding data of Store,Location,StoreType,
                MeijerDivision,Territory etc. Also holding SCD Type1 and Type2 changes as per
                the transformation logic. It will update the stats of the table after insert
 =============================================
 Change History
 20-Mar-2021    MS      Initial Version
 15-Apr-2021    MS        Updated the SCD Type1  and SCD Type2 changes.

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
 @EXEC_LYR  = 'EDAA_DW'
,@EXEC_JOB  = 'SP_REF_DW_DIM_GEO_TABLELOAD'
,@SRC_ENTY  = 'DIM_GEO'
,@TGT_ENTY = 'DIM_GEO'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

-------------------------------------------------------

--SCD Type 1 change in Target
 UPDATE EDAA_DW.DIM_GEO
  SET
		Geo_Hist_Sk=stg.Geo_Hist_Sk
		,Geo_Sk=stg.Geo_Sk
		,Str_Id=stg.Str_Id
		,Str_Nm=stg.Str_Nm
		,Gross_Flr_Area=stg.Gross_Flr_Area
		,Rgn_Nm=stg.Rgn_Nm
		,Mkt_Nm=stg.Mkt_Nm
		,Loc_Lngtd=stg.Loc_Lngtd
		,Loc_Lttd=stg.Loc_Lttd
		,Str_Cls_Id=stg.Str_Cls_Id
		,Str_Cls_Nm=stg.Str_Cls_Nm
		,Loc_St_Id=stg.Loc_St_Id
		,Loc_Cnty_Id=stg.Loc_Cnty_Id
		,Loc_Cty=stg.Loc_Cty
		,Loc_Zp_Code=stg.Loc_Zp_Code
		,Div_Id=stg.Div_Id
		,Div_Nm=stg.Div_Nm
		,Curr_Yr_New_Str_Ind=stg.Curr_Yr_New_Str_Ind
		,Lst_Yr_New_Str_Ind=stg.Lst_Yr_New_Str_Ind
		,Two_Yrs_Ago_New_Str_Ind=stg.Two_Yrs_Ago_New_Str_Ind
		,Rgn_Id=stg.Rgn_Id
		,Mkt_Id=stg.Mkt_Id
		,Str_Opn_Dt=stg.Str_Opn_Dt
		,Str_Cls_Dt=stg.Str_Cls_Dt
		,Str_Ctgry_Nm=stg.Str_Ctgry_Nm
		,Str_Zn_Id=stg.Str_Zn_Id
		,Str_Zn_Nm=stg.Str_Zn_Nm
		,Str_Cmp_Ctgry_Id=stg.Str_Cmp_Ctgry_Id
		,Str_Cmp_Ctgry_Desc=stg.Str_Cmp_Ctgry_Desc
		,Vld_Frm=stg.Vld_Frm
		,Vld_To=stg.Vld_To
		,Is_Curr_Ind=stg.Is_Curr_Ind
		,Is_Dmy_Ind=stg.Is_Dmy_Ind
		,Is_Emb_Ind=stg.Is_Emb_Ind
		,Etl_Actn = stg.Etl_Actn
		,Aud_Upd_sk = @NEW_AUD_SKY
FROM EDAA_REF.DIM_GEO stg, EDAA_DW.DIM_GEO tgt
where stg.Geo_Hist_Sk =tgt.Geo_Hist_Sk
and   stg.Geo_Sk = tgt.Geo_Sk
and   stg.row_stat_cd = 'SCD1'

--------------------------------------------------------------------

--SCD type 2 change
-- close current row
 UPDATE EDAA_DW.DIM_GEO
  SET
		Vld_To = DATEADD(day,-2,cast((SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time') as date))
		,Is_Curr_Ind = 0
		,Etl_Actn = 'SCD 2 update Record'
		,Aud_Upd_sk = @NEW_AUD_SKY
FROM EDAA_REF.DIM_GEO stg, EDAA_DW.DIM_GEO tgt
where stg.Geo_Sk = tgt.Geo_Sk
and   tgt.Is_Curr_Ind = 1
and   stg.row_stat_cd = 'SCD2'

---------------------------------------------------------------------------------
--Insert updated SCD type 2 row

INSERT INTO EDAA_DW.DIM_GEO
SELECT
		Geo_Hist_Sk
		,Geo_Sk
		,Str_Id
		,Str_Nm
		,Gross_Flr_Area
		,Rgn_Nm
		,Mkt_Nm
		,Loc_Lngtd
		,Loc_Lttd
		,Str_Cls_Id
		,Str_Cls_Nm
		,Loc_St_Id
		,Loc_Cnty_Id
		,Loc_Cty
		,Loc_Zp_Code
		,Div_Id
		,Div_Nm
		,Curr_Yr_New_Str_Ind
		,Lst_Yr_New_Str_Ind
		,Two_Yrs_Ago_New_Str_Ind
		,Rgn_Id
		,Mkt_Id
		,Str_Opn_Dt
		,Str_Cls_Dt
		,Str_Ctgry_Nm
		,Str_Zn_Id
		,Str_Zn_Nm
		,Str_Cmp_Ctgry_Id
		,Str_Cmp_Ctgry_Desc
		,DATEADD(day,-1,cast((SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time') as date)) as Vld_Frm
		,Vld_To
		,Is_Curr_Ind
		,Is_Dmy_Ind
		,Is_Emb_Ind
		,Etl_Actn
		,@NEW_AUD_SKY as Aud_Ins_sk
		,NULL as Aud_Upd_sk
	FROM EDAA_REF.DIM_GEO a where a.row_stat_cd = 'SCD2'
-----------------------------------------------------------------------------------

--insert new rows
INSERT INTO EDAA_DW.DIM_GEO
SELECT
		 Geo_Hist_Sk
		,Geo_Sk
		,Str_Id
		,Str_Nm
		,Gross_Flr_Area
		,Rgn_Nm
		,Mkt_Nm
		,Loc_Lngtd
		,Loc_Lttd
		,Str_Cls_Id
		,Str_Cls_Nm
		,Loc_St_Id
		,Loc_Cnty_Id
		,Loc_Cty
		,Loc_Zp_Code
		,Div_Id
		,Div_Nm
		,Curr_Yr_New_Str_Ind
		,Lst_Yr_New_Str_Ind
		,Two_Yrs_Ago_New_Str_Ind
		,Rgn_Id
		,Mkt_Id
		,Str_Opn_Dt
		,Str_Cls_Dt
		,Str_Ctgry_Nm
		,Str_Zn_Id
		,Str_Zn_Nm
		,Str_Cmp_Ctgry_Id
		,Str_Cmp_Ctgry_Desc
		,Vld_Frm
		,Vld_To
		,Is_Curr_Ind
		,Is_Dmy_Ind
		,Is_Emb_Ind
		,Etl_Actn
		,@NEW_AUD_SKY as Aud_Ins_sk
		,NULL as Aud_Upd_sk
	FROM EDAA_REF.DIM_GEO a where a.row_stat_cd = 'INS'

/* Temporary fix for UT_ID=320, 22-12-2020 */
--UPDATE EDAA_DW.dim_geo SET UT_LNGT='-80.707372',UT_LAT='41.026778' where ut_id=320

BEGIN

SELECT @NBR_OF_RW_ISRT = COUNT(1)  FROM EDAA_DW.DIM_GEO WHERE [Aud_Ins_Sk] = @NEW_AUD_SKY
SELECT @NBR_OF_RW_UDT  = COUNT(1)  FROM EDAA_DW.DIM_GEO WHERE [Aud_Upd_Sk] = @NEW_AUD_SKY

------------ UPDATE Statistics-------

UPDATE STATISTICS  EDAA_DW.DIM_GEO;
END

/*Audit Log End*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT  = @NBR_OF_RW_UDT

END TRY
BEGIN CATCH
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = 'EDAA_ETL.SP_REF_DW_DIM_GEO_TABLELOAD'
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
