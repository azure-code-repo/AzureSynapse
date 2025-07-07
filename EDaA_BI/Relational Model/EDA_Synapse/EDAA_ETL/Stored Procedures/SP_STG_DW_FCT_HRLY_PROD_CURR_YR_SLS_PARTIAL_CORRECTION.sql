CREATE PROC [EDAA_ETL].[SP_STG_DW_FCT_HRLY_PROD_CURR_YR_SLS_PARTIAL_CORRECTION] @HOUR [INT] AS

/*
 =============================================
 Author:        Shrikant Sakpal
 Create date:   30-May-2021
 Description:   Stored proc truncate and load Hourly Product Current Year Sales DW table for Partial correction.
				Input parameters is required for Hourly correction for eg "9" for 9th Hour.
 =============================================
 Change History
 30-May-2021   SS      Initial version

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
,@EXEC_JOB  = 'SP_STG_DW_FCT_HRLY_PROD_CURR_YR_SLS_PARTIAL_CORRECTION'
,@SRC_ENTY  = 'EDAA_STG.FCT_HRLY_PROD_CURR_YR_SLS'
,@TGT_ENTY = 'FCT_HRLY_PROD_CURR_YR_SLS'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

/* Validate Late arriving dimension: DIM_GEO */
PRINT('LATEARRIVINGDIMENSION: DIM_GEO')

IF OBJECT_ID('TEMPDB..#LATEARRIVINGGEODIMENSIONFACTRECORDS') IS NOT NULL
DROP TABLE #LATEARRIVINGGEODIMENSIONFACTRECORDS

CREATE TABLE #LATEARRIVINGGEODIMENSIONFACTRECORDS
WITH (DISTRIBUTION = ROUND_ROBIN)
AS
SELECT DISTINCT FACT_DATA_FEED.Str_Id,
CASE WHEN GEO.Str_Id IS NULL THEN 1 ELSE 0 END AS IS_GEO_MISSING
FROM
(
SELECT DISTINCT Str_Id
FROM  EDAA_STG.FCT_HRLY_PROD_CURR_YR_SLS
) AS FACT_DATA_FEED

LEFT  JOIN	(SELECT DISTINCT Str_Id
				FROM EDAA_DW.DIM_GEO
				---WHERE IS_CURR_IND<>0
					) AS GEO
	ON GEO.Str_Id = FACT_DATA_FEED.Str_Id
	WHERE  GEO.Str_Id IS NULL

DECLARE @NEXT_GEO_HIST_SK INT
SELECT @NEXT_GEO_HIST_SK =ISNULL(MAX(GEO_HIST_SK),0)   FROM EDAA_DW.DIM_GEO

PRINT('LATEARRIVINGDIMENSION: DIM_GEO LOAD')

IF(@NEXT_GEO_HIST_SK IS NOT NULL)
BEGIN


INSERT INTO EDAA_DW.DIM_GEO

SELECT
		   (ROW_NUMBER() OVER (ORDER BY STR_ID)) + @NEXT_GEO_HIST_SK 		AS GEO_HIST_SK
		  ,(ROW_NUMBER() OVER (ORDER BY STR_ID)) + @NEXT_GEO_HIST_SK  		AS GEO_SK
		  ,CONVERT(INTEGER,STR_ID)   								 		AS STR_ID
		  ,CONVERT(VARCHAR(5),'n/a')		STR_NM
		  ,CONVERT(INTEGER,0)				GROSS_FLR_AREA
		  ,CONVERT(VARCHAR(5),'n/a')		RGN_NM
		  ,CONVERT(VARCHAR(5),'n/a')		MKT_NM
		  ,NULL                    AS		LOC_LNGTD
		  ,NULL                    AS		LOC_LTTD
		  ,CONVERT(INTEGER,-1)				STR_CLS_ID
		  ,CONVERT(VARCHAR(5),'n/a')		STR_CLS_NM
		  ,CONVERT(VARCHAR(5),'n/a')		LOC_ST_ID
		  ,CONVERT(VARCHAR(5),'n/a')		LOC_CNTY_ID
		  ,CONVERT(VARCHAR(5),'n/a')		LOC_CTY
		  ,CONVERT(VARCHAR(5),'n/a')		LOC_ZP_CODE
		  ,CONVERT(VARCHAR(5),'n/a')		DIV_ID
		  ,CONVERT(VARCHAR(5),'n/a')		DIV_NM
		  ,NULL                    AS		CURR_YR_NEW_STR_IND
		  ,NULL                    AS		LST_YR_NEW_STR_IND
		  ,NULL                    AS		TWO_YRS_AGO_NEW_STR_IND
		  ,NULL                    AS		RGN_ID
		  ,NULL                    AS		MKT_ID
		  ,NULL                    AS		STR_OPN_DT
		  ,NULL                    AS		STR_CLS_DT
		  ,CONVERT(VARCHAR(5),'n/a')		STR_CTGRY_NM
		  ,NULL                    AS		STR_ZN_ID
		  ,CONVERT(VARCHAR(5),'n/a')		STR_ZN_NM
		  ,NULL                    AS		STR_CMP_CTGRY_ID
		  ,CONVERT(VARCHAR(5),'n/a')		STR_CMP_CTGRY_DESC
		  ,CAST('2000-01-01' AS DATE)	    VLD_FROM
		  ,CAST('2099-01-01' AS DATE)		VLD_TO
		  ,CAST(1 AS BIT)					IS_CURR_IND
		  ,CAST(0 AS BIT)					IS_DMY_IND
		  ,CAST(1 AS BIT)					IS_EMB_IND
		  ,'New embryo'					    ETL_ACTN
		  ,@NEW_AUD_SKY						AUD_INS_SK
		  ,NULL								AUD_UPD_SK

FROM
	(
	SELECT
			DISTINCT STR_ID
	FROM #LATEARRIVINGGEODIMENSIONFACTRECORDS
	WHERE 	IS_GEO_MISSING = 1
	) AS MISSING_GEO
	WHERE 	NOT EXISTS
	(
	SELECT 1 FROM EDAA_DW.DIM_GEO DIM
	WHERE
	MISSING_GEO.STR_ID = DIM.STR_ID
	)


END

/* Validate Late arriving dimension: DIM_PROD */
PRINT('LATEARRIVINGDIMENSION: DIM_PROD')

IF OBJECT_ID('TEMPDB..#LATEARRIVINGPRODDIMENSIONFACTRECORDS') IS NOT NULL
DROP TABLE #LATEARRIVINGPRODDIMENSIONFACTRECORDS

CREATE TABLE #LATEARRIVINGPRODDIMENSIONFACTRECORDS
WITH (DISTRIBUTION = ROUND_ROBIN)
AS
SELECT DISTINCT FACT_DATA_FEED.Itm_Sku, FACT_DATA_FEED.Mprs_Ctgry_Id as Fnc_Lvl2_Mprs_Ctgry_Id
, FACT_DATA_FEED.Fnc_Lvl3_Pky_Id, CASE WHEN PROD.Itm_Sku IS NULL THEN 1 ELSE 0 END AS IS_PROD_MISSING
FROM
(
SELECT DISTINCT Itm_Sku, Mprs_Ctgry_Id, Fnc_Lvl3_Pky_Id
FROM  EDAA_STG.FCT_HRLY_PROD_CURR_YR_SLS
) AS FACT_DATA_FEED

LEFT  JOIN	(SELECT DISTINCT Itm_Sku, Fnc_Lvl2_Mprs_Ctgry_Id, Fnc_Lvl3_Pky_Id
				FROM EDAA_DW.DIM_PROD
			---	WHERE IS_CURR_IND<>0
					) AS PROD
	ON PROD.Itm_Sku = FACT_DATA_FEED.Itm_Sku
     -- AND PROD.Fnc_Lvl2_Mprs_Ctgry_Id = FACT_DATA_FEED.Mprs_Ctgry_Id
     -- AND PROD.Fnc_Lvl3_Pky_Id = FACT_DATA_FEED.Fnc_Lvl3_Pky_Id
	WHERE  PROD.Itm_Sku IS NULL


DECLARE @NEXT_PROD_HIST_SK INT
SELECT @NEXT_PROD_HIST_SK =ISNULL(MAX(PROD_HIST_SK),0)   FROM EDAA_DW.DIM_PROD

PRINT('LATEARRIVINGDIMENSION: DIM_PROD LOAD')

IF(@NEXT_PROD_HIST_SK IS NOT NULL)
BEGIN


INSERT INTO EDAA_DW.DIM_PROD

SELECT
            (ROW_NUMBER() OVER (ORDER BY Itm_Sku)) + @NEXT_PROD_HIST_SK 	AS PROD_HIST_SK
            ,(ROW_NUMBER() OVER (ORDER BY Itm_Sku)) + @NEXT_PROD_HIST_SK  	AS PROD_SK
            ,CONVERT(VARCHAR(150),MISSING_PROD.Itm_Sku)   					AS Itm_Sku
            ,'n/a'        AS      Itm_Nm
            ,'n/a'        AS      Lvl1_Prod_Id
            ,'n/a'        AS      Lvl1_Prod_Desc
            ,'n/a'        AS      Lvl2_Prod_Clsfctn_Id
            ,'n/a'        AS      Lvl2_Prod_Clsfctn_Desc
            ,'n/a'        AS      Lvl3_Prod_Sub_Ctgry_Id
            ,'n/a'        AS      Lvl3_Prod_Sub_Ctgry_Desc
            ,'n/a'        AS      Lvl4_Prod_Ctgry_Id
            ,'n/a'        AS      Lvl4_Prod_Ctgry_Desc
            ,'n/a'        AS      Lvl5_Bsns_Sgmt_Id
            ,'n/a'        AS      Lvl5_Bsns_Sgmt_Desc
            ,'n/a'        AS      Lvl6_MDS_Area_Id
            ,'n/a'        AS      Lvl6_MDS_Area_Desc
            ,'n/a'        AS      Fnc_Lvl1_Prod_Ctgry_Id
            ,'n/a'        AS      Fnc_Lvl1_Prod_Ctgry_Desc
            ,MISSING_PROD.Fnc_Lvl2_Mprs_Ctgry_Id AS Fnc_Lvl2_Mprs_Ctgry_Id
            ,'n/a'        AS Fnc_Lvl2_Mprs_Ctgry_Desc
            ,MISSING_PROD.Fnc_Lvl3_Pky_Id AS Fnc_Lvl3_Pky_Id
            ,'n/a'        AS      Fnc_Lvl3_Pky_Desc
            ,'n/a'        AS      Fnc_Lvl4_Area_Id
            ,'n/a'        AS      Fnc_Lvl4_Area_Desc
            ,NULL   AS Str_Area_Dtl_Id
            ,'n/a'        AS      Str_Area_Dtl_Desc
            ,NULL   AS Str_Area_Summ_Id
            ,'n/a'        AS      Str_Area_Summ_Desc
            ,NULL   AS Str_Dept_Dtl_Id
            ,'n/a'        AS      Str_Dept_Dtl_Desc
            ,NULL   AS Str_Dept_Summ_Id
            ,'n/a'        AS      Str_Dept_Summ_Desc
            ,CAST('2000-01-01' AS DATE) AS Vld_Frm
            ,CAST('2099-01-01' AS DATE) AS Vld_To
            ,CAST(1 AS BIT)					Is_Curr_Ind
            ,CAST(0 AS BIT)					Is_Dmy_Ind
            ,CAST(1 AS BIT)					Is_Emb_Ind
            ,'New embryo'					Etl_Actn
            ,@NEW_AUD_SKY				    Aud_Ins_sk
            ,NULL					        Aud_Upd_sk
            --      Added for new attributes
            ,NULL                           Itm_Prim_Ind
            ,NULL                           Prod_Size_Desc
            ,NULL                           Brnd_Nm
            ,NULL                           Brnd_Desc
            ,NULL                           Brnd_Ctgry
            ,NULL                           Prod_Desc
            ,NULL                           Prod_Desc_Note
            ,NULL                           Outdate
            ,NULL                           Byr_Id
            ,NULL                           Prod_Sts
            ,NULL                           Prod_Sts_Note
            ,NULL                           Vdr_Id
            ,NULL                           Sku_Ct
FROM
	(
	SELECT
			DISTINCT Itm_Sku, Fnc_Lvl2_Mprs_Ctgry_Id, Fnc_Lvl3_Pky_Id
	FROM #LATEARRIVINGPRODDIMENSIONFACTRECORDS
	WHERE 	IS_PROD_MISSING = 1
	) AS MISSING_PROD
	WHERE 	NOT EXISTS
	(
	SELECT 1 FROM EDAA_DW.DIM_PROD DIM_PROD
	WHERE
	      MISSING_PROD.Itm_Sku = DIM_PROD.Itm_Sku
      --AND MISSING_PROD.Fnc_Lvl2_Mprs_Ctgry_Id = DIM_PROD.Fnc_Lvl2_Mprs_Ctgry_Id
      --AND MISSING_PROD.Fnc_Lvl3_Pky_Id = DIM_PROD.Fnc_Lvl3_Pky_Id
	)


END
DELETE FROM EDAA_DW.FCT_HRLY_PROD_CURR_YR_SLS
WHERE DATEPART(HOUR, DT_TM_HR) = @HOUR;


INSERT INTO EDAA_DW.FCT_HRLY_PROD_CURR_YR_SLS
(
 Dt_Tm_Hr
,Geo_Hist_Sk
,Prod_Hist_Sk
,Dt_Sk
,Str_Id
,Sls_Amt
,Prm_To_Prm_Drct_Mgn_Amt
,Sls_Qty
,Prm_To_Sls_Amt
,Prm_To_Sls_Drct_Mgn_Amt
,Prm_To_Clrnc_Drct_Mgn_Amt
,Scn_Bsd_Trdng_Ind
,Mprs_Src_Ctgry
,Mprs_Prcs_Id
,Mprs_Ctgry_Id
,Fnc_Lvl3_Pky_Id
,Str_Sls_Key_Ind
,Prod_Drct_Mgn_Ind
,Wgtd_Qty
,Prm_To_Sls_Qty
,Prm_To_Sls_Wgtd_Qty
,Prm_To_Sls_Mkdn_Amt
,Prm_To_Clrnc_Amt
,Prm_To_Clrnc_Qty
,Prm_To_Clrnc_Wgtd_Qty
,Prm_To_Clrnc_Mkdn_Amt
,Cogs_Fnc_Chrg_Amt
,Cogs_Fnc_Cr_Amt
,Cogs_Strge_Chrg_Amt
,Cogs_Frt_Amt
,Cogs_Dist_Fclt_Hdl_Amt
,Str_Hdl_Mrkng_Amt
,Str_Hdl_Rcvng_Amt
,Str_Hdl_Chckng_Amt
,Str_Hdl_Stckng_Amt
,Edi_Prm_To_Prm_Appld_Qty
,Edi_Prm_To_Prm_Appld_Wgt_Qty
,Dgtl_Cst_Ctgry
,Aud_Ins_Sk
,Aud_Upd_Sk
)
SELECT
         Dt_Tm_Hr
        ,ISNULL(geo.Geo_Hist_Sk, -1) as Geo_Hist_Sk
        ,ISNULL(prod.Prod_Hist_Sk, -1) as Prod_Hist_Sk
        ,ISNULL(cal.Dt_Sk, -1) as Dt_Sk
        ,sls.Str_Id
        ,Sls_Amt
        ,Prm_To_Prm_Drct_Mgn_Amt
        ,Sls_Qty
        ,Prm_To_Sls_Amt
        ,Prm_To_Sls_Drct_Mgn_Amt
        ,Prm_To_Clrnc_Drct_Mgn_Amt
        ,Scn_Bsd_Trdng_Ind
        ,Mprs_Src_Ctgry
        ,Mprs_Prcs_Id
        ,Mprs_Ctgry_Id
        ,sls.Fnc_Lvl3_Pky_Id
        ,Str_Sls_Key_Ind
        ,Prod_Drct_Mgn_Ind
        ,Wgtd_Qty
        ,Prm_To_Sls_Qty
        ,Prm_To_Sls_Wgtd_Qty
        ,Prm_To_Sls_Mkdn_Amt
        ,Prm_To_Clrnc_Amt
        ,Prm_To_Clrnc_Qty
        ,Prm_To_Clrnc_Wgtd_Qty
        ,Prm_To_Clrnc_Mkdn_Amt
        ,Cogs_Fnc_Chrg_Amt
        ,Cogs_Fnc_Cr_Amt
        ,Cogs_Strge_Chrg_Amt
        ,Cogs_Frt_Amt
        ,Cogs_Dist_Fclt_Hdl_Amt
        ,Str_Hdl_Mrkng_Amt
        ,Str_Hdl_Rcvng_Amt
        ,Str_Hdl_Chckng_Amt
        ,Str_Hdl_Stckng_Amt
        ,Edi_Prm_To_Prm_Appld_Qty
        ,Edi_Prm_To_Prm_Appld_Wgt_Qty
        ,Dgtl_Cst_Ctgry
		,@NEW_AUD_SKY	AS Aud_Ins_Sk
		,NULL 			AS Aud_Upd_Sk
FROM EDAA_STG.FCT_HRLY_PROD_CURR_YR_SLS sls
LEFT JOIN EDAA_DW.DIM_PROD prod
ON prod.itm_sku = sls.itm_sku
AND CAST(sls.Dt_Tm_Hr as date)>=prod.VLD_FRM and CAST(sls.Dt_Tm_Hr as date)<=prod.VLD_TO
AND prod.Fnc_Lvl2_Mprs_Ctgry_Id = sls.Mprs_Ctgry_Id
AND prod.Fnc_Lvl3_Pky_Id = sls.Fnc_Lvl3_Pky_Id
AND prod.Fnc_Lvl2_Mprs_Ctgry_Id <> -1 and prod.Fnc_Lvl3_Pky_Id <> -1
LEFT JOIN EDAA_DW.DIM_GEO geo
ON geo.Str_id = sls.Str_Id
AND CAST(sls.Dt_Tm_Hr as date)>=geo.VLD_FRM AND CAST(sls.Dt_Tm_Hr as date)<=geo.VLD_TO
LEFT JOIN EDAA_DW.DIM_DLY_CAL cal
on  cal.cal_dt = CAST(sls.Dt_Tm_Hr as date)
WHERE DATEPART(HOUR, sls.DT_TM_HR)= @HOUR;



BEGIN
SELECT @NBR_OF_RW_ISRT = COUNT(1)  FROM EDAA_DW.FCT_HRLY_PROD_CURR_YR_SLS WHERE Aud_Ins_Sk = @NEW_AUD_SKY
SELECT @NBR_OF_RW_UDT  = COUNT(1)  FROM EDAA_DW.FCT_HRLY_PROD_CURR_YR_SLS WHERE Aud_Upd_Sk = @NEW_AUD_SKY

------------ UPDATE Statistics-------

UPDATE STATISTICS  EDAA_DW.FCT_HRLY_PROD_CURR_YR_SLS;

END

/*Audit Log End*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT  = @NBR_OF_RW_UDT

END TRY

BEGIN CATCH
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = 'EDAA_ETL.SP_STG_DW_FCT_HRLY_PROD_CURR_YR_SLS_PARTIAL_CORRECTION'
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
GO
