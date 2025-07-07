/****** Object:  StoredProcedure [EDAA_ETL].[SP_STG_DW_FCT_MPRS_PRODUCT_RCV_SUMMARY]    Script Date: 10/25/2022 7:49:42 AM ******/

CREATE PROC [EDAA_ETL].[SP_STG_DW_FCT_MPRS_PRODUCT_RCV_SUMMARY] AS

/*
 =============================================
 Author:        Core Build Team
 Create date:   25-Feb-2022
 Description:   Stored proc load MPRS_RCV_DTL table.
 =============================================
*/

BEGIN TRY

SET NOCOUNT ON
SET XACT_ABORT ON

/*Datamart Auditing variables*/
DECLARE @NEW_AUD_SKY BIGINT
DECLARE @NBR_OF_RW_ISRT BIGINT
DECLARE @NBR_OF_RW_UDT BIGINT
DECLARE @EXEC_LYR VARCHAR(255)
DECLARE @EXEC_JOB VARCHAR(500)
DECLARE @SRC_ENTY VARCHAR(500)
DECLARE @TGT_ENTY VARCHAR(500)
DECLARE @GETDATETIME datetime = GETDATE()
DECLARE @From_DT_SK INT
DECLARE @TO_DT_SK INT

/*Audit Log Start*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START
 @EXEC_LYR  = 'EDAA_DW'
,@EXEC_JOB  = 'SP_STG_DW_FCT_MPRS_PRODUCT_RCV_SUMMARY'
,@SRC_ENTY  = 'EDAA_STG.FCT_MPRS_PRODUCT_RCV_SUMMARY'
,@TGT_ENTY = 'FCT_MPRS_PRODUCT_RCV_SUMMARY'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

/* Set Variable Values */
SET @From_DT_SK=(SELECT MIN(DT_SK) FROM EDAA_STG.FCT_MPRS_PRODUCT_RCV_SUMMARY);
SET @TO_DT_SK=(SELECT MAX(DT_SK) FROM EDAA_STG.FCT_MPRS_PRODUCT_RCV_SUMMARY);

/* Validate Late arriving Dimesnion: DIM_GEO */
PRINT('LATEARRIVINGDIMENSION: DIM_GEO')



DECLARE @NEXT_GEO_HIST_SK INT
SELECT @NEXT_GEO_HIST_SK =ISNULL(MAX(GEO_HIST_SK),0)   FROM EDAA_DW.DIM_GEO

PRINT('LATEARRIVINGDIMENSION: DIM_GEO LOAD')

IF(@NEXT_GEO_HIST_SK IS NOT NULL)
BEGIN
WITH LATEARRIVINGDIMENSIONFACTRECORDS AS
(
SELECT DISTINCT FACT_DATA_FEED.Str_Id,
CASE WHEN GEO.Str_Id IS NULL THEN 1 ELSE 0 END AS IS_GEO_MISSING
FROM
(
SELECT DISTINCT Str_Id
FROM  EDAA_STG.FCT_MPRS_PRODUCT_RCV_SUMMARY
) AS FACT_DATA_FEED

LEFT  JOIN	(SELECT DISTINCT Str_Id
				FROM EDAA_DW.DIM_GEO
				--WHERE IS_CURR_IND<>0
					) AS GEO
	ON GEO.Str_Id = FACT_DATA_FEED.Str_Id
WHERE  GEO.Str_Id IS NULL
)

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
		,'New embryo'				    ETL_ACTN
		,@NEW_AUD_SKY					AUD_INS_SK
		,NULL							AUD_UPD_SK
FROM
	(
	SELECT
			DISTINCT STR_ID
	FROM LATEARRIVINGDIMENSIONFACTRECORDS
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




DECLARE @NEXT_PROD_HIST_SK INT
SELECT @NEXT_PROD_HIST_SK =ISNULL(MAX(PROD_HIST_SK),0)   FROM EDAA_DW.DIM_PROD

PRINT('LATEARRIVINGDIMENSION: DIM_PROD LOAD')

IF(@NEXT_PROD_HIST_SK IS NOT NULL)
BEGIN
WITH LATEARRIVINGPRODDIMENSIONFACTRECORDS AS
(
SELECT DISTINCT FACT_DATA_FEED.Itm_Sku,
--'' as Fnc_Lvl2_Mprs_Ctgry_Id,
--'' as Fnc_Lvl3_Pky_Id,
CASE WHEN PROD.Itm_Sku IS NULL THEN 1 ELSE 0 END AS IS_PROD_MISSING
FROM
(
SELECT DISTINCT Itm_Sku
FROM  EDAA_STG.FCT_MPRS_PRODUCT_RCV_SUMMARY
) AS FACT_DATA_FEED

LEFT  JOIN	(SELECT DISTINCT Itm_Sku--, Fnc_Lvl2_Mprs_Ctgry_Id
				FROM EDAA_DW.DIM_PROD
			---	WHERE IS_CURR_IND<>0
					) AS PROD
	ON PROD.Itm_Sku = FACT_DATA_FEED.Itm_Sku
	WHERE  PROD.Itm_Sku IS NULL
)

INSERT INTO EDAA_DW.DIM_PROD

SELECT
            (ROW_NUMBER() OVER (ORDER BY Itm_Sku)) + @NEXT_PROD_HIST_SK 	AS PROD_HIST_SK
            ,(ROW_NUMBER() OVER (ORDER BY Itm_Sku)) + @NEXT_PROD_HIST_SK  	AS PROD_SK
            ,CONVERT(VARCHAR(150),MISSING_PROD.Itm_Sku)   					AS Itm_Sku
            ,CONVERT(VARCHAR(255),'n/a')        Itm_Nm
            ,CONVERT(VARCHAR(10),'n/a')        Lvl1_Prod_Id
            ,CONVERT(VARCHAR(80),'n/a')        Lvl1_Prod_Desc
            ,CONVERT(VARCHAR(20),'n/a')        Lvl2_Prod_Clsfctn_Id
            ,CONVERT(VARCHAR(80),'n/a')        Lvl2_Prod_Clsfctn_Desc
            ,CONVERT(VARCHAR(10),'n/a')        Lvl3_Prod_Sub_Ctgry_Id
            ,CONVERT(VARCHAR(80),'n/a')        Lvl3_Prod_Sub_Ctgry_Desc
            ,CONVERT(VARCHAR(10),'n/a')        Lvl4_Prod_Ctgry_Id
            ,CONVERT(VARCHAR(80),'n/a')        Lvl4_Prod_Ctgry_Desc
            ,CONVERT(VARCHAR(10),'n/a')        Lvl5_Bsns_Sgmt_Id
            ,CONVERT(VARCHAR(80),'n/a')        Lvl5_Bsns_Sgmt_Desc
            ,CONVERT(VARCHAR(10),'n/a')        Lvl6_MDS_Area_Id
            ,CONVERT(VARCHAR(80),'n/a')        Lvl6_MDS_Area_Desc
            ,CONVERT(VARCHAR(10),'n/a')        Fnc_Lvl1_Prod_Ctgry_Id
            ,CONVERT(VARCHAR(80),'n/a')        Fnc_Lvl1_Prod_Ctgry_Desc
            ,-1							       Fnc_Lvl2_Mprs_Ctgry_Id--not present in staging table
            ,CONVERT(VARCHAR(80),'n/a')        Fnc_Lvl2_Mprs_Ctgry_Desc
            ,-1								   Fnc_Lvl3_Pky_Id--not present in staging table
            ,CONVERT(VARCHAR(80),'n/a')        Fnc_Lvl3_Pky_Desc
            ,CONVERT(VARCHAR(15),'n/a')        Fnc_Lvl4_Area_Id
            ,CONVERT(VARCHAR(80),'n/a')        Fnc_Lvl4_Area_Desc
            ,NULL   					AS 	  Str_Area_Dtl_Id
            ,CONVERT(VARCHAR(50),'n/a')        Str_Area_Dtl_Desc
            ,NULL     					AS 	  Str_Area_Summ_Id
            ,CONVERT(VARCHAR(50),'n/a')        Str_Area_Summ_Desc
            ,NULL   					AS 	  Str_Dept_Dtl_Id
            ,CONVERT(VARCHAR(50),'n/a')        Str_Dept_Dtl_Desc
            ,NULL   					AS 	  Str_Dept_Summ_Id
            ,CONVERT(VARCHAR(50),'n/a')        Str_Dept_Summ_Desc
            ,CAST('2000-01-01' AS DATE) AS    Vld_Frm
            ,CAST('2099-01-01' AS DATE) AS    Vld_To
            ,CAST(1 AS BIT)					  Is_Curr_Ind
            ,CAST(0 AS BIT)					  Is_Dmy_Ind
            ,CAST(1 AS BIT)					  Is_Emb_Ind
            ,'New embryo'					  Etl_Actn
            ,@NEW_AUD_SKY					  Aud_Ins_sk
            ,NULL					          Aud_Upd_sk
			,CAST(1 AS BIT) 		  		  Itm_Prim_Ind
			,CONVERT(VARCHAR(500),'n/a') 		  Prod_Size_Desc
			,CONVERT(VARCHAR(250),'n/a') 		  Brnd_Nm
			,CONVERT(VARCHAR(250),'n/a') 		  Brnd_Desc
			,CONVERT(VARCHAR(250),'n/a') 		  Brnd_Ctgry
			,CONVERT(VARCHAR(500),'n/a') 		  Prod_Desc
			,CONVERT(VARCHAR(500),'n/a') 		  Prod_Desc_Note
			,CAST('2099-12-31' AS DATE) AS    Outdate
			,NULL						AS 	  Byr_Id
			,CONVERT(VARCHAR(100),'n/a') 		  Prod_Sts
			,CONVERT(VARCHAR(500),'n/a') 		  Prod_Sts_Note
			,NULL		 		 		AS 	  Vdr_Id
			,NULL   AS Sku_Ct
FROM
	(
	SELECT
			DISTINCT Itm_Sku--, Fnc_Lvl2_Mprs_Ctgry_Id
	FROM LATEARRIVINGPRODDIMENSIONFACTRECORDS
	WHERE 	IS_PROD_MISSING = 1
	) AS MISSING_PROD
	WHERE 	NOT EXISTS
	(
	SELECT 1 FROM EDAA_DW.DIM_PROD DIM_PROD
	WHERE
	MISSING_PROD.Itm_Sku = DIM_PROD.Itm_Sku
	)

END

/*Set the Active Records on Key Combination of Datasets w.r.t Latest DateKey*/

-- BEGIN

-- WITH CTE AS(
-- SELECT DISTINCT Str_Id AS St,Itm_Sku AS Itm FROM EDAA_STG.FCT_MPRS_PRODUCT_RCV_SUMMARY WHERE Is_Curr_Ind='1'  )

-- UPDATE EDAA_DW.FCT_MPRS_PRODUCT_RCV_SUMMARY   SET Is_Curr_Ind='0',End_Dt=Day_Dt FROM EDAA_DW.FCT_MPRS_PRODUCT_RCV_SUMMARY AS DW INNER JOIN CTE
--  ON DW.Str_Id=CTE.St and DW.Itm_Sku=CTE.Itm WHERE  DW.Is_Curr_Ind='1'

-- END

/*Inserting Stage records into Datamart table*/

INSERT INTO EDAA_DW.FCT_MPRS_PRODUCT_RCV_SUMMARY
(
		Prod_Hist_Sk,
		Geo_Hist_Sk,
		Str_Id,
		Itm_Sku,
		Dt_Sk,
		Strt_Dt,
		End_Dt,
		Day_Dt,
		Is_Curr_Ind,
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
		Aud_Ins_Sk,
		Aud_Upd_Sk,
		Create_Date,
		Update_Date
)
SELECT 	ISNULL(PROD.Prod_Hist_Sk,-1) AS Prod_Hist_Sk,
		ISNULL(GEO.Geo_Hist_Sk,-1) AS Geo_Hist_Sk,
		MPRS.Str_Id,
		MPRS.Itm_Sku,
		ISNULL(CAL.Dt_Sk,-1) AS Dt_Sk,
		MPRS.Strt_Dt,
		MPRS.End_Dt,
		MPRS.Day_Dt,
		MPRS.Is_Curr_Ind,
		MPRS.Mjr_Pkg_Ut_Qty,
		MPRS.Itm_Grss_Cst_Amt,
		MPRS.Itm_Net_Cst_Amt,
		MPRS.Itm_Chrg_Ea_Amt,
		MPRS.Itm_Alw_Ea_Amt,
		MPRS.Po_Chrg_Ea_Amt,
		MPRS.Po_Alw_Ea_Amt,
		MPRS.Csh_Dct_Ea_Amt,
		MPRS.Frt_Chrg_Ea_Amt,
		MPRS.Frt_Alw_Ea_Amt,
		MPRS.Frt_Unld_Chrg_Amt,
		MPRS.Ipt_Ld_Chrg_Amt,
		MPRS.Bkh_Frt_Chrg_Amt,
		MPRS.Extd_Bkh_Frt_Chrg_Amt,
		@NEW_AUD_SKY AS Aud_Ins_Sk,
		NULL AS Aud_Upd_Sk,
		@GETDATETIME,
		@GETDATETIME
FROM  EDAA_STG.FCT_MPRS_PRODUCT_RCV_SUMMARY MPRS
LEFT JOIN  EDAA_DW.DIM_GEO GEO ON GEO.Str_Id = MPRS.Str_Id AND GEO.IS_Curr_Ind=1
LEFT JOIN  EDAA_DW.DIM_PROD PROD ON PROD.Itm_Sku=MPRS.Itm_Sku AND PROD.IS_Curr_Ind=1
LEFT JOIN  EDAA_DW.Dim_Dly_Cal CAL ON MPRS.Dt_Sk=CAL.Dt_Sk



/* Delete the Duplicate Records If Same Key Combination Records Inserted Multiple times w.r.t Same Days*/

BEGIN
WITH DELETE_DUPLICATE_CTE
AS
(
SELECT
	Str_Id,
	ITM_SKU,
	DT_SK,
    ROW_NUMBER() OVER (PARTITION BY Str_Id,ITM_SKU,DT_SK ORDER BY UPDATE_DATE DESC) AS RNK
FROM EDAA_DW.FCT_MPRS_PRODUCT_RCV_SUMMARY WHERE DT_SK>=@From_DT_SK AND DT_SK<=@TO_DT_SK
)
DELETE FROM DELETE_DUPLICATE_CTE WHERE RNK >1
END

BEGIN
SELECT @NBR_OF_RW_ISRT = COUNT_BIG(1)  FROM EDAA_DW.FCT_MPRS_PRODUCT_RCV_SUMMARY WHERE Aud_Ins_Sk = @NEW_AUD_SKY
SELECT @NBR_OF_RW_UDT  = COUNT_BIG(1)  FROM EDAA_DW.FCT_MPRS_PRODUCT_RCV_SUMMARY WHERE Aud_Upd_Sk = @NEW_AUD_SKY

------------ UPDATE Statistics-------

UPDATE STATISTICS  EDAA_DW.FCT_MPRS_PRODUCT_RCV_SUMMARY;

END

/*Audit Log End*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT  = @NBR_OF_RW_UDT

END TRY

BEGIN CATCH
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = 'EDAA_CNTL.SP_STG_DW_FCT_MPRS_RCV_DTL'
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
