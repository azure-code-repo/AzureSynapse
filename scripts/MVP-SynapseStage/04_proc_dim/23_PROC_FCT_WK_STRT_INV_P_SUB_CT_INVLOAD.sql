
CREATE PROC [ETL].[PROC_FCT_WK_STRT_INV_P_SUB_CT_INVLOAD] AS

BEGIN TRY


/*Datamart Auditing variables*/
DECLARE @NEW_AUD_SKY BIGINT ;
DECLARE @NBR_OF_RW_ISRT INT ;
DECLARE @NBR_OF_RW_UDT INT ;
DECLARE @EXEC_LYR VARCHAR(255) ;
DECLARE @EXEC_JOB VARCHAR(500) ;
DECLARE @SRC_ENTY VARCHAR(500) ;
DECLARE @TGT_ENTY VARCHAR(500) ;
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = 'ETL.PROC_FCT_WK_STRT_INV_P_SUB_CT_INVLOAD' ;
DECLARE @ERROR_LINE AS INT ;
DECLARE @ERROR_MSG AS NVARCHAR(max);

BEGIN

/*Audit Log Start*/
EXEC ETL.AUDIT_DATA_LOAD_START
 @EXEC_LYR  = 'DM'
,@EXEC_JOB  = 'PROC_FCT_WK_STRT_INV_P_SUB_CT_INVLOAD'
,@SRC_ENTY  = 'WK_DC_MPRS_CT_HST_INV'
,@TGT_ENTY = 'FCT_WK_STRT_INV_P_SUB_CT'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

;
WITH newdata as
(
SELECT b.mjr_p_sub_ct_id AS P_SUB_CT_ID,
         --DATEADD(DAY, 1, CAST(wk_end_dt as date))           AS WK_STRT_DT,
		 REPLACE(CAST(aa.wk_end_dt  AS date),'-','') as TM_SKY,
       aa.ut_id as UT_ID,
	   ISNULL(G.GEO_HST_SKY, -1) AS GEO_HST_SKY,
	   ISNULL(BYR.BYR_HST_SKY, -1) AS BYR_HST_SKY,
	     CAST(b.pky_id as smallint) as FNC_PKY_ID,
	   CAST(b.mprs_ct_id as smallint) as  FNC_MPRS_CT_ID,
       Sum(CAST(aa.tg_inv_cst_am as decimal(11, 2)))      AS TG_INV_CST_AM,
       Sum(CAST(aa.tg_inv_qt as decimal(11, 2)))          AS TG_INV_QT,
       Sum(CAST(aa.tg_clrn_inv_cst_am as decimal(11, 2))) AS TG_CLRN_INV_CST_AM,
       Sum(CAST(aa.tg_clrn_inv_qt as decimal(11, 2)))     AS INV_CLRN_QTY
FROM   stg.wk_dc_mprs_ct_hst_INV AS aa
       INNER JOIN (

	   SELECT pky_id, mprs_ct_id, mjr_p_sub_ct_id
					               FROM  dm_edw.byr_emp_pky_mprs_currprev_inf
								   WHERE CAST(INS_DT as date) = (SELECT CAST(MAX(INS_DT) as date) AS INS_DT FROM dm_edw.byr_emp_pky_mprs_currprev_inf)

	   ) AS b
               ON CAST(b.pky_id as smallint) = CAST(aa.pky_id as smallint)
                  AND CAST(b.mprs_ct_id as smallint) = CAST(aa.mprs_ct_id as smallint)

LEFT JOIN (SELECT GEO_HST_SKY, UT_ID, VLD_FROM, VLD_TO FROM DM.DIM_GEO) AS G ON G.UT_ID = aa.UT_ID
AND aa.wk_end_dt BETWEEN G.VLD_FROM AND ISNULL(G.VLD_TO, CAST('2099-01-01' as date))
LEFT JOIN
	(SELECT BYR_HST_SKY, P_SUB_CT_ID, VLD_FROM, VLD_TO   FROM DM.DIM_BYR_P_SUB_CT ) AS BYR
	ON BYR.P_SUB_CT_ID  = b.mjr_p_sub_ct_id AND aa.wk_end_dt BETWEEN BYR.VLD_FROM AND ISNULL(BYR.VLD_TO, CAST('2099-01-01' as date))
group by b.mjr_p_sub_ct_id ,
        aa.ut_id ,
        aa.wk_end_dt ,
		CAST(b.pky_id as smallint) ,
		CAST(b.mprs_ct_id as smallint),
		ISNULL(GEO_HST_SKY, -1) ,
		ISNULL(BYR_HST_SKY, -1)
)

update DM.FCT_WK_STRT_INV_P_SUB_CT  SET
	 TG_INV_CST_AM =  SOURCE.TG_INV_CST_AM,
	TG_CLRN_INV_CST_AM = SOURCE.TG_CLRN_INV_CST_AM,
    INV_CLRN_QTY = SOURCE.INV_CLRN_QTY ,
    TG_INV_QT = SOURCE.TG_INV_QT,
	AUD_UPD_SKY = @NEW_AUD_SKY
FROM DM.FCT_WK_STRT_INV_P_SUB_CT AS TARGET
INNER JOIN newdata AS SOURCE
ON (TARGET.p_sub_ct_id = SOURCE.p_sub_ct_id and
	TARGET.ut_id = SOURCE.ut_id and
	TARGET.TM_SKY = SOURCE.TM_SKY and
    TARGET.FNC_PKY_ID = SOURCE.FNC_PKY_ID and
	TARGET.FNC_MPRS_CT_ID = SOURCE.FNC_MPRS_CT_ID )
;



WITH newdata as
(
SELECT b.mjr_p_sub_ct_id AS P_SUB_CT_ID,
         --DATEADD(DAY, 1, CAST(wk_end_dt as date))           AS WK_STRT_DT,
		 REPLACE(CAST(aa.wk_end_dt  AS date),'-','') as TM_SKY,
       aa.ut_id as UT_ID,
	   ISNULL(G.GEO_HST_SKY, -1) AS GEO_HST_SKY,
	   ISNULL(BYR.BYR_HST_SKY, -1) AS BYR_HST_SKY,
	     CAST(b.pky_id as smallint) as FNC_PKY_ID,
	   CAST(b.mprs_ct_id as smallint) as  FNC_MPRS_CT_ID,
       Sum(CAST(aa.tg_inv_cst_am as decimal(11, 2)))      AS TG_INV_CST_AM,
       Sum(CAST(aa.tg_inv_qt as decimal(11, 2)))          AS TG_INV_QT,
       Sum(CAST(aa.tg_clrn_inv_cst_am as decimal(11, 2))) AS TG_CLRN_INV_CST_AM,
       Sum(CAST(aa.tg_clrn_inv_qt as decimal(11, 2)))     AS INV_CLRN_QTY
FROM   stg.wk_dc_mprs_ct_hst_INV AS aa
       INNER JOIN (

	   SELECT pky_id, mprs_ct_id, mjr_p_sub_ct_id
					               FROM  dm_edw.byr_emp_pky_mprs_currprev_inf
								   WHERE CAST(INS_DT as date) = (SELECT CAST(MAX(INS_DT) as date) AS INS_DT FROM dm_edw.byr_emp_pky_mprs_currprev_inf)

	   ) AS b
               ON CAST(b.pky_id as smallint) = CAST(aa.pky_id as smallint)
                  AND CAST(b.mprs_ct_id as smallint) = CAST(aa.mprs_ct_id as smallint)

LEFT JOIN (SELECT GEO_HST_SKY, UT_ID, VLD_FROM, VLD_TO FROM DM.DIM_GEO) AS G ON G.UT_ID = aa.UT_ID
AND aa.wk_end_dt BETWEEN G.VLD_FROM AND ISNULL(G.VLD_TO, CAST('2099-01-01' as date))
LEFT JOIN
	(SELECT BYR_HST_SKY, P_SUB_CT_ID, VLD_FROM, VLD_TO   FROM DM.DIM_BYR_P_SUB_CT ) AS BYR
	ON BYR.P_SUB_CT_ID  = b.mjr_p_sub_ct_id AND aa.wk_end_dt BETWEEN BYR.VLD_FROM AND ISNULL(BYR.VLD_TO, CAST('2099-01-01' as date))
group by b.mjr_p_sub_ct_id ,
        aa.ut_id ,
        aa.wk_end_dt ,
		CAST(b.pky_id as smallint) ,
		CAST(b.mprs_ct_id as smallint),
		ISNULL(GEO_HST_SKY, -1) ,
		ISNULL(BYR_HST_SKY, -1)
)

INSERT INTO DM.FCT_WK_STRT_INV_P_SUB_CT

	select
	GEO_HST_SKY,
	TM_SKY,
	BYR_HST_SKY,
	P_SUB_CT_ID,
	FNC_MPRS_CT_ID,
	FNC_PKY_ID,
    UT_ID,
	TG_INV_CST_AM,
	TG_CLRN_INV_CST_AM,
	INV_CLRN_QTY,
	TG_INV_QT,
    @NEW_AUD_SKY     as AUD_INS_SKY,
	NULL as AUD_UPD_SKY
	from newdata AS SOURCE
	where not exists ( 	select 1 from 	DM.FCT_WK_STRT_INV_P_SUB_CT AS TARGET
	where	(TARGET.p_sub_ct_id = SOURCE.p_sub_ct_id and
			TARGET.ut_id = SOURCE.ut_id and
			TARGET.TM_SKY = SOURCE.TM_SKY and
			TARGET.FNC_PKY_ID = SOURCE.FNC_PKY_ID and
			TARGET.FNC_MPRS_CT_ID = SOURCE.FNC_MPRS_CT_ID ) )
;


BEGIN


SELECT @NBR_OF_RW_ISRT = COUNT(1)  FROM DM.FCT_WK_STRT_INV_P_SUB_CT WHERE AUD_INS_SKY = @NEW_AUD_SKY
SELECT @NBR_OF_RW_UDT = COUNT(1)  FROM DM.FCT_WK_STRT_INV_P_SUB_CT WHERE AUD_UPD_SKY = @NEW_AUD_SKY

-- SET @NBR_OF_RW_UDT_TOTAL = @NBR_OF_RW_UDT_TOTAL + @NBR_OF_RW_UDT
-- SET @NBR_OF_RW_ISRT_TOTAL = @NBR_OF_RW_ISRT_TOTAL + @NBR_OF_RW_ISRT


END



/*Audit Log End*/
EXEC ETL.AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT  = @NBR_OF_RW_UDT


END ;

END TRY

BEGIN CATCH

    SELECT
      @ERROR_LINE =  ERROR_NUMBER()
       ,@ERROR_MSG = ERROR_MESSAGE();
--------- Log execution error ----------

EXEC [ETL].[LOG_AUD_ERR]
@AUD_SKY = @NEW_AUD_SKY,
@ERROR_PROCEDURE_NAME = @ERROR_PROCEDURE_NAME,
@ERROR_LINE = @ERROR_LINE,
@ERROR_MSG = @ERROR_MSG
;

   THROW;


  END CATCH
GO
