CREATE PROC [EDAA_ETL].[PROC_FCT_RWB_UT_P_FCST_HST_DATA_LOAD] AS
BEGIN

  BEGIN TRY

	SET NOCOUNT ON;

     /*DATAMART AUDITING VARIABLES*/
    DECLARE @NEW_AUD_SKY bigint;
    DECLARE @NBR_OF_RW_ISRT int;
    DECLARE @NBR_OF_RW_UDT int;
    DECLARE @EXEC_LYR varchar(255);
    DECLARE @EXEC_JOB varchar(500);
    DECLARE @SRC_ENTY varchar(500);
    DECLARE @TGT_ENTY varchar(500);
    DECLARE @ERROR_PROCEDURE_NAME AS varchar(60) ='EDAA_ETL.PROC_FCT_RWB_UT_P_FCST_HST_DATA_LOAD';
    DECLARE @ERROR_LINE AS int;
    DECLARE @ERROR_MSG AS nvarchar(max);


    /*AUDIT LOG START*/
    EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR ='EDAA_DW',
                                   @EXEC_JOB ='EDAA_ETL.PROC_FCT_RWB_UT_P_FCST_HST_DATA_LOAD',
                                   @SRC_ENTY ='EDAA_STG.FCT_RWB_UT_P_FCST_HST',
                                   @TGT_ENTY ='EDAA_DW.FCT_RWB_UT_P_FCST_HST',
                                   @NEW_AUD_SKY=@NEW_AUD_SKY OUTPUT

--LATEARRIVINGDIMENSIONS DIM_GEO
PRINT('LATEARRIVINGDIMENSION: DIM_GEO')

IF OBJECT_ID('TEMPDB..#LATEARRIVINGGEODIMENSIONFACTRECORDS3') IS NOT NULL
DROP TABLE #LATEARRIVINGGEODIMENSIONFACTRECORDS3

CREATE TABLE #LATEARRIVINGGEODIMENSIONFACTRECORDS3
WITH (DISTRIBUTION = ROUND_ROBIN)
AS
SELECT DISTINCT
	FACT_DATA_FEED_SB.UT_ID,

CASE WHEN GEO_SB.Str_Id
IS NULL THEN 1
ELSE 0
END
AS IS_GEO_MISSING
FROM
(
SELECT DISTINCT UT_ID
FROM  EDAA_STG.FCT_RWB_UT_P_FCST_HST
) AS FACT_DATA_FEED_SB

LEFT  JOIN	(SELECT DISTINCT Str_Id
				FROM EDAA_DW.DIM_GEO
				---WHERE IS_CURR_IND<>0
					) AS GEO_SB
	ON GEO_SB.Str_Id = FACT_DATA_FEED_SB.UT_ID
	WHERE  GEO_SB.Str_Id IS NULL

DECLARE @NEXT_GEO_HIST_SK INT
SELECT @NEXT_GEO_HIST_SK =ISNULL(MAX(GEO_HIST_SK),0)   FROM EDAA_DW.DIM_GEO

PRINT('LATEARRIVINGDIMENSION: DIM_GEO LOAD')

IF(@NEXT_GEO_HIST_SK IS NOT NULL)
BEGIN


INSERT INTO EDAA_DW.DIM_GEO

SELECT
		   (ROW_NUMBER() OVER (ORDER BY UT_ID)) + @NEXT_GEO_HIST_SK 		AS GEO_HIST_SK
		  ,(ROW_NUMBER() OVER (ORDER BY UT_ID)) + @NEXT_GEO_HIST_SK  		AS GEO_SK
		  ,CONVERT(INTEGER,UT_ID)   								 		AS STR_ID
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
		  ,@NEW_AUD_SKY			     	    AUD_INS_SK
		  ,NULL								AUD_UPD_SK

FROM
	(
	SELECT
			DISTINCT
			UT_ID

	FROM #LATEARRIVINGGEODIMENSIONFACTRECORDS3
	WHERE 	IS_GEO_MISSING = 1
	) AS MISSING_GEO_SB
	WHERE 	NOT EXISTS
	(
	SELECT 1 FROM EDAA_DW.DIM_GEO DIM
	WHERE
	MISSING_GEO_SB.UT_ID = DIM.STR_ID
	)


END

--- Delete last 7 days of data from today in DW table
	  DELETE FROM [EDAA_DW].[FCT_RWB_UT_P_FCST_HST]
      WHERE cast([Rwb_Blk_Max_P_Ut_End_Dt] as date) >= DATEADD(DAY, -7, CAST(GETDATE() AS date))

--- To prevent duplicate records from inserting into DW table
      DELETE A
      from  [EDAA_DW].[FCT_RWB_UT_P_FCST_HST] A
	  inner join [EDAA_STG].[FCT_RWB_UT_P_FCST_HST] B
      On   A.Promo_Vhl_P_Grp_Id = B.P_GRP_ID
      AND  A.Promo_Vhl_P_Sub_Grp_Id = B.P_SGRP_ID
	  AND  A.P_ID = B.P_ID
	  AND  A.UT_ID = B.UT_ID


    --INSERT into [EDAA_DW].[FCT_RWB_UT_P_FCST_HST]
    INSERT INTO
	  [EDAA_DW].[FCT_RWB_UT_P_FCST_HST] (
		    [Promo_Adv_Blk_Id],
			[Promo_Vhl_P_Grp_Id],
			[Promo_Vhl_P_Sub_Grp_Id],
			[Geo_Hist_Sk],
			[Dt_Sk] ,
			[P_ID],
			[UT_ID],
			[Rwb_Blk_Min_P_Ut_Strt_Dt],
			[Rwb_Blk_Max_P_Ut_End_Dt],
			[Rwb_Fcst_T_Sl_Qty],
			[Rwb_Fcst_Bsln_Sl_Qty],
			[Rwb_Fcst_T_Sl_Amt],
			[Rwb_Fcst_T_Dmgn_Amt],
			[Rwb_Avg_Rtl_Amt] ,
			[Rgl_Avg_Cst_Amt],
			[Rgl_Avg_Rtl_Pr_Amt],
			[Promo_Byr_Sts_Ct],
			[Promo_Rplnm_Sts_Ct] ,
			[Promo_Rplnm_Appr_Ind],
			[Promo_Rplnm_Snt_Lift_Manu_Ind],
			[Promo_Rplnm_Ovlp_Ind],
			[Promo_Rplnm_Extrl_Promo_Buf_Ind],
			[Promo_Rplnm_Sbmt_Qty]
	)

      SELECT

	     P.[Promo_Blk_Id],
         P.[P_GRP_ID],
         P.[P_SGRP_ID],
         B.[Geo_Hist_Sk],
		CAST(format(cast(P.RWB_BLK_MIN_P_UT_STRT_DT as date), N'yyyyMMdd') as int) as Dt_Sk,
		 P.[P_ID] ,
		 P.[UT_ID] ,
		 P.[RWB_BLK_MIN_P_UT_STRT_DT] ,
		 P.[RWB_BLK_MAX_P_UT_END_DT] ,
		 P.[RWB_T_SL_QT] ,
		 P.[RWB_BSLN_SL_QT] ,
		 P.[RWB_T_SL_AM] ,
		 P.[RWB_T_DMGN_AM],
		 P.[RWB_AVG_RTL_AM] ,
		 P.[RGL_AVG_RTL_PR_AM] ,
		 P.[RGL_AVG_CST_AM] ,
		 P.[PROMO_BYR_STS_CT] ,
		 P.[PROMO_RPLNM_STS_CT] ,
		 P.[PROMO_RPLNM_APPR_FLG] ,
		 P.[PROMO_RPLNM_SNT_LIFT_MANU_FLG] ,
		 P.[PROMO_RPLNM_OVLP_FLG] ,
		 P.[PROMO_RPLNM_EXTRLPROMO_BUF_FLG] ,
		 P.[PROMO_RPLNM_SBMT_QT]

      FROM [EDAA_STG].[FCT_RWB_UT_P_FCST_HST] P
		LEFT JOIN EDAA_DW.DIM_GEO  B
	    ON B.[Str_Id] = P.UT_ID
		and B.[Is_Curr_Ind] = 1
		where cast(P.[RWB_BLK_MIN_P_UT_STRT_DT] as DATE) >= DATEADD(year, -3 , CAST(getdate() AS DATE))


		----To delete more than three years Data from DW Table

		DELETE FROM [EDAA_DW].[FCT_RWB_UT_P_FCST_HST]
        where cast([Rwb_Blk_Min_P_Ut_Strt_Dt] as DATE) < DATEADD(year, -3, CAST(getdate() AS DATE))


    PRINT 'Data Load completed..'


  END TRY

  BEGIN CATCH

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

 END
