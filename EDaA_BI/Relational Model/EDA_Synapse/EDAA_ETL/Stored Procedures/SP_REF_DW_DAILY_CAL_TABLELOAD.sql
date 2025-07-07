CREATE PROC [EDAA_ETL].[SP_REF_DW_DAILY_CAL_TABLELOAD] AS

/*
 =============================================
 Author:        Shrikant Sakpal
 Create date:   20-Mar-2021
 Description:   This SP will UPSERT the daily Cal from Dm_edw schema to DW schema.
                This SP will load all the data realted to calendar like Last year fiscal,
                Holiday and Calendar date, Holiday description, YTD, QTD, Week day Name etc.
                Source [dm_edw].[DT_INF] of the SP loaded on demand Adhoc basis.
 =============================================
 Change History
 20-Mar-2021	SS      Initial Version
 21-Apr-2021	SS		Formatted the SP.

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
,@EXEC_JOB  = 'SP_REF_DW_DAILY_CAL_TABLELOAD'
,@SRC_ENTY  = 'DT_INF & HLDY_DT_XRF_INF'
,@TGT_ENTY = 'DIM_DLY_CAL'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT


BEGIN

WITH CTE AS
        (

            SELECT
			CAST(convert(varchar,DT.[DAY_DT],112) as int) AS [DT_SK]
            ,DT.[DAY_DT] AS CAL_DT
			,DT.[FCL_WK_END_DT_LY] AS [LST_YR_FSC_WKND_DT]
            ,DT.[DAY_DT_LY] AS [LST_YR_CAL_DT]
            ,DT.[FCL_DAY_DT_LY] AS [LST_YR_FSC_DT]
            ,DT.[WK_END_DT] AS [WKND_DT]
            ,DT.[WK_END_DT_LY] AS [LST_YR_WKND_DT]
            ,HOLS.[LY_HLDY_DAY_DT] AS [LST_YR_HLDY_DT]
            ,DT.[CLDR_DAY_OF_WK_ID] AS [CAL_DY_OF_WK_ID]
            ,DT.[CLDR_DAY_OF_WK_NM] AS [DY_OF_WK_NM]
			,DT.[CLDR_DAY_OF_WK_NM] AS [CAL_DY_OF_WK_NM]
            ,DT.[CLDR_MTH_OF_YR_NM] AS [CAL_MON_OF_YR_NM]
            ,DT.[CLDR_MTH_OF_YR_SHRT_NM] AS [CAL_MON_OF_THE_YR_SHRT_NM]
            ,DT.[CLDR_YR_ID] AS [CAL_YR]
			,DT.[CLDR_YR_ID] AS [CAL_TYP_ORDR_ID]
            ,DT.[FCL_YR_ID] AS [FSC_YR]
            ,DT.[FCL_YR_BEGN_DT] AS [FSC_YR_BGN_DT]
            ,DT.[FCL_YR_END_DT] AS [FSC_YR_END_DT]
            ,DT.[FCL_QTR_BEGN_DT] AS [FSC_QTR_BGN_DT]
            ,DT.[FCL_QTR_END_DT] AS [FSC_QTR_END_DT]
			,DT.[FCL_PER_SEQ_ID] AS [FSC_PRD_SEQ_ID]
			,DT.[FCL_QTR_SEQ_ID] AS [FSC_QTR_SEQ_ID]
            ,CAST( ISNULL([HLDY_DT_DSC], 'n/a') as varchar(255)) AS [HLDY_NM]
            ,CAST(convert(varchar,DT.[FCL_DAY_DT_LY], 112) as int) AS [LST_YR_FSC_DT_SK]
			,CAST(convert(varchar, CAST(HOLS.[LY_HLDY_DAY_DT] as date), 112) as int) AS [LST_YR_HLDY_DT_SK]
            ,CAST(convert(varchar,DT.[DAY_DT_LY], 112) as int) AS [LST_YR_CAL_DT_SK]
			,'Insert' as ETL_ACTN
			,@NEW_AUD_SKY	AS AUD_INS_SK
			,NULL 			AS AUD_UPD_SK
			,CAST(DATEPART(MONTH, DT.day_dt) as varchar(2)) + '/' +
			CAST(DATEPART(DAY, DT.day_dt) as varchar(2)) + ' (' + ltrim(rtrim(DT.cldr_day_of_wk_shrt_nm)) + ')' AS CAL_DT_NM
			,'Q' + Cast(FCL_QTR_OF_YR_ID AS VARCHAR(4)) + '-' + Substring(Cast(FCL_YR_ID AS CHAR(4)),3,2) 	AS FSC_QTR_NM
			,RTRIM(LTrim('P' + Cast(FCL_PER_OF_YR_ID AS VARCHAR(4))) + '-' + Substring(Cast(FCL_YR_ID AS CHAR(4)),3,2))   AS FSC_PRD_NM

            FROM [dm_edw].[DT_INF] AS DT
            LEFT JOIN
            (
                SELECT  [DAY_DT]
                    ,[LY_DAY_DT]
                    ,[LY_FCL_DAY_DT]
                    ,[LY_HLDY_DAY_DT]
                    ,[HLDY_DT_DSC]
                FROM [dm_edw].[HLDY_DT_XRF_INF]
               -- WHERE [HLDY_DT_DSC] IS NOT NULL AND [HLDY_DT_DSC] != ''

            ) AS HOLS
            ON HOLS.DAY_DT = DT.DAY_DT

        )
       UPDATE EDAA_DW.DIM_DLY_CAL
        SET LST_YR_HLDY_DT_SK = CTE.LST_YR_HLDY_DT_SK,
            HLDY_NM = CTE.HLDY_NM,
            LST_YR_HLDY_DT = CTE.LST_YR_HLDY_DT,
			ETL_ACTN = 'Update',
			AUD_UPD_SK=@NEW_AUD_SKY

        FROM CTE ,EDAA_DW.DIM_DLY_CAL  tm
        WHERE tm.DT_SK = CTE.DT_SK;


	WITH CTE AS
        (

            SELECT
			CAST(convert(varchar,DT.[DAY_DT],112) as int) AS [DT_SK]
            ,DT.[DAY_DT] AS CAL_DT
			,DT.[FCL_WK_END_DT_LY] AS [LST_YR_FSC_WKND_DT]
            ,DT.[DAY_DT_LY] AS [LST_YR_CAL_DT]
            ,DT.[FCL_DAY_DT_LY] AS [LST_YR_FSC_DT]
            ,DT.[WK_END_DT] AS [WKND_DT]
            ,DT.[WK_END_DT_LY] AS [LST_YR_WKND_DT]
            ,HOLS.[LY_HLDY_DAY_DT] AS [LST_YR_HLDY_DT]
            ,DT.[CLDR_DAY_OF_WK_ID] AS [CAL_DY_OF_WK_ID]
            ,DT.[CLDR_DAY_OF_WK_NM] AS [DY_OF_WK_NM]
			,DT.[CLDR_DAY_OF_WK_NM] AS [CAL_DY_OF_WK_NM]
            ,DT.[CLDR_MTH_OF_YR_NM] AS [CAL_MON_OF_YR_NM]
            ,DT.[CLDR_MTH_OF_YR_SHRT_NM] AS [CAL_MON_OF_THE_YR_SHRT_NM]
            ,DT.[CLDR_YR_ID] AS [CAL_YR]
			,DT.[CLDR_YR_ID] AS [CAL_TYP_ORDR_ID]
            ,DT.[FCL_YR_ID] AS [FSC_YR]
            ,DT.[FCL_YR_BEGN_DT] AS [FSC_YR_BGN_DT]
            ,DT.[FCL_YR_END_DT] AS [FSC_YR_END_DT]
            ,DT.[FCL_QTR_BEGN_DT] AS [FSC_QTR_BGN_DT]
            ,DT.[FCL_QTR_END_DT] AS [FSC_QTR_END_DT]
			,DT.[FCL_PER_SEQ_ID] AS [FSC_PRD_SEQ_ID]
			,DT.[FCL_QTR_SEQ_ID] AS [FSC_QTR_SEQ_ID]
            ,CAST( ISNULL([HLDY_DT_DSC], 'n/a') as varchar(255)) AS [HLDY_NM]
            ,CAST(convert(varchar,DT.[FCL_DAY_DT_LY], 112) as int) AS [LST_YR_FSC_DT_SK]
			,CAST(convert(varchar, CAST(HOLS.[LY_HLDY_DAY_DT] as date), 112) as int) AS [LST_YR_HLDY_DT_SK]
            ,CAST(convert(varchar,DT.[DAY_DT_LY], 112) as int) AS [LST_YR_CAL_DT_SK]
			,'Insert' as ETL_ACTN
			,@NEW_AUD_SKY	AS AUD_INS_SK
			,NULL 			AS AUD_UPD_SK
			,CAST(DATEPART(MONTH, DT.day_dt) as varchar(2)) + '/' +
			CAST(DATEPART(DAY, DT.day_dt) as varchar(2)) + ' (' + ltrim(rtrim(DT.cldr_day_of_wk_shrt_nm)) + ')' AS CAL_DT_NM
			,'Q' + Cast(FCL_QTR_OF_YR_ID AS VARCHAR(4)) + '-' + Substring(Cast(FCL_YR_ID AS CHAR(4)),3,2) 	AS FSC_QTR_NM
			,RTRIM(LTrim('P' + Cast(FCL_PER_OF_YR_ID AS VARCHAR(4))) + '-' + Substring(Cast(FCL_YR_ID AS CHAR(4)),3,2))   AS FSC_PRD_NM

            FROM [dm_edw].[DT_INF] AS DT
            LEFT JOIN
            (
                SELECT  [DAY_DT]
                    ,[LY_DAY_DT]
                    ,[LY_FCL_DAY_DT]
                    ,[LY_HLDY_DAY_DT]
                    ,[HLDY_DT_DSC]
                FROM [dm_edw].[HLDY_DT_XRF_INF]
               -- WHERE [HLDY_DT_DSC] IS NOT NULL AND [HLDY_DT_DSC] != ''

            ) AS HOLS
            ON HOLS.DAY_DT = DT.DAY_DT

        )

INSERT INTO EDAA_DW.DIM_DLY_CAL

	SELECT
		 CTE.[Dt_Sk]
		,CTE.[Cal_Dt]
		,CTE.[Cal_Dt_Nm]
		,CTE.[Lst_Yr_Fsc_Dt]
		,CTE.[Lst_Yr_Hldy_Dt]
		,CTE.[Hldy_Nm]
		,CTE.[Lst_Yr_Cal_Dt]
		,CTE.[Wknd_Dt]
		,CTE.[Lst_Yr_Wknd_Dt]
		,CTE.[Lst_Yr_Fsc_Wknd_Dt]
		,CTE.[Cal_Yr]
		,CTE.[Dy_Of_Wk_Nm]
		,CTE.[Fsc_Yr]
		,CTE.[Fsc_Yr_Bgn_Dt]
		,CTE.[Fsc_Yr_End_Dt]
		,CTE.[Fsc_Qtr_Nm]
		,CTE.[Fsc_Qtr_Bgn_Dt]
		,CTE.[Fsc_Qtr_End_Dt]
		,CTE.[Fsc_Prd_Nm]
		,CTE.[Fsc_Prd_Seq_Id]
		,CTE.[Fsc_Qtr_Seq_Id]
		,CTE.[Cal_Typ_Ordr_Id]
		,CTE.[Cal_Dy_Of_Wk_Id]
		,CTE.[Cal_Dy_Of_Wk_Nm]
		,CTE.[Cal_Mon_Of_Yr_Nm]
		,CTE.[Cal_Mon_Of_The_Yr_Shrt_Nm]
		,CTE.[Lst_Yr_Fsc_Dt_Sk]
		,CTE.[Lst_Yr_Cal_Dt_Sk]
		,CTE.[Lst_Yr_Hldy_Dt_Sk]
		,CTE.[Etl_Actn]
		,CTE.[Aud_Ins_Sk]
		,CTE.[Aud_Upd_Sk]
	FROM CTE
	WHERE NOT EXISTS (SELECT 1 FROM  [EDAA_DW].[DIM_DLY_CAL] T1 WHERE T1.[DT_SK]= CTE.[DT_SK]);

END

BEGIN

SELECT @NBR_OF_RW_ISRT = COUNT(1)  FROM [EDAA_DW].[DIM_DLY_CAL]
WHERE [AUD_INS_SK] = @NEW_AUD_SKY
SELECT @NBR_OF_RW_UDT = COUNT(1)  FROM [EDAA_DW].[DIM_DLY_CAL]
WHERE [AUD_UPD_SK] = @NEW_AUD_SKY

END


/*Audit Log End*/
EXEC [EDAA_CNTL].[SP_AUDIT_DATA_LOAD_END] @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT  = @NBR_OF_RW_UDT

END TRY

BEGIN CATCH
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = '[EDAA_ETL].[SP_REF_DW_DAILY_CAL_TABLELOAD]'
DECLARE @ERROR_LINE AS INT;
DECLARE @ERROR_MSG AS NVARCHAR(max);

 SELECT
      @ERROR_LINE =  ERROR_NUMBER()
       ,@ERROR_MSG = ERROR_MESSAGE();
--------- Log execution error ----------



EXEC [EDAA_CNTL].[SP_LOG_AUD_ERR]
@AUD_SKY = @NEW_AUD_SKY,
@ERROR_PROCEDURE_NAME = @ERROR_PROCEDURE_NAME,
@ERROR_LINE = @ERROR_LINE,
@ERROR_MSG = @ERROR_MSG;


   THROW;




END CATCH
GO
