
CREATE PROC [ETL].[PROC_DIM_TM_TABLELOAD] AS


 BEGIN TRY
 /*Datamart Auditing variables*/
DECLARE @NEW_AUD_SKY BIGINT
DECLARE @NBR_OF_RW_ISRT INT
DECLARE @NBR_OF_RW_UDT INT
DECLARE @EXEC_LYR VARCHAR(255)
DECLARE @EXEC_JOB VARCHAR(500)
DECLARE @SRC_ENTY VARCHAR(500)
DECLARE @TGT_ENTY VARCHAR(500)

/*Audit Log Start*/
EXEC etl.proc_AUDIT_DATA_LOAD_START
 @EXEC_LYR  = 'DM'
,@EXEC_JOB  = 'PROC_DIM_TM_TABLELOAD'
,@SRC_ENTY  = 'DT_INF & HLDY_DT_XRF_INF'
,@TGT_ENTY = 'DIM_TM'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT


BEGIN

		with CTE as
        (

            SELECT
			CAST(convert(varchar,DT.[DAY_DT],112) as int) AS [TM_SKY]
            ,DT.[DAY_DT]
            ,DT.[DAY_DT_LY]
            ,DT.[FCL_DAY_DT_LY]
            ,DT.[WK_END_DT]
            ,DT.[WK_END_DT_LY]
            ,DT.[FCL_WK_END_DT_LY]
            ,HOLS.[LY_HLDY_DAY_DT]
            ,DT.[CLDR_DAY_OF_WK_ID]
            ,DT.[CLDR_DAY_OF_WK_NM]
            ,DT.[CLDR_DAY_OF_WK_SHRT_NM]
            ,DT.[CLDR_MTH_OF_YR_ID]
            ,DT.[CLDR_MTH_OF_YR_NM]
            ,DT.[CLDR_MTH_OF_YR_SHRT_NM]
            ,DT.[CLDR_YR_ID]
            ,DT.[FCL_YR_ID]
            ,DT.[FCL_YR_BEGN_DT]
            ,DT.[FCL_YR_END_DT]
            ,DT.[FCL_QTR_BEGN_DT]
            ,DT.[FCL_QTR_END_DT]
            ,DT.[FCL_PER_BEGN_DT_LY]
            ,DT.[FCL_PER_END_DT_LY]
			,CASE WHEN HOLS.[DAY_DT] IS NULL THEN CAST(0 as bit) ELSE CAST(1 as bit) END AS IS_HLDY
            ,CAST( ISNULL([HLDY_DT_DSC], 'n/a') as varchar(255)) AS [HLDY_DT_DSC]
            ,CAST(convert(varchar,DT.[FCL_DAY_DT_LY], 112) as int) AS TM_SKY_LY_FSC
			,CAST(convert(varchar, CAST(HOLS.[LY_HLDY_DAY_DT] as date), 112) as int) AS TM_SKY_LY_HLDY
            ,CAST(convert(varchar,DT.[DAY_DT_LY], 112) as int) AS TM_SKY_LY_CLDR
			,0 as IS_DMY
			,'Insert' as ETL_ACT
			,@NEW_AUD_SKY	AS AUD_INS_SKY
			,NULL 			AS AUD_UPD_SKY

            FROM [dm_edw].[DT_INF] AS DT
            LEFT JOIN
            (
                SELECT  [DAY_DT]
                    ,[LY_DAY_DT]
                    ,[LY_FCL_DAY_DT]
                    ,[LY_HLDY_DAY_DT]
                    ,[HLDY_DT_DSC]
                FROM [dm_edw].[HLDY_DT_XRF_INF]
                WHERE [HLDY_DT_DSC] IS NOT NULL AND [HLDY_DT_DSC] != ''

            ) AS HOLS
            ON HOLS.DAY_DT = DT.DAY_DT

        )
       update [dm].[DIM_TM]

        set TM_SKY_LY_HLDY = CTE.TM_SKY_LY_HLDY,
            IS_HLDY = CTE.IS_HLDY,
            HLDY_DT_DSC = CTE.HLDY_DT_DSC,
            LY_HLDY_DAY_DT = CTE.LY_HLDY_DAY_DT,
			ETL_ACT = 'Update',
			AUD_UPD_SKY=@NEW_AUD_SKY

        from CTE , dm.DIM_TM tm
        where tm.TM_SKY = CTE.TM_SKY;

	with CTE as
        (

            SELECT
			CAST(convert(varchar,DT.[DAY_DT],112) as int) AS [TM_SKY]
            ,DT.[DAY_DT]
            ,DT.[DAY_DT_LY]
            ,DT.[FCL_DAY_DT_LY]
            ,DT.[WK_END_DT]
            ,DT.[WK_END_DT_LY]
            ,DT.[FCL_WK_END_DT_LY]
            ,HOLS.[LY_HLDY_DAY_DT]
            ,DT.[CLDR_DAY_OF_WK_ID]
            ,DT.[CLDR_DAY_OF_WK_NM]
            ,DT.[CLDR_DAY_OF_WK_SHRT_NM]
            ,DT.[CLDR_MTH_OF_YR_ID]
            ,DT.[CLDR_MTH_OF_YR_NM]
            ,DT.[CLDR_MTH_OF_YR_SHRT_NM]
            ,DT.[CLDR_YR_ID]
            ,DT.[FCL_YR_ID]
            ,DT.[FCL_YR_BEGN_DT]
            ,DT.[FCL_YR_END_DT]
            ,DT.[FCL_QTR_BEGN_DT]
            ,DT.[FCL_QTR_END_DT]
            ,DT.[FCL_PER_BEGN_DT_LY]
            ,DT.[FCL_PER_END_DT_LY]
			,CASE WHEN HOLS.[DAY_DT] IS NULL THEN CAST(0 as bit) ELSE CAST(1 as bit) END AS IS_HLDY
            ,CAST( ISNULL([HLDY_DT_DSC], 'n/a') as varchar(255)) AS [HLDY_DT_DSC]
            ,CAST(convert(varchar,DT.[FCL_DAY_DT_LY], 112) as int) AS TM_SKY_LY_FSC
			,CAST(convert(varchar, CAST(HOLS.[LY_HLDY_DAY_DT] as date), 112) as int) AS TM_SKY_LY_HLDY
            ,CAST(convert(varchar,DT.[DAY_DT_LY], 112) as int) AS TM_SKY_LY_CLDR
			,0 as IS_DMY
			,'Insert' as ETL_ACT
			,@NEW_AUD_SKY	AS AUD_INS_SKY
			,NULL 			AS AUD_UPD_SKY

            FROM [dm_edw].[DT_INF] AS DT
            LEFT JOIN
            (
                SELECT  [DAY_DT]
                    ,[LY_DAY_DT]
                    ,[LY_FCL_DAY_DT]
                    ,[LY_HLDY_DAY_DT]
                    ,[HLDY_DT_DSC]
                FROM [dm_edw].[HLDY_DT_XRF_INF]
                WHERE [HLDY_DT_DSC] IS NOT NULL AND [HLDY_DT_DSC] != ''

            ) AS HOLS
            ON HOLS.DAY_DT = DT.DAY_DT
        )

		Insert into [dm].[DIM_TM]

		SELECT
			CTE.TM_SKY
            ,CTE.DAY_DT
            ,CTE.DAY_DT_LY
            ,CTE.FCL_DAY_DT_LY
            ,CTE.WK_END_DT
            ,CTE.WK_END_DT_LY
            ,CTE.FCL_WK_END_DT_LY
            ,CTE.LY_HLDY_DAY_DT
            ,CTE.CLDR_DAY_OF_WK_ID
            ,CTE.CLDR_DAY_OF_WK_NM
            ,CTE.CLDR_DAY_OF_WK_SHRT_NM
            ,CTE.CLDR_MTH_OF_YR_ID
            ,CTE.CLDR_MTH_OF_YR_NM
            ,CTE.CLDR_MTH_OF_YR_SHRT_NM
            ,CTE.CLDR_YR_ID
            ,CTE.FCL_YR_ID
            ,CTE.FCL_YR_BEGN_DT
            ,CTE.FCL_YR_END_DT
            ,CTE.FCL_QTR_BEGN_DT
            ,CTE.FCL_QTR_END_DT
            ,CTE.FCL_PER_BEGN_DT_LY
            ,CTE.FCL_PER_END_DT_LY
			,CTE.TM_SKY_LY_FSC
			,CTE.TM_SKY_LY_HLDY
			,CTE.TM_SKY_LY_CLDR
			,CTE.IS_HLDY
            ,CTE.HLDY_DT_DSC
            ,CTE.ETL_ACT
			,CTE.AUD_INS_SKY
			,CTE.AUD_UPD_SKY
		from CTE
		where not exists (select 1 from  [DM].[DIM_TM] T1 where T1.[TM_SKY]= CTE.[TM_SKY])
		;


END

BEGIN

SELECT @NBR_OF_RW_ISRT = COUNT(1)  FROM [DM].DIM_TM
WHERE [AUD_INS_SKY] = @NEW_AUD_SKY
SELECT @NBR_OF_RW_UDT = COUNT(1)  FROM [DM].DIM_TM
WHERE [AUD_UPD_SKY] = @NEW_AUD_SKY

END



/*Audit Log End*/
EXEC etl.proc_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT  = @NBR_OF_RW_UDT

END TRY

BEGIN CATCH
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = '[ETL].[PROC_DIM_TM_TABLELOAD]'
DECLARE @ERROR_LINE AS INT;
DECLARE @ERROR_MSG AS NVARCHAR(max);

 SELECT
      @ERROR_LINE =  ERROR_NUMBER()
       ,@ERROR_MSG = ERROR_MESSAGE();
--------- Log execution error ----------



EXEC [ETL].[LOG_AUD_ERR]
@AUD_SKY = @NEW_AUD_SKY,
@ERROR_PROCEDURE_NAME = @ERROR_PROCEDURE_NAME,
@ERROR_LINE = @ERROR_LINE,
@ERROR_MSG = @ERROR_MSG;


   THROW;




END CATCH
GO
