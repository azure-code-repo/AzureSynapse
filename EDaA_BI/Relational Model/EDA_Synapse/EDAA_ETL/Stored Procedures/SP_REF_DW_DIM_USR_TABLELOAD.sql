/****** Object:  StoredProcedure [EDAA_ETL].[SP_REF_DW_DIM_USR_TABLELOAD_New]    Script Date: 10/26/2021 7:51:15 AM ******/
CREATE PROC [EDAA_ETL].[SP_REF_DW_DIM_USR_TABLELOAD] AS

/*
 =============================================
 Author:        Krishna Kumar Shaw
 Create date:   20-Apr-2021
 Description:   Stored proc truncate and load Hourly User dimension table.
 =============================================
 Change History
 20-Apr-2021   KKS      Initial version
 23-Apr-2021	SS		Updated UNION ALL to UNION to fix the duplicate user issue.
 31-May-2021    MS      Updated the col_names for [EDAA_STG_EMP].[CUST]
 22-Jun-2021    MS	    Changed Secured schema "EDAA_STG_EMP" to unsecured schema "EDAA_STG"
 21-Oct-2021    SS      Mapped as per workday changes
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
,@EXEC_JOB  = 'SP_REF_DW_DIM_USR_TABLELOAD'
,@SRC_ENTY  = 'DIM_USER'
,@TGT_ENTY = 'DIM_USER'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

-------------------------------------------------------

INSERT INTO EDAA_DW.DIM_USR
SELECT
	E.Emp_Id AS Usr_Id,
	E.Str_Id AS Str_Id,
	'Store' AS Usr_Rl_Nm,
	Store.Mkt_Id AS Mkt_Id,
	Store.Rgn_Id AS Rgn_Id,
	@NEW_AUD_SKY AS Aud_Ins_sk,
	NULL AS Aud_Upd_sk
FROM
(
SELECT
DISTINCT
	L.Emp_Id,
	L.Loc_Id,
	L.Loc_Typ_Id
FROM
	[EDAA_STG].[EMP_LOC] L WITH(NOLOCK)
INNER JOIN
(
SELECT
DISTINCT
	E.Emp_Id,S.[EMP_STAT_TYP_ID]
FROM
	[EDAA_STG].[EMP_STAT] S WITH(NOLOCK)
	--INNER JOIN [EDAA_STG].[EMP] Cust ON Cust.Emp_Id=S.Emp_Id
	INNER JOIN [EDAA_STG].[EMP_ASSC] E WITH(NOLOCK) ON S.Emp_Id = E. Emp_Id
	AND S.[EMP_STAT_TYP_ID] = 62
	AND E.Assc_Id=1
) AS B ON L.Emp_Id = B.Emp_Id
WHERE
--Emp_Loc_Note <> 'Store'
--and
L.Loc_Typ_Id = 4 ) AS D
	INNER JOIN [EDAA_STG].[EMP] E ON E.Emp_Id = D.Emp_Id
	INNER JOIN [EDAA_REF].[DIM_UT_INF_Temp] Store ON E.Str_Id=Store.Str_Id
UNION
SELECT
	Usr_Id,
	Str_Id,
	Usr_Rl_Nm,
	Mkt_Id,
	Rgn_Id,
	@NEW_AUD_SKY AS Aud_Ins_sk,
	NULL AS Aud_Upd_sk
FROM
	EDAA_REF.DIM_USR_Temp

BEGIN

SELECT @NBR_OF_RW_ISRT = COUNT(1)  FROM EDAA_DW.DIM_USR WHERE [Aud_Ins_Sk] = @NEW_AUD_SKY
SELECT @NBR_OF_RW_UDT  = COUNT(1)  FROM EDAA_DW.DIM_USR WHERE [Aud_Upd_Sk] = @NEW_AUD_SKY

------------ UPDATE Statistics-------

UPDATE STATISTICS  EDAA_DW.DIM_USR;

END

/*Audit Log End*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT  = @NBR_OF_RW_UDT

END TRY
BEGIN CATCH
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = 'EDAA_ETL.SP_REF_DW_DIM_USR_TABLELOAD'
DECLARE @ERROR_LINE AS INT;
DECLARE @ERROR_MSG AS NVARCHAR(MAX);

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
