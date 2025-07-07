CREATE PROC [EDAA_ETL].[SP_DW_PRES_FCT_HRLY_PROD_SUB_CTGRY_SLS_CALC_TABLELOAD] AS

/*
 =============================================
 Author:        Shrikant Sakpal
 Create date:   20-Mar-2021
 Description:   This SP Truncate and Load every hour run to trasform the  TY , CY and Plan data
				on store, L3 Product and Hour grain and load to PRES Fact table.
				Data volumne ~3.5 million. It will update the stats of the table after insert
 =============================================
 Change History
 20-Mar-2021	SS      Initial Version
 15-Apr-2021	SS		Updated the LY and CY CTE to consider new L3 from Buyer table.
 03-Jun-2021	SS		Updated LY CTE with LEFT join and COALESCE to consider all thr L3's from LY Fiscal and Holiday.
 22-FEB-2023	MAJJI	CHECKING WITH BUYER TABLE TO MAKE SURE WE HAVE VALID BUYER FOR EACH L3'S
 04-may-2023	MAJJI	CHECKING WITH BUYER TABLE TO MAKE SURE WE HAVE VALID BUYER FOR EACH L3'S and having extracheck to make sure it's active in DIM_prod
*/

BEGIN TRY

SET NOCOUNT ON
SET XACT_ABORT ON

/*Datamart Auditing variables*/
DECLARE @NEW_AUD_SKY BIGINT ;
DECLARE @NBR_OF_RW_ISRT INT ;
DECLARE @NBR_OF_RW_UDT INT ;
DECLARE @EXEC_LYR VARCHAR(255) ;
DECLARE @EXEC_JOB VARCHAR(500) ;
DECLARE @SRC_ENTY VARCHAR(500) ;
DECLARE @TGT_ENTY VARCHAR(500) ;

DECLARE @CntrlCode1 VARCHAR(250)='L6';
DECLARE @CntrlValue1 VARCHAR(1000);
DECLARE @CntrlCode2 VARCHAR(250)='L2';
DECLARE @CntrlValue2 VARCHAR(1000);
DECLARE @CntrlCode3 VARCHAR(250)='div_id';
DECLARE @CntrlValue3 VARCHAR(1000);
DECLARE @DatetimeHour AS DATETIME=CONVERT(DATE,SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time');
DECLARE @Cal_Date_Key AS INT = CONVERT(CHAR(8),(SELECT DISTINCT CAL_DT FROM [EDAA_ETL].[V_STG_DW_DAY_DT_HR_INF]),112);

/*Audit Log Start*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START
 @EXEC_LYR  = 'EDAA_PRES'
,@EXEC_JOB  = 'SP_STG_DW_FCT_HRLY_PROD_SUB_CTGRY_SLS_CALC'
,@SRC_ENTY  = 'EDAA_STG.FCT_HRLY_PROD_CURR_YR_SLS'
,@TGT_ENTY = 'FCT_HRLY_PROD_SUB_CTGRY_SLS_CALC'
,@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT


BEGIN

------------ Truncate and Load the Table at every call of Stored Procedure-------

TRUNCATE TABLE EDAA_PRES.FCT_HRLY_PROD_SUB_CTGRY_SLS_CALC;

--------- SET [Lvl6_Mds_Area_Id] & [Lvl2_Prod_Clsfctn_Id] Value------------------


SELECT @CntrlValue1=CntrlValue FROM EDAA_CNTL.ETL_KEY_VAL_PAIRS WHERE CntrlCode=@CntrlCode1;
SELECT @CntrlValue2=CntrlValue FROM EDAA_CNTL.ETL_KEY_VAL_PAIRS WHERE CntrlCode=@CntrlCode2;
SELECT @CntrlValue3=CntrlValue FROM EDAA_CNTL.ETL_KEY_VAL_PAIRS WHERE CntrlCode=@CntrlCode3;

WITH CTE_PRODLvl6MdsAreaId
AS
(
SELECT
	   P.[Prod_Hist_Sk]
      ,P.[Prod_Sk]
      ,P.[Itm_Sku]
      ,P.[Lvl1_Prod_Id]
      ,P.[Lvl2_Prod_Clsfctn_Id]
      ,P.[Lvl3_Prod_Sub_Ctgry_Id]
      ,P.[Lvl4_Prod_Ctgry_Id]
      ,P.[Lvl5_Bsns_Sgmt_Id]
      ,P.[Lvl6_Mds_Area_Id]
	  ,P.[Fnc_Lvl1_Prod_Ctgry_Id]
      ,P.[Fnc_Lvl2_Mprs_Ctgry_Id]
      ,P.[Fnc_Lvl3_Pky_Id]
      ,P.[Fnc_Lvl4_Area_Id]
      ,P.[Is_Curr_Ind]
FROM
	[EDAA_DW].[DIM_PROD] P
WHERE
P.[Lvl6_Mds_Area_Id] IN (SELECT VALUE FROM STRING_SPLIT(@CntrlValue1, ','))
	AND P.[Lvl2_Prod_Clsfctn_Id] NOT IN (SELECT VALUE FROM STRING_SPLIT(@CntrlValue2, ','))
),


CTE_LstYrFscMetrics
AS
(
SELECT
	CAL.Dt_Tm_Hr,
	LY.Str_Id,
	LY.Lvl3_Prod_Sub_Ctgry_Id,
	L3.FNC_LVL2_MPRS_CTGRY_ID,
	L3.FNC_LVL3_PKY_ID,
	SUM(LY.Sls_Amt) AS Lst_Yr_Fsc_Sls_Amt,
	SUM(LY.Drct_Mgn_Amt) AS Lst_Yr_Fsc_Prm_To_Prm_Drct_Mgn_Amt,
	SUM(LY.Sls_Qty) AS Lst_Yr_Fsc_Sls_Qty,
	SUM(LY.Prm_To_Sls_Amt) AS Lst_Yr_Fsc_Prm_To_Sls_Amt
FROM
	EDAA_DW.FCT_HRLY_LST_YR_SLS LY
	RIGHT JOIN [EDAA_ETL].[V_STG_DW_DAY_DT_HR_INF] AS CAL ON
					 CONVERT(DATETIME2(0),LY.DT_TM_HR) = CAL.LST_YR_FSC_DT_TM_HR
	INNER JOIN
		(SELECT  DISTINCT P.FNC_LVL2_MPRS_CTGRY_ID,
					  P.FNC_LVL3_PKY_ID,
					  P.LVL3_PROD_SUB_CTGRY_ID
		FROM [EDAA_DW].[DIM_PROD] AS P
		INNER JOIN [EDAA_PRES].[DIM_BYR] B ON B.LVL3_PROD_SUB_CTGRY_ID = P.LVL3_PROD_SUB_CTGRY_ID
		WHERE P.IS_CURR_IND = 1 AND B.IS_CURR_IND = 1
		) L3
		ON LY.Lvl3_Prod_Sub_Ctgry_Id = L3.LVL3_PROD_SUB_CTGRY_ID
WHERE
	LY.Dt_Tm_Hr IS NOT NULL
GROUP BY
	CAL.Dt_Tm_Hr,
	LY.Str_Id,
	LY.Lvl3_Prod_Sub_Ctgry_Id,
	L3.FNC_LVL2_MPRS_CTGRY_ID,
	L3.FNC_LVL3_PKY_ID
),


CTE_LstYrHldyMetrics
AS
(
SELECT
	CAL.Dt_Tm_Hr,
	LY.Str_Id,
	LY.Lvl3_Prod_Sub_Ctgry_Id,
	L3.FNC_LVL2_MPRS_CTGRY_ID,
	L3.FNC_LVL3_PKY_ID,
	SUM(LY.Sls_Amt) AS Lst_Yr_Hldy_Sls_Amt,
	SUM(LY.Drct_Mgn_Amt) AS Lst_Yr_Hldy_Prm_To_Prm_Drct_Mgn_Amt,
	SUM(LY.Sls_Qty) AS Lst_Yr_Hldy_Sls_Qty,
	SUM(LY.Prm_To_Sls_Amt) AS Lst_Yr_Hldy_Prm_To_Sls_Amt
FROM
	EDAA_DW.FCT_HRLY_LST_YR_SLS LY
	RIGHT JOIN [EDAA_ETL].[V_STG_DW_DAY_DT_HR_INF] AS CAL ON
					 CONVERT(DATETIME2(0),LY.DT_TM_HR) = CAL.LST_YR_HLDY_DT_TM_HR
	INNER JOIN
		(SELECT  DISTINCT P.FNC_LVL2_MPRS_CTGRY_ID,
					  P.FNC_LVL3_PKY_ID,
					  P.LVL3_PROD_SUB_CTGRY_ID
		FROM [EDAA_DW].[DIM_PROD] AS P
		INNER JOIN [EDAA_PRES].[DIM_BYR] B ON B.LVL3_PROD_SUB_CTGRY_ID = P.LVL3_PROD_SUB_CTGRY_ID
		WHERE P.IS_CURR_IND = 1 AND B.IS_CURR_IND = 1
		) L3
		ON LY.Lvl3_Prod_Sub_Ctgry_Id = L3.LVL3_PROD_SUB_CTGRY_ID
WHERE
	LY.Dt_Tm_Hr IS NOT NULL
GROUP BY
	CAL.Dt_Tm_Hr,
	LY.Str_Id,
	LY.Lvl3_Prod_Sub_Ctgry_Id,
	L3.FNC_LVL2_MPRS_CTGRY_ID,
	L3.FNC_LVL3_PKY_ID
),

CTE_LY_L3
AS
(
SELECT
	COALESCE(F.DT_TM_HR, H.DT_TM_HR) AS DT_TM_HR,
	COALESCE(F.Str_Id, H.Str_Id) AS Str_Id,
	COALESCE(F.Lvl3_Prod_Sub_Ctgry_Id, H.Lvl3_Prod_Sub_Ctgry_Id) AS Lvl3_Prod_Sub_Ctgry_Id,
	COALESCE(F.FNC_LVL2_MPRS_CTGRY_ID, H.FNC_LVL2_MPRS_CTGRY_ID) AS FNC_LVL2_MPRS_CTGRY_ID,
	COALESCE(F.FNC_LVL3_PKY_ID, H.FNC_LVL3_PKY_ID) AS FNC_LVL3_PKY_ID,
	F.Lst_Yr_Fsc_Sls_Amt,
	F.Lst_Yr_Fsc_Prm_To_Prm_Drct_Mgn_Amt,
	F.Lst_Yr_Fsc_Sls_Qty,
	F.Lst_Yr_Fsc_Prm_To_Sls_Amt,
	H.Lst_Yr_Hldy_Sls_Amt,
	H.Lst_Yr_Hldy_Prm_To_Prm_Drct_Mgn_Amt,
	H.Lst_Yr_Hldy_Sls_Qty,
	H.Lst_Yr_Hldy_Prm_To_Sls_Amt
FROM
	CTE_LstYrFscMetrics AS F
	FULL OUTER JOIN CTE_LstYrHldyMetrics AS H
	ON F.DT_TM_HR = H.DT_TM_HR
		AND F.Lvl3_Prod_Sub_Ctgry_Id = H.Lvl3_Prod_Sub_Ctgry_Id
			AND F.Str_Id = H.Str_Id
),

CTE_LY
AS
(
SELECT
	CTE.DT_TM_HR,
	CTE.Str_Id,
	--CTE.Lvl3_Prod_Sub_Ctgry_Id,
	COALESCE(B.MJR_P_SUB_CT_ID, CTE.Lvl3_Prod_Sub_Ctgry_Id) AS Lvl3_Prod_Sub_Ctgry_Id,
	CTE.Lst_Yr_Fsc_Sls_Amt,
	CTE.Lst_Yr_Fsc_Prm_To_Prm_Drct_Mgn_Amt,
	CTE.Lst_Yr_Fsc_Sls_Qty,
	CTE.Lst_Yr_Fsc_Prm_To_Sls_Amt,
	CTE.Lst_Yr_Hldy_Sls_Amt,
	CTE.Lst_Yr_Hldy_Prm_To_Prm_Drct_Mgn_Amt,
	CTE.Lst_Yr_Hldy_Sls_Qty,
	CTE.Lst_Yr_Hldy_Prm_To_Sls_Amt
FROM
	CTE_LY_L3 AS CTE
    LEFT JOIN [EDAA_REF].[BYR_EMP_PKY_MPRS_CURRPREV_INF] AS B
	ON CTE.FNC_LVL3_PKY_ID = B.PKY_ID AND CTE.FNC_LVL2_MPRS_CTGRY_ID = B.MPRS_CT_ID
),


CTE_TY
AS
(

SELECT
	Curr.Dt_Tm_Hr,
	Curr.Str_Id,
	--P.Lvl3_Prod_Sub_Ctgry_Id,
	B.MJR_P_SUB_CT_ID AS Lvl3_Prod_Sub_Ctgry_Id,
	P.Fnc_Lvl3_Pky_Id,
	Curr.Mprs_Ctgry_Id,
	SUM(Curr.Sls_Amt) AS Sls_Amt,
	SUM(Curr.Prm_To_Sls_Amt) AS Prm_To_Sls_Amt,
	SUM(Curr.Prm_To_Prm_Drct_Mgn_Amt) AS Prm_To_Prm_Drct_Mgn_Amt,
	SUM(Curr.Sls_Qty) AS Sls_Qty
FROM
	[EDAA_DW].[FCT_HRLY_PROD_CURR_YR_SLS] AS Curr
	INNER JOIN CTE_PRODLvl6MdsAreaId AS P ON Curr.Prod_Hist_Sk = P.Prod_Hist_Sk
	INNER JOIN [EDAA_REF].[BYR_EMP_PKY_MPRS_CURRPREV_INF] AS B on Curr.Fnc_Lvl3_Pky_Id = B.PKY_ID AND Curr.Mprs_Ctgry_Id = B.MPRS_CT_ID
	INNER JOIN [EDAA_ETL].[V_STG_DW_DAY_DT_HR_INF] AS CAL ON
					 CONVERT(DATETIME2(0),Curr.DT_TM_HR) = CAL.DT_TM_HR
WHERE
	P.Lvl3_Prod_Sub_Ctgry_Id IS NOT NULL AND Curr.Str_Id IS NOT NULL
GROUP BY
	Curr.Dt_Tm_Hr,
	Curr.Str_Id,
	B.MJR_P_SUB_CT_ID,
	P.Fnc_Lvl3_Pky_Id,
	Curr.Mprs_Ctgry_Id
),

CTE_TY_MPRK_ADJ
AS
(
SELECT
	TY.DT_TM_HR,
	TY.Str_Id,
	TY.Lvl3_Prod_Sub_Ctgry_Id,
	SUM(TY.Sls_Amt) AS Sls_Amt,
	SUM(TY.Prm_To_Prm_Drct_Mgn_Amt) AS Prm_To_Prm_Drct_Mgn_Amt,
	SUM(TY.Sls_Qty) AS Sls_Qty,
	SUM(TY.Prm_To_Sls_Amt) AS Prm_To_Sls_Amt,
	SUM(Cast (Coalesce(Cast((TY.Sls_Amt * adj.[Mprk_Adj_Prcntg] ) AS DECIMAL (18,4)), TY.Sls_Amt)  AS DECIMAL (18,4))) AS Mprk_Sls_Amt,
	SUM(Cast(Coalesce(TY.[Prm_To_Prm_Drct_Mgn_Amt] - Cast((TY.Sls_Amt * (1 - adj.[Mprk_Adj_Prcntg])) AS DECIMAL(18,4)), TY.[Prm_To_Prm_Drct_Mgn_Amt])  AS DECIMAL(18,4) )) AS Mprk_Prm_To_Prm_Drct_Mgn_Amt,
	SUM(Cast (Coalesce(TY.[Prm_To_Sls_Amt] - Cast((TY.Sls_Amt*(1-adj.[Mprk_Adj_Prcntg])) AS DECIMAL(18,4)), TY.[Prm_To_Sls_Amt])  AS DECIMAL(18,4))) AS [Mprk_Prm_To_Sls_Amt]
FROM
CTE_TY  AS TY
	LEFT JOIN
	(
	SELECT
		adj.[Fnc_Lvl3_Pky_Id],
		adj.[Dt_Sk],
		[Mprk_Adj_Prcntg],
		Mprs.Fnc_Lvl2_Mprs_Ctgry_Id,
		Geo.Str_Id
	FROM
		[EDAA_DW].[FCT_WKND_PKY_MPRK_ADJ] as adj
		INNER JOIN
		(
				SELECT
				[Fnc_Lvl2_Mprs_Ctgry_Id],
				[Fnc_Lvl3_Pky_Id], count(1) as Records
				FROM
				[EDAA_DW].[DIM_PROD] P
				INNER JOIN [EDAA_PRES].[DIM_BYR] B ON B.LVL3_PROD_SUB_CTGRY_ID=P.LVL3_PROD_SUB_CTGRY_ID
				WHERE B.IS_CURR_IND = 1 AND P.IS_CURR_IND = 1
				GROUP BY
					[Fnc_Lvl2_Mprs_Ctgry_Id],
					[Fnc_Lvl3_Pky_Id]
		) AS Mprs
			ON Mprs.Fnc_Lvl3_Pky_Id = adj.Fnc_Lvl3_Pky_Id
			AND adj.Dt_Sk = @Cal_Date_Key

	CROSS JOIN
	(
	SELECT
		div_id,
		div_nm,
		str_id,
		count(1) AS Records
	FROM [EDAA_DW].[DIM_GEO]
	WHERE
		div_id IN (SELECT VALUE FROM STRING_SPLIT(@CntrlValue3, ','))
		AND [Str_Cls_Dt] IS NULL
	GROUP BY
		div_id,
		div_nm,
		str_id
	) AS Geo
	) AS adj
	ON adj.[Fnc_Lvl3_Pky_Id] = TY.[Fnc_Lvl3_Pky_Id]
	AND adj.Fnc_Lvl2_Mprs_Ctgry_Id = TY.Mprs_Ctgry_Id
	AND TY.Str_Id = adj.Str_Id
GROUP BY
	TY.DT_TM_HR,
	TY.Str_Id,
	TY.Lvl3_Prod_Sub_Ctgry_Id
),


CTE_TYLY
AS
(
SELECT
	COALESCE(L.DT_TM_HR, T.DT_TM_HR) AS DT_TM_HR,
	COALESCE(L.Str_Id, T.Str_Id) AS Str_Id,
	COALESCE(L.Lvl3_Prod_Sub_Ctgry_Id, T.Lvl3_Prod_Sub_Ctgry_Id) AS Lvl3_Prod_Sub_Ctgry_Id,
	--T.Fnc_Lvl3_Pky_Id,
	--T.Mprs_Ctgry_Id,
	T.[Sls_Amt],
	T.[Prm_To_Prm_Drct_Mgn_Amt],
	T.[Sls_Qty],
	T.[Prm_To_Sls_Amt],
	T.Mprk_Sls_Amt,
	T.Mprk_Prm_To_Prm_Drct_Mgn_Amt,
	T.[Mprk_Prm_To_Sls_Amt],
	L.Lst_Yr_Fsc_Sls_Amt,
	L.Lst_Yr_Fsc_Prm_To_Prm_Drct_Mgn_Amt,
	L.Lst_Yr_Fsc_Sls_Qty,
	L.Lst_Yr_Fsc_Prm_To_Sls_Amt,
	L.Lst_Yr_Hldy_Sls_Amt,
	L.Lst_Yr_Hldy_Prm_To_Prm_Drct_Mgn_Amt,
	L.Lst_Yr_Hldy_Sls_Qty,
	L.Lst_Yr_Hldy_Prm_To_Sls_Amt
FROM
	CTE_LY AS L
	FULL OUTER JOIN CTE_TY_MPRK_ADJ AS T
	ON L.DT_TM_HR = T.DT_TM_HR
		AND L.Lvl3_Prod_Sub_Ctgry_Id = T.Lvl3_Prod_Sub_Ctgry_Id AND L.Str_Id = T.Str_Id
),


CTE_SLS_FINAL
AS
(
SELECT
	COALESCE(Adj.Lvl3_Prod_Sub_Ctgry_Id,PL.Lvl3_Prod_Sub_Ctgry_Id) AS Lvl3_Prod_Sub_Ctgry_Id,
	COALESCE(Adj.DT_TM_HR, PL.DT_TM_HR) AS Dt_Tm_Hr,
	COALESCE(Adj.Str_Id, PL.Str_Id) AS Str_Id,
	Adj.Sls_Amt,
	Adj.Prm_To_Sls_Amt,
	Adj.Prm_To_Prm_Drct_Mgn_Amt,
	Adj.Sls_Qty,
	Adj.Mprk_Sls_Amt,
	Adj.Mprk_Prm_To_Sls_Amt,
	Adj.Mprk_Prm_To_Prm_Drct_Mgn_Amt,
	Adj.Lst_Yr_Fsc_Sls_Amt,
	Adj.Lst_Yr_Fsc_Prm_To_Prm_Drct_Mgn_Amt,
	Adj.Lst_Yr_Fsc_Sls_Qty,
	Adj.Lst_Yr_Fsc_Prm_To_Sls_Amt,
	Adj.Lst_Yr_Hldy_Sls_Amt,
	Adj.Lst_Yr_Hldy_Prm_To_Prm_Drct_Mgn_Amt,
	Adj.Lst_Yr_Hldy_Sls_Qty,
	Adj.Lst_Yr_Hldy_Prm_To_Sls_Amt,
	PL.Pln_Sls_Amt AS Pln_Sls_Amt --

FROM
	CTE_TYLY AS Adj
	FULL OUTER JOIN [EDAA_STG].[FCT_DW_STG_HRLY_SLS_PLN] AS PL
		ON CONVERT(DATETIME2(0),Adj.DT_TM_HR) = CONVERT(DATETIME2(0),PL.Dt_Tm_Hr)
			AND Adj.Lvl3_Prod_Sub_Ctgry_Id = PL.Lvl3_Prod_Sub_Ctgry_Id
			AND Adj.Str_Id = PL.Str_Id

)


-- Final Select Query

INSERT INTO EDAA_PRES.FCT_HRLY_PROD_SUB_CTGRY_SLS_CALC(
	Geo_Hist_Sk,
	Byr_L3_Hist_Sk,
	Lvl3_Prod_Sub_Ctgry_Id,
	Dt_Tm_Hr,
	Byr_L3_Sk,
	Geo_Sk,
	Str_Id,
	Sls_Amt,
	Prm_To_Sls_Amt,
	Prm_To_Prm_Drct_Mgn_Amt,
	Sls_Qty,
	Mprk_Sls_Amt,
	Mprk_Prm_To_Sls_Amt,
	Mprk_Prm_To_Prm_Drct_Mgn_Amt,
	Lst_Yr_Fsc_Sls_Amt,
	Lst_Yr_Fsc_Prm_To_Prm_Drct_Mgn_Amt,
	Lst_Yr_Fsc_Sls_Qty,
	Lst_Yr_Fsc_Prm_To_Sls_Amt,
	Lst_Yr_Hldy_Sls_Amt,
	Lst_Yr_Hldy_Prm_To_Prm_Drct_Mgn_Amt,
	Lst_Yr_Hldy_Sls_Qty,
	Lst_Yr_Hldy_Prm_To_Sls_Amt,
	Pln_Sls_Amt,
	Aud_Ins_Sk,
	Aud_Upd_Sk
	)

SELECT DISTINCT
	ISNULL(G.Geo_Hist_Sk,-1) AS Geo_Hist_Sk,
	ISNULL(B.Byr_L3_Hist_Sk, -1) AS Byr_L3_Hist_Sk,
	Adj.Lvl3_Prod_Sub_Ctgry_Id,
	Adj.Dt_Tm_Hr,
	B.Byr_L3_Sk,
	ISNULL(G.Geo_Sk, -1) AS Geo_Sk,
	Adj.Str_Id,
	Adj.Sls_Amt,
	Adj.Prm_To_Sls_Amt,
	Adj.Prm_To_Prm_Drct_Mgn_Amt,
	Adj.Sls_Qty,
	Adj.Mprk_Sls_Amt,
	Adj.Mprk_Prm_To_Sls_Amt,
	Adj.Mprk_Prm_To_Prm_Drct_Mgn_Amt,
	Adj.Lst_Yr_Fsc_Sls_Amt,
	Adj.Lst_Yr_Fsc_Prm_To_Prm_Drct_Mgn_Amt,
	Adj.Lst_Yr_Fsc_Sls_Qty,
	Adj.Lst_Yr_Fsc_Prm_To_Sls_Amt,
	Adj.Lst_Yr_Hldy_Sls_Amt,
	Adj.Lst_Yr_Hldy_Prm_To_Prm_Drct_Mgn_Amt,
	Adj.Lst_Yr_Hldy_Sls_Qty,
	Adj.Lst_Yr_Hldy_Prm_To_Sls_Amt,
	Adj.Pln_Sls_Amt,
	@NEW_AUD_SKY 	AS Aud_Ins_Sk,
	NULL 			AS Aud_Upd_Sk
FROM
	CTE_SLS_FINAL AS Adj
	LEFT JOIN EDAA_DW.DIM_GEO G
		ON Adj.[Str_Id]=G.[Str_Id]  AND G.Is_Curr_Ind = 1
	LEFT JOIN [EDAA_PRES].[DIM_BYR] AS B
		ON B.[lvl3_Prod_Sub_Ctgry_Id] = Adj.[Lvl3_Prod_Sub_Ctgry_Id]
			AND B.Is_Curr_Ind = 1

END ;

BEGIN
SELECT @NBR_OF_RW_ISRT = COUNT(1)  FROM EDAA_PRES.FCT_HRLY_PROD_SUB_CTGRY_SLS_CALC WHERE Aud_Ins_Sk = @NEW_AUD_SKY
SELECT @NBR_OF_RW_UDT  = COUNT(1)  FROM EDAA_PRES.FCT_HRLY_PROD_SUB_CTGRY_SLS_CALC WHERE Aud_Upd_Sk = @NEW_AUD_SKY

------------ UPDATE Statistics-------

UPDATE STATISTICS  EDAA_PRES.FCT_HRLY_PROD_SUB_CTGRY_SLS_CALC;

END

/*Audit Log End*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_END @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT  = @NBR_OF_RW_UDT

END TRY
BEGIN CATCH

DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(250) = 'EDAA_ETL.SP_DW_PRES_FCT_HRLY_PROD_SUB_CTGRY_SLS_CALC_TABLELOAD'
DECLARE @ERROR_LINE AS INT;
DECLARE @ERROR_MSG AS NVARCHAR(MAX);

 SELECT
      @ERROR_LINE =  ERROR_NUMBER()
     ,@ERROR_MSG = ERROR_MESSAGE();
----------- Log execution error ----------

EXEC EDAA_CNTL.SP_LOG_AUD_ERR
@AUD_SKY = @NEW_AUD_SKY,
@ERROR_PROCEDURE_NAME = @ERROR_PROCEDURE_NAME,
@ERROR_LINE = @ERROR_LINE,
@ERROR_MSG = @ERROR_MSG;

---- Detect the change

   THROW;
END CATCH

GO
