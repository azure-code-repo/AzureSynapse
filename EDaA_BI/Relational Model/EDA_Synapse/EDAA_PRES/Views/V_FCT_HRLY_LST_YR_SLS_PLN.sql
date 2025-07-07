CREATE VIEW [EDAA_PRES].[V_FCT_HRLY_LST_YR_SLS_PLN] AS WITH CTE_CAL AS
  (SELECT DT_SK AS TY_DT_SK,
          format(Lst_Yr_Fsc_Dt, 'yyyyMMdd') AS LY_DT_SK
   FROM [V_EDAA_DW].[DIM_DLY_CAL]
   WHERE Fsc_Yr = YEAR(GETDATE()) --ORDER BY DT_SK
),
CTE_V_FCT_DLY_SL_P_SUB_CT_CALC AS
  (SELECT LY.UT_ID,
          LY.P_SUB_CT_ID,
          LY.GEO_HST_SKY,
          LY.TM_SKY AS LY_DT_SK,
          CTE_CAL.TY_DT_SK,
          SUM(LY.SLS_FNC_AM_LY_FSC) AS LY_SLS_AMT
   FROM [PRES].[V_FCT_DLY_SL_P_SUB_CT_CALC] LY
   LEFT OUTER JOIN CTE_CAL ON LY.TM_SKY = CTE_CAL.LY_DT_SK
   WHERE YEAR(LY_DT_SK) = YEAR(GETDATE()) - 1 --AND LY.UT_ID=19
GROUP BY LY.UT_ID,
         LY.P_SUB_CT_ID,
         LY.Geo_Hst_Sky,
         LY.TM_SKY,
         CTE_CAL.TY_DT_SK),
CTE_LY_PROD AS
  (SELECT DISTINCT UT_ID,
                   P_SUB_CT_ID,
                   GEO_HST_SKY,
                   LY_DT_SK,
                   TY_DT_SK,
                   LY_SLS_AMT
   FROM CTE_V_FCT_DLY_SL_P_SUB_CT_CALC
   INNER JOIN
     (SELECT DISTINCT Lvl3_Prod_Sub_Ctgry_Id
      FROM [EDAA_DW].[DIM_PROD]
      WHERE IS_CURR_IND = 1) PROD ON [P_SUB_CT_ID] = PROD.Lvl3_Prod_Sub_Ctgry_Id),
LY_DATE_POPULATE AS
  (SELECT a.UT_ID,
          a.P_SUB_CT_ID,
          a.GEO_HST_SKY,
          a.LY_DT_SK,
          b.TY_DT_SK,
          a.LY_SLS_AMT
   FROM CTE_LY_PROD a
   INNER JOIN CTE_CAL b ON a.LY_DT_SK = b.LY_DT_SK),
 FCT_DLY_SL_PLN_P_SUB_CT AS
  (SELECT PL.UT_ID,
          PL.FNC_PKY_ID,
          PL.GEO_HST_SKY,
          PL.TM_SKY AS TY_DT_SK,
          PL.P_SUB_CT_ID,
          CTE_CAL.LY_DT_SK,
          SUM(PL.SLS_FNC_AN_PLN_AM) AS PLN_Sls_Amt
   FROM [DM].[FCT_DLY_SL_PLN_P_SUB_CT] PL
   LEFT OUTER JOIN CTE_CAL ON PL.TM_SKY = CTE_CAL.TY_DT_SK
   WHERE YEAR(LY_DT_SK) = YEAR(GETDATE()) - 1 -- AND PL.UT_ID=19
GROUP BY PL.UT_ID,
         PL.FNC_PKY_ID,
         PL.GEO_HST_SKY,
         PL.TM_SKY,
         CTE_CAL.LY_DT_SK,
         P_SUB_CT_ID),
CTE_TY_PROD AS
  (SELECT DISTINCT UT_ID,
                   FNC_PKY_ID,
                   GEO_HST_SKY,
                   LY_DT_SK,
                   TY_DT_SK,
                   P_SUB_CT_ID,
                   PLN_Sls_Amt
   FROM FCT_DLY_SL_PLN_P_SUB_CT
   INNER JOIN
     (SELECT DISTINCT Lvl3_Prod_Sub_Ctgry_Id
      FROM [EDAA_DW].[DIM_PROD]
      WHERE IS_CURR_IND = 1) PROD ON [P_SUB_CT_ID] = PROD.Lvl3_Prod_Sub_Ctgry_Id),
CTE_NEXT_WEEK AS
  (SELECT DISTINCT a.UT_ID,
                   a.P_SUB_CT_ID,
                   a.Geo_Hst_Sky,
                   a.TY_DT_SK,
                   a.LY_DT_SK,
                   a.LY_SLS_AMT,
                   0 AS PLN_Sls_Amt
   FROM LY_DATE_POPULATE a
   UNION ALL SELECT DISTINCT a.UT_ID,
                             a.P_SUB_CT_ID,
                             a.Geo_Hst_Sky,
                             a.TY_DT_SK,
                             a.LY_DT_SK,
                             0 AS LY_SLS_AMT,
                             PLN_Sls_Amt
   FROM CTE_TY_PROD a),
                                                            CAL AS
  (SELECT format(DATEADD(DAY, -6, Wknd_Dt), 'yyyyMMdd') AS MIN_DT_SK,
          format(DATEADD(DAY, 7, Wknd_Dt), 'yyyyMMdd') AS MAX_DT_SK
   FROM [EDAA_DW].[DIM_DLY_CAL]
   WHERE DT_SK = format(GETDATE(), 'yyyyMMdd'))


SELECT DISTINCT a.UT_ID,
                REPLACE(a.P_SUB_CT_ID, ' ', '') AS P_SUB_CT_ID,
                a.Geo_Hst_Sky,
                a.TY_DT_SK AS DT_SK, --a.LY_DT_SK,
 SUM(a.LY_SLS_AMT) AS LY_SLS_AMT,
 SUM(a.PLN_Sls_Amt) AS PLN_Sls_Amt
FROM CTE_NEXT_WEEK a
JOIN CAL b ON a.TY_DT_SK <= b.MAX_DT_SK
AND a.TY_DT_SK >= b.MIN_DT_SK
GROUP BY a.UT_ID,
         a.P_SUB_CT_ID,
         a.Geo_Hst_Sky,
         a.TY_DT_SK;
