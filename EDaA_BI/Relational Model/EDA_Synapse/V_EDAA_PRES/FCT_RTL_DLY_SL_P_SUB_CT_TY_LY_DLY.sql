CREATE VIEW [V_EDAA_PRES].[FCT_RTL_DLY_SL_P_SUB_CT_TY_LY_DLY]
AS SELECT dtsel.ROW_ID_VAL,
       dtsel.PRE_DEF_ID,
       CONVERT(date, CONVERT(varchar(10), dtsel.MAX_DT_SK, 120)) AS DT_VAL,
       a.GEO_HST_SKY,
       a.GEO_SKY,
       a.P_HST_SKY,
       a.P_SKY,
       a.BYR_HST_SKY,
       a.BYR_SKY,
       a.UT_ID,
       a.SLS_FNC_AM,
       a.SLS_FNC_QTY,
       a.DM_FNC_AM,
       a.SLS_FNC_SCN_QTY,
       a.SLS_FNC_AM_LY_CAL,
       a.SLS_FNC_QTY_LY_CAL,
       a.DM_FNC_AM_LY_CAL,
       a.SLS_FNC_SCN_QTY_LY_CAL,
       a.SLS_FNC_AM_LY_FSC,
       a.SLS_FNC_QTY_LY_FSC,
       a.DM_FNC_AM_LY_FSC,
       a.SLS_FNC_SCN_QTY_LY_FSC,
       a.SLS_FNC_AM_LY_HLDY,
       a.SLS_FNC_QTY_LY_HLDY,
       a.DM_FNC_AM_LY_HLDY,
       a.SLS_FNC_SCN_QTY_LY_HLDY,
       a.UPDATEDTIMESTAMP
FROM [EDAA_PRES].[FCT_RTL_DLY_SL_P_SUB_CT_TY_LY] AS a WITH (NOLOCK)
    INNER JOIN
    (
        SELECT PRE_DEF_NM,
               PRE_DEF_ID,
               AGG_TYPE,
               MIN_DT_SK,
               MAX_DT_SK,
               ROW_NUMBER() OVER (ORDER BY PRE_DEF_ID) AS ROW_ID_VAL
        FROM [V_EDAA_PRES].[DIM_PRE_DEF_DATE]
        WHERE AGG_TYPE = 'DLY'
    ) dtsel
        ON a.DT_SK = dtsel.PRE_DEF_ID;
