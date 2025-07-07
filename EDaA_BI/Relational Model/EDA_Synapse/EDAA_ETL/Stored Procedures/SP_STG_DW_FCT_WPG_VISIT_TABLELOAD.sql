/****** Object:  StoredProcedure [EDAA_ETL].[SP_STG_DW_FCT_WPG_VISIT_TABLELOAD]    Script Date: 3/9/2023 6:17:47 PM ******/
CREATE PROC [EDAA_ETL].[SP_STG_DW_FCT_WPG_VISIT_TABLELOAD] AS


BEGIN TRY

/*DATAMART AUDITING VARIABLES*/
DECLARE @NEW_AUD_SKY BIGINT
DECLARE @NBR_OF_RW_ISRT BIGINT
DECLARE @NBR_OF_RW_UDT BIGINT
DECLARE @EXEC_LYR VARCHAR(255)
DECLARE @EXEC_JOB VARCHAR(500)
DECLARE @SRC_ENTY VARCHAR(500)
DECLARE @TGT_ENTY VARCHAR(500)

/*AUDIT LOG START*/
EXEC EDAA_CNTL.SP_AUDIT_DATA_LOAD_START @EXEC_LYR  = 'EDAA_DW',
@EXEC_JOB  = 'SP_STG_DW_FCT_WPG_VISIT_TABLELOAD',
@SRC_ENTY  = '[EDAA_STG].[FCT_WPG_VISIT]',
@TGT_ENTY  = '[EDAA_DW].[FCT_WPG_VISIT]',
@NEW_AUD_SKY = @NEW_AUD_SKY OUTPUT

print('---Finding update records---');

;WITH update_data
AS (SELECT
A.WPG_VISIT_STRT_TMS                 AS WPG_VISIT_STRT_TMS,
A.EXCL_HIT_ID                        AS EXCL_HIT_ID,
A.DSG_MKT_AREA_ID                    AS DSG_MKT_AREA_ID,
A.WPG_CHL_NM                         AS WPG_CHL_NM,
A.CLCK_MAP_LNK                       AS CLCK_MAP_LNK,
A.CLCK_MAP_PG                        AS CLCK_MAP_PG,
A.CLCK_MAP_RGN                       AS CLCK_MAP_RGN,
A.WPG_NM                             AS WPG_NM,
A.WPG_FN_NM                          AS WPG_FN_NM,
A.DSG_STR_ID                         AS DSG_STR_ID,
A.DSG_STR_TXT                        AS DSG_STR_TXT,
A.CUST_ACCT_ID                       AS CUST_ACCT_ID,
A.CKI_ID                             AS CKI_ID,
A.INTRL_ADV_CMPN_TAG                 AS INTRL_ADV_CMPN_TAG,
A.DGTL_CPN_ID                        AS DGTL_CPN_ID,
A.DGTL_ORDR_ID                       AS DGTL_ORDR_ID,
A.DGTL_SRC_ID                        AS DGTL_SRC_ID,
A.SRCH_STRNG_TXT                     AS SRCH_STRNG_TXT,
A.SRCH_RSUL_SORT_TXT                 AS SRCH_RSUL_SORT_TXT,
A.SRCH_RSUL_FLTR_TXT                 AS SRCH_RSUL_FLTR_TXT,
A.VISITR_TYP_ID                      AS VISITR_TYP_ID,
A.ADD_TO_CAT_LOC                     AS ADD_TO_CAT_LOC,
A.CLCTN_NM                           AS CLCTN_NM,
A.EXTRL_ADV_CMPN_TAG                 AS EXTRL_ADV_CMPN_TAG,
A.ITM_SKU                            AS ITM_SKU,
A.SRCH_RSUL_RNK_VAL                  AS SRCH_RSUL_RNK_VAL,
A.FLYR_TYP_NM                        AS FLYR_TYP_NM,
A.FLYR_INTRL_RUN_ID                  AS FLYR_INTRL_RUN_ID,
A.PROMO_OFR_ID                       AS PROMO_OFR_ID,
A.PROMO_OFR_TXT                      AS PROMO_OFR_TXT,
A.WKLY_ADV_DGTL_CPN_ID               AS WKLY_ADV_DGTL_CPN_ID,
A.CAT_ID                             AS CAT_ID,
A.CAT_TXT                            AS CAT_TXT,
A.SRCH_MTHD_NM                       AS SRCH_MTHD_NM,
A.PLN_PKP_TM_SLT                     AS PLN_PKP_TM_SLT,
A.FST_AVL_TM_SLT                     AS FST_AVL_TM_SLT,
A.PLN_PKP_DT                         AS PLN_PKP_DT,
A.SRCH_RSUL_CNT                      AS SRCH_RSUL_CNT,
A.CLN_SRCH_STRNG_TXT                 AS CLN_SRCH_STRNG_TXT,
A.FST_AVL_TM_SLT_DT                  AS FST_AVL_TM_SLT_DT,
A.CLLT_LVL                           AS CLLT_LVL,
A.ADD_TO_CAT_PG_NM                   AS ADD_TO_CAT_PG_NM,
A.HM_STR_ID                          AS HM_STR_ID,
A.HM_STR_TXT                         AS HM_STR_TXT,
A.CRSL_PROD_IMPSN                    AS CRSL_PROD_IMPSN,
A.MOB_APL_HM_SC_TYP                  AS MOB_APL_HM_SC_TYP,
A.LND_PG_CMPN_TAG                    AS LND_PG_CMPN_TAG,
A.MKT_CMPN_TAG                       AS MKT_CMPN_TAG,
A.CPN_LOC                            AS CPN_LOC,
A.ADD_TO_CAT_CPN_INF                 AS ADD_TO_CAT_CPN_INF,
A.MOB_APL_VSN_ID                     AS MOB_APL_VSN_ID,
A.DY_SNCE_LST_USE                    AS DY_SNCE_LST_USE,
A.MOB_DVC_NM                         AS MOB_DVC_NM,
A.MOB_OPR_SYS_VRSN                   AS MOB_OPR_SYS_VRSN,
A.EVNT_RSP_CD                        AS EVNT_RSP_CD,
A.EVNT_NM                            AS EVNT_NM,
A.WPG_URL                            AS WPG_URL,
A.APL_TYP_NM                         AS APL_TYP_NM,
A.WEB__NM                            AS WEB__NM,
A.RF_PG_URL                          AS RF_PG_URL,
A.SRCH_ENG_ID                        AS SRCH_ENG_ID,
A.RF_DMN_NM                          AS RF_DMN_NM,
A.RF_LNK_TYP_ID                      AS RF_LNK_TYP_ID,
A.SRCH_ENG_PG_NUM                    AS SRCH_ENG_PG_NUM,
A.USR_AGNT_DESC                      AS USR_AGNT_DESC,
A.VISITR_LST_TCH_ATTR                AS VISITR_LST_TCH_ATTR,
A.VISITR_LST_TCH_ID                  AS VISITR_LST_TCH_ID,
A.VISITR_FST_TCH_ATTR                AS VISITR_FST_TCH_ATTR,
A.VISITR_FST_TCH_ID                  AS VISITR_FST_TCH_ID,
A.VISITR_INST_EVNT_IND               AS VISITR_INST_EVNT_IND,
A.NW_VISITR_IND                      AS NW_VISITR_IND,
A.VISITR_NUM                         AS VISITR_NUM,
A.VISIT_PG_NUM                       AS VISIT_PG_NUM,
A.SESN_ID                            AS SESN_ID,
A.MSTR_CMPN_ID                       AS MSTR_CMPN_ID,
A.UPD_TMS                            AS UPD_TMS,
A.UPD_BY                             AS UPD_BY
FROM [EDAA_STG].[FCT_WPG_VISIT] AS A)

UPDATE [EDAA_DW].[FCT_WPG_VISIT]
SET
WPG_VISIT_STRT_TMS                  = src.WPG_VISIT_STRT_TMS,
EXCL_HIT_ID                         = src.EXCL_HIT_ID,
DSG_MKT_AREA_ID                     = src.DSG_MKT_AREA_ID,
WPG_CHL_NM                          = src.WPG_CHL_NM,
CLCK_MAP_LNK                        = src.CLCK_MAP_LNK,
CLCK_MAP_PG                         = src.CLCK_MAP_PG,
CLCK_MAP_RGN                        = src.CLCK_MAP_RGN,
WPG_NM                              = src.WPG_NM,
WPG_FN_NM                           = src.WPG_FN_NM,
DSG_STR_ID                          = src.DSG_STR_ID,
DSG_STR_TXT                         = src.DSG_STR_TXT,
CUST_ACCT_ID                        = src.CUST_ACCT_ID,
CKI_ID                              = src.CKI_ID,
INTRL_ADV_CMPN_TAG                  = src.INTRL_ADV_CMPN_TAG,
DGTL_CPN_ID                         = src.DGTL_CPN_ID,
DGTL_ORDR_ID                        = src.DGTL_ORDR_ID,
DGTL_SRC_ID                         = src.DGTL_SRC_ID,
SRCH_STRNG_TXT                      = src.SRCH_STRNG_TXT,
SRCH_RSUL_SORT_TXT                  = src.SRCH_RSUL_SORT_TXT,
SRCH_RSUL_FLTR_TXT                  = src.SRCH_RSUL_FLTR_TXT,
VISITR_TYP_ID                       = src.VISITR_TYP_ID,
ADD_TO_CAT_LOC                      = src.ADD_TO_CAT_LOC,
CLCTN_NM                            = src.CLCTN_NM,
EXTRL_ADV_CMPN_TAG                  = src.EXTRL_ADV_CMPN_TAG,
ITM_SKU                             = src.ITM_SKU,
SRCH_RSUL_RNK_VAL                   = src.SRCH_RSUL_RNK_VAL,
FLYR_TYP_NM                         = src.FLYR_TYP_NM,
FLYR_INTRL_RUN_ID                   = src.FLYR_INTRL_RUN_ID,
PROMO_OFR_ID                        = src.PROMO_OFR_ID,
PROMO_OFR_TXT                       = src.PROMO_OFR_TXT,
WKLY_ADV_DGTL_CPN_ID                = src.WKLY_ADV_DGTL_CPN_ID,
CAT_ID                              = src.CAT_ID,
CAT_TXT                             = src.CAT_TXT,
SRCH_MTHD_NM                        = src.SRCH_MTHD_NM,
PLN_PKP_TM_SLT                      = src.PLN_PKP_TM_SLT,
FST_AVL_TM_SLT                      = src.FST_AVL_TM_SLT,
PLN_PKP_DT                          = src.PLN_PKP_DT,
SRCH_RSUL_CNT                       = src.SRCH_RSUL_CNT,
CLN_SRCH_STRNG_TXT                  = src.CLN_SRCH_STRNG_TXT,
FST_AVL_TM_SLT_DT                   = src.FST_AVL_TM_SLT_DT,
CLLT_LVL                            = src.CLLT_LVL,
ADD_TO_CAT_PG_NM                    = src.ADD_TO_CAT_PG_NM,
HM_STR_ID                           = src.HM_STR_ID,
HM_STR_TXT                          = src.HM_STR_TXT,
CRSL_PROD_IMPSN                     = src.CRSL_PROD_IMPSN,
MOB_APL_HM_SC_TYP                   = src.MOB_APL_HM_SC_TYP,
LND_PG_CMPN_TAG                     = src.LND_PG_CMPN_TAG,
MKT_CMPN_TAG                        = src.MKT_CMPN_TAG,
CPN_LOC                             = src.CPN_LOC,
ADD_TO_CAT_CPN_INF                  = src.ADD_TO_CAT_CPN_INF,
MOB_APL_VSN_ID                      = src.MOB_APL_VSN_ID,
DY_SNCE_LST_USE                     = src.DY_SNCE_LST_USE,
MOB_DVC_NM                          = src.MOB_DVC_NM,
MOB_OPR_SYS_VRSN                    = src.MOB_OPR_SYS_VRSN,
EVNT_RSP_CD                         = src.EVNT_RSP_CD,
EVNT_NM                             = src.EVNT_NM,
WPG_URL                             = src.WPG_URL,
APL_TYP_NM                          = src.APL_TYP_NM,
WEB__NM                             = src.WEB__NM,
RF_PG_URL                           = src.RF_PG_URL,
SRCH_ENG_ID                         = src.SRCH_ENG_ID,
RF_DMN_NM                           = src.RF_DMN_NM,
RF_LNK_TYP_ID                       = src.RF_LNK_TYP_ID,
SRCH_ENG_PG_NUM                     = src.SRCH_ENG_PG_NUM,
USR_AGNT_DESC                       = src.USR_AGNT_DESC,
VISITR_LST_TCH_ATTR                 = src.VISITR_LST_TCH_ATTR,
VISITR_LST_TCH_ID                   = src.VISITR_LST_TCH_ID,
VISITR_FST_TCH_ATTR                 = src.VISITR_FST_TCH_ATTR,
VISITR_FST_TCH_ID                   = src.VISITR_FST_TCH_ID,
VISITR_INST_EVNT_IND                = src.VISITR_INST_EVNT_IND,
NW_VISITR_IND                       = src.NW_VISITR_IND,
VISITR_NUM                          = src.VISITR_NUM,
MSTR_CMPN_ID                        = src.MSTR_CMPN_ID,
UPD_TMS                             = src.UPD_TMS,
UPD_BY                              = src.UPD_BY,
AUD_UPD_SK                          = @NEW_AUD_SKY
FROM [EDAA_DW].[FCT_WPG_VISIT] tgt, update_data src
WHERE tgt.SESN_ID = src.SESN_ID  and tgt.VISIT_PG_NUM = src.VISIT_PG_NUM;


print('---Finding insert records---');
;WITH insert_data
AS (SELECT
A.WPG_VISIT_STRT_TMS                 AS WPG_VISIT_STRT_TMS,
A.EXCL_HIT_ID                        AS EXCL_HIT_ID,
A.DSG_MKT_AREA_ID                    AS DSG_MKT_AREA_ID,
A.WPG_CHL_NM                         AS WPG_CHL_NM,
A.CLCK_MAP_LNK                       AS CLCK_MAP_LNK,
A.CLCK_MAP_PG                        AS CLCK_MAP_PG,
A.CLCK_MAP_RGN                       AS CLCK_MAP_RGN,
A.WPG_NM                             AS WPG_NM,
A.WPG_FN_NM                          AS WPG_FN_NM,
A.DSG_STR_ID                         AS DSG_STR_ID,
A.DSG_STR_TXT                        AS DSG_STR_TXT,
A.CUST_ACCT_ID                       AS CUST_ACCT_ID,
A.CKI_ID                             AS CKI_ID,
A.INTRL_ADV_CMPN_TAG                 AS INTRL_ADV_CMPN_TAG,
A.DGTL_CPN_ID                        AS DGTL_CPN_ID,
A.DGTL_ORDR_ID                       AS DGTL_ORDR_ID,
A.DGTL_SRC_ID                        AS DGTL_SRC_ID,
A.SRCH_STRNG_TXT                     AS SRCH_STRNG_TXT,
A.SRCH_RSUL_SORT_TXT                 AS SRCH_RSUL_SORT_TXT,
A.SRCH_RSUL_FLTR_TXT                 AS SRCH_RSUL_FLTR_TXT,
A.VISITR_TYP_ID                      AS VISITR_TYP_ID,
A.ADD_TO_CAT_LOC                     AS ADD_TO_CAT_LOC,
A.CLCTN_NM                           AS CLCTN_NM,
A.EXTRL_ADV_CMPN_TAG                 AS EXTRL_ADV_CMPN_TAG,
A.ITM_SKU                            AS ITM_SKU,
A.SRCH_RSUL_RNK_VAL                  AS SRCH_RSUL_RNK_VAL,
A.FLYR_TYP_NM                        AS FLYR_TYP_NM,
A.FLYR_INTRL_RUN_ID                  AS FLYR_INTRL_RUN_ID,
A.PROMO_OFR_ID                       AS PROMO_OFR_ID,
A.PROMO_OFR_TXT                      AS PROMO_OFR_TXT,
A.WKLY_ADV_DGTL_CPN_ID               AS WKLY_ADV_DGTL_CPN_ID,
A.CAT_ID                             AS CAT_ID,
A.CAT_TXT                            AS CAT_TXT,
A.SRCH_MTHD_NM                       AS SRCH_MTHD_NM,
A.PLN_PKP_TM_SLT                     AS PLN_PKP_TM_SLT,
A.FST_AVL_TM_SLT                     AS FST_AVL_TM_SLT,
A.PLN_PKP_DT                         AS PLN_PKP_DT,
A.SRCH_RSUL_CNT                      AS SRCH_RSUL_CNT,
A.CLN_SRCH_STRNG_TXT                 AS CLN_SRCH_STRNG_TXT,
A.FST_AVL_TM_SLT_DT                  AS FST_AVL_TM_SLT_DT,
A.CLLT_LVL                           AS CLLT_LVL,
A.ADD_TO_CAT_PG_NM                   AS ADD_TO_CAT_PG_NM,
A.HM_STR_ID                          AS HM_STR_ID,
A.HM_STR_TXT                         AS HM_STR_TXT,
A.CRSL_PROD_IMPSN                    AS CRSL_PROD_IMPSN,
A.MOB_APL_HM_SC_TYP                  AS MOB_APL_HM_SC_TYP,
A.LND_PG_CMPN_TAG                    AS LND_PG_CMPN_TAG,
A.MKT_CMPN_TAG                       AS MKT_CMPN_TAG,
A.CPN_LOC                            AS CPN_LOC,
A.ADD_TO_CAT_CPN_INF                 AS ADD_TO_CAT_CPN_INF,
A.MOB_APL_VSN_ID                     AS MOB_APL_VSN_ID,
A.DY_SNCE_LST_USE                    AS DY_SNCE_LST_USE,
A.MOB_DVC_NM                         AS MOB_DVC_NM,
A.MOB_OPR_SYS_VRSN                   AS MOB_OPR_SYS_VRSN,
A.EVNT_RSP_CD                        AS EVNT_RSP_CD,
A.EVNT_NM                            AS EVNT_NM,
A.WPG_URL                            AS WPG_URL,
A.APL_TYP_NM                         AS APL_TYP_NM,
A.WEB__NM                            AS WEB__NM,
A.RF_PG_URL                          AS RF_PG_URL,
A.SRCH_ENG_ID                        AS SRCH_ENG_ID,
A.RF_DMN_NM                          AS RF_DMN_NM,
A.RF_LNK_TYP_ID                      AS RF_LNK_TYP_ID,
A.SRCH_ENG_PG_NUM                    AS SRCH_ENG_PG_NUM,
A.USR_AGNT_DESC                      AS USR_AGNT_DESC,
A.VISITR_LST_TCH_ATTR                AS VISITR_LST_TCH_ATTR,
A.VISITR_LST_TCH_ID                  AS VISITR_LST_TCH_ID,
A.VISITR_FST_TCH_ATTR                AS VISITR_FST_TCH_ATTR,
A.VISITR_FST_TCH_ID                  AS VISITR_FST_TCH_ID,
A.VISITR_INST_EVNT_IND               AS VISITR_INST_EVNT_IND,
A.NW_VISITR_IND                      AS NW_VISITR_IND,
A.VISITR_NUM                         AS VISITR_NUM,
A.VISIT_PG_NUM                       AS VISIT_PG_NUM,
A.SESN_ID                            AS SESN_ID,
A.MSTR_CMPN_ID                       AS MSTR_CMPN_ID,
A.UPD_TMS                            AS UPD_TMS,
A.UPD_BY                             AS UPD_BY,
@NEW_AUD_SKY                         AS AUD_INS_SK,
NULL                                 AS AUD_UPD_SK
FROM [EDAA_STG].[FCT_WPG_VISIT] AS A)

INSERT INTO [EDAA_DW].[FCT_WPG_VISIT]
(
WPG_VISIT_STRT_TMS,
EXCL_HIT_ID,
DSG_MKT_AREA_ID,
WPG_CHL_NM,
CLCK_MAP_LNK,
CLCK_MAP_PG,
CLCK_MAP_RGN,
WPG_NM,
WPG_FN_NM,
DSG_STR_ID,
DSG_STR_TXT,
CUST_ACCT_ID,
CKI_ID,
INTRL_ADV_CMPN_TAG,
DGTL_CPN_ID,
DGTL_ORDR_ID,
DGTL_SRC_ID,
SRCH_STRNG_TXT,
SRCH_RSUL_SORT_TXT,
SRCH_RSUL_FLTR_TXT,
VISITR_TYP_ID,
ADD_TO_CAT_LOC,
CLCTN_NM,
EXTRL_ADV_CMPN_TAG,
ITM_SKU,
SRCH_RSUL_RNK_VAL,
FLYR_TYP_NM,
FLYR_INTRL_RUN_ID,
PROMO_OFR_ID,
PROMO_OFR_TXT,
WKLY_ADV_DGTL_CPN_ID,
CAT_ID,
CAT_TXT,
SRCH_MTHD_NM,
PLN_PKP_TM_SLT,
FST_AVL_TM_SLT,
PLN_PKP_DT,
SRCH_RSUL_CNT,
CLN_SRCH_STRNG_TXT,
FST_AVL_TM_SLT_DT,
CLLT_LVL,
ADD_TO_CAT_PG_NM,
HM_STR_ID,
HM_STR_TXT,
CRSL_PROD_IMPSN,
MOB_APL_HM_SC_TYP,
LND_PG_CMPN_TAG,
MKT_CMPN_TAG,
CPN_LOC,
ADD_TO_CAT_CPN_INF,
MOB_APL_VSN_ID,
DY_SNCE_LST_USE,
MOB_DVC_NM,
MOB_OPR_SYS_VRSN,
EVNT_RSP_CD,
EVNT_NM,
WPG_URL,
APL_TYP_NM,
WEB__NM,
RF_PG_URL,
SRCH_ENG_ID,
RF_DMN_NM,
RF_LNK_TYP_ID,
SRCH_ENG_PG_NUM,
USR_AGNT_DESC,
VISITR_LST_TCH_ATTR,
VISITR_LST_TCH_ID,
VISITR_FST_TCH_ATTR,
VISITR_FST_TCH_ID,
VISITR_INST_EVNT_IND,
NW_VISITR_IND,
VISITR_NUM,
VISIT_PG_NUM,
SESN_ID,
MSTR_CMPN_ID,
UPD_TMS,
UPD_BY,
AUD_INS_SK,
AUD_UPD_SK
)
SELECT
*
from insert_data A where not EXISTS (select 1  from [EDAA_DW].[FCT_WPG_VISIT] B where  B.SESN_ID=A.SESN_ID and B.VISIT_PG_NUM=A.VISIT_PG_NUM )  ;

print('---DW load completed---');

UPDATE STATISTICS [EDAA_DW].[FCT_WPG_VISIT]


BEGIN

SELECT @NBR_OF_RW_ISRT = COUNT(1)  FROM [EDAA_DW].[FCT_WPG_VISIT] WHERE AUD_INS_SK = @NEW_AUD_SKY
print(@NBR_OF_RW_ISRT)

SELECT @NBR_OF_RW_UDT = COUNT(1)  FROM [EDAA_DW].[FCT_WPG_VISIT] WHERE AUD_UPD_SK = @NEW_AUD_SKY
print(@NBR_OF_RW_UDT)

END


/*AUDIT LOG END*/
EXEC [EDAA_CNTL].[SP_AUDIT_DATA_LOAD_END] @AUD_SKY = @NEW_AUD_SKY, @NBR_OF_RW_ISRT = @NBR_OF_RW_ISRT, @NBR_OF_RW_UDT = @NBR_OF_RW_UDT

END TRY

BEGIN CATCH
DECLARE @ERROR_PROCEDURE_NAME AS VARCHAR(60) = '[EDAA_ETL].[SP_STG_DW_FCT_WPG_VISIT_TABLELOAD]'
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
