CREATE TABLE [stg_edw].[WK_UT_MPRS_CT_INV_ADJ_HST] (
    [PKY_ID]              SMALLINT        NOT NULL,
    [MPRS_CT_ID]          SMALLINT        NOT NULL,
    [UT_ID]               SMALLINT        NOT NULL,
    [WK_END_DT]           DATE            NOT NULL,
    [ADJ_TYP]             CHAR (3)        NOT NULL,
    [TN_TYP]              SMALLINT        NULL,
    [TG_INV_QT]           INT             NULL,
    [TG_INV_ADJ_QT]       INT             NULL,
    [TG_INV_CST_AM]       DECIMAL (13, 2) NULL,
    [TG_INV_T_INV_ADJ_AM] DECIMAL (13, 2) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);
