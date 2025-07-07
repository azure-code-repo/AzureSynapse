CREATE TABLE [stg_edw].[DLY_UT_MPRS_CT_INV_ADJ_HST] (
    [PKY_ID]              SMALLINT        NOT NULL,
    [MPRS_CT_ID]          SMALLINT        NOT NULL,
    [UT_ID]               SMALLINT        NOT NULL,
    [DAY_DT]              DATE            NOT NULL,
    [ADJ_TYP]             CHAR (3)        NOT NULL,
    [TN_TYP]              SMALLINT        NULL,
    [TG_INV_QT]           INT             NOT NULL,
    [TG_INV_ADJ_QT]       INT             NOT NULL,
    [TG_INV_CST_AM]       DECIMAL (11, 2) NOT NULL,
    [TG_INV_T_INV_ADJ_AM] DECIMAL (11, 2) NOT NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);
