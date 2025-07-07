CREATE TABLE [stg].[DLY_UT_MPRS_CT_INV_ADJ_HST_ADJ] (
    [P_UPC_ID]            NVARCHAR (4000) NULL,
    [UT_ID]               SMALLINT        NOT NULL,
    [MPRS_CT_ID]          SMALLINT        NOT NULL,
    [PKY_ID]              SMALLINT        NOT NULL,
    [DAY_DT]              NVARCHAR (4000) NOT NULL,
    [TN_TYP]              SMALLINT        NULL,
    [ADJ_TYP]             CHAR (3)        NOT NULL,
    [TG_INV_QT]           INT             NULL,
    [TG_INV_ADJ_QT]       INT             NULL,
    [TG_INV_CST_AM]       DECIMAL (13, 2) NULL,
    [FILL_CC]             NVARCHAR (4000) NULL,
    [TG_INV_T_INV_ADJ_AM] DECIMAL (13, 2) NULL,
    [FILL_DD]             NVARCHAR (4000) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);
