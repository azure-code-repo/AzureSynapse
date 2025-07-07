CREATE TABLE [stg].[WK_DC_MPRS_CT_HST_ADJ] (
    [PKY_ID]             SMALLINT        NULL,
    [UT_ID]              SMALLINT        NULL,
    [FILLER]             VARCHAR (20)    NULL,
    [MPRS_CT_ID]         SMALLINT        NULL,
    [WK_END_DT]          VARCHAR (30)    NULL,
    [TG_INV_QT]          INT             NULL,
    [TG_CLRN_INV_QT]     INT             NULL,
    [TG_INV_CST_AM]      DECIMAL (13, 2) NULL,
    [TG_CLRN_INV_CST_AM] DECIMAL (13, 2) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);
