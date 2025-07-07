CREATE TABLE [stg_edw].[WK_DC_MPRS_CT_HST] (
    [PKY_ID]              SMALLINT        NOT NULL,
    [MPRS_CT_ID]          SMALLINT        NOT NULL,
    [UT_ID]               SMALLINT        NOT NULL,
    [WK_END_DT]           DATE            NOT NULL,
    [TG_INV_CST_AM]       DECIMAL (11, 2) NOT NULL,
    [TG_INV_QT]           INT             NOT NULL,
    [TG_CLRN_INV_CST_AM]  DECIMAL (11, 2) NOT NULL,
    [TG_CLRN_INV_QT]      INT             NOT NULL,
    [TG_FNC_CR_AM]        DECIMAL (11, 4) NOT NULL,
    [TG_CLRN_FNC_CR_AM]   DECIMAL (11, 4) NOT NULL,
    [TG_FNC_CHRG_AM]      DECIMAL (11, 4) NOT NULL,
    [TG_CLRN_FNC_CHRG_AM] DECIMAL (11, 4) NOT NULL,
    [TG_STG_CHRG_AM]      DECIMAL (11, 4) NOT NULL,
    [TG_CLRN_STG_CHRG_AM] DECIMAL (11, 4) NOT NULL,
    [TG_INV_CS_QT]        INT             NOT NULL,
    [TG_CLRN_INV_CS_QT]   INT             NOT NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);
