CREATE TABLE [DM].[DIM_TM] (
    [TM_SKY]                 INT           NOT NULL,
    [DAY_DT]                 DATE          NOT NULL,
    [DAY_DT_LY]              DATE          NOT NULL,
    [FCL_DAY_DT_LY]          DATE          NOT NULL,
    [WK_END_DT]              DATE          NOT NULL,
    [WK_END_DT_LY]           DATE          NOT NULL,
    [FCL_WK_END_DT_LY]       DATE          NOT NULL,
    [LY_HLDY_DAY_DT]         DATE          NULL,
    [CLDR_DAY_OF_WK_ID]      SMALLINT      NOT NULL,
    [CLDR_DAY_OF_WK_NM]      VARCHAR (10)  NOT NULL,
    [CLDR_DAY_OF_WK_SHRT_NM] VARCHAR (3)   NOT NULL,
    [CLDR_MTH_OF_YR_ID]      SMALLINT      NOT NULL,
    [CLDR_MTH_OF_YR_NM]      VARCHAR (10)  NOT NULL,
    [CLDR_MTH_OF_YR_SHRT_NM] VARCHAR (3)   NOT NULL,
    [CLDR_YR_ID]             SMALLINT      NOT NULL,
    [FCL_YR_ID]              SMALLINT      NOT NULL,
    [FCL_YR_BEGN_DT]         DATE          NOT NULL,
    [FCL_YR_END_DT]          DATE          NOT NULL,
    [FCL_QTR_BEGN_DT]        DATE          NOT NULL,
    [FCL_QTR_END_DT]         DATE          NOT NULL,
    [FCL_PER_BEGN_DT_LY]     DATE          NOT NULL,
    [FCL_PER_END_DT_LY]      DATE          NOT NULL,
    [TM_SKY_LY_FSC]          INT           NULL,
    [TM_SKY_LY_HLDY]         INT           NULL,
    [TM_SKY_LY_CLDR]         INT           NULL,
    [IS_HLDY]                BIT           NOT NULL,
    [HLDY_DT_DSC]            VARCHAR (255) NULL,
    [ETL_ACT]                VARCHAR (20)  NOT NULL,
    [AUD_INS_SKY]            BIGINT        NOT NULL,
    [AUD_UPD_SKY]            BIGINT        NULL,
    PRIMARY KEY NONCLUSTERED ([TM_SKY] ASC) NOT ENFORCED
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [DIM_TM_TM_SKY_LY_CLDR_NCIDX]
    ON [DM].[DIM_TM]([TM_SKY_LY_CLDR] ASC);


GO
CREATE NONCLUSTERED INDEX [DIM_TM_TM_SKY_LY_HLDY_NCIDX]
    ON [DM].[DIM_TM]([TM_SKY_LY_HLDY] ASC);


GO
CREATE NONCLUSTERED INDEX [DIM_TM_TM_SKY_LY_FSC_NCIDX]
    ON [DM].[DIM_TM]([TM_SKY_LY_FSC] ASC);


GO
CREATE NONCLUSTERED INDEX [DIM_TM_DAY_DT_NCIDX]
    ON [DM].[DIM_TM]([DAY_DT] ASC);
