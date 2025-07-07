CREATE TABLE [DM].[DIM_GEO] (
    [GEO_HST_SKY]    INT            NOT NULL,
    [GEO_SKY]        INT            NOT NULL,
    [UT_ID]          SMALLINT       NOT NULL,
    [UT_DSC]         VARCHAR (150)  NOT NULL,
    [UNIT_SQF]       INT            NULL,
    [RGN]            VARCHAR (100)  NOT NULL,
    [OPR_MKT]        VARCHAR (35)   NOT NULL,
    [UT_LNGT]        DECIMAL (9, 6) NULL,
    [UT_LAT]         DECIMAL (9, 6) NULL,
    [ST_CLS_ID]      SMALLINT       NULL,
    [ST_CLS]         VARCHAR (15)   NULL,
    [ST]             VARCHAR (3)    NULL,
    [CNTY]           VARCHAR (20)   NULL,
    [CITY]           VARCHAR (50)   NULL,
    [ZIP]            VARCHAR (5)    NULL,
    [DIV_ID]         VARCHAR (3)    NOT NULL,
    [DIV_NM]         VARCHAR (40)   NOT NULL,
    [NW_STO_TY_FLG]  SMALLINT       NULL,
    [NW_STO_LY_FLG]  SMALLINT       NULL,
    [NW_STO_2LY_FLG] SMALLINT       NULL,
    [VLD_FROM]       DATETIME       NOT NULL,
    [VLD_TO]         DATETIME       NULL,
    [IS_CURR]        BIT            NOT NULL,
    [IS_DMY]         BIT            NOT NULL,
    [IS_EMBR]        BIT            NOT NULL,
    [ETL_ACT]        VARCHAR (20)   NOT NULL,
    [AUD_INS_SKY]    BIGINT         NOT NULL,
    [AUD_UPD_SKY]    BIGINT         NULL,
    PRIMARY KEY NONCLUSTERED ([GEO_HST_SKY] ASC) NOT ENFORCED
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [DIM_GEO_UT_ID_NCIDX]
    ON [DM].[DIM_GEO]([UT_ID] ASC);
