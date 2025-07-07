CREATE TABLE [DM].[DIM_CHNL] (
    [CHNL_HST_SKY]        INT           NOT NULL,
    [CHNL_SKY]            INT           NOT NULL,
    [CHNL_BUS_KEY]        VARCHAR (10)  NOT NULL,
    [SHOP_CHNL_CT]        SMALLINT      NOT NULL,
    [SHOP_CHNL_GRP_CT]    SMALLINT      NOT NULL,
    [SHOP_CHNL_CLS_CT]    VARCHAR (3)   NULL,
    [SHOP_CHNL_CLS_CT_NM] VARCHAR (40)  NULL,
    [MBL_SLF_CHKOT_CT]    VARCHAR (3)   NOT NULL,
    [MBL_SLF_CHKOT_DSC]   VARCHAR (60)  NOT NULL,
    [MPRKS]               VARCHAR (10)  NOT NULL,
    [MPRKS_TN_CT]         VARCHAR (3)   NOT NULL,
    [MPERKS_TN_DSC]       VARCHAR (100) NOT NULL,
    [CHNL_DTL]            VARCHAR (50)  NOT NULL,
    [SHOP_CHNL]           VARCHAR (50)  NOT NULL,
    [SHOP_AND_SCN]        VARCHAR (30)  NOT NULL,
    [SHOP_AND_SCN_FLG]    BIT           NOT NULL,
    [DGTL_UT_FLG]         SMALLINT      NOT NULL,
    [VLD_FROM]            DATETIME      NOT NULL,
    [VLD_TO]              DATETIME      NULL,
    [IS_CURR]             BIT           NOT NULL,
    [IS_DMY]              BIT           NOT NULL,
    [IS_EMBR]             BIT           NULL,
    [ETL_ACT]             VARCHAR (20)  NOT NULL,
    [AUD_INS_SKY]         BIGINT        NOT NULL,
    [AUD_UPD_SKY]         BIGINT        NULL,
    PRIMARY KEY NONCLUSTERED ([CHNL_HST_SKY] ASC) NOT ENFORCED
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [DIM_CHNL_CHNL_BUS_KEY_NCIDX]
    ON [DM].[DIM_CHNL]([CHNL_BUS_KEY] ASC);
