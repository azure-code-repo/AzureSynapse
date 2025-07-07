CREATE TABLE [dm_edw].[DIM_BYR_P_SUB_CT_TMP] (
    [BYR_HST_SKY]  INT           NOT NULL,
    [BYR_SKY]      INT           NOT NULL,
    [BYR_ID]       INT           NULL,
    [BYR]          NVARCHAR (256) NULL,
    [P_SUB_CT_ID]  VARCHAR (200) NULL,
    [P_SUB_CT_DSC] VARCHAR (200) NULL,
    [GVP]          VARCHAR (200) NULL,
    [MGR]          VARCHAR (200) NULL,
    [VP]           VARCHAR (200) NULL,
    [VLD_FROM]     DATETIME      NULL,
    [VLD_TO]       DATETIME      NULL,
    [IS_CURR]      BIT           NULL,
    [IS_DMY]       BIT           NULL,
    [IS_EMBR]      BIT           NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);
