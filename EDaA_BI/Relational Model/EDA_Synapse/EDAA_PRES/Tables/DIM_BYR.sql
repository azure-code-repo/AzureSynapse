CREATE TABLE [EDAA_PRES].[DIM_BYR] (
    [Byr_L3_Hist_Sk]           INT           NOT NULL,
    [Byr_L3_Sk]                INT           NOT NULL,
    [Byr_Id]                   INT           NULL,
    [lvl3_Prod_Sub_Ctgry_Id]   VARCHAR (200) NULL,
    [lvl3_Prod_Sub_Ctgry_Desc] VARCHAR (200) NULL,
    [Grp_Vp_Nm]                VARCHAR (200) NULL,
    [Mngr_Nm]                  VARCHAR (200) NULL,
    [Vp_Nm]                    VARCHAR (200) NULL,
    [Byr_Nm]                   VARCHAR (200) NULL,
    [Vld_Frm]                  DATETIME      NULL,
    [Vld_To]                   DATETIME      NULL,
    [Is_Curr_Ind]              BIT           NULL,
    [Is_Dmy_Ind]               BIT           NULL,
    [Is_Emb_Ind]               BIT           NULL,
    [Etl_Actn]                 VARCHAR (20)  NOT NULL,
    [Aud_Ins_Sk]               BIGINT        NOT NULL,
    [Aud_Upd_Sk]               BIGINT        NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);
