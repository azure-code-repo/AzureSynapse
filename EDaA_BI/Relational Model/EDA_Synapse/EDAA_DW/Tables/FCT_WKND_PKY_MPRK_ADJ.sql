CREATE TABLE [EDAA_DW].[FCT_WKND_PKY_MPRK_ADJ] (
    [Fnc_Lvl3_Pky_Id] VARCHAR (15)    NOT NULL,
    [Dt_Sk]           INT             NOT NULL,
    [Mprk_Adj_Prcntg] DECIMAL (11, 8) NULL,
    [Aud_Ins_Sk]      BIGINT          NULL,
    [Aud_Upd_Sk]      BIGINT          NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([Fnc_Lvl3_Pky_Id]));
