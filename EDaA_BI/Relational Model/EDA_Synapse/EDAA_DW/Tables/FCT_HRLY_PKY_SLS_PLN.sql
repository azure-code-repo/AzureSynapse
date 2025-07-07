CREATE TABLE [EDAA_DW].[FCT_HRLY_PKY_SLS_PLN] (
    [Dt_Tm_Hr]        DATETIME        NOT NULL,
    [Geo_Hist_Sk]     INT             NOT NULL,
    [Fnc_Lvl3_Pky_Id] VARCHAR (15)    NOT NULL,
    [Dt_Sk]           INT             NOT NULL,
    [Str_Id]          INT             NOT NULL,
    [Sls_Amt]         DECIMAL (13, 4) NULL,
    [Aud_Ins_Sk]      BIGINT          NULL,
    [Aud_Upd_Sk]      BIGINT          NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([Str_Id]));
