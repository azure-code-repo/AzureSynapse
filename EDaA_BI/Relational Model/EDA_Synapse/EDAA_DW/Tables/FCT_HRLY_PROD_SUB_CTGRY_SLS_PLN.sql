CREATE TABLE [EDAA_DW].[FCT_HRLY_PROD_SUB_CTGRY_SLS_PLN] (
    [Dt_Tm_Hr]               DATETIME        NOT NULL,
    [Lvl3_Prod_Sub_Ctgry_Id] VARCHAR (10)    NOT NULL,
    [Fnc_Lvl2_Mprs_Ctgry_Id] VARCHAR (15)    NOT NULL,
    [Fnc_Lvl3_Pky_Id]        VARCHAR (15)    NOT NULL,
    [Sls_Amt]                DECIMAL (16, 2) NULL,
    [Drct_Mgn_Amt]           DECIMAL (13, 4) NULL,
    [Sls_Promo_Amt]          DECIMAL (13, 4) NULL,
    [Same_Str_Amt]           DECIMAL (13, 4) NULL,
    [Aud_Ins_Sk]             BIGINT          NULL,
    [Aud_Upd_Sk]             BIGINT          NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([Dt_Tm_Hr]));
