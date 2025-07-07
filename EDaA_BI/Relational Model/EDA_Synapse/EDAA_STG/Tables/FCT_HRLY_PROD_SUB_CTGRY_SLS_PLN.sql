CREATE TABLE [EDAA_STG].[FCT_HRLY_PROD_SUB_CTGRY_SLS_PLN] (
    [Dt_Tm_Hr]               DATETIME        NOT NULL,
    [Fnc_Lvl2_Mprs_Ctgry_Id] VARCHAR (15)    NOT NULL,
    [Fnc_Lvl3_Pky_Id]        VARCHAR (15)    NOT NULL,
    [Sls_Amt]                DECIMAL (16, 2) NULL,
    [Drct_Mgn_Amt]           DECIMAL (13, 4) NULL,
    [Sls_Promo_Amt]          DECIMAL (13, 4) NULL,
    [Same_Str_Amt]           DECIMAL (13, 4) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);
