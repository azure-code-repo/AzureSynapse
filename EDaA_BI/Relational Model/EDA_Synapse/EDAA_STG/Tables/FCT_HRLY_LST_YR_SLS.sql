CREATE TABLE [EDAA_STG].[FCT_HRLY_LST_YR_SLS] (
    [Dt_Tm_Hr]               DATETIME2 (7)   NULL,
    [Lvl3_Prod_Sub_Ctgry_Id] NVARCHAR (4000) NULL,
    [Str_Id]                 INT             NULL,
    [Sls_Amt]                DECIMAL (15, 4) NULL,
    [Drct_Mgn_Amt]           DECIMAL (15, 4) NULL,
    [Sls_Qty]                INT             NULL,
    [Prm_To_Sls_Amt]         DECIMAL (15, 4) NULL,
    [Dt_Sk]                  INT NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);
