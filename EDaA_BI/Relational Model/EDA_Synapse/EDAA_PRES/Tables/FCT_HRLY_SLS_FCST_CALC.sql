CREATE TABLE [EDAA_PRES].[FCT_HRLY_SLS_FCST_CALC] (
    [Geo_Hist_Sk]  INT             NOT NULL,
    [Dt_Tm_Hr]     DATETIME        NOT NULL,
    [Geo_Sk]       INT             NOT NULL,
    [Str_Id]       INT             NOT NULL,
    [Fcst_Sls_Amt] DECIMAL (20, 2) NULL,
    [Aud_Ins_Sk]   BIGINT          NULL,
    [Aud_Upd_Sk]   BIGINT          NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);
