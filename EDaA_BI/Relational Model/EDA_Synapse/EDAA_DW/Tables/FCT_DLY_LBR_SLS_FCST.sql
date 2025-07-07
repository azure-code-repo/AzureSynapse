CREATE TABLE [EDAA_DW].[FCT_DLY_LBR_SLS_FCST] (
    [Geo_Hist_Sk]     INT             NOT NULL,
    [Dt_Sk]           INT             NOT NULL,
    [Str_Id]          INT             NOT NULL,
    [Typ_Id]          SMALLINT        NULL,
    [Sch_Dpt_Id]      SMALLINT        NULL,
    [Job_Id]          INT             NULL,
    [Avg_Wge_Rte_Amt] DECIMAL (10, 4) NULL,
    [Hr_Qty]          DECIMAL (12, 4) NULL,
    [Hr_Pdtv_Qty]     DECIMAL (12, 4) NULL,
    [Sls_Amt]         DECIMAL (13, 2) NULL,
    [Wge_Amt]         DECIMAL (12, 4) NULL,
    [Wge_Pdtv_Amt]    DECIMAL (23, 8) NULL,
    [Aud_Ins_Sk]      BIGINT          NULL,
    [Aud_Upd_Sk]      BIGINT          NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([Str_Id]));
