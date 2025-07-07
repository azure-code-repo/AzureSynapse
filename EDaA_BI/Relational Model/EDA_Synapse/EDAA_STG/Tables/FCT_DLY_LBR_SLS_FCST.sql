CREATE TABLE [EDAA_STG].[FCT_DLY_LBR_SLS_FCST] (
    [Dly_Dt]          DATE            NOT NULL,
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
    [Wge_Pdtv_Amt]    DECIMAL (23, 8) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);
