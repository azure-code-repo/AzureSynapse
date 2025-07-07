CREATE TABLE [EDAA_REF].[BYR_EMP_PKY_MPRS_CURRPREV_INF] (
    [MJR_P_SUB_CT_ID] VARCHAR (256) NULL,
    [BYR_ID]          INT           NULL,
    [INDV_LST_NM]     VARCHAR (256) NULL,
    [MGR_EMP_ID]      INT           NULL,
    [MGR_ID]          INT           NULL,
    [PKY_ID]          INT           NULL,
    [MPRS_CT_ID]      INT           NULL,
    [INS_DT]          DATETIME2 (7) NULL,
    [CURR_PKY_ID]     INT           NULL,
    [PKY_DSC]         VARCHAR (256) NULL,
    [CURR_MPRS_CT_ID] INT           NULL,
    [MPRS_CT_DSC]     VARCHAR (256) NULL,
    [EMP_ID]          INT           NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN);
