CREATE TABLE [EDAA_STG].[FCT_HRLY_PKY_SLS_PLN] (
    [Dt_Tm_Hr]        DATETIME        NOT NULL,
    [Fnc_Lvl3_Pky_Id] VARCHAR (15)    NOT NULL,
    [Dt_Sk]           INT             NOT NULL,
    [Day_dt]          DATE            NOT NULL,
    [Str_Id]          INT             NOT NULL,
    [Sls_Amt]         DECIMAL (13, 4) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([Str_Id]));
