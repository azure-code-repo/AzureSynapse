CREATE TABLE [EDAA_PRES].[DIM_HRLY_CAL_CALC] (
    [Dt_Tm_Hr]        DATETIME     NOT NULL,
    [Cal_Typ_Id]      SMALLINT     NOT NULL,
    [Pre_Def_Fltr_Id] INT          NOT NULL,
    [Cal_Dt]          DATE         NULL,
    [Cal_Dt_Nm]       VARCHAR (12) NULL,
    [Lst_Yr_Fsc_Dt]   DATETIME     NULL,
    [Lst_Yr_Hldy_Dt]  DATETIME     NULL,
    [Lst_Yr_Cal_Dt]   DATETIME     NULL,
    [Hldy_Dt_Desc]    VARCHAR (50) NULL,
    [Dy_Of_Wk_Nm]     VARCHAR (10) NULL,
    [Cal_Typ]         VARCHAR (10) NOT NULL,
    [Dy_Tm_Intrvl_Id] INT          NOT NULL,
    [Pre_Def_Fltr]    VARCHAR (25) NULL,
    [Aud_Ins_Sk]      BIGINT       NULL,
    [Aud_Upd_Sk]      BIGINT       NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);
