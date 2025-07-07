/****** Object:  View [EDAA_PRES].[DIM_DLY_CAL]    Script Date: 9/19/2022 12:52:40 AM ******/

CREATE VIEW [EDAA_PRES].[DIM_DLY_CAL]
AS SELECT
Dt_Sk,
Cal_Dt,
Cal_Dt_Nm,
Lst_Yr_Fsc_Dt,
Lst_Yr_Hldy_Dt,
Hldy_Nm,
Lst_Yr_Cal_Dt,
Wknd_Dt,
Lst_Yr_Wknd_Dt,
Lst_Yr_Fsc_Wknd_Dt,
Cal_Yr,
Dy_Of_Wk_Nm,
Fsc_Yr,
Fsc_Yr_Bgn_Dt,
Fsc_Yr_End_Dt,
Fsc_Qtr_Nm,
Fsc_Qtr_Bgn_Dt,
Fsc_Qtr_End_Dt,
Fsc_Prd_Nm,
Fsc_Prd_Seq_Id,
Fsc_Qtr_Seq_Id,
Cal_Typ_Ordr_Id,
Cal_Dy_Of_Wk_Id,
Cal_Dy_Of_Wk_Nm,
Cal_Mon_Of_Yr_Nm,
Cal_Mon_Of_The_Yr_Shrt_Nm,
CASE Cal_Mon_Of_The_Yr_Shrt_Nm
WHEN 'Jan' THEN 1
WHEN 'Feb' THEN 2
WHEN 'Mar' THEN 3
WHEN 'Apr' THEN 4
WHEN 'May' THEN 5
WHEN 'Jun' THEN 6
WHEN 'Jul' THEN 7
WHEN 'Aug' THEN 8
WHEN 'Sept' THEN 9
WHEN 'Oct' THEN 10
WHEN 'Nov' THEN 11
WHEN 'Dec' THEN 12
END AS Cal_Mon_Of_Yr_Id,
Lst_Yr_Fsc_Dt_Sk,
Lst_Yr_Cal_Dt_Sk,
Lst_Yr_Hldy_Dt_Sk,
Etl_Actn,
Aud_Ins_Sk,
Aud_Upd_Sk
FROM
EDAA_DW.DIM_DLY_CAL;
