CREATE VIEW [V_EDAA_DW].[DIM_USR]
AS
SELECT [Usr_Id]
      ,[Str_Id]
      ,[Usr_Rl_Nm]
      ,[Mkt_Id]
      ,[Rgn_Id]
      ,[Aud_Ins_Sk]
      ,[Aud_Upd_Sk]
  FROM [EDAA_DW].[DIM_USR] WITH(NOLOCK)
