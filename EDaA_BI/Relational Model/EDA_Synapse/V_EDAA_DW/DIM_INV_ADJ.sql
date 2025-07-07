CREATE VIEW [V_EDAA_DW].[DIM_INV_ADJ]
AS
SELECT [INV_ADJ_SK]
      ,[ADJ_TYP_ID]
      ,[ADJ_TYP]
      ,[ADJ_TYP_DESC]
      ,[TXN_TYP]
      ,[INV_ADJ_DESC]
      ,[IS_DMY_IND]
      ,[IS_EMB_IND]
      ,[ETL_ACTN]
      ,[AUD_INS_SK]
      ,[AUD_UPD_SK]
  FROM [EDAA_DW].[DIM_INV_ADJ] WITH(NOLOCK)
