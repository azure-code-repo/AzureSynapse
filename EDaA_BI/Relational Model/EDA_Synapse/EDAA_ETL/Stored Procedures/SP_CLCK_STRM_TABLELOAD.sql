CREATE PROC [EDAA_ETL].[SP_CLCK_STRM_TABLELOAD] @ENTITY_NAME [NVARCHAR](100), @FULL_OR_INCL [NVARCHAR](2) AS
BEGIN
   -- SET NOCOUNT ON added to prevent extra result sets from
   -- interfering with SELECT statements.
   SET
      NOCOUNT
      ON
      DECLARE @TRUNCATE_TABLE varchar(100)
      DECLARE @SQL varchar(255)

	  IF UPPER(@FULL_OR_INCL) = 'F'

	  BEGIN

	-- Webpagevisit & websitevisit DM tables are not truncated in F mode as we will do IPOP in batches

	--   IF @ENTITY_NAME = 'WebSiteVisit'
    --   BEGIN

    --      SET @TRUNCATE_TABLE = '[EDAA_DW].[FCT_WSTE_VISIT]'
	--   END

	--   IF @ENTITY_NAME = 'WebPageVisit'
    --   BEGIN

    --       SET @TRUNCATE_TABLE = '[EDAA_DW].[FCT_WPG_VISIT]'
	--    END

      IF @ENTITY_NAME = 'VisitorType'
      BEGIN

	      SET @TRUNCATE_TABLE = '[EDAA_DW].[REF_VISITR_TYP]'
	  END

	  IF @ENTITY_NAME = 'SearchEngine'
      BEGIN

		  SET @TRUNCATE_TABLE = '[EDAA_DW].[REF_SRCH_ENGN]'
	   END

	  IF @ENTITY_NAME = 'ReferringLinkType'
	  BEGIN

	     SET @TRUNCATE_TABLE = '[EDAA_DW].[REF_RF_LNK_TYP]'
	  END

	  --TRUNCATE TABLE
       SET @SQL = 'TRUNCATE TABLE ' + @TRUNCATE_TABLE
	   EXECUTE (@SQL)
	   print('---Existing records of Datamart Table is Truncated as it is Full Load---');

	 END

	 IF @ENTITY_NAME = 'WebSiteVisit'
	 BEGIN
	 EXEC [EDAA_ETL].[SP_STG_DW_FCT_WSTE_VISIT_TABLELOAD]
	 END

	 IF @ENTITY_NAME = 'WebPageVisit'
	 BEGIN
     EXEC [EDAA_ETL].[SP_STG_DW_FCT_WPG_VISIT_TABLELOAD]
	 END

	 IF @ENTITY_NAME = 'VisitorType'
	 BEGIN
	 EXEC [EDAA_ETL].[SP_STG_DW_REF_VISITR_TYP_TABLELOAD]
	 END

	 IF @ENTITY_NAME = 'SearchEngine'
	 BEGIN
	 EXEC [EDAA_ETL].[SP_STG_DW_REF_SRCH_ENGN_TABLELOAD]
	 END

	 IF @ENTITY_NAME = 'ReferringLinkType'
	 BEGIN
	 EXEC [EDAA_ETL].[SP_STG_DW_REF_RF_LNK_TYP_TABLELOAD]
	 END

  END
