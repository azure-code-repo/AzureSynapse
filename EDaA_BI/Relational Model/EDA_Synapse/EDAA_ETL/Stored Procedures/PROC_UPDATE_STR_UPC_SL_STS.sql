/******
Object:  StoredProcedure [EDAA_ETL].[PROC_UPDATE_STR_UPC_SL_STS]
Created ON: 12/27/2021 1:52:15 AM
Created for Data Driven Alert
******/

CREATE PROC [EDAA_ETL].[PROC_UPDATE_STR_UPC_SL_STS] AS
Begin

DECLARE @SALESDT INT ;
DECLARE @TOTUT INT ;
DECLARE @TOTPHRM INT ;
DECLARE @UTSALES INT ;
DECLARE @PHRMSALES INT ;
DECLARE @STRUTNOSALE INT ;
DECLARE @PHRMUTNOSALE INT ;
DECLARE @STRUTNO varchar(max) ;
DECLARE @PHRMUTNO varchar(max) ;


update [EDAA_DW].[STR_UPC_SL_STS] set FLG_LOADED='N' where cast(last_refreshed as date)<= dateadd(dd,-1, cast(getdate() as date)) and flg_loaded<>'C'
;
print('Flag is reset')



;with cte_alrt as
(select POS_UT_ID as UT_ID, max(POS_STO_POS_BUS_DT) max_dt from EDAA_STG.UT_UPC_DLY_SL_HST group by POS_UT_ID --having sum(POS_SSK_T_SL_AM)>0.00 or sum(tg_sl_am)<-0.00 )
)
update
 [EDAA_DW].[STR_UPC_SL_STS] set
	TM_SKY_SL = T2.max_dt,
	FLG_LOADED = case when T2.max_dt=Convert(CHAR(8), getdate()-1,112) then 'Y' else 'N' end,
	LAST_REFRESHED = getdate()
from [EDAA_DW].[STR_UPC_SL_STS] AS T1
JOIN cte_alrt AS T2
ON ( T1.STR_ID = T2.UT_ID)
;

SELECT @SALESDT = max(POS_STO_POS_BUS_DT)  from EDAA_STG.UT_UPC_DLY_SL_HST ;
SELECT @TOTUT = count(distinct STR_ID) from  [EDAA_DW].[STR_UPC_SL_STS]  where FLG_LOADED<>'C' ;
SELECT @TOTPHRM = count(distinct STR_ID) from  [EDAA_DW].[STR_UPC_SL_STS]  where PHM_FLG<>'N' and FLG_LOADED<>'C' ;
SELECT @UTSALES = count(distinct STR_ID) from  [EDAA_DW].[STR_UPC_SL_STS]  where FLG_LOADED='Y' ;
SELECT @PHRMSALES = count(distinct STR_ID) from  [EDAA_DW].[STR_UPC_SL_STS]  where FLG_LOADED='Y' and PHM_FLG<>'N' ;
SELECT @STRUTNOSALE = count(distinct STR_ID) from  [EDAA_DW].[STR_UPC_SL_STS]  where FLG_LOADED='N' ;
SELECT @PHRMUTNOSALE = count(distinct STR_ID) from  [EDAA_DW].[STR_UPC_SL_STS]  where FLG_LOADED='N' and PHM_FLG<>'N' ;
SELECT @STRUTNO = STRING_AGG( ISNULL(STR_ID, ' '), ',') from  [EDAA_DW].[STR_UPC_SL_STS]  where FLG_LOADED='N' and PHM_FLG='N';
SELECT @PHRMUTNO = STRING_AGG( ISNULL(STR_ID, ' '), ',') from  [EDAA_DW].[STR_UPC_SL_STS]  where FLG_LOADED='N' and PHM_FLG<>'N' ;

delete from [EDAA_DW].[STR_UPC_SL_STS_ALRT];
insert into [EDAA_DW].[STR_UPC_SL_STS_ALRT] values
(	@SALESDT ,
	@TOTUT ,
	@TOTPHRM ,
	@UTSALES ,
	@PHRMSALES ,
	@STRUTNO ,
	@PHRMUTNO
	) ;
print('Message inserted!')

End
