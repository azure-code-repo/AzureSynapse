CREATE PROC [EDAA_ETL].[PROC_FCT_DLY_STR_ITMSKU_INV_INCL_POSTPROCESS] @flag [char] AS

declare @mydate date
select @mydate = cast(case when @flag='F' then '2022-04-01' else DATEADD(d,-45, getdate()) end as date)
print(@flag)
print(@mydate)

begin
----step1
;with inv as (
select case when inv_row_end_dt>dateadd(day,-1, dt) then dateadd(day,-1, dt) else inv_row_end_dt end  dd, x.*
from (select lead(inv_row_start_dt) over (partition by str_id, itm_sku order by inv_row_start_dt) dt, t.*
from [EDAA_DW].[FCT_DLY_STR_ITMSKU_INV] t where (inv_row_start_dt>=@mydate or inv_row_end_dt>=@mydate)) x
)
update [EDAA_DW].[FCT_DLY_STR_ITMSKU_INV] set
inv_row_end_dt = t2.dd
from [EDAA_DW].[FCT_DLY_STR_ITMSKU_INV] t1, inv t2
where t1.str_id=t2.str_id and t1.itm_sku=t2.itm_sku and t1.inv_row_start_dt=t2.inv_row_start_dt and t1.inv_row_end_dt=t2.inv_row_end_dt
;
----step2
;with inv2 as (select  concat(inv_each_amt,'-',inv_qty,'-',inv_clrnc_ind) cn,
lead(concat(inv_each_amt,'-',inv_qty,'-',inv_clrnc_ind),1) over (partition by str_id, itm_sku order by inv_row_start_dt) rn, t.*
from [EDAA_DW].[FCT_DLY_STR_ITMSKU_INV] t where (inv_row_start_dt>=@mydate or inv_row_end_dt>=@mydate)
)
update [EDAA_DW].[FCT_DLY_STR_ITMSKU_INV]
set aud_ins_sk= t1.aud_ins_sk + 20000000
from [EDAA_DW].[FCT_DLY_STR_ITMSKU_INV] t1, inv2 as t2
where t1.str_id=t2.str_id and t1.itm_sku=t2.itm_sku and t1.inv_row_start_dt=t2.inv_row_start_dt and t1.inv_row_end_dt=t2.inv_row_end_dt and t2.cn=t2.rn
;
----step3
delete from  [EDAA_DW].[FCT_DLY_STR_ITMSKU_INV] where aud_ins_sk>20000000 ;

----step4
with inv3 as (
select dateadd(day,1,lag(inv_row_end_dt) over (partition by str_id, itm_sku order by inv_row_end_dt)) stdt,  *
from [EDAA_DW].[FCT_DLY_STR_ITMSKU_INV] where
(inv_row_start_dt>=@mydate or inv_row_end_dt>=@mydate)  )
update [EDAA_DW].[FCT_DLY_STR_ITMSKU_INV]
set inv_row_start_dt=t2.stdt
from [EDAA_DW].[FCT_DLY_STR_ITMSKU_INV] t1, inv3 as t2
where t1.str_id=t2.str_id and t1.itm_sku=t2.itm_sku and t1.inv_row_start_dt=t2.inv_row_start_dt and t1.inv_row_end_dt=t2.inv_row_end_dt
and t1.inv_row_start_dt<>t2.stdt and t2.stdt is not null
;

end;
