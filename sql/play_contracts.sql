with base as(
select 
   date(date,'start of month') as month_period,
--    leads_id,
--    contract_id,
--    reten_flag
   sum( case when reten_flag = 0 then 1 else 0 end) as new_contratcs,
   sum( case when reten_flag = 1 then 1 else 0 end) as extent_contratcs,
   count( distinct leads_id) as total_contracts
from contracts
   group by 1 
order by date(date,'start of month'),leads_id
)
select 
   *,
   sum(total_contracts) over(order by month_period) as runnning
from base;