with base_contracts as (
select 
   leads_id,
   date(date) as start_date,
   cast(contract_id as int) as contract_id,
   lag(subscription_type) over ( 
         partition by leads_id
         order by cast(contract_id as int) ) as prev_subs_type,
    subscription_type,
    date(date, '+1 years') as expired_date
from contracts
-- order by cast(leads_id as int),2;
),
inside_period as (
select
   md.month_period as month_period,
   leads_id,
   max(cast(contract_id as int)) as contract_id,
   max(date(start_date,'start of month')) start_date,
   max(date(expired_date,'start of month')) expired_date,
   max(
   case 
   when date(start_date,'start of month') <= md.month_period
   and md.month_period <= date(expired_date,'start of month') 
   then 1 else 0 end) as in_period

from base_contracts, month_dim md
--where  leads_id in (2,6,8,10)
where date(start_date,'start of month') <= date(md.month_period)
and date(md.month_period) <= date(expired_date,'start of month') 
group by 1,2
order by 
         cast(leads_id as int),
         md.month_period,
         -- cast(leads_id as int),
          start_date
)
select 
   *
from inside_period
;