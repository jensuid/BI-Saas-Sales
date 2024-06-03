with base_contracts as (
select 
   leads_id,
   cast(contract_id as int) as contract_id,
   date(date) as start_date,
   date(date, '+1 years') as expired_date
from contracts

),
current_contracts as(
    select
        leads_id,
        month_period,
        max(contract_id) as current_contract_id,
        max(start_date) as starting_date,
        max(expired_date) as expired_date

    from  base_contracts bc, month_dim as md
    where 
        date(bc.start_date,'start of month') <= date(md.month_period)
        and date(md.month_period,'-1 months') <= date(bc.expired_date,'start of month')   
    group by leads_id,month_period
    order by cast(leads_id as int), month_period
),
activity_contracts as (
    select
        *,
        case 
            when  date(starting_date,'start of month') <= month_period
            and month_period <= date(expired_date,'start of month') 
            then 1 else 0 end as active_flag

    from current_contracts
),
psf_contracts as (
    select 
       cc.*,
       c.subscription_type,
       c.number_of_employee,
       c.reten_flag,
       c.user_price,
       c.gmv as contract_value,
       lag(c.subscription_type) over wd as prev_subscription_type,
       case when  lag(c.subscription_type) over wd is not null then
          (case when lag(c.subscription_type) over wd != c.subscription_type
          then 1 else 0 end) end as changing_flag

    from  activity_contracts  cc
          left join contracts c
          on cc.current_contract_id = contract_id
    window wd as (
            partition by cc.leads_id
            order by cc.month_period )
)
select 
   *
from psf_contracts
-- limit 500
;