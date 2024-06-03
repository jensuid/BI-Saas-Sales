
------ combined txn discount to contract
with base_discount_contract as (
    select
       c.*,
       d.discount_type_code discount_type_code,
       d.user_price_after user_price_after
    from contracts c
         left join discounts d
         on c.contract_id = d.contract_id 
),
--- contract fact  combined to discount info
base_contracts_fact as (
select
    contract_id,
    leads_id,
    date,
    subscription_type,
    number_of_employee no_of_employee,
    reten_flag,
    user_price,
    gmv contract_value,
    case when discount_type_code is null
         then 'D0' else discount_type_code end discount_type_code,
    case when discount_type_code is null 
         then user_price else user_price_after end user_price_after,
    case  
       when discount_type_code is null then 0
       when discount_type_code = 'D1' then 10
       when discount_type_code = 'D2' then 20
    end as discount_percent
from base_discount_contract
),
-- discount monthly schedule
base_discount_schedule as (
select
   distinct
--    strftime('%Y-%m',date) month,
   date(date,'start of month') month,
   discount_type_code,
   discount_percent
from base_contracts_fact
order by date(date)
)

select 
   *
from base_contracts_fact
;