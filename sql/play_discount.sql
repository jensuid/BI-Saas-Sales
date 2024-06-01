--- dimension discount
with base_discount_dim as (
select
   'D0' as discount_type_code,
   'No_Discount' as discount_type,
   0 as discount_percent
union
select
   'D1' as discount_type_code,
   '2021_quarter_discount' as discount_type,
   10 as discount_percent
union
select
   'D2' as discount_type_code,
   '2022_quarter_discount' as discount_type,
   20 as discount_percent
),
--- create discount fact
base_discounts_fact as (
    select
       contract_id,
       discount_type_code,
       discount_type,
       month_discount,
       deal_won won_date,
       user_price_after,
       discount_reten_flag reten_flag,
       -- add discount percent 
       case  discount_type_code
         when 'D1' then 10
         when 'D2' then 20
        end discount_percent
    from discounts
),
--- no of contract with discount
with_discount as (
select 
   strftime('%Y-%m',deal_won) month,
   discount_type_code,
   count(distinct contract_id) as no_of_contract
from base_discounts_fact
group by 1,2
),
-- final discount fact table
final_discounts_fact as (
select 
    d.*,
    c.number_of_employee no_of_employee,
    c.user_price user_price,
    c.user_price -  d.user_price_after as discount_user_price,
    c.number_of_employee * (c.user_price -  d.user_price_after) as discount_amount
from base_discounts_fact d
left join contracts c 
on d.contract_id = c.contract_id
)
 select *
 from final_discounts_fact
-- .sclimit 5
;