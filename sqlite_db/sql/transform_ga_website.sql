-- transform ga-website fact
with base_ga_fact as (
select
   cast(leads_id as int) as leads_id,
   ga_id,
   Page_Name,
   datetime(page_timestamp) as page_ts,
   cast(row_number() over (
      partition by leads_id
      order by datetime(page_timestamp)
   )as int) as seq_id,
   
  round((lead(julianday(page_timestamp)) over wd - 
       julianday(page_timestamp))*24*60*60,4) as time_spent,

--   round((first_value(julianday(page_timestamp)) over (
--          partition by leads_id
--          order by datetime(page_timestamp) desc) - 
--          first_value(julianday(page_timestamp)) over wd)*24*60*60,4) as duration,
  
  instr(Page_Name,'/form') as form_page,
  case when (instr(Page_Name,'/home') +
            instr(Page_Name,'/form')) > 0
       then 0 else 1 end as info_page 

from 'ga-website'
window wd as (
      partition by leads_id
      order by datetime(page_timestamp)
   )
order by 1,4,2
),
---- accumulated periodic ga
base_acc_ga as (
    select
        distinct
        leads_id,
        last_value(seq_id) over wd as no_of_page,
        first_value(page_name) over wd as first_page,
        last_value(page_name) over wd as last_page,
        first_value(page_ts) over wd as first_ts,
        last_value(page_ts) over wd as last_ts,
        round((julianday(last_value(page_ts) over wd)-
              julianday(first_value(page_ts) over wd))*24*60*60,2) 
              as session_duration,
        sum(form_page) over wd as no_form_page,
        sum(info_page) over wd as no_info_page,
        last_value(replace(page_name,'/form/','')) over wd as cta
    from base_ga_fact
    window wd as (
            partition by leads_id
            order by seq_id
            rows between unbounded preceding and unbounded following)
)
select *
from base_ga_fact
order by leads_id
;