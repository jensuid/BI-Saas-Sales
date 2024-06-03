
with recursive weekly as (
   select 
      1 as week_id,
      first_week as week_date
   from date_range
   union all
   select
     week_id + 1,
     date_add(week_date, INTERVAL 1 week) as week_date
   from weekly
   where week_date <= (select last_week from date_range)
   and week_id < 200
),
date_range as (
select 
  min(stage_date) as first_date,
  max(stage_date) as last_date,
  min(date_trunc('week',stage_date)) as first_week,
  max(date_trunc('week',stage_date)) as last_week,
  min(date_trunc('month',stage_date)) as first_month,
  max(date_trunc('month',stage_date)) as last_month,
  min(date_trunc('quarter',stage_date)) as first_week,
  max(date_trunc('quarter',stage_date)) as last_week
from v_funnel 
)
-- sampling data funnel
select 
  *
 from weekly
 ;
 