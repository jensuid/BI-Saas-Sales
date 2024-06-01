select
   -- monthly agg win rate each stage 

   date_trunc('month',stage_date) as month,
   stage_id,
   count(distinct leads_id) num_leads,
   first_value(count(distinct leads_id)) over (
       partition by date_trunc('month',stage_date)
       order by stage_id
   ) new_leads,
   round(100 * count(distinct leads_id) /
   first_value(count(distinct leads_id)) over (
       partition by date_trunc('month',stage_date)
       order by stage_id
   ),2) as win_rate
from v_funnel
   where stage_drop = 0
   and date_trunc('month',first_date) = date_trunc('month',stage_date)
   group by 1,2
   order by 1,2
;
