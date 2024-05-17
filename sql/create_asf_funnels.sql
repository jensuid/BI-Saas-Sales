with base_funnels as (select 
  cast(leads_id as integer) leads_id, 
  stage,
  datetime(replace(timestamp,"UTC","")) as timestamp,
  case reason when "" then 1 else 0  end as isPassed,
  reason
from funnels
)
select 
 distinct leads_id,

 last_value(stage) over lead_window as last_stage,
 first_value(timestamp) over lead_window as start_ts,
 last_value(timestamp) over lead_window as last_ts,
 last_value(isPassed) over lead_window as last_isPassed,
 last_value(reason) over lead_window  as reason,
 round(julianday(last_value(timestamp) over lead_window) -
 julianday(first_value(timestamp) over lead_window),4) as cycle_duration,

 case (last_value(stage) over lead_window) when "deal_won" then  
       case (last_value(isPassed) over lead_window) when 1 then "won"
             else "loss" end
 else 
     (case (last_value(isPassed) over lead_window) when 1 then "open" 
             else "loss" end)
 end as status

 -- if last_stage = deal_won then  
 --   (if last_isPassed = 1 then won else loss)
 --  else ( if last_isPassed = 1 then open else loss ) 
 -- last-status result win , lost , open

from base_funnels
window lead_window as (
  partition by leads_id
     order by timestamp 
     rows between unbounded preceding and unbounded following
)
order by leads_id
;