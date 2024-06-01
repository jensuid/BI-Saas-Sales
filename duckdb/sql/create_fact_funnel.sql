-- with funnel_base_trf as 
create view v_funnel as 
( select
   leads_id,
   stage_id,
   funnel_id,
   first_value(cast(timestamp as date)) over wd as first_date,
   cast(timestamp as date) stage_date,
   cast(timestamp as time) stage_time,
   lag(cast(timestamp as date)) over wd as start_date,
   stage,
   date_diff('day',lag(cast(timestamp as date))over wd, timestamp) as days_duration_stage,
   case when reason != '' then 1 else 0 end as stage_drop,
   reason,
   last_value(stage_id) over wd as last_stage_id,
   last_value(cast(timestamp as date)) over wd as last_date,
   date_diff('day',first_value(cast(timestamp as date)) over wd,
   last_value(cast(timestamp as date)) over wd) as days_duration_pipeline,
   case when stage_id = 101 then 1 else 0 end first_stage_flag,
   case when stage_id = 108 then 1 else 0 end final_stage_flag,
   case when stage_id = 108 and reason is null then 1 else 0 end deal_won
from raw_funnel
window wd as (
    partition by leads_id 
    order by stage_id
    rows between unbounded preceding and unbounded following)
-- )
-- copy (
-- select
--  *
-- from funnel_base_trf
-- -- where leads_id < 25
-- order by 1,2
-- -- limit 50
) 
-- to 'duckdb/output/fact_funel.csv' (format csv)
;