with win_rate as 
(
-- time frame  agg win rate each stage 
select
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
),
-- agg duration each stage and pipeline by timeframe
-- timeframe start-date - stage_date
duration_stage as (
   select
       date_trunc('month',stage_date) as period,
       stage_id,
       round(avg(days_duration_stage),2) as avg_days
   from v_funnel
       --- time frame
       where date_trunc('month',stage_date) = date_trunc('month',start_date) 
       group by grouping sets((2),(1,2))
),
---  trend pipeline duration by timeframe
---  measure duration from first step until final step of pipeline (closed procesess)
--   based on time range
duration_pipeline as (
   select
       date_trunc('quarter',first_date) as period,
       deal_won,
       min(days_duration_pipeline) as min_days,
       round(avg(days_duration_pipeline),2) as avg_days,
       max(days_duration_pipeline) as max_days,
   
   from v_funnel
       where last_stage_id > 101 
       --- time range 
       and date_trunc('quarter',first_date) = date_trunc('quarter',last_date)
       group by 1,2
),
----  reason drop trend  by time frame
reason as (
    select
        date_trunc('quarter',first_date) as period,
        stage_id,
        reason,
        count(distinct leads_id) as num_leads
    from v_funnel
    where stage_drop = 1
    and date_trunc('quarter',first_date) = date_trunc('quarter',last_date)
    group by 1,2,3
),
--- raason pivot for display / presentation only
reason_pivot as (
    pivot (select
           date_trunc('quarter',first_date) as period,
           leads_id,stage_id,reason 
           from v_funnel
           where stage_drop = 1
           and date_trunc('quarter',first_date) = date_trunc('quarter',last_date)
    )
    on  reason
    using count(distinct leads_id) as num_leads
    group by period, stage_id
)

select 
    *
from reason_pivot
order by 1,2
;
