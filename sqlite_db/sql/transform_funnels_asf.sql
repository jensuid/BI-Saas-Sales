with sales_steps as (
select
   leads_id,
   funnel_id,
   stage,
   cast(stage_id as int) stage_id,
   timestamp,
   cast(stage_id as int) - 101  as step_id,
   lag(funnel_id) over wd as prev_funnel_id,
   lag(timestamp) over wd as starting_ts,
   round(julianday(timestamp) - julianday(lag(timestamp) over wd ),2) as duration,
   case when reason = '' then 0 else 1 end as drop_flag,
   reason
from funnels
   window wd as (
        partition by leads_id
        order by cast(stage_id as int)
    )
),
sales_pipeline as (
   select 
      distinct 
      leads_id ,
      first_value(stage_id) over wd as last_stage_id,
      first_value(stage) over wd as last_stage,
      first_value(step_id) over wd as last_step,
      datetime(first_value(timestamp)  over (
          partition by leads_id
          order by stage_id
      )) as starting_ts,
      first_value(timestamp) over wd as ending_ts,

    case when (first_value(stage_id) over wd = 108) then
               case when first_value(drop_flag) over wd = 0 then 'won' else 'lost' end
          else
               case when first_value(drop_flag) over wd = 0 then 'open' else 'lost' end
    end as status, 
    first_value(reason) over wd as reason,
     
     round(julianday(first_value(timestamp) over wd)- 
     julianday(first_value(timestamp)  over (
          partition by leads_id
          order by stage_id
      )),2) as cycle_duration

   from sales_steps
   window wd as (
      partition by leads_id
      order by stage_id desc
   )
),
base_weekly_series as(
    select 
         1 as row_id,
         date('2020-11-23') as week_period
    union all
    select
         row_id + 1,
         date(week_period,'+7 days') as week_period
    from base_weekly_series
    where  week_period < date('2022-12-31')
    and row_id <= 1000
),

final_steps as (
  select 
      *,
      case step_id
        when 1 then 'Lead Qualification'
        when 2 then 'Starting Communication'
        when 3 then 'Approach'
        when 4 then 'Discussion'
        when 5 then 'Sending Offer'
        when 6 then 'Checking Quotation Received'
        when 7 then 'Dealing to Contract Signing'
      end as step_name
  from sales_steps
--   limit 200
),
weekly_series as (
select 
   row_id as week_id,
   week_period as start_of_week,
   date(week_period,'+6 days') as end_of_week,
   strftime('%W',week_period) as week_of_year
from base_weekly_series
)

--- cross join sales_steps and weekly series
select
  s.leads_id,
  s.stage_id,
  s.step_id,
  w.start_of_week
from  sales_steps s,(select * from weekly_series limit 10) w;
where date(s.starting_ts) <= date(w.start_of_week)
and  date(s.ending_ts) >= date(w.end_of_week)
and cast(s.leads_id as int) < 50
limit 50
;