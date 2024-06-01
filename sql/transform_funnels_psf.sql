with sample_fc as (
     select 
         leads_id,
         step_id,
         case when starting_ts = '' 
              then date(timestamp)
              else date(starting_ts) end as starting_ts,
         date(timestamp) as ending_ts
     from funnels_fact
    --  where cast(leads_id as int) < 50
     order by cast(leads_id as int),2
    -- limit 25
),
 wp as (
     select *
     from weekly_dim
    --  limit 10
 ),

in_range_period as (
select
   fc.leads_id,
   fc.step_id,
   date(fc.starting_ts) starting_ts,
   wp.start_of_week,
   date(fc.ending_ts) as ending_ts,
   wp.end_of_week
   
from sample_fc fc,wp
where fc.step_id > -1
and date(fc.ending_ts) >= date(wp.start_of_week,'-7 days')
    and  date(fc.starting_ts) <= date(wp.end_of_week)
order by cast(fc.leads_id as int),2
),
all_open as (
select 
    start_of_week,
    -- step_id,
    count(distinct leads_id) as total_leads,
    count(distinct case when step_id = 0 then leads_id  end ) as new_leads,
    count(distinct leads_id)-  
         count(distinct case when step_id = 0 then leads_id  end ) 
         as open_leads
from in_range_period
group by 1
order by 1
),
monthly_open as (
select 
    distinct
    leads_id,
    step_id step,
    -- date(start_of_week,'start of month') as month_period,
    strftime('%m-%Y',start_of_week) as month,
    start_of_week sow,
    starting_ts start,
    strftime('%m-%Y',starting_ts) starting,
    ending_ts end,
    end_of_week eow,
    case 
       when (date(starting_ts,'start of month') <= date(start_of_week,'start of month'))
          and (date(start_of_week,'start of month') <= date(ending_ts,'start of month')) 
       then 1 else 0 
    end as open
from in_range_period
--where --cast(leads_id as int) < 10
--  (date(starting_ts,'start of month') <= date(start_of_week,'start of month'))
-- and (date(start_of_week,'start of month') <= date(ending_ts,'start of month')) 
order by 1,2,3,4
-- limit 50
)
select 
   *
from in_range_period
;