with base_funnel as (
select 
   cast(funnel_id as int) funnel_id,
   cast(leads_id as int) leads_id,
   cast(stage_id as int) stage_id,
   case stage_id
      when '101' then "Lead Registered"
      when  '102' then "Lead Qualification"
      when  '103' then 'Initial Communication Success'
      when  '104' then 'Approach Success'
      when  '105' then 'Discussion Success'
      when  '106' then 'Offer Sent'
      when  '107' then 'Quatation Received'
      when  '108' then 'Deal Won'
   end as stage_name,
   date(timestamp) as stage_date,
   time(timestamp) as stage_time,
   case reason
      when 'not answered' then 'Not Answered'
      when 'not selected' then 'Not Selected' 
      when 'price not fit' then 'Price Not Fit'
      when 'not interested ' then 'Not Interested'
      when 'already use other products' then 
            'Use Other Products'
   end as reason,
   case reason
      when '' then 0
      when 'not answered' then 1
      when 'not selected' then 2 
      when 'price not fit' then 3
      when 'not interested ' then 4
      when 'already use other products' then 5
   end as reason_id,
   case when reason = '' then 0 else 1 end drop_flag
from funnels
-- limit 10
),
final_funnel as ( 
select 
   f.*,
   md.month_id as stage_month_id,
   month_period as stage_month
from base_funnel f
join date_dim md
on date(f.stage_date,'start of month') = md.month_period
),
--- find first date 
lead_generated as (
    select
       md.month_id as  stage_month_id,
       md.month_period,
       f.leads_id,
       f.stage_date
    from final_funnel f, date_dim md
    where f.stage_id = 101
    and md.month_period = f.stage_month
),
-- list all leads id stage pass thru
base_acc_funnel as (
    select
       f.*,
       lg.stage_date as first_date,
       lg.month_period as first_month
    from final_funnel f 
    left join lead_generated lg
    on f.leads_id = lg.leads_id
),
 --- monthly periodic status
 --  materialized into periodic stage
base_acc_periodic as (
   select
       md.month_period,
       f.*
   from base_acc_funnel f, date_dim md
   where f.first_month <= md.month_period
   and f.stage_month >= md.month_period
) ,
 
--- current status each leads_id
current_status_leads as (
    select
       distinct
       month_period,
       leads_id,
       first_value(stage_id) over wd as current_stage_id,
       first_value(drop_flag) over wd as current_drop_flag,
       last_value(stage_id) over wd as first_stage_id,
       -- new leads 
       case when (last_value(stage_id) over wd) = '101' then
            1 else 0 end as new_flag,

       -- status
       case when (first_value(drop_flag) over wd ) = '1' then 'lost'
       else case when (first_value(stage_id) over wd) = '108' then 'won' 
            else 'open' end
       end as current_status 
    from periodic_stage
    where  stage_month = month_period -- limit only this period
    window wd as (
        partition by month_period,leads_id
        order by stage_id desc
        rows between unbounded preceding and
                     unbounded following
    )
),

-- aggregate current status  monthly
agg_current_status as (
    select
    month_period,
    sum(new_flag) as total_new,
    coalesce(lag(sum(case when current_status = 'open' then 1 else 0 end))
          over(),0) as last_period_open,

    -- total opputunities
     sum(new_flag) +
     coalesce(lag(sum(case when current_status = 'open' then 1 else 0 end))
          over(),0) as total_opportunities,
    
    sum(case when current_status = 'won' then 1 else 0 end) total_won,
    sum(case when current_status = 'lost' then 1 else 0 end) total_lost,
    
    sum(case when current_status = 'open' then 1 else 0 end) total_open
from current_status_leads
group by 1
),
--- periodic lower grain is weekly
base_first_date as (
    select
       distinct
       leads_id,
       first_value(stage_date) over(
         partition by leads_id
         order by stage_id
       ) as first_date

    from base_funnel
  order by 1
),
 --- transform first date week 
 base_first_week as (
    select
       leads_id,
       first_date,
       --- transform into weekly date
       case when strftime('%w',first_date)  = 1 then first_date
            else case when strftime('%w',first_date)  > 1  then
            date(first_date,'-'||(strftime('%w',first_date) - 1)||' days')
            else  date(first_date,'-6 days')
            end end as first_week
    from base_first_date
 ),
 --- combined to funnel
 merge_first_week as (
    select
        f.*,
        w.first_date,
        w.first_week,
        --- transform into weekly date
       case when strftime('%w',stage_date)  = 1 then stage_date
            else case when strftime('%w',stage_date)  > 1  then
            date(stage_date,'-'||(strftime('%w',stage_date) - 1)||' days')
            else  date(stage_date,'-6 days')
            end end as stage_week
    from base_funnel f
    join base_first_week w 
    on f.leads_id = w.leads_id

 ),
----- create cross join
---  periodik weekly time frame
 weekly_acc_periodic as (
   select
       f.*,
       dd.week_id periodic_week_id,
       dd.start_of_week periodic_sow,
       dd.end_of_week periodic_eow,
       date(f.stage_date,'start of month') periodic_month
   from merge_first_week f, date_dim dd
   where f.first_date <= dd.end_of_week
   and f.stage_date >= dd.start_of_week
   -- and f.stage_date >= date(dd.start_of_week,'-7 days')
 ),
 ---------- final  periodic funnel 
 final_acc_periodic as (
      select 
         w.*,
         dd.start_of_quarter periodic_quarter,
         dd.quarter_label periodic_quarter_label
      from weekly_acc_periodic w
      left join (select distinct month_period,start_of_quarter,quarter_label from date_dim) dd
      on w.periodic_month = dd.month_period
 )
----- query -----
select
   leads_id,stage_id,first_week,stage_week,periodic_sow
from weekly_acc_periodic
limit 50
;