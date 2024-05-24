-- query duration per process sales step 
-- per drop state
-- select
--     step_id,
--     -- drop_flag,
--     ceil(avg(duration)) as average,
--     ceil(min(duration)) as min,
--     ceil(max(duration)) as max
-- from funnels_fact
-- group by step_id --,drop_flag

--- query total no of leads every funnels stage
-- select
--     f.stage,
--     count(distinct f.leads_id) as total_leads,
--     100 * count(distinct f.leads_id) /
--           first_value( count(distinct f.leads_id)) over 
--           (order by f.stage_id) as win_rate,
--     100 * count(distinct f.leads_id)/
--           lag( count(distinct f.leads_id)) over 
--           (order by f.stage_id) as conversion_rate
-- from funnels_fact f
-- join pipeline_acf p on f.leads_id = p.leads_id 
-- where drop_flag = 0
-- -- insert filter period
-- -- exmaple last 60 days ( range from max day - 60 days)
-- -- last daya '2022-12-31'
-- and date(p.starting_ts) >= date('2022-12-31','-3 years')
-- -- and date(f.timestamp) <= date('2022-03-30')
-- and date(p.starting_ts) <= date('2022-12-31')
-- -- and leads_id registered on that 
-- group by 1
-- order by f.stage_id
----------------

--- reason by stage 
-- select 
--    case step_id
--         when 1 then 'Lead Qualification'
--         when 2 then 'Starting Communication'
--         when 3 then 'Approach'
--         when 4 then 'Discussion'
--         when 5 then 'Sending Offer'
--         when 6 then 'Checking Quotation Received'
--         when 7 then 'Dealing to Contract Signing'
--       end as step_name,
--    step_id,
--    reason,
--    count(distinct leads_id) as total,
--    100 * count(distinct leads_id)  /
--    sum (count(distinct leads_id)) over (
--      partition by step_id
--    ) as percent
   
-- from funnels_fact
-- where reason != ''
-- group by 1,2,3
-- order by step_id
--------------------------------

-- query weekly open_leads periodic
with base_weekly as (
select
   distinct
   leads_id,
   step_id,
   start_of_week,
   case when date(ending_ts) >= date(start_of_week)
        and date(starting_ts) <= date(end_of_week)
        then 1 else 0 end as open
from open_leads_psf
),
base_monthly as (
select
   distinct
   leads_id,
   step_id,
   date(start_of_week,'start of month') month,
   case when date(ending_ts,'start of month') >= date(start_of_week,'start of month')
        and date(starting_ts,'start of month') <= date(start_of_week,'start of month')
        then 1 else 0 end as open
from open_leads_psf
),
total_monthly as (
select
--    start_of_week,
   strftime('%Y-%m',month) month,
--    step_id,
   count(distinct leads_id) as total_leads,
   count(case when step_id = 0 then leads_id  end)  as new_leads,
   count(distinct leads_id) -
    count(case when step_id = 0 then leads_id  end) as open_leads
from base_monthly
where open =  1
group by 1
order by 1
),
----- weekly and monthly
combined_open as (
select 
   leads_id,
   step_id,
   start_of_week,
   date(start_of_week,'start of month') month,
   starting_ts,
   ending_ts,
  -- open flag for weekly
   case when date(ending_ts) >= date(start_of_week)
        and date(starting_ts) <= date(end_of_week)
        then 1 else 0 end as open_weekly,

   -- open flag for monthly
   case when date(ending_ts,'start of month') >= date(start_of_week,'start of month')
        and date(starting_ts,'start of month') <= date(start_of_week,'start of month')
        then 1 else 0 end as open_monthly
    
from open_leads_psf
order by cast(leads_id as int),2,3
),
final_open_leads as (
select
   *
 from combined_open
 where cast(open_weekly as int) +
       cast(open_monthly as int) != 0
)
 select *
 from final_open_leads

;