with date_range as(
    -- store first and last date value 
    select 
         min(date(replace(timestamp,"UTC",''))) as first_dt,
        date(max(date(replace(timestamp,"UTC",''))),'+1 months') as last_dt
    from funnels
), 
date2quarter as 
(
     select 
         first_dt,
         quarter,
         date(case 
            when month < 10 then (year||'-0'||month||'-01') 
            else(year||'-'||month||'-01')
         end) as first_quarter_dt,
         'Q'||quarter||'-'||year as label
         
     from (
            select
                first_dt,
                cast(ceil(cast(strftime('%m',first_dt,'start of month')as real)/3)as int) as quarter,
                1 + (cast(ceil(cast(strftime('%m',first_dt,'start of month')as real)/3)as int)-1)*3  as month,
                strftime('%Y',first_dt) as year
            from date_range)
),
monthly_series as(
    select 
         1 as row_id,
         date((select first_dt from date_range),'start of month') as month_period
    union all
    select
         row_id + 1,
         date(month_period,'+1 months') as month_period
    from monthly_series
    where  month_period < date((select last_dt from date_range),'start of month')
),
quarter_series as (
    select 
        1 as cnt,
        date((select first_quarter_dt from date2quarter),'start of month') as period
    union all
    -- recursive part
    select
       cnt + 1,
       date(period,'+3 months') as period
    from quarter_series 
    where period < date((select last_dt from date_range),'start of month')
),
final_quarter as (
    select
    cnt as quarter_id,
    period as start_of_quarter,
    date(period,'+3 months') as end_of_quarter
 from quarter_series
),
 --- month series to quarter series
 adding_quarter as (
    select 
    row_id as month_id,
    month_period,
    -- find no of quarter ceil(month/3)
    -- find start of quarter date
    -- find  month part 1 + (month -1)/3
    case when cast(1+ (ceil(strftime('%m',month_period) / 3.0)-1)*3 as int) < 10 then
    date(strftime('%Y',month_period)||'-0'||cast(1+ (ceil(strftime('%m',month_period) / 3.0)-1)*3 as int)||'-01')
    else date(strftime('%Y',month_period)||'-'||cast(1+ (ceil(strftime('%m',month_period) / 3.0)-1)*3 as int)||'-01') end
         as start_of_quarter,

    --- add quarter label
    strftime('%Y',month_period)||'-Q'||cast(ceil(strftime('%m',month_period) / 3.0) as int) as quarter_label
 from monthly_series
 ),
 --- full month_quarter
 month_quarter as (
    select 
    aq.month_id,
    aq.month_period,
    fq.*,
    aq.quarter_label
 from adding_quarter aq
 full join final_quarter fq
 on aq.start_of_quarter = fq.start_of_quarter
 ),
 ----- create weekly series
 base_weekly_series as(
    select 
         1 as row_id,
         date('2020-11-23','-7 days') as week_period
    union all
    select
         row_id + 1,
         date(week_period,'+7 days') as week_period
    from base_weekly_series
    where  week_period < date('2022-12-31')
    and row_id <= 1000
 ),
weekly_series as (
select 
   row_id as week_id,
   week_period as start_of_week,
   date(week_period,'+6 days') as end_of_week,
   strftime('%W',week_period) as week_of_year,
   date(week_period,'start of month') as month_part
from base_weekly_series
)

select 
   w.week_id,
   w.start_of_week,
   w.end_of_week,
   w.week_of_year,
   mq.*
from weekly_series W
full join month_quarter mq 
on w.month_part = mq.month_period
 ;