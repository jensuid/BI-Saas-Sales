with date_range as(
    -- store first and last date value 
    select 
         min(date(replace(timestamp,"UTC",''))) as first_dt,
         max(date(replace(timestamp,"UTC",''))) as last_dt
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
    from series 
    where period < date((select last_dt from date_range),'start of month')
)
select * from monthly_series;