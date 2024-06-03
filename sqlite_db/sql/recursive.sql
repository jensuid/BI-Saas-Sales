-- cte recursive

with  R as (
    select 
         1 as cnt,
         (select min(julianday(datetime(replace(timestamp,"UTC","")))) from funnels ) as dt
    union all
    select 
       cnt+1,
       dt + 1
    from R 
    where cnt < 10
)
select datetime(dt )from R ;