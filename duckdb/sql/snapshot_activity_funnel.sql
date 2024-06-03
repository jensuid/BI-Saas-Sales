--- CREATE VIEW v_psfact_funnel as (
--- first CTE
with sampling_funnel as (
   SELECT 
      leads_id,
      funnel_id,
      stage_id,
      start_date,
      stage_date,
      last_stage_id,
      first_stage_flag,
      final_stage_flag,
      stage_drop,
      deal_won
   FROM v_funnel
   --limit 1000
),
---- create monthly series 
monthly_series as (    
      select
         DISTINCT
         --row_number() over () as month_id,
         date_trunc('month',week_date) as month_date
      from main.weekly 
),
---- create quaarterly series 
quarterly_series as (    
      select
         DISTINCT
         --row_number() over () as quarter_id,
         date_trunc('quarter', week_date) as quarter_date
      from main.weekly 
),

---- monthly periode begin
snapshot_monthly_range as (  
    select 
       m.month_date,
       f.*
   from v_funnel f, monthly_series m
   where date_trunc('month',start_date) <=  m.month_date
   and date_trunc('month',stage_date) >= m.month_date
   --and f.leads_id = 1
),
 
---- snapshot weekly using cross join 
snapshot_weekly_range AS (
   select 
       w.week_date,
       date_trunc('month',w.week_date) AS month_week,
       f.leads_id,stage_id,
       funnel_id,
       start_date,
       stage_date
   from sampling_funnel f, weekly w
   where date_trunc('week',start_date) <=  w.week_date
   and date_trunc('week',stage_date) >= w.week_date
  -- and f.leads_id = 1
),

---- merge all range snapshot timeframe
snapshot_merge_range AS (
   SELECT 
      --m.month_date,
       w.week_date,
       date_trunc('quarter',m.month_date) AS quarter_date,
       m.*
      
   FROM snapshot_weekly_range w 
   right  JOIN snapshot_monthly_range m 
   ON m.month_date = w.month_week
   AND m.funnel_id = w.funnel_id
 ),
 
---- evaluate state all time frame
evaluate_state_merge as (
       select 
        *,
       --month_date,week_date,quarter_date,
       --leads_id,
       -- stage_id,
      -- funnel_id,start_date,stage_date,
       --- asses in process
       --CASE WHEN week_date IS NOT NULL then
       --(case when date_trunc('week',stage_date) > week_date then 1 else 0 END) end as ongoing_flag,
       --stage_drop,
       --- if done over weekly
       CASE WHEN stage_id > 101 THEN stage_id END AS step_id, 
       CASE WHEN week_date IS NOT NULL then
       (case when date_trunc('week',stage_date) > week_date   then 'in_process'
           else  case when stage_drop = 1 then 'lost' else 'success' end end) end as weekly_activity_status,
        -- state assesment by monthly
        case when date_trunc('month',stage_date) > month_date   then 'in_process'
        else  case when stage_drop = 1 then 'lost' else 'success' end END AS monthly_activity_status,
         -- state assesment by qurterly
        case when date_trunc('quarter',stage_date) >  quarter_date   then 'in_process'
        else  case when stage_drop = 1 then 'lost' else 'success' end END AS quarterly_activity_status  
        --from snapshot_merge_range
        from v_snapshot_activity_funnel
),

---- Aggreagate Number of Acricitiy sales process
weekly_agg_state as (
  select 
  week_date,
 -- stage_id,
  weekly_state,
  count(distinct leads_id) as num_activity
from evaluate_state_merge
WHERE week_date IS NOT NULL
AND weekly_state IS NOT NUll
  group by grouping sets (
      (1,2),(1)
   )
),
 --- aggreagate 
quarter_agg_state as (
  select 
  quarter_date,
 -- stage_id,
  quarter_state,
  count(distinct leads_id) as num_activity
from evaluate_state_merge
  group by grouping sets (
      (1,2),(1)
   )
),
---- prepare to calc pipeline summary
 pipeline_status AS (
     select 
   --DISTINCT 
   snapshot_id,
   month_date,week_date,quarter_date,
   leads_id,
   funnel_id,
   first_date,
   stage_date,
  -- first_value(stage_id) over wd as current_stage_id,
  -- first_value(stage_drop) over wd as current_stage_drop,
  --  last_value(stage_id) over wd as first_stage_id,
   case when (last_value(first_stage_flag) over wd) = 1 then
            1 else 0 end as new_lead_flag,
   
   CASE WHEN first_value(stage_drop) over wd = 1 THEN 'lost' ELSE 
        CASE WHEN first_value(final_stage_flag) over wd = 1 THEN 'won ' ELSE 'open' END
   END AS monthly_pipeline_status,
    
    CASE WHEN first_value(stage_drop) over qrt = 1 THEN 'lost' ELSE 
        CASE WHEN first_value(final_stage_flag) over qrt = 1 THEN 'won ' ELSE 'open' END
    END AS quarterly_pipeline_status
    
FROM v_snapshot_step_status
WHERE date_trunc('month',stage_date) = month_date
--OR date_trunc('quarter',stage_date) = quarter_date 
WINDOW wd AS (
  PARTITION BY month_date,leads_id
  order by stage_id desc
        rows between unbounded preceding and
                     unbounded following
      ),
   qrt AS (
   PARTITION BY quarter_date,leads_id
   order by stage_id desc
        rows between unbounded preceding and
                     unbounded following
   ) 
 ),
 ---- merge pipeline status into snapshot table
 merge_snapshot AS (
     select
       s.*,
       p.new_lead_flag,
       p.monthly_pipeline_status,
       p.quarterly_pipeline_status,
     FROM pipeline_status p 
     RIGHT JOIN v_snapshot_step_status s
     ON p.snapshot_id = s.snapshot_id
 )

 --  main query
 SELECT
     *
 FROM merge_snapshot
 ORDER BY 1,2

--)
--) to '/Volumes/JensData/jensWork/BI_Project/BI-Saas-Sales/duckdb/output/quarter_snapshot_activity.csv'
;