-- use periodic snapshot fact funnel table
-- to get summary of sales activity and leads status activity

-- monthly summary leads status in sales pipeline
with monthly_leads_status as (
  SELECT 
  month_date,
  monthly_pipeline_status,
  count(distinct leads_id) num_leads
  --sum(new_leads) new_leads
FROM v_psfact_funnel
where monthly_pipeline_status  is not null
group by grouping sets ( (1,2),(1))
),

-- format snapshot table 
format_snapshot as (
  SELECT 
        month_date,week_date,quarter_date,
        leads_id,
        step_id,
        case  step_id
        when  102 then 'Lead Qualification' 
        when  103 then 'Starting Communication'
        when  104 then 'Setting Up Meeting'
        when  105 then 'Meeting and Discussion'
        when  106 then 'Offering Proposal'
        when  107 then 'Sending  Quotation'
        when  108 then 'Closing The Deal'
        end as step_name,
        weekly_activity_status,
        monthly_activity_status,
        quarter_activity_status,        
    from v_psfact_funnel

),

-- summary monthly sales activity in sales pipeline
monthly_sales_activity as (
    SELECT 
        week_date,
        step_name,
        weekly_activity_status,
        count(distinct leads_id)as num_activity
    from format_snapshot
    where step_name is not null
    group by grouping sets ((1,2,3),(1,2),(1))
),
--- number leads per stage over period
--- to calculate conversion rate and win reate
total_leads as (
     SELECT 
      quarter_date,
        stage_id,
        count(distinct leads_id) num_leads
     from v_psfact_funnel
     where stage_drop = 0
     and date_trunc('quarter',first_date) = quarter_date 
     and date_trunc('quarter',stage_date) = quarter_date 
     group by 1,2
) 

-- main query
SELECT 
   *
FROM total_leads
--where week_date = '2021-02-15'
order by 1,2
;