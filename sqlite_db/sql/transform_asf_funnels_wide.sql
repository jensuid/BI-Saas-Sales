with base_table as (
select 
   leads_id,
   max(stage_id) as last_stage,
   count(distinct stage_id) as no_of_stage,
   -- pivot timestamp , reason by stage_id
   max(case stage_id when 101 then datetime(replace(timestamp,"UTC","")) end ) as start_ts,
   max(case stage_id when 102 then datetime(replace(timestamp,"UTC","")) end ) as stage2_ts,
   max(case stage_id when 103 then datetime(replace(timestamp,"UTC","")) end ) as stage3_ts,
   max(case stage_id when 104 then datetime(replace(timestamp,"UTC","")) end ) as stage4_ts,
   max(case stage_id when 105 then datetime(replace(timestamp,"UTC","")) end ) as stage5_ts,
   max(case stage_id when 106 then datetime(replace(timestamp,"UTC","")) end ) as stage6_ts,
   max(case stage_id when 107 then datetime(replace(timestamp,"UTC","")) end ) as stage7_ts,
   max(case stage_id when 108 then datetime(replace(timestamp,"UTC","")) end ) as stage8_ts,
  

   -- reason
   min(case stage_id when 102 then 
   	(case reason when '' then 1 else 0 end )end ) as stage2_state,
   min(case stage_id when 103 then 
   	(case  reason when '' then 1 else 0 end ) end ) as stage3_state,
   min(case stage_id when 104 then 
   	(case reason when '' then 1 else 0 end )end ) as stage4_state,
   min(case stage_id when 105 then 
   	(case  reason when '' then 1 else 0 end ) end ) as stage5_state,
   min(case stage_id when 106 then 
   	(case  reason when ''  then 1 else 0 end ) end ) as stage6_state,
   min(case stage_id when 107 then 
   	(case  reason when '' then 1 else 0 end ) end ) as stage7_state,
   min(case stage_id when 108 then 
   	(case  reason when ''  then 1 else 0 end ) end ) as stage8_state,
   max(reason) as reason

from funnels
group by 1
-- order by 1
-- limit 50
),

processing_table as (
select
   *,
   case  last_stage
       when  '101' then start_ts
       when  '102' then stage2_ts
       when  '103' then stage3_ts
       when  '104' then stage4_ts
       when  '105' then stage5_ts
       when  '106' then stage6_ts
       when  '107' then stage7_ts
       when  '108' then stage8_ts
   end as last_ts,

   --duration
   case when stage2_ts != '' then 
       round(julianday(stage2_ts) - julianday(start_ts),2) end as stage2_duration,

   case when stage3_ts != '' then 
       round(julianday(stage3_ts) - julianday(stage2_ts),2) end as stage3_duration,

   case when stage4_ts != '' then 
       round(julianday(stage4_ts) - julianday(stage3_ts),2) end as stage4_duration,

   case when stage5_ts != '' then 
       round(julianday(stage5_ts) - julianday(stage4_ts),2) end as stage5_duration,

   case when stage6_ts != '' then 
       round(julianday(stage6_ts) - julianday(stage5_ts),2) end as stage6_duration,

   case when stage7_ts != '' then 
       round(julianday(stage7_ts) - julianday(stage6_ts),2) end as stage7_duration,

   case when stage8_ts != '' then 
       round(julianday(stage8_ts) - julianday(stage7_ts),2) end as stage8_duration,
   
   -- last state
   case when last_stage  = '108' then 
        case when reason = '' then 'won' else 'loss' end
      else
        case when reason = '' then 'open' else 'loss' end
   end as  last_state

from base_table
)

select 
   * ,
    --  cycle_duration
   round(julianday(last_ts) - julianday(start_ts),2) as cycle_duration
  
from processing_table
--limit 50
