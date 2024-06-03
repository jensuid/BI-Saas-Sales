with steps_duration as (
select 
    round(avg(stage2_duration),2) step1,
    round(avg(stage3_duration),2) step2,
    round(avg(stage4_duration),2) step3,
    round(avg(stage5_duration),2) step4,
    round(avg(stage6_duration),2) step5,
    round(avg(stage7_duration),2) step6
from funnels_asf
where stage2_state = 1
      or stage3_state = 1 
      or stage4_state = 1
      or stage5_state = 1 
      or stage6_state = 1 
      or stage7_state = 1 
),
steps as (
    select  1 as step_id ,'step1' as steps 
    union 
    select  2 as step_id ,'step2' as steps 
    union 
    select  3 as step_id ,'step3' as steps
    union 
    select  4 as step_id ,'step4' as steps 
    union 
    select  5 as step_id ,'step5' as steps
    union 
    select  6 as step_id ,'step6' as steps 
    union 
    select  7 as step_id ,'step7' as steps
)
select 1 as id, 'step1' as steps,(select step1 from steps_duration ) as duration 
   union 
   select 2 as id, 'step2' as steps,(select step2 from steps_duration ) as duration 
   union 
   select 3 as id, 'step3' as steps,(select step3 from steps_duration ) as duration 
   union 
   select 4 as id, 'step4' as steps,(select step4 from steps_duration ) as duration 
   union 
   select 5 as id, 'step5' as steps,(select step5 from steps_duration ) as duration 
   union 
   select 6 as id, 'step6' as steps,(select step6 from steps_duration ) as duration 
;