with Ancestor as (
    select 
       parent as p 
    from Parentof
    where child = "Frank"

    union all

    select parent
    from Ancestor, parentof as pf
    where Ancestor.p = pf.child
)

select * from Ancestor;