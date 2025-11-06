with src_smry as (
    select
        date_trunc('day', created_at_utc) as calendar_date,
        source_type,
        count(*) as conversations,
        avg(first_response_time_sec) as avg_first_response_time_sec,
        avg(nullif(rating_value,0)) as avg_csat,
        sum(case when ai_agent_participated then 1 else 0 end)::float
            / count(*) as ai_participation_rate
    from {{ ref('stg_intercom__data') }}
    group by 1,2
    order by 1,2
)
select *
from  src_smry