with ai_v_hmn as (
    select
        date_trunc('day', created_at_utc) as calendar_date,
        ai_agent_participated,
        count(*) as conversations,
        avg(first_response_time_sec) as avg_first_response_time_sec,
        avg(nullif(rating_value,0)) as avg_csat
    from {{ ref('stg_intercom__data') }}
    group by 1,2
    order by 1,2
)
select *
from ai_v_hmn
