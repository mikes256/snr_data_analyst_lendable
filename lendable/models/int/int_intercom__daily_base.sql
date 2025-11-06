with base as (
    select
        date_trunc('day', created_at_utc) as calendar_date,
        count(*) as conversations,
        avg(extract(epoch from (stats_first_contact_reply_at_utc - created_at_utc))) as avg_first_response_time_sec,
        avg(nullif(rating_value,0)) as avg_csat,
        sum(case when state = 'open' then 1 else 0 end) as open_conversations,
        sum(case when ai_agent_participated then 1 else 0 end) as ai_conversations
    from {{ ref('stg_intercom__data') }}
    group by 1
)
select
    c.calendar_date,
    coalesce(b.conversations,0) as conversations,
    b.avg_first_response_time_sec,
    b.avg_csat,
    b.open_conversations,
    b.ai_conversations
from {{ ref('int_intercom__calendar') }} c
left join base b on c.calendar_date = b.calendar_date
order by 1
