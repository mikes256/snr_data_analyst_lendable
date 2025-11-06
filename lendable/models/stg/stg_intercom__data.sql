with casted as (
  select
    cast(id as varchar) as conversation_id,
    cast(created_at as timestamptz) as created_at_utc,
    source_delivered_as,
    cast(first_contact_reply_created_at as timestamptz) as first_contact_reply_created_at_utc,
    cast(conversation_rating_created_at as timestamptz) as rating_created_at_utc,
    cast(conversation_rating_value as int) as rating_value,
    cast(conversation_rating_teammate_id as varchar) as rating_teammate_id,
    cast(updated_at as timestamptz) as updated_at_utc,
    source_type,
    cast(assignee_id as varchar) as assignee_id,
    cast(waiting_since as timestamptz) as waiting_since_utc,
    state,
    cast(statistics_first_contact_reply_at as timestamptz) as stats_first_contact_reply_at_utc,
    cast(statistics_first_admin_reply_at as timestamptz) as stats_first_admin_reply_at_utc,
    cast(statistics_time_to_last_close as bigint) as time_to_last_close_sec,
    cast(statistics_time_to_first_close as bigint) as time_to_first_close_sec,
    cast(source_author_id as varchar) as source_author_id,
    cast(ai_agent_participated as boolean) as ai_agent_participated
  from {{ ref('MI_Manager_Test-Raw_Data_cleaned') }}
),
derived as (
  select
    *,
    extract(epoch from (stats_first_contact_reply_at_utc - created_at_utc)) as first_response_time_sec
  from casted
)
select
    conversation_id,
    created_at_utc,
    source_delivered_as,
    first_contact_reply_created_at_utc,
    rating_created_at_utc,
    rating_value,
    rating_teammate_id,
    updated_at_utc,
    source_type,
    assignee_id,
    waiting_since_utc,
    state,
    stats_first_contact_reply_at_utc,
    stats_first_admin_reply_at_utc,
    time_to_last_close_sec,
    time_to_first_close_sec,
    source_author_id,
    ai_agent_participated,
    first_response_time_sec
from derived