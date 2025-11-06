with derived as (
  select
    *,
    -- Absolute close timestamps if available:
    case
      when time_to_first_close_sec is not null
        then created_at_utc + (time_to_first_close_sec || ' seconds')::interval
    end as first_close_at_utc,
    case
      when time_to_last_close_sec is not null
        then created_at_utc + (time_to_last_close_sec || ' seconds')::interval
    end as last_close_at_utc,
    -- Response metrics:
    extract(epoch from (stats_first_contact_reply_at_utc - created_at_utc)) as first_response_time_sec,
    extract(epoch from (stats_first_admin_reply_at_utc    - created_at_utc)) as first_admin_reply_time_sec,
    -- Date parts (assume business in Europe/London; convert if warranted):
    date_trunc('day', created_at_utc) as created_date,
    upper(strftime(created_at_utc, '%a')) as created_dow,   -- e.g. MON, TUE
    extract(hour from created_at_utc) as created_hour
  from {{ ref('stg_intercom__data') }}
)
select * 
from derived