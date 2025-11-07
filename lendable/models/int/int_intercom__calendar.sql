-- Continuous agg daily date
with calendar as (
    select
    generate_series(
        (select min(created_at_utc)::date from {{ ref('stg_intercom__data') }}),
        (select max(updated_at_utc)::date from {{ ref('stg_intercom__data') }}),
        interval '1 day'
    )::date as calendar_date
)
select *
from calendar
