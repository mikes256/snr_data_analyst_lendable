-- models/marts/mart_intercom__operations_performance.sql
-- Purpose: Combine all key intermediate models into one unified reporting layer

with daily as (
  select
    calendar_date::date as calendar_date,
    conversations,
    avg_first_response_time_sec,
    avg_csat,
    cast(open_conversations as bigint) as open_conversations,
    ai_conversations
  from {{ ref('int_intercom__daily_base') }}
  -- Daily overall metrics
),
ai as (
  select
    calendar_date::date as calendar_date,
    ai_agent_participated,
    conversations as ai_human_conversations,
    avg_first_response_time_sec as ai_human_avg_frt_sec,
    avg_csat as ai_human_avg_csat
  from {{ ref('int_intercom__ai_v_human_metrics') }}
  -- AI vs Human performance metrics
),
source as (
  select
    calendar_date::date as calendar_date,
    source_type,
    conversations as source_conversations,
    avg_first_response_time_sec as source_avg_frt_sec,
    avg_csat as source_avg_csat,
    ai_participation_rate
  from {{ ref('int_intercom__source_summary') }}
  -- Source/channel metrics
),
calendar as (
  select 
    calendar_date::date as calendar_date
  from {{ ref('int_intercom__calendar') }}
  -- Calendar spine
)

select
  c.calendar_date,
  s.source_type,

  -- from source summary
  s.source_conversations,
  s.source_avg_frt_sec,
  s.source_avg_csat,
  s.ai_participation_rate,

  -- from daily summary
  d.conversations as total_conversations,
  coalesce(d.open_conversations, 0) as open_conversations,     -- ✅ ensures scalar bigint
  d.avg_first_response_time_sec,
  d.avg_csat,

  -- from AI vs Human metrics
  a.ai_human_conversations,
  a.ai_human_avg_frt_sec,
  a.ai_human_avg_csat

from calendar c
left join source s on c.calendar_date = s.calendar_date
left join daily d on c.calendar_date = d.calendar_date
left join ai a on c.calendar_date = a.calendar_date
order by c.calendar_date, s.source_type