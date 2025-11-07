WITH d_types as (
    SELECT 
        id TEXT,
        created_at TEXT,  -- my reminder sqlite does not have a dedicated date or timestamp type
        source_delivered_as TEXT,
        first_contact_reply_created_at TEXT,
        conversation_rating_created_at TEXT,
        conversation_rating_value INTEGER,
        conversation_rating_teammate_id TEXT,
        updated_at TEXT,
        source_type TEXT,
        assignee_id TEXT,
        waiting_since TEXT,
        state TEXT,
        statistics_first_contact_reply_at TEXT,
        statistics_first_admin_reply_at TEXT,
        statistics_time_to_last_close INTEGER,
        statistics_time_to_first_close INTEGER,
        source_author_id TEXT,
        ai_agent_participated TEXT
    FROM conversations
    -- to ensure the datatypes are the source of truth for all downstream models i am adding light casting at this stage, this will help prevent any datatype related issues downstream
    -- i am using sqllite so casting is limited to basic types only, i would normally be `cast()` or `::` into more specific types like `timestamp` or `boolean`
), 
cleaned as (
    SELECT 
        id,
        datetime(created_at),
        source_delivered_as,
        datetime(first_contact_reply_created_at) as first_contact_reply_created_at,
        datetime(conversation_rating_created_at) as conversation_rating_created_at,
        conversation_rating_value,
        conversation_rating_teammate_id,
        datetime(updated_at) as updated_at,
        source_type,
        assignee_id,
        datetime(waiting_since) as waiting_since,
        state,
        datetime(statistics_first_contact_reply_at) as statistics_first_contact_reply_at,
        datetime(statistics_first_admin_reply_at) as statistics_first_admin_reply_at,
        statistics_time_to_last_close,
        statistics_time_to_first_close,
        source_author_id,
        CASE 
            WHEN ai_agent_participated IN ('1', 'true', 'TRUE') THEN 1
            ELSE 0
        END as ai_agent_participated
    FROM d_types
    -- performing basic cleaning and transformations
    -- converting date strings to datetime format
    -- converting ai_agent_participated to boolean representation (1/0)
)
SELECT *
FROM cleaned;