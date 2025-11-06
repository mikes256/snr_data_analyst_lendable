WITH all_records as (
    SELECT 
        count(*) as total_records
    FROM conversations
-- understand the size of the dataset
),
duplicates as (
    SELECT 
        id, 
        COUNT(*) as cnt
    FROM conversations
    GROUP BY id
    HAVING cnt > 1
-- identify duplicate records based on 'id' column, in the brief it mentioned 'unique' conversation id 
),
missing as (
    SELECT 
        COUNT(*) as missing_count
    FROM conversations
    WHERE id IS NULL
-- identify missing values in the 'id' column
),
date_range as (
    SELECT 
        MIN(created_at) as min_date,
        MAX(created_at) as max_date
    FROM conversations
-- find the date range of the conversations to spot any anomalies
)
SELECT *
FROM date_range;