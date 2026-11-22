
{{
    config(
        materialized='view',
        database='workspace'
    )
}}

SELECT
    event_date,
    event_timestamp,
    event_name,
    user_pseudo_id,
    session_id,
    session_key,
    event_params,
    items
FROM {{ source('journeyiq', 'silver_ga4_events') }}