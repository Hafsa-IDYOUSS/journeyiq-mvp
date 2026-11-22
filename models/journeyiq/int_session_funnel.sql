{{
    config(
        materialized='view',
        database='workspace'
    )
}}

WITH first_views AS (
    SELECT
        session_key,
        MIN(CASE
            WHEN event_name = 'view_item'
            THEN event_timestamp
        END) AS first_view_at
    FROM {{ ref('stg_ga4_events') }}
    WHERE session_key IS NOT NULL
    GROUP BY session_key
),

checkouts AS (
    SELECT
        v.session_key,
        v.first_view_at,
        MIN(CASE
            WHEN e.event_name = 'begin_checkout'
             AND e.event_timestamp >= v.first_view_at
            THEN e.event_timestamp
        END) AS first_checkout_at
    FROM first_views v
    LEFT JOIN {{ ref('stg_ga4_events') }} e
        ON v.session_key = e.session_key
    GROUP BY v.session_key, v.first_view_at
),

purchases AS (
    SELECT
        c.session_key,
        c.first_view_at,
        c.first_checkout_at,
        MIN(CASE
            WHEN e.event_name = 'purchase'
             AND e.event_timestamp >= c.first_checkout_at
            THEN e.event_timestamp
        END) AS first_purchase_at
    FROM checkouts c
    LEFT JOIN {{ ref('stg_ga4_events') }} e
        ON c.session_key = e.session_key
    GROUP BY
        c.session_key,
        c.first_view_at,
        c.first_checkout_at
)

SELECT
    session_key,
    first_view_at,
    first_checkout_at,
    first_purchase_at,
    CASE WHEN first_view_at IS NOT NULL THEN 1 ELSE 0 END AS viewed,
    CASE WHEN first_checkout_at IS NOT NULL THEN 1 ELSE 0 END AS checked_out,
    CASE WHEN first_purchase_at IS NOT NULL THEN 1 ELSE 0 END AS purchased
FROM purchases
