
{{
    config(
        materialized='view',
        database='workspace'
    )
}}

WITH counts AS (
    SELECT
        SUM(viewed) AS views,
        SUM(checked_out) AS checkouts,
        SUM(purchased) AS purchases
    FROM {{ ref('int_session_funnel') }}
),

funnel AS (
    SELECT
        1 AS stage_order,
        'Product View' AS stage,
        views AS sessions,
        views AS previous_sessions,
        views AS initial_sessions
    FROM counts

    UNION ALL

    SELECT
        2,
        'Checkout',
        checkouts,
        views,
        views
    FROM counts

    UNION ALL

    SELECT
        3,
        'Purchase',
        purchases,
        checkouts,
        views
    FROM counts
)

SELECT
    stage_order,
    stage,
    sessions,

    previous_sessions - sessions AS drop_off_sessions,

    ROUND(
        100.0 * try_divide(sessions, previous_sessions),
        2
    ) AS step_conversion_pct,

    ROUND(
        100.0 * try_divide(sessions, initial_sessions),
        2
    ) AS overall_conversion_pct,

    ROUND(
        100.0 * try_divide(previous_sessions - sessions, previous_sessions),
        2
    ) AS drop_off_pct

FROM funnel
ORDER BY stage_order
