
SELECT
    session_key,
    viewed,
    checked_out,
    purchased
FROM {{ ref('int_session_funnel') }}
WHERE
    checked_out > viewed
    OR purchased > checked_out
    OR viewed NOT IN (0, 1)
    OR checked_out NOT IN (0, 1)
    OR purchased NOT IN (0, 1)
    OR viewed IS NULL
    OR checked_out IS NULL
    OR purchased IS NULL
