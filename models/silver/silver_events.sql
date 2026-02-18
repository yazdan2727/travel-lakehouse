{{ config(materialized='table') }}

SELECT
    row_number() OVER (ORDER BY event_ts) AS event_id,
    booking_id,
    event_type,
    event_ts
FROM {{ ref('bronze_events') }}
