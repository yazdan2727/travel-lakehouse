{{ config(materialized='table') }}

WITH booking_base AS (
    -- Get latest booking record per booking_id from bookings table
    SELECT
        booking_id,
        user_id,
        hotel_id,
        status,
        price,
        created_at,
        updated_at,
        updated_at AS source_timestamp
    FROM {{ ref('bronze_bookings') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY updated_at DESC) = 1
),

booking_events AS (
    -- Get latest event per booking_id from events table
    SELECT
        booking_id,
        event_type AS status,
        event_ts AS source_timestamp
    FROM {{ ref('bronze_events') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY event_ts DESC) = 1
),

merged AS (
    -- Merge both sources and pick the latest state
    SELECT
        COALESCE(b.booking_id, e.booking_id) AS booking_id,
        b.user_id,
        b.hotel_id,
        b.price,
        b.created_at,
        
        -- Conflict resolution: use timestamp to determine latest status
        CASE 
            WHEN e.source_timestamp >= b.source_timestamp THEN e.status
            ELSE b.status
        END AS status,
        
        -- Track which source gave us the latest state
        CASE 
            WHEN e.source_timestamp >= b.source_timestamp THEN e.source_timestamp
            ELSE b.source_timestamp
        END AS last_updated_at,
        
        CASE 
            WHEN e.source_timestamp >= b.source_timestamp THEN 'events'
            ELSE 'bookings'
        END AS source_of_truth
        
    FROM booking_base b
    FULL OUTER JOIN booking_events e
        ON b.booking_id = e.booking_id
)

SELECT
    booking_id,
    user_id,
    hotel_id,
    status,
    price,
    created_at,
    last_updated_at,
    source_of_truth,
    datediff('day', created_at, last_updated_at) AS processing_days
FROM merged
