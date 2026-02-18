{{ config(materialized='table') }}

WITH silver AS (
    
    SELECT
        b.booking_id,
        b.hotel_id,
        b.status,
        b.price,
        b.created_at,
        
        CAST(b.created_at AS DATE) AS booking_date
    FROM {{ ref('silver_bookings') }} b
),

hotels AS (
    SELECT
        hotel_id,
        city
    FROM {{ ref('bronze_hotels') }}
),

joined AS (
    SELECT
        s.booking_id,
        s.booking_date,
        h.city,
        s.status,
        s.price
    FROM silver s
    LEFT JOIN hotels h ON s.hotel_id = h.hotel_id
),

aggregated AS (
    SELECT
        booking_date,
        city,

        
        COUNT(DISTINCT booking_id)                                          AS total_bookings,
        COUNT(DISTINCT CASE WHEN status = 'confirmed' THEN booking_id END) AS confirmed_bookings,
        COUNT(DISTINCT CASE WHEN status = 'cancelled' THEN booking_id END) AS cancelled_bookings,

        
        ROUND(
            COUNT(DISTINCT CASE WHEN status = 'cancelled' THEN booking_id END)
            / NULLIF(COUNT(DISTINCT booking_id), 0),
        4)                                                                  AS cancellation_rate,

        
        COALESCE(
            SUM(CASE WHEN status = 'confirmed' THEN price END), 0
        )                                                                   AS total_revenue,

        
        ROUND(AVG(price), 2)                                                AS avg_booking_price

    FROM joined
    GROUP BY booking_date, city
)

SELECT
    booking_date,
    city,
    total_bookings,
    confirmed_bookings,
    cancelled_bookings,
    cancellation_rate,
    total_revenue,
    avg_booking_price
FROM aggregated
ORDER BY booking_date, city
