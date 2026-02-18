import duckdb
conn = duckdb.connect("travel_lakehouse.db")

print(conn.execute("""
    SELECT 
        booking_date, city,
        total_bookings, confirmed_bookings, cancelled_bookings,
        ROUND(cancellation_rate * 100, 1) AS cancel_rate_pct,
        total_revenue, avg_booking_price
    FROM gold_daily_bookings_kpi
    ORDER BY booking_date, city
""").fetchdf().to_string())

conn.close()
