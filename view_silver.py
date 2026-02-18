import duckdb

conn = duckdb.connect("travel_lakehouse.db")

# Check deduplication
print("=" * 70)
print("DEDUPLICATION CHECK")
print("=" * 70)
result = conn.execute("""
    SELECT 
        COUNT(*) AS total_bookings,
        COUNT(DISTINCT booking_id) AS unique_bookings
    FROM silver_bookings
""").fetchdf()
print(result)

# Check source of truth distribution
print("\n" + "=" * 70)
print("SOURCE OF TRUTH DISTRIBUTION")
print("=" * 70)
result = conn.execute("""
    SELECT 
        source_of_truth,
        COUNT(*) AS count
    FROM silver_bookings
    GROUP BY source_of_truth
""").fetchdf()
print(result)

# View sample data
print("\n" + "=" * 70)
print("SAMPLE SILVER BOOKINGS (First 10)")
print("=" * 70)
result = conn.execute("SELECT * FROM silver_bookings LIMIT 10").fetchdf()
print(result)

# Check status distribution
print("\n" + "=" * 70)
print("STATUS DISTRIBUTION")
print("=" * 70)
result = conn.execute("""
    SELECT 
        status,
        COUNT(*) AS count
    FROM silver_bookings
    GROUP BY status
    ORDER BY count DESC
""").fetchdf()
print(result)

conn.close()
