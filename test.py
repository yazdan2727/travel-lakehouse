"""
Diagnostic Script - Check Bronze Table Columns
This will show us exactly what columns exist in your bronze tables
"""

import duckdb
import sys

try:
    conn = duckdb.connect('travel_lakehouse.db')
    
    tables = ['bronze_bookings', 'bronze_events', 'bronze_hotels']
    
    for table in tables:
        print(f"\n{'='*60}")
        print(f"TABLE: {table}")
        print('='*60)
        
        # Check if table exists
        result = conn.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = '{table}'
        """).fetchone()
        
        if result[0] == 0:
            print(f"❌ Table {table} does not exist!")
            continue
        
        # Get columns
        print("\n📋 COLUMNS:")
        columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
        for col in columns:
            print(f"   - {col[1]} ({col[2]})")
        
        # Get sample data
        print("\n📊 SAMPLE DATA (first 3 rows):")
        try:
            sample = conn.execute(f"SELECT * FROM {table} LIMIT 3").fetchall()
            for i, row in enumerate(sample, 1):
                print(f"   Row {i}: {row}")
        except Exception as e:
            print(f"   ❌ Error fetching data: {e}")
    
    conn.close()
    print(f"\n{'='*60}")
    
except FileNotFoundError:
    print("❌ Error: travel_lakehouse.db not found!")
    print("   Make sure you're running this from the project root directory")
    sys.exit(1)
except Exception as e:
    print(f"❌ Unexpected error: {e}")
    sys.exit(1)
