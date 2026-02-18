# Travel Lakehouse – dbt Data Pipeline

A modern **Data Lakehouse** implementation using **dbt** and **DuckDB** to build a three-layer analytics pipeline:

> Bronze → Silver → Gold

Designed for travel booking data with advanced state modeling, conflict resolution, and analytics-ready outputs.

---

# 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Architecture](#-architecture)
- [Data Model](#-data-model)
- [Setup Instructions](#-setup-instructions)
- [Usage](#-usage)
- [Layer Specifications](#-layer-specifications)
- [Data Quality & Testing](#-data-quality--testing)
- [Troubleshooting](#-troubleshooting)
- [Project Structure](#-project-structure)
- [Design Decisions](#-design-decisions)
- [Learning Outcomes](#-learning-outcomes)

---

# 🎯 Project Overview

This project implements a **Medallion Architecture** (Bronze / Silver / Gold) using dbt and DuckDB to process travel booking data.

## Key Features

- State Management – Resolves booking state from multiple data sources  
- Late-Arriving Events Handling – Supports out-of-order event ingestion  
- Conflict Resolution – Timestamp-based source-of-truth logic  
- Deduplication – Guarantees one row per `booking_id` in Silver  
- Analytics-Ready KPIs – Daily city-level metrics  

---

## Technologies

- dbt – SQL transformation framework  
- DuckDB – Embedded analytical database  
- Python – Validation and testing  

---

# 🏗 Architecture

```
┌─────────────────────────────────────────────┐
│ BRONZE LAYER (Raw)                         │
│ - bronze_bookings                          │
│ - bronze_events                            │
│ - bronze_hotels                            │
└──────────────────────────┬──────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────┐
│ SILVER LAYER (Curated)                     │
│ - silver_bookings (state table)             │
│ - silver_events (immutable event log)       │
└──────────────────────────┬──────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────┐
│ GOLD LAYER (Analytics)                     │
│ - gold_daily_bookings_kpi                   │
└─────────────────────────────────────────────┘
```

---

# 📊 Data Model

## Source Data

### data/bookings.csv

```
booking_id, user_id, hotel_id, status, price, created_at, updated_at
```

### data/booking_events.csv

```
booking_id, event_type, event_ts
```

### data/hotels.csv

```
hotel_id, name, city, country, rating
```

---

## Silver Layer Schema

### silver_bookings (State Table)

**Grain:** One row per `booking_id`

Columns:

- booking_id  
- user_id  
- hotel_id  
- status  
- price  
- created_at  
- last_updated_at  
- source_of_truth  
- processing_days  

Logic:

- Deduplication (latest per booking)
- Timestamp-based conflict resolution
- Tracks final authoritative source

---

### silver_events (Event Log)

**Grain:** One row per event occurrence

- event_id  
- booking_id  
- event_type  
- event_ts  

Immutable audit log.

---

## Gold Layer Schema

### gold_daily_bookings_kpi

**Grain:** One row per `day × city`

Metrics:

- total_bookings  
- confirmed_bookings  
- cancelled_bookings  
- cancellation_rate  
- total_revenue  
- avg_booking_price  

---

# 🚀 Setup Instructions

## Prerequisites

- Python 3.8+
- pip

---

## Installation

### 1️⃣ Navigate to Project

```bash
cd travel_lakehouse
```

### 2️⃣ Create Virtual Environment

```bash
python -m venv venv
```

Activate:

Windows:
```bash
.\venv\Scripts\activate
```

Mac/Linux:
```bash
source venv/bin/activate
```

---

### 3️⃣ Install Dependencies

```bash
pip install dbt-duckdb duckdb
```

Verify installation:

```bash
dbt --version
```

---

## dbt Configuration

### dbt_project.yml

```yaml
name: "travel_lakehouse"
version: "1.0.0"
profile: "travel_lakehouse"

models:
  travel_lakehouse:
    bronze:
      +materialized: table
    silver:
      +materialized: table
    gold:
      +materialized: table
```

---

### profiles.yml

```yaml
travel_lakehouse:
  outputs:
    dev:
      type: duckdb
      path: travel_lakehouse.db
  target: dev
```

---

# 💻 Usage

## Build Entire Pipeline

```bash
dbt clean
dbt run
```

Or build by layer:

```bash
dbt run --select bronze
dbt run --select silver
dbt run --select gold
```

---

## View Results (Python)

Create `view_results.py`:

```python
import duckdb

conn = duckdb.connect("travel_lakehouse.db")

print("=" * 70)
print("SILVER BOOKINGS (Sample)")
print("=" * 70)
print(conn.execute("SELECT * FROM silver_bookings LIMIT 5").fetchdf())

print("\n" + "=" * 70)
print("GOLD DAILY KPIs")
print("=" * 70)
print(conn.execute("""
SELECT
    booking_date,
    city,
    total_bookings,
    confirmed_bookings,
    cancelled_bookings,
    ROUND(cancellation_rate * 100, 1) AS cancel_rate_pct,
    total_revenue
FROM gold_daily_bookings_kpi
ORDER BY booking_date, city
""").fetchdf())

conn.close()
```

Run:

```bash
python view_results.py
```

---

## Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

Open in browser:

```
http://localhost:8080
```

---

# 📦 Layer Specifications

## Bronze Layer – Raw Ingestion

```sql
{{ config(materialized='table') }}

SELECT *
FROM read_csv_auto('data/bookings.csv')
```

Important: `{{ config(...) }}` must be the first line in the file.

---

## Silver Layer – Conflict Resolution

```sql
CASE
  WHEN events.event_ts >= bookings.updated_at
    THEN events.status
  ELSE bookings.status
END AS status
```

Guarantees:

- One row per booking_id  
- Latest state wins  
- No double counting  
- Full auditability  

---

## Gold Layer – Metric Logic

| Metric | Formula |
|--------|----------|
| Revenue | SUM(price WHERE status='confirmed') |
| Avg Price | AVG(price) |
| Cancellation Rate | cancelled / total |

---

# 🧪 Data Quality & Testing

Example validation script:

```python
import duckdb

conn = duckdb.connect("travel_lakehouse.db")

result = conn.execute("""
SELECT COUNT(*), COUNT(DISTINCT booking_id)
FROM silver_bookings
""").fetchone()

assert result[0] == result[1], "Duplicates found"

print("Deduplication test passed")

conn.close()
```

---

# 🐛 Troubleshooting

## Referenced column not found

Wrong:
```sql
SELECT * FROM read_csv_auto('bronze_bookings.sql')
```

Correct:
```sql
SELECT * FROM read_csv_auto('data/bookings.csv')
```

---

## dbt command not found

Activate virtual environment:

Windows:
```bash
.\venv\Scripts\activate
```

Mac/Linux:
```bash
source venv/bin/activate
```

---

# 📁 Project Structure

```
travel_lakehouse/
│
├── data/
├── models/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── dbt_project.yml
├── profiles.yml
├── view_results.py
├── validate_pipeline.py
└── README.md
```

---

# 📝 Design Decisions

Why DuckDB?

- Embedded database (no server)
- Fast analytics
- Full SQL support
- Ideal for local lakehouse development

Why Timestamp Conflict Resolution?

- Deterministic logic
- Handles late events
- Auditable
- Business-aligned

---

# 🎓 Learning Outcomes

This project demonstrates:

- Medallion Architecture with dbt  
- State modeling & deduplication  
- Idempotent pipelines  
- Data validation patterns  
- Production-grade SQL design  

---

# 📄 License

Educational purposes.

---

Last Updated: February 2026  
dbt Version: 1.x  
DuckDB Version: Latest
