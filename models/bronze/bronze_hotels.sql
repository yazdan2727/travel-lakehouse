{{ config(materialized='table') }}

SELECT *
FROM read_csv_auto('C:/Users/LENOVO/Desktop/travel_lakehouse/data/hotels_raw.csv')