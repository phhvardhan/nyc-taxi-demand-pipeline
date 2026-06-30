{{
    config(
        materialized='incremental',
        unique_key=['pickup_date', 'pickup_hour', 'pickup_location_id'],
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

WITH demand AS (
    SELECT * FROM {{ ref('stg_hourly_zone_demand') }}

    {% if is_incremental() %}
    WHERE (pickup_year, pickup_month) NOT IN (
        SELECT DISTINCT pickup_year, pickup_month
        FROM {{ this }}
        WHERE loaded_at >= DATEADD('day', -2, CURRENT_TIMESTAMP())
    )
    {% endif %}
),

enriched AS (
    SELECT
        pickup_date,
        pickup_hour,
        pickup_year,
        pickup_month,
        pickup_location_id,
        trip_count,
        avg_trip_distance_miles,
        avg_trip_duration_mins,
        avg_fare_amount,
        total_fare_amount,
        avg_tip_amount,
        demand_tier,
        time_segment,
        is_weekend,
        loaded_at,
        ROUND(total_fare_amount / NULLIF(trip_count, 0), 2)     AS revenue_per_trip,
        ROUND(avg_tip_amount / NULLIF(avg_fare_amount, 0), 3)   AS tip_rate,
        CASE WHEN is_weekend THEN TRUE ELSE FALSE END            AS is_weekend_flag,
        CURRENT_TIMESTAMP()                                      AS dbt_updated_at
    FROM demand
)

SELECT * FROM enriched
