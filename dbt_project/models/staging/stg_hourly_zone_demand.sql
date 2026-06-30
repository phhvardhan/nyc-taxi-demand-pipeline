WITH source AS (
    SELECT * FROM {{ source('raw', 'hourly_zone_demand') }}
),

renamed AS (
    SELECT
        pickup_date,
        pickup_hour,
        pickup_year,
        pickup_month,
        pickup_location_id,
        time_segment,
        is_weekend,
        trip_count,
        avg_trip_distance_miles,
        avg_trip_duration_mins,
        avg_fare_amount,
        total_fare_amount,
        avg_tip_amount,
        demand_tier,
        _loaded_at AS loaded_at
    FROM source
    WHERE pickup_date IS NOT NULL
      AND trip_count > 0
)

SELECT * FROM renamed
