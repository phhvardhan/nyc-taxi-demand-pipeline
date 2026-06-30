"""
transform/silver_to_gold.py

Reads clean Delta from silver, writes 4 aggregated gold marts.
Each mart answers a specific business question.

Run: python3 transform/silver_to_gold.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from datetime import datetime

SILVER_PATH = "data/silver"
GOLD_PATH   = "data/gold"

builder = (
    SparkSession.builder
    .appName("nyc_taxi_silver_to_gold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "8")
    .master("local[*]")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("Spark session started.")

# ── Read Silver ───────────────────────────────────────────────────────────────
print("\nReading silver Delta...")
silver_df = spark.read.format("delta").load(SILVER_PATH)
silver_df.createOrReplaceTempView("silver_trips")
total_silver = silver_df.count()
print(f"Silver rows loaded: {total_silver:,}")


# ── MART 1: Hourly Zone Demand ────────────────────────────────────────────────
# Business question: Where do I need drivers, and when?
# Ops teams use this for real-time driver allocation decisions.
print("\nBuilding mart 1: hourly_zone_demand...")

hourly_zone_demand = spark.sql("""
    SELECT
        pickup_date,
        pickup_hour,
        pickup_year,
        pickup_month,
        pickup_location_id,
        time_segment,
        is_weekend,
        COUNT(*)                                        AS trip_count,
        ROUND(AVG(trip_distance), 2)                    AS avg_trip_distance_miles,
        ROUND(AVG(trip_duration_mins), 2)               AS avg_trip_duration_mins,
        ROUND(AVG(fare_amount), 2)                      AS avg_fare_amount,
        ROUND(SUM(fare_amount), 2)                      AS total_fare_amount,
        ROUND(AVG(tip_amount), 2)                       AS avg_tip_amount,
        CASE
            WHEN COUNT(*) > 500  THEN 'HIGH'
            WHEN COUNT(*) > 100  THEN 'MEDIUM'
            ELSE                      'LOW'
        END                                             AS demand_tier
    FROM silver_trips
    GROUP BY
        pickup_date, pickup_hour, pickup_year,
        pickup_month, pickup_location_id,
        time_segment, is_weekend
""")

(
    hourly_zone_demand.write.format("delta")
    .mode("overwrite")
    .partitionBy("pickup_year", "pickup_month")
    .save(f"{GOLD_PATH}/hourly_zone_demand")
)
print(f"  Rows written: {hourly_zone_demand.count():,}")


# ── MART 2: Daily Vendor Revenue ──────────────────────────────────────────────
# Business question: Which vendor generates more revenue? Is that shifting?
print("\nBuilding mart 2: daily_vendor_revenue...")

daily_vendor_revenue = spark.sql("""
    SELECT
        pickup_date,
        pickup_year,
        pickup_month,
        vendor_id,
        vendor_name,
        COUNT(*)                                        AS total_trips,
        ROUND(SUM(fare_amount), 2)                      AS total_fare_revenue,
        ROUND(SUM(tip_amount), 2)                       AS total_tip_revenue,
        ROUND(SUM(total_amount), 2)                     AS gross_revenue,
        ROUND(AVG(fare_amount), 2)                      AS avg_fare_per_trip,
        ROUND(AVG(tip_amount / NULLIF(fare_amount,0)) * 100, 2) AS tip_rate_pct,
        ROUND(AVG(trip_distance), 2)                    AS avg_distance_miles,
        ROUND(AVG(passenger_count), 2)                  AS avg_passengers
    FROM silver_trips
    WHERE vendor_id IS NOT NULL
    GROUP BY
        pickup_date, pickup_year, pickup_month,
        vendor_id, vendor_name
""")

(
    daily_vendor_revenue.write.format("delta")
    .mode("overwrite")
    .partitionBy("pickup_year", "pickup_month")
    .save(f"{GOLD_PATH}/daily_vendor_revenue")
)
print(f"  Rows written: {daily_vendor_revenue.count():,}")


# ── MART 3: Monthly Payment Mix ───────────────────────────────────────────────
# Business question: Is cash declining? How fast is card adoption growing?
print("\nBuilding mart 3: monthly_payment_mix...")

monthly_payment_mix = spark.sql("""
    SELECT
        pickup_year,
        pickup_month,
        payment_type,
        payment_type_desc,
        COUNT(*)                                        AS trip_count,
        ROUND(SUM(fare_amount), 2)                      AS total_fare,
        ROUND(SUM(tip_amount), 2)                       AS total_tips,
        ROUND(AVG(tip_amount), 2)                       AS avg_tip
    FROM silver_trips
    WHERE payment_type IS NOT NULL
    GROUP BY
        pickup_year, pickup_month,
        payment_type, payment_type_desc
""")

# Add market share % — what fraction of trips each payment type represents
from pyspark.sql.window import Window
window = Window.partitionBy("pickup_year", "pickup_month")
monthly_payment_mix = monthly_payment_mix.withColumn(
    "monthly_total_trips", F.sum("trip_count").over(window)
).withColumn(
    "market_share_pct",
    F.round(F.col("trip_count") / F.col("monthly_total_trips") * 100, 2)
)

(
    monthly_payment_mix.write.format("delta")
    .mode("overwrite")
    .partitionBy("pickup_year", "pickup_month")
    .save(f"{GOLD_PATH}/monthly_payment_mix")
)
print(f"  Rows written: {monthly_payment_mix.count():,}")


# ── MART 4: Pipeline Health ───────────────────────────────────────────────────
# This is the monitoring mart — NOT a business mart.
# Tracks data volume, quality metrics, and freshness per month.
# Interview answer: "I monitored data health separately from job health.
# A job can succeed while delivering garbage. Volume trend and freshness
# checks catch what job-level monitoring misses."
print("\nBuilding mart 4: pipeline_health...")

pipeline_health = spark.sql("""
    SELECT
        pickup_year,
        pickup_month,
        COUNT(*)                                        AS total_rows,
        ROUND(SUM(fare_amount), 2)                      AS total_fare_volume,
        ROUND(AVG(trip_distance), 2)                    AS avg_trip_distance,
        ROUND(AVG(fare_amount), 2)                      AS avg_fare,
        ROUND(AVG(trip_duration_mins), 2)               AS avg_duration_mins,
        MIN(pickup_datetime)                            AS earliest_pickup,
        MAX(pickup_datetime)                            AS latest_pickup,
        COUNT(DISTINCT pickup_location_id)              AS distinct_pickup_zones,
        COUNT(DISTINCT vendor_id)                       AS distinct_vendors,
        SUM(CASE WHEN fare_amount = 0 THEN 1 ELSE 0 END)   AS zero_fare_rows,
        SUM(CASE WHEN passenger_count IS NULL THEN 1 ELSE 0 END) AS null_passenger_rows
    FROM silver_trips
    GROUP BY pickup_year, pickup_month
    ORDER BY pickup_year, pickup_month
""")

pipeline_health = pipeline_health.withColumn("health_run_at", F.current_timestamp())

(
    pipeline_health.write.format("delta")
    .mode("overwrite")
    .save(f"{GOLD_PATH}/pipeline_health")
)

print("\nPipeline health by month:")
pipeline_health.show(truncate=False)


# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("SILVER → GOLD COMPLETE")
print(f"  Silver rows processed: {total_silver:,}")
print(f"  Gold marts written:    4")
print(f"  Written to:            {GOLD_PATH}/")
print("  Marts:")
print("    hourly_zone_demand    → driver allocation")
print("    daily_vendor_revenue  → vendor performance")
print("    monthly_payment_mix   → payment trends")
print("    pipeline_health       → monitoring")
print("="*60)

spark.stop()