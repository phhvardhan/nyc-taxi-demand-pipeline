# Databricks Notebook: Silver → Gold Transform
# File: notebooks/02_silver_to_gold.py
#
# Gold layer = business-ready aggregations.
# These are the tables Snowflake loads and dbt models serve to Tableau.
# Every mart here answers a specific business question.
#
# Business frame: "NYC Taxi Demand Intelligence"
# Questions answered:
#   1. Hourly demand by zone — where do I need drivers right now?
#   2. Revenue by day/vendor — which vendors are performing?
#   3. Payment mix trends — cash vs card shift over time
#   4. Trip distance distribution — are trips getting shorter post-COVID?

# ─── CELL 1: Config ───────────────────────────────────────────────────────────

# Silence IDE linting errors for Databricks globals
from typing import Any
spark: Any = globals().get("spark")
dbutils: Any = globals().get("dbutils")

BUCKET      = "nyc-taxi-pipeline-yourname"   # <-- change this
SILVER_PATH = f"s3a://{BUCKET}/silver/yellow_taxi"
GOLD_PATH   = f"s3a://{BUCKET}/gold/yellow_taxi"

# Re-set S3 credentials if running in a fresh session
spark.conf.set("fs.s3a.access.key",  dbutils.secrets.get("aws", "access_key"))
spark.conf.set("fs.s3a.secret.key",  dbutils.secrets.get("aws", "secret_key"))
spark.conf.set("fs.s3a.endpoint",    "s3.amazonaws.com")
spark.conf.set("fs.s3a.impl",        "org.apache.hadoop.fs.s3a.S3AFileSystem")

from pyspark.sql import functions as F
from datetime import datetime

silver_df = spark.read.format("delta").load(SILVER_PATH)
print(f"Silver rows loaded: {silver_df.count():,}")
silver_df.createOrReplaceTempView("silver_trips")


# ─── CELL 2: Gold Mart 1 — Hourly Demand by Zone ─────────────────────────────
# This is the ops dashboard mart — shows pickup demand by hour and location.
# A logistics company would use this for driver allocation.

hourly_zone_demand = spark.sql("""
    SELECT
        pickup_date,
        pickup_hour,
        pickup_year,
        pickup_month,
        pickup_location_id,
        COUNT(*)                                    AS trip_count,
        ROUND(AVG(trip_distance), 2)                AS avg_trip_distance_miles,
        ROUND(AVG(trip_duration_mins), 2)           AS avg_trip_duration_mins,
        ROUND(AVG(fare_amount), 2)                  AS avg_fare_amount,
        ROUND(SUM(fare_amount), 2)                  AS total_fare_amount,
        ROUND(AVG(tip_amount), 2)                   AS avg_tip_amount,
        COUNT(DISTINCT vendor_id)                   AS vendor_count
    FROM silver_trips
    GROUP BY
        pickup_date, pickup_hour, pickup_year,
        pickup_month, pickup_location_id
    ORDER BY
        pickup_date, pickup_hour, pickup_location_id
""")

print(f"Hourly zone demand rows: {hourly_zone_demand.count():,}")
hourly_zone_demand.show(5)

(
    hourly_zone_demand
    .write.format("delta")
    .mode("overwrite")
    .partitionBy("pickup_year", "pickup_month")
    .save(f"{GOLD_PATH}/hourly_zone_demand")
)
print("Gold mart 1 written: hourly_zone_demand")


# ─── CELL 3: Gold Mart 2 — Daily Revenue by Vendor ───────────────────────────
# Revenue performance by vendor per day.
# Answers: "Which vendor generated more revenue this week? Is that shifting?"

daily_vendor_revenue = spark.sql("""
    SELECT
        pickup_date,
        pickup_year,
        pickup_month,
        vendor_id,
        vendor_name,
        COUNT(*)                                    AS total_trips,
        ROUND(SUM(fare_amount), 2)                  AS total_fare_revenue,
        ROUND(SUM(tip_amount), 2)                   AS total_tip_revenue,
        ROUND(SUM(total_amount), 2)                 AS gross_revenue,
        ROUND(AVG(fare_amount), 2)                  AS avg_fare_per_trip,
        ROUND(AVG(tip_amount / NULLIF(fare_amount, 0)) * 100, 2) AS tip_rate_pct,
        ROUND(AVG(trip_distance), 2)                AS avg_distance_miles,
        ROUND(AVG(passenger_count), 2)              AS avg_passengers
    FROM silver_trips
    WHERE vendor_id IS NOT NULL
    GROUP BY
        pickup_date, pickup_year, pickup_month,
        vendor_id, vendor_name
    ORDER BY pickup_date, vendor_id
""")

print(f"Daily vendor revenue rows: {daily_vendor_revenue.count():,}")
daily_vendor_revenue.show(5)

(
    daily_vendor_revenue
    .write.format("delta")
    .mode("overwrite")
    .partitionBy("pickup_year", "pickup_month")
    .save(f"{GOLD_PATH}/daily_vendor_revenue")
)
print("Gold mart 2 written: daily_vendor_revenue")


# ─── CELL 4: Gold Mart 3 — Payment Mix Trends ────────────────────────────────
# Monthly payment type breakdown.
# Answers: "Is cash usage declining? How fast is card adoption growing?"

monthly_payment_mix = spark.sql("""
    SELECT
        pickup_year,
        pickup_month,
        payment_type,
        payment_type_desc,
        COUNT(*)                                    AS trip_count,
        ROUND(SUM(fare_amount), 2)                  AS total_fare,
        ROUND(SUM(tip_amount), 2)                   AS total_tips,
        ROUND(AVG(tip_amount), 2)                   AS avg_tip,
        -- Window to get total trips per month for percentage calc
        COUNT(*) OVER (PARTITION BY pickup_year, pickup_month) AS monthly_total_trips
    FROM silver_trips
    WHERE payment_type IS NOT NULL
    GROUP BY
        pickup_year, pickup_month,
        payment_type, payment_type_desc
    ORDER BY pickup_year, pickup_month, payment_type
""")

# Add percentage share
monthly_payment_mix = monthly_payment_mix.withColumn(
    "market_share_pct",
    F.round(F.col("trip_count") / F.col("monthly_total_trips") * 100, 2)
)

print(f"Monthly payment mix rows: {monthly_payment_mix.count():,}")
monthly_payment_mix.show(10)

(
    monthly_payment_mix
    .write.format("delta")
    .mode("overwrite")
    .partitionBy("pickup_year", "pickup_month")
    .save(f"{GOLD_PATH}/monthly_payment_mix")
)
print("Gold mart 3 written: monthly_payment_mix")


# ─── CELL 5: Gold Mart 4 — Pipeline Health ───────────────────────────────────
# This is the meta-mart — pipeline monitoring data for the ops dashboard.
# Interview answer: "I monitored data health separately from job health.
# A job can succeed while delivering garbage — volume trend and freshness
# checks catch that."

pipeline_health = spark.sql("""
    SELECT
        pickup_year,
        pickup_month,
        COUNT(*)                                    AS total_rows_in_silver,
        ROUND(SUM(fare_amount), 2)                  AS total_fare_volume,
        ROUND(AVG(trip_distance), 2)                AS avg_trip_distance,
        ROUND(AVG(fare_amount), 2)                  AS avg_fare,
        MIN(pickup_datetime)                        AS earliest_pickup,
        MAX(pickup_datetime)                        AS latest_pickup,
        COUNT(DISTINCT pickup_location_id)          AS distinct_pickup_zones,
        COUNT(DISTINCT vendor_id)                   AS distinct_vendors,
        SUM(CASE WHEN fare_amount <= 0 THEN 1 ELSE 0 END) AS zero_fare_rows,
        SUM(CASE WHEN passenger_count IS NULL THEN 1 ELSE 0 END) AS null_passenger_rows
    FROM silver_trips
    GROUP BY pickup_year, pickup_month
    ORDER BY pickup_year, pickup_month
""")

pipeline_health = pipeline_health.withColumn("health_run_at", F.current_timestamp())

print("Pipeline health by month:")
pipeline_health.show(truncate=False)

(
    pipeline_health
    .write.format("delta")
    .mode("overwrite")
    .save(f"{GOLD_PATH}/pipeline_health")
)
print("Gold mart 4 written: pipeline_health")
print("\nAll gold marts written. Ready for Snowflake load.")
