"""
transform/bronze_to_silver.py

Reads raw parquet from local bronze, applies Spark transforms,
writes clean Delta table to local silver — partitioned by year/month.

Run: python3 transform/bronze_to_silver.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip
from datetime import datetime

# ── Paths ─────────────────────────────────────────────────────────────────────
BRONZE_PATH  = "data/bronze"
SILVER_PATH  = "data/silver"

# ── Spark Session with Delta support ──────────────────────────────────────────
builder = (
    SparkSession.builder
    .appName("nyc_taxi_bronze_to_silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "8")   # Low for local — 200 is default (too high)
    .master("local[*]")                             # Use all local CPU cores
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")   # Suppress INFO noise
print("Spark session started.")


# ── STEP 1: Read Bronze ───────────────────────────────────────────────────────
# Why mergeSchema=true: each monthly file may have slightly different columns
# as TLC updates their schema over time. mergeSchema unifies them safely.
print("\nReading bronze parquet...")
bronze_df = (
    spark.read
    .option("mergeSchema", "true")
    .parquet(BRONZE_PATH)
)

total_bronze = bronze_df.count()
print(f"Bronze rows loaded: {total_bronze:,}")
print(f"Columns: {len(bronze_df.columns)}")


# ── STEP 2: Transform ─────────────────────────────────────────────────────────
# Every decision here has a reason — understand each one:
#
#  Rename → standard snake_case so downstream SQL is consistent
#  Cast   → don't trust inferred types from parquet, be explicit
#  Derive → new columns that make analytics easier
#  Filter → belt-and-suspenders quality check (ingestion already quarantined bad rows,
#            but Spark transforms can surface edge cases ingestion missed)

print("\nApplying silver transforms...")

silver_df = (
    bronze_df

    # ── Rename to snake_case standard ──
    .withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    .withColumnRenamed("PULocationID",          "pickup_location_id")
    .withColumnRenamed("DOLocationID",          "dropoff_location_id")
    .withColumnRenamed("RatecodeID",            "rate_code_id")
    .withColumnRenamed("VendorID",              "vendor_id")

    # ── Explicit type casts ──
    .withColumn("pickup_datetime",     F.to_timestamp("pickup_datetime"))
    .withColumn("dropoff_datetime",    F.to_timestamp("dropoff_datetime"))
    .withColumn("passenger_count",     F.col("passenger_count").cast(IntegerType()))
    .withColumn("trip_distance",       F.col("trip_distance").cast(DoubleType()))
    .withColumn("fare_amount",         F.col("fare_amount").cast(DoubleType()))
    .withColumn("tip_amount",          F.col("tip_amount").cast(DoubleType()))
    .withColumn("total_amount",        F.col("total_amount").cast(DoubleType()))
    .withColumn("pickup_location_id",  F.col("pickup_location_id").cast(IntegerType()))
    .withColumn("dropoff_location_id", F.col("dropoff_location_id").cast(IntegerType()))
    .withColumn("payment_type",        F.col("payment_type").cast(IntegerType()))

    # ── Derived columns ──
    # trip_duration_mins: how long was the trip? Core analytics field.
    .withColumn("trip_duration_mins",
        F.round(
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60, 2
        )
    )
    # Time components: enables hour-of-day and day-of-week analysis
    .withColumn("pickup_hour",         F.hour("pickup_datetime"))
    .withColumn("pickup_day_of_week",  F.dayofweek("pickup_datetime"))  # 1=Sun, 7=Sat
    .withColumn("pickup_date",         F.to_date("pickup_datetime"))
    .withColumn("pickup_year",         F.year("pickup_datetime"))
    .withColumn("pickup_month",        F.month("pickup_datetime"))

    # Payment type decoded: makes downstream SQL readable without joining a lookup table
    .withColumn("payment_type_desc",
        F.when(F.col("payment_type") == 1, "Credit card")
        .when(F.col("payment_type") == 2, "Cash")
        .when(F.col("payment_type") == 3, "No charge")
        .when(F.col("payment_type") == 4, "Dispute")
        .otherwise("Unknown")
    )

    # Vendor decoded
    .withColumn("vendor_name",
        F.when(F.col("vendor_id") == 1, "Creative Mobile Technologies")
        .when(F.col("vendor_id") == 2, "VeriFone")
        .otherwise("Unknown")
    )

    # Time of day segment: operational scheduling insight
    .withColumn("time_segment",
        F.when(F.col("pickup_hour").between(6,  9),  "Morning Rush")
        .when(F.col("pickup_hour").between(10, 15),  "Midday")
        .when(F.col("pickup_hour").between(16, 19),  "Evening Rush")
        .when(F.col("pickup_hour").between(20, 23),  "Night")
        .otherwise("Late Night")
    )

    # Weekend flag
    .withColumn("is_weekend",
        F.when(F.dayofweek("pickup_datetime").isin(1, 7), True).otherwise(False)
    )

    # ── Quality filters ──
    # Why filter here AND at ingestion? Ingestion catches nulls and negatives.
    # Spark catches derived field issues like duration = 0 (same pickup/dropoff time)
    # and extreme outliers that pass individual column checks but fail together.
    .filter(F.col("pickup_datetime").isNotNull())
    .filter(F.col("dropoff_datetime").isNotNull())
    .filter(F.col("pickup_datetime") < F.col("dropoff_datetime"))
    .filter(F.col("trip_distance") >= 0)
    .filter(F.col("fare_amount") >= 0)
    .filter(F.col("trip_duration_mins") > 0)
    .filter(F.col("trip_duration_mins") < 1440)   # Under 24 hours
    .filter(F.col("trip_distance") < 500)          # Under 500 miles
    .filter(F.col("pickup_year").between(2023, 2025))

    # Pipeline metadata
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source",      F.lit("nyc_tlc_yellow_taxi"))
)


# ── STEP 3: Deduplicate ───────────────────────────────────────────────────────
# Why: TLC occasionally publishes corrected files that overlap with prior months.
# Natural key = vendor + pickup time + pickup location + fare.
# Same vendor, same pickup time and place, same fare = same trip.
# We keep the last-seen occurrence (most recent _ingested_at).

print("\nDeduplicating...")
dedup_key = ["vendor_id", "pickup_datetime", "pickup_location_id", "fare_amount"]
window_spec = Window.partitionBy(dedup_key).orderBy(F.col("_ingested_at").desc())

silver_deduped = (
    silver_df
    .withColumn("_rn", F.row_number().over(window_spec))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

total_silver = silver_deduped.count()
dupes_removed = total_bronze - total_silver
print(f"Rows after dedup: {total_silver:,}")
print(f"Duplicates removed: {dupes_removed:,}")


# ── STEP 4: Write Silver as Delta ─────────────────────────────────────────────
# Why Delta over plain Parquet?
#   - ACID: no partial writes — reader always sees a complete dataset
#   - Time travel: roll back to any previous version if a bad transform slips through
#   - Schema enforcement: new unexpected columns fail loudly
# Why partition by year/month?
#   - Partition pruning: queries for one month scan one folder, not everything
#   - Idempotency: re-running overwrites the same partition, not duplicating data

print(f"\nWriting silver Delta to {SILVER_PATH}...")
(
    silver_deduped
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("pickup_year", "pickup_month")
    .option("overwriteSchema", "true")
    .save(SILVER_PATH)
)
print("Silver Delta written.")


# ── STEP 5: Verify ────────────────────────────────────────────────────────────
print("\nVerifying silver...")
silver_verify = spark.read.format("delta").load(SILVER_PATH)
verify_count = silver_verify.count()
print(f"Silver verification count: {verify_count:,}")

# Show sample of key columns
silver_verify.select(
    "pickup_datetime", "dropoff_datetime", "trip_duration_mins",
    "trip_distance", "fare_amount", "payment_type_desc",
    "vendor_name", "time_segment", "is_weekend"
).show(5, truncate=False)

# Show partition breakdown
print("\nRows by month:")
(
    silver_verify
    .groupBy("pickup_year", "pickup_month")
    .count()
    .orderBy("pickup_year", "pickup_month")
    .show()
)

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("BRONZE → SILVER COMPLETE")
print(f"  Bronze rows in:    {total_bronze:,}")
print(f"  Duplicates removed:{dupes_removed:,}")
print(f"  Silver rows out:   {total_silver:,}")
print(f"  Reject rate:       {(total_bronze - total_silver) / total_bronze * 100:.2f}%")
print(f"  Written to:        {SILVER_PATH}")
print("="*60)

spark.stop()