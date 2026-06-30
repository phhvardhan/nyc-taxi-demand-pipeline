# Databricks Notebook: Bronze → Silver Transform
# Paste this cell by cell into Databricks Community Edition
# File: notebooks/01_bronze_to_silver.py
#
# What this does:
#   Reads raw parquet from S3 bronze, applies Spark transforms,
#   writes clean Delta table to S3 silver — partitioned by year/month.
#
# Interview story: "Bronze is immutable raw. Silver is cleaned, typed,
# deduplicated, and enriched — I can replay silver from bronze any time
# a bug is found in the transform logic without re-hitting the source."

# ─── CELL 1: Configuration ────────────────────────────────────────────────────
# Run this cell first. Replace values with your actual bucket name.

# Silence IDE linting errors for Databricks globals
from typing import Any
spark: Any = globals().get("spark")
dbutils: Any = globals().get("dbutils")

BUCKET       = "nyc-taxi-pipeline-yourname"   # <-- change this
BRONZE_PATH  = f"s3a://{BUCKET}/bronze/yellow_taxi"
SILVER_PATH  = f"s3a://{BUCKET}/silver/yellow_taxi"
CONTROL_PATH = f"s3a://{BUCKET}/control/yellow_taxi"

# AWS credentials — in Databricks Community, set in cluster env vars or here
# For production: use Databricks Secrets, never hardcode
spark.conf.set("fs.s3a.access.key",        dbutils.secrets.get("aws", "access_key"))
spark.conf.set("fs.s3a.secret.key",        dbutils.secrets.get("aws", "secret_key"))
spark.conf.set("fs.s3a.endpoint",          "s3.amazonaws.com")
spark.conf.set("fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")

# If not using secrets yet (local dev), hardcode temporarily and remove before commit:
# spark.conf.set("fs.s3a.access.key", "AKIAIOSFODNN7EXAMPLE")
# spark.conf.set("fs.s3a.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")

print("Config set.")


# ─── CELL 2: Read Bronze ──────────────────────────────────────────────────────
# Read the full bronze layer — Spark handles partition discovery automatically.
# In production you'd filter to a specific year/month partition (incremental).

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# Read all bronze parquet — Spark auto-discovers partitions
bronze_df = (
    spark.read
    .option("mergeSchema", "true")   # Handle schema evolution across months
    .parquet(BRONZE_PATH)
)

print(f"Bronze rows: {bronze_df.count():,}")
print(f"Columns: {bronze_df.columns}")
bronze_df.printSchema()


# ─── CELL 3: Inspect a sample ─────────────────────────────────────────────────
# Always look at the data before transforming it. This is where you find the
# quirks that become your interview stories (negative fares, future dates, etc.)

bronze_df.sample(0.001).show(20, truncate=False)

# Check null rates per column
from pyspark.sql.functions import col, count, when, isnan

total = bronze_df.count()
null_counts = bronze_df.select([
    (count(when(col(c).isNull(), c)) / total * 100).alias(c)
    for c in bronze_df.columns
]).collect()[0].asDict()

print("\nNull rates (%):")
for col_name, rate in sorted(null_counts.items(), key=lambda x: -x[1]):
    if rate > 0:
        print(f"  {col_name}: {rate:.2f}%")


# ─── CELL 4: Silver Transform ─────────────────────────────────────────────────
# Bronze → Silver rules:
#   1. Rename columns to snake_case standard
#   2. Cast types explicitly (don't trust inferred)
#   3. Derive useful columns (trip_duration_mins, pickup_hour, day_of_week)
#   4. Filter out invalid rows (keep quarantine logic from ingestion too)
#   5. Deduplicate on a surrogate key
#   6. Add pipeline metadata columns

silver_df = (
    bronze_df

    # ── Rename to standard snake_case ──
    .withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    .withColumnRenamed("PULocationID",          "pickup_location_id")
    .withColumnRenamed("DOLocationID",          "dropoff_location_id")
    .withColumnRenamed("RatecodeID",            "rate_code_id")
    .withColumnRenamed("VendorID",              "vendor_id")
    .withColumnRenamed("store_and_fwd_flag",    "store_and_fwd_flag")

    # ── Explicit casts ──
    .withColumn("pickup_datetime",    F.to_timestamp("pickup_datetime"))
    .withColumn("dropoff_datetime",   F.to_timestamp("dropoff_datetime"))
    .withColumn("passenger_count",    F.col("passenger_count").cast(IntegerType()))
    .withColumn("trip_distance",      F.col("trip_distance").cast(DoubleType()))
    .withColumn("fare_amount",        F.col("fare_amount").cast(DoubleType()))
    .withColumn("tip_amount",         F.col("tip_amount").cast(DoubleType()))
    .withColumn("total_amount",       F.col("total_amount").cast(DoubleType()))
    .withColumn("pickup_location_id", F.col("pickup_location_id").cast(IntegerType()))
    .withColumn("dropoff_location_id",F.col("dropoff_location_id").cast(IntegerType()))
    .withColumn("payment_type",       F.col("payment_type").cast(IntegerType()))

    # ── Derived columns (these become the interesting analytics fields) ──
    .withColumn("trip_duration_mins",
        F.round(
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60,
            2
        )
    )
    .withColumn("pickup_hour",        F.hour("pickup_datetime"))
    .withColumn("pickup_day_of_week", F.dayofweek("pickup_datetime"))  # 1=Sun, 7=Sat
    .withColumn("pickup_date",        F.to_date("pickup_datetime"))
    .withColumn("pickup_year",        F.year("pickup_datetime"))
    .withColumn("pickup_month",       F.month("pickup_datetime"))

    # ── Payment type decoded (makes gold layer easier) ──
    .withColumn("payment_type_desc",
        F.when(F.col("payment_type") == 1, "Credit card")
        .when(F.col("payment_type") == 2, "Cash")
        .when(F.col("payment_type") == 3, "No charge")
        .when(F.col("payment_type") == 4, "Dispute")
        .otherwise("Unknown")
    )

    # ── Vendor decoded ──
    .withColumn("vendor_name",
        F.when(F.col("vendor_id") == 1, "Creative Mobile Technologies")
        .when(F.col("vendor_id") == 2, "VeriFone")
        .otherwise("Unknown")
    )

    # ── Data quality filters (belt-and-suspenders, ingestion already quarantined bad rows) ──
    .filter(F.col("pickup_datetime").isNotNull())
    .filter(F.col("dropoff_datetime").isNotNull())
    .filter(F.col("pickup_datetime") < F.col("dropoff_datetime"))
    .filter(F.col("trip_distance") >= 0)
    .filter(F.col("fare_amount") >= 0)
    .filter(F.col("trip_duration_mins") > 0)
    .filter(F.col("trip_duration_mins") < 1440)   # Less than 24 hours
    .filter(F.col("trip_distance") < 500)          # Less than 500 miles

    # ── Pipeline metadata ──
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit("nyc_tlc_yellow_taxi"))
)

print(f"Silver rows after transform: {silver_df.count():,}")


# ─── CELL 5: Deduplication ────────────────────────────────────────────────────
# TLC files occasionally overlap on re-ingestion. Deduplicate on a
# surrogate key: vendor + pickup_datetime + pickup_location_id + fare_amount.
# Keep the last occurrence (in case a correction was published).
#
# Interview answer: "I deduped on a natural key rather than a generated ID
# because the source doesn't provide a stable row identifier. The surrogate
# key captures the business identity of a trip — same vendor, same pickup
# time and place, same fare — that's the same trip."

from pyspark.sql.window import Window

dedup_key = ["vendor_id", "pickup_datetime", "pickup_location_id", "fare_amount"]

window_spec = Window.partitionBy(dedup_key).orderBy(F.col("_ingested_at").desc())

silver_deduped = (
    silver_df
    .withColumn("_rn", F.row_number().over(window_spec))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

dupe_count = silver_df.count() - silver_deduped.count()
print(f"Removed {dupe_count:,} duplicate rows")
print(f"Final silver rows: {silver_deduped.count():,}")


# ─── CELL 6: Write to Silver as Delta ────────────────────────────────────────
# Partitioned by year and month for efficient downstream reads.
# Delta format gives us:
#   - ACID transactions (no partial writes)
#   - Time travel (roll back to any version)
#   - Schema enforcement (new columns fail loudly unless you evolve explicitly)
#   - Efficient MERGE for incremental runs

(
    silver_deduped
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("pickup_year", "pickup_month")
    .option("overwriteSchema", "true")   # Allow schema evolution on explicit runs
    .save(SILVER_PATH)
)

print(f"Written to silver Delta: {SILVER_PATH}")


# ─── CELL 7: Verify silver ────────────────────────────────────────────────────

silver_verify = spark.read.format("delta").load(SILVER_PATH)
print(f"Silver verification count: {silver_verify.count():,}")

# Show sample
silver_verify.select(
    "pickup_datetime", "dropoff_datetime", "trip_duration_mins",
    "trip_distance", "fare_amount", "payment_type_desc", "vendor_name",
    "pickup_location_id", "dropoff_location_id"
).show(5, truncate=False)

# Schema evolution check — show current Delta history
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, SILVER_PATH)
dt.history(5).select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)


# ─── CELL 8: Write control record ─────────────────────────────────────────────

control_record = {
    "pipeline": "nyc_yellow_taxi",
    "layer": "silver",
    "status": "success",
    "rows_written": silver_deduped.count(),
    "dupes_removed": dupe_count,
    "run_timestamp": datetime.utcnow().isoformat() + "Z",
}

control_df = spark.createDataFrame([control_record])
(
    control_df
    .write
    .mode("append")
    .json(f"{CONTROL_PATH}/silver_runs/")
)

print(f"Control record written. Silver transform complete.")
print(control_record)
