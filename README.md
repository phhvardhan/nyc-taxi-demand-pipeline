# NYC Taxi Demand Intelligence Pipeline

**Built by:** Hema Harsha Vardhan Peela
**Stack:** Python · AWS S3 · PySpark · Delta Lake · Snowflake · dbt · Tableau Public
**Architecture:** Medallion (Bronze → Silver → Gold)
**Live dashboard:** [Tableau Public](https://public.tableau.com/app/profile/harsha.peela/viz/NYCTaxiDemandIntelliigencePipeline/Dashboard2)

---

## The business problem

NYC taxi operators and fleet dispatchers need real-time demand signals — which zones need drivers right now, what time-of-day patterns look like, and how revenue splits across vendors and payment types. This pipeline ingests TLC's public Yellow Taxi dataset and transforms it into actionable demand intelligence dashboards, with a parallel pipeline-health mart so data quality is monitored separately from the business numbers.

---

## Architecture

```
NYC TLC Public S3 (Jan–Mar 2024 Yellow Taxi, ~9.5M raw rows)
        │
        ▼
Python Ingestion (ingestion/ingest.py)
  · Retry with exponential backoff + full jitter
  · Row-level validation → quarantine bad rows to a separate S3 path
  · MD5 checksum per file
  · Control record written to S3 after every run (audit trail)
        │
        ▼
S3 Bronze (raw, immutable, partitioned by year/month)
  · Original parquet, never mutated — replay point if a transform bug is found
        │
        ▼
PySpark, run locally (transform/bronze_to_silver.py)
  · Schema standardization + explicit type casting
  · Derived fields: trip_duration_mins, pickup_hour, time_segment, is_weekend
  · Deduplication on a natural key (vendor + pickup time + location + fare)
  · Year-range filter to catch corrupt timestamps (see bugs below)
  · Writes Delta Lake → local silver, 9.4M clean rows
        │
        ▼
PySpark (transform/silver_to_gold.py)
  · 4 aggregated marts: hourly zone demand, daily vendor revenue,
    monthly payment mix, pipeline health
  · Uploaded to S3 Gold as Delta
        │
        ▼
Snowflake (load/snowflake_setup.sql)
  · Storage integration + external stage reading S3 Gold directly
  · Two warehouses: TAXI_LOAD_WH (ingestion) · TAXI_BI_WH (serving)
  · Auto-suspend 60s/120s → near-zero idle cost
  · COPY INTO with file-tracking metadata for incremental loads
        │
        ▼
dbt (dbt_project/)
  · Typed staging view with dbt tests (not_null, accepted_values)
  · Incremental mart model, MERGE strategy on (date, hour, location_id)
  · 14/14 tests passing
        │
        ▼
Tableau Public — 4-panel live dashboard
  · Demand by hour (colored by demand tier)
  · Daily revenue by vendor
  · Monthly payment mix
  · Pipeline health monitor (row counts, null rates, freshness)
```

---

## Real bugs found and fixed (these are the actual interview stories)

### 1. Corrupt timestamps from a 2024 dataset
After the first bronze→silver run, the silver output contained trips dated 2002, 2008, and 2009 — clearly corrupt source rows that passed individual null/negative checks but failed on the calendar. Fixed with a year-range filter (`pickup_year BETWEEN 2023 AND 2025`, not a hardcoded `= 2024`) so legitimate late-arriving December trips from the prior year still pass.

### 2. OutOfMemoryError on local Spark write
Writing 9.5M rows from a single local JVM hit Java's default heap limit. Diagnosed as `java.lang.OutOfMemoryError: Java heap space` in the Parquet writer, fixed by setting `SPARK_DRIVER_MEMORY=4g` before launching the session — a real example of right-sizing compute for the actual data volume rather than assuming defaults are enough.

### 3. Delta partition columns silently missing in Snowflake (the best one)
After loading the `monthly_payment_mix` gold mart into Snowflake, every row had `pickup_year` and `pickup_month` as **NULL**, even though the values looked correct in Spark. Root cause: when Delta Lake writes with `.partitionBy("pickup_year", "pickup_month")`, those columns are stripped from the actual Parquet file content and encoded *only* in the S3 folder path (`pickup_year=2024/pickup_month=1/...`). The original `COPY INTO` was extracting from the JSON file body (`$1:pickup_year`), where the column no longer existed. Fixed by extracting the values from `METADATA$FILENAME` with `REGEXP_SUBSTR` against the partition path instead. This is the kind of bug that only surfaces when you trace a dashboard anomaly three layers back to its source — Tableau → Snowflake → S3 file structure.

### 4. Databricks Community Edition platform restrictions
Originally planned to run the Spark transforms on Databricks Community Edition. Hit two hard platform walls: serverless compute blocks `spark.conf.set` for S3 credentials (`CONFIG_NOT_AVAILABLE` error, a deliberate multi-tenant security boundary), and the public DBFS root is disabled on free-tier accounts (`DBFS_DISABLED`). Pivoted to running the identical PySpark code locally with `delta-spark`, which is environment-agnostic — the same notebooks deploy unchanged to a real Databricks cluster.

---

## Key design decisions

**Why S3 bronze is immutable** — Raw files land once and are never mutated. If the silver transform has a bug, replay from bronze without re-hitting the rate-limited source. Idempotency key: `year=YYYY/month=MM/filename.parquet` — re-running the same month overwrites the same key rather than duplicating data.

**Why Spark instead of pure SQL/ELT** — The pre-load work is heavy: parsing raw parquet, dedup on a composite key, multi-field enrichment. Doing that in Spark on cheap compute before loading to Snowflake keeps warehouse credits focused on serving, not transformation.

**Why Delta Lake at silver** — ACID transactions (no partial writes), Time Travel (roll back to any version), and schema enforcement (new unexpected columns fail loudly via `mergeSchema=true` rather than silently coercing types).

**Why two Snowflake warehouses** — Load and serve are different workload profiles. One shared warehouse means COPY INTO jobs contend with Tableau queries for compute. Separate warehouses with aggressive auto-suspend keep both fast and keep idle cost near zero.

**How incremental loads work** — Snowflake's COPY INTO tracks loaded files in metadata for 64 days; re-running against the same stage skips already-loaded files automatically (verified directly — a re-run showed `0 files processed`). New monthly gold files are picked up on the next scheduled run without manual intervention. A `PATTERN` filter excludes Delta's internal `_delta_log` and `.crc` files from being attempted.

**Data quality, in layers** — Ingestion checks nulls, row-count floor, and business rules (non-negative fare/distance, pickup < dropoff), routing failures to a quarantine path rather than failing the whole batch. Silver adds a year-range filter for corrupt timestamps. dbt enforces `not_null` and `accepted_values` contracts on every dimension column before a mart is considered safe to serve. The pipeline_health mart tracks volume and null-rate trends separately, so a job can be "successful" while data quality is independently flagged — this caught a real, growing null-rate trend in `passenger_count` (4.7% in January → 11.7% in March) that traditional job monitoring would have missed entirely.

**Cost optimization** — Local Spark compute for the heaviest transform work (free); Snowflake auto-suspend at 60s/120s; Parquet + Snappy throughout; partition pruning on year/month so queries scan only relevant data; S3 lifecycle policy aging bronze to Standard-IA after 30 days and Glacier after 90.

---

## Project structure

```
nyc_taxi_pipeline/
├── config/
│   ├── settings.py              # Central config loaded from env vars
│   └── s3_lifecycle.json        # S3 cost optimization policy
├── ingestion/
│   └── ingest.py                # TLC → S3 bronze, retry + validation + quarantine
├── transform/
│   ├── bronze_to_silver.py      # Local PySpark: raw → clean Delta
│   └── silver_to_gold.py        # Local PySpark: clean → 4 aggregated marts
├── load/
│   └── snowflake_setup.sql      # Warehouses, storage integration, COPY INTO, freshness check
├── dbt_project/
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       │   ├── stg_hourly_zone_demand.sql
│       │   └── schema.yml       # Sources + dbt tests
│       └── marts/
│           └── mart_demand_intelligence.sql   # Incremental MERGE model
├── csv_files/samples/           # Small data samples (full data lives in S3/Snowflake)
├── scripts/
│   └── setup_day1.sh
└── README.md
```

---

## Build status

| Day | Goal | Status |
|-----|------|--------|
| 1 | AWS setup, S3 buckets, ingestion script — 3 months / 9.5M rows landed | ✅ |
| 2 | PySpark bronze→silver→gold, 2 real bugs found and fixed | ✅ |
| 3 | Snowflake load via storage integration, dbt 14/14 tests passing | ✅ |
| 4 | Tableau Public dashboard published, GitHub repo cleaned and pushed | ✅ |

---

## Running the pipeline

```bash
# 1. Setup
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt   # boto3, pandas, pyarrow, pyspark, delta-spark, dbt-snowflake

# 2. Configure credentials (never commit these — see .gitignore)
cp config/.env.example config/.env
# Fill in AWS keys + S3 bucket name

# 3. Ingest (downloads TLC parquet → S3 bronze)
python3 ingestion/ingest.py --year 2024 --month 1

# 4. Transform locally
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
export SPARK_DRIVER_MEMORY=4g
python3 transform/bronze_to_silver.py
python3 transform/silver_to_gold.py

# 5. Upload gold to S3, then load into Snowflake
aws s3 cp data/gold/ s3://your-bucket/gold/yellow_taxi/ --recursive
# Run load/snowflake_setup.sql in a Snowflake worksheet

# 6. Model and test with dbt
cd dbt_project && dbt run && dbt test

# 7. Tableau Public connects via CSV export (Tableau Public Desktop has no
#    live Snowflake connector — export marts as CSV, then build dashboards)
```

---

## Honest scope notes

This is a portfolio project built to demonstrate end-to-end pipeline ownership, not a production system. Three honest caveats: Tableau Public requires CSV exports rather than a live Snowflake connection (a Tableau Server/Desktop deployment would connect live); the orchestration is manual script execution rather than Airflow/a scheduler; and the dataset is a 3-month historical slice rather than a live daily feed. Every architectural decision above is written the way it would be defended for a production version of this system.