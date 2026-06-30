-- ============================================================
-- Snowflake Setup: NYC Taxi Pipeline
-- File: load/snowflake_setup.sql
--
-- Run these in order after creating your Snowflake trial account.
-- Each section is a logical step — read the comment before running.
--
-- Interview answer: "I separated compute by workload — a dedicated
-- warehouse for loading, a separate one for BI queries, so dashboard
-- performance never competed with ingestion jobs."
-- ============================================================


-- ── STEP 1: Warehouses ────────────────────────────────────────────────────────
-- Two warehouses: one for pipeline loads, one for BI/Tableau queries.
-- Auto-suspend keeps costs near zero between runs.

USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS TAXI_LOAD_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60          -- Suspend after 60s of inactivity
    AUTO_RESUME    = TRUE
    COMMENT        = 'Used exclusively for pipeline COPY INTO operations';

CREATE WAREHOUSE IF NOT EXISTS TAXI_BI_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 120
    AUTO_RESUME    = TRUE
    COMMENT        = 'Used for Tableau / dbt query workloads';


-- ── STEP 2: Database and Schemas ──────────────────────────────────────────────
-- Three schemas mirror the medallion architecture:
--   RAW      → loaded directly from S3 gold parquet (COPY INTO)
--   STAGING  → dbt staging models (typed, renamed, tested)
--   MARTS    → dbt mart models (aggregated, business-ready, Tableau-facing)

CREATE DATABASE IF NOT EXISTS NYC_TAXI;

USE DATABASE NYC_TAXI;

CREATE SCHEMA IF NOT EXISTS RAW     COMMENT = 'Direct COPY INTO from S3 gold layer';
CREATE SCHEMA IF NOT EXISTS STAGING COMMENT = 'dbt staging models';
CREATE SCHEMA IF NOT EXISTS MARTS   COMMENT = 'dbt mart models served to Tableau';
CREATE SCHEMA IF NOT EXISTS MONITORING COMMENT = 'Pipeline health and run logs';


-- ── STEP 3: Storage Integration (one-time setup) ──────────────────────────────
-- Allows Snowflake to read from your S3 bucket without long-term credentials.
-- After running this, copy the IAM ARN from DESCRIBE INTEGRATION output
-- and add it to your S3 bucket policy (see README for exact steps).

USE ROLE ACCOUNTADMIN;   -- Storage integration requires ACCOUNTADMIN

CREATE STORAGE INTEGRATION IF NOT EXISTS s3_nyc_taxi
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::YOUR_ACCOUNT_ID:role/snowflake-s3-role'  -- replace
    STORAGE_ALLOWED_LOCATIONS = ('s3://nyc-taxi-pipeline-yourname/gold/');              -- replace bucket

-- After running, get the Snowflake IAM user to add to your S3 bucket policy:
DESCRIBE INTEGRATION s3_nyc_taxi;
-- Copy: STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- Then update your IAM role trust policy with these values.

USE ROLE SYSADMIN;


-- ── STEP 4: External Stage ────────────────────────────────────────────────────
-- Stage points Snowflake at your S3 gold prefix.

USE SCHEMA NYC_TAXI.RAW;

CREATE STAGE IF NOT EXISTS gold_stage
    STORAGE_INTEGRATION = s3_nyc_taxi
    URL                 = 's3://nyc-taxi-pipeline-yourname/gold/yellow_taxi/'
    FILE_FORMAT         = (TYPE = PARQUET)
    COMMENT             = 'Points to S3 gold Delta/Parquet output from Databricks';

-- Verify Snowflake can see the files:
LIST @gold_stage;


-- ── STEP 5: Raw Tables (COPY INTO targets) ────────────────────────────────────

USE WAREHOUSE TAXI_LOAD_WH;
USE SCHEMA NYC_TAXI.RAW;

-- Mart 1: Hourly Zone Demand
CREATE TABLE IF NOT EXISTS hourly_zone_demand (
    pickup_date             DATE,
    pickup_hour             INTEGER,
    pickup_year             INTEGER,
    pickup_month            INTEGER,
    pickup_location_id      INTEGER,
    trip_count              BIGINT,
    avg_trip_distance_miles FLOAT,
    avg_trip_duration_mins  FLOAT,
    avg_fare_amount         FLOAT,
    total_fare_amount       FLOAT,
    avg_tip_amount          FLOAT,
    vendor_count            INTEGER,
    _loaded_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Mart 2: Daily Vendor Revenue
CREATE TABLE IF NOT EXISTS daily_vendor_revenue (
    pickup_date             DATE,
    pickup_year             INTEGER,
    pickup_month            INTEGER,
    vendor_id               INTEGER,
    vendor_name             VARCHAR(100),
    total_trips             BIGINT,
    total_fare_revenue      FLOAT,
    total_tip_revenue       FLOAT,
    gross_revenue           FLOAT,
    avg_fare_per_trip       FLOAT,
    tip_rate_pct            FLOAT,
    avg_distance_miles      FLOAT,
    avg_passengers          FLOAT,
    _loaded_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Mart 3: Monthly Payment Mix
CREATE TABLE IF NOT EXISTS monthly_payment_mix (
    pickup_year             INTEGER,
    pickup_month            INTEGER,
    payment_type            INTEGER,
    payment_type_desc       VARCHAR(50),
    trip_count              BIGINT,
    total_fare              FLOAT,
    total_tips              FLOAT,
    avg_tip                 FLOAT,
    monthly_total_trips     BIGINT,
    market_share_pct        FLOAT,
    _loaded_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Mart 4: Pipeline Health
CREATE TABLE IF NOT EXISTS pipeline_health_log (
    pickup_year             INTEGER,
    pickup_month            INTEGER,
    total_rows_in_silver    BIGINT,
    total_fare_volume       FLOAT,
    avg_trip_distance       FLOAT,
    avg_fare                FLOAT,
    earliest_pickup         TIMESTAMP,
    latest_pickup           TIMESTAMP,
    distinct_pickup_zones   INTEGER,
    distinct_vendors        INTEGER,
    zero_fare_rows          BIGINT,
    null_passenger_rows     BIGINT,
    health_run_at           TIMESTAMP,
    _loaded_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


-- ── STEP 6: COPY INTO (run after each Databricks gold write) ──────────────────
-- Pattern: COPY INTO truncates and reloads by default (full load per mart).
-- For truly incremental, use MERGE — see load/incremental_merge.sql.

COPY INTO NYC_TAXI.RAW.hourly_zone_demand (
    pickup_date, pickup_hour, pickup_year, pickup_month, pickup_location_id,
    trip_count, avg_trip_distance_miles, avg_trip_duration_mins,
    avg_fare_amount, total_fare_amount, avg_tip_amount, vendor_count
)
FROM (
    SELECT
        $1:pickup_date::DATE,
        $1:pickup_hour::INTEGER,
        $1:pickup_year::INTEGER,
        $1:pickup_month::INTEGER,
        $1:pickup_location_id::INTEGER,
        $1:trip_count::BIGINT,
        $1:avg_trip_distance_miles::FLOAT,
        $1:avg_trip_duration_mins::FLOAT,
        $1:avg_fare_amount::FLOAT,
        $1:total_fare_amount::FLOAT,
        $1:avg_tip_amount::FLOAT,
        $1:vendor_count::INTEGER
    FROM @gold_stage/hourly_zone_demand/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR    = 'CONTINUE'    -- Log bad rows, don't fail the whole load
PURGE       = FALSE;        -- Keep files in S3 (they're immutable raw)

-- Verify load
SELECT COUNT(*), MIN(pickup_date), MAX(pickup_date) FROM NYC_TAXI.RAW.hourly_zone_demand;

COPY INTO NYC_TAXI.RAW.daily_vendor_revenue (
    pickup_date, pickup_year, pickup_month, vendor_id, vendor_name,
    total_trips, total_fare_revenue, total_tip_revenue, gross_revenue,
    avg_fare_per_trip, tip_rate_pct, avg_distance_miles, avg_passengers
)
FROM (
    SELECT
        $1:pickup_date::DATE,
        $1:pickup_year::INTEGER,
        $1:pickup_month::INTEGER,
        $1:vendor_id::INTEGER,
        $1:vendor_name::VARCHAR,
        $1:total_trips::BIGINT,
        $1:total_fare_revenue::FLOAT,
        $1:total_tip_revenue::FLOAT,
        $1:gross_revenue::FLOAT,
        $1:avg_fare_per_trip::FLOAT,
        $1:tip_rate_pct::FLOAT,
        $1:avg_distance_miles::FLOAT,
        $1:avg_passengers::FLOAT
    FROM @gold_stage/daily_vendor_revenue/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE;

COPY INTO NYC_TAXI.RAW.monthly_payment_mix (
    pickup_year, pickup_month, payment_type, payment_type_desc,
    trip_count, total_fare, total_tips, avg_tip,
    monthly_total_trips, market_share_pct
)
FROM (
    SELECT
        $1:pickup_year::INTEGER,
        $1:pickup_month::INTEGER,
        $1:payment_type::INTEGER,
        $1:payment_type_desc::VARCHAR,
        $1:trip_count::BIGINT,
        $1:total_fare::FLOAT,
        $1:total_tips::FLOAT,
        $1:avg_tip::FLOAT,
        $1:monthly_total_trips::BIGINT,
        $1:market_share_pct::FLOAT
    FROM @gold_stage/monthly_payment_mix/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE;

COPY INTO NYC_TAXI.RAW.pipeline_health_log (
    pickup_year, pickup_month, total_rows_in_silver, total_fare_volume,
    avg_trip_distance, avg_fare, earliest_pickup, latest_pickup,
    distinct_pickup_zones, distinct_vendors, zero_fare_rows,
    null_passenger_rows, health_run_at
)
FROM (
    SELECT
        $1:pickup_year::INTEGER,
        $1:pickup_month::INTEGER,
        $1:total_rows_in_silver::BIGINT,
        $1:total_fare_volume::FLOAT,
        $1:avg_trip_distance::FLOAT,
        $1:avg_fare::FLOAT,
        $1:earliest_pickup::TIMESTAMP,
        $1:latest_pickup::TIMESTAMP,
        $1:distinct_pickup_zones::INTEGER,
        $1:distinct_vendors::INTEGER,
        $1:zero_fare_rows::BIGINT,
        $1:null_passenger_rows::BIGINT,
        $1:health_run_at::TIMESTAMP
    FROM @gold_stage/pipeline_health/
)
FILE_FORMAT = (TYPE = PARQUET)
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE;


-- ── STEP 7: Freshness check query (run on a schedule to alert on staleness) ──

SELECT
    MAX(pickup_date)                                    AS last_data_date,
    DATEDIFF('day', MAX(pickup_date), CURRENT_DATE())   AS days_since_last_data,
    MAX(_loaded_at)                                     AS last_load_time,
    DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP()) AS hours_since_load,
    CASE
        WHEN DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP()) > 48
        THEN 'STALE — pipeline may be broken'
        ELSE 'FRESH'
    END                                                 AS freshness_status
FROM NYC_TAXI.RAW.hourly_zone_demand;
