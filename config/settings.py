"""
config/settings.py
Central config loaded from environment variables.
All pipeline code imports from here — never hardcode values elsewhere.
"""

import os
from dotenv import load_dotenv
from dataclasses import dataclass
from typing import List

load_dotenv()


@dataclass
class AWSConfig:
    access_key_id: str
    secret_access_key: str
    region: str
    bucket_name: str
    bronze_prefix: str
    silver_prefix: str
    gold_prefix: str
    quarantine_prefix: str
    control_prefix: str


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    role: str


@dataclass
class PipelineConfig:
    name: str
    # NYC TLC public data — Yellow taxi, one file per month
    # Files live at this base URL; we parameterize year/month at runtime
    tlc_base_url: str
    # How many months back to backfill on first run
    backfill_months: int
    # Max retries for HTTP download with exponential backoff
    max_retries: int
    retry_base_seconds: float
    # Row count sanity check — reject file if fewer rows than this
    min_expected_rows: int
    # Reject rate threshold — alert if > X% of rows are quarantined
    max_reject_rate_pct: float
    # Required columns that must be non-null for a row to be valid
    required_columns: List[str]


def load_aws_config() -> AWSConfig:
    return AWSConfig(
        access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        bucket_name=os.environ["S3_BUCKET_NAME"],
        bronze_prefix=os.getenv("S3_BRONZE_PREFIX", "bronze/yellow_taxi"),
        silver_prefix=os.getenv("S3_SILVER_PREFIX", "silver/yellow_taxi"),
        gold_prefix=os.getenv("S3_GOLD_PREFIX", "gold/yellow_taxi"),
        quarantine_prefix=os.getenv("S3_QUARANTINE_PREFIX", "quarantine/yellow_taxi"),
        control_prefix=os.getenv("S3_CONTROL_PREFIX", "control"),
    )


def load_snowflake_config() -> SnowflakeConfig:
    return SnowflakeConfig(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "TAXI_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "NYC_TAXI"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
    )


def load_pipeline_config() -> PipelineConfig:
    return PipelineConfig(
        name=os.getenv("PIPELINE_NAME", "nyc_yellow_taxi"),
        tlc_base_url="https://d37ci6vzurychx.cloudfront.net/trip-data",
        backfill_months=3,
        max_retries=3,
        retry_base_seconds=2.0,
        min_expected_rows=100_000,   # Any monthly file with fewer rows is suspect
        max_reject_rate_pct=5.0,     # Alert if >5% of rows quarantined
        required_columns=[
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "PULocationID",
            "DOLocationID",
            "fare_amount",
        ],
    )
