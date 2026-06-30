"""
ingestion/ingest.py

Day 1 core: Downloads NYC Yellow Taxi parquet files from TLC's public S3
and lands them in YOUR S3 bronze layer — raw, immutable, partitioned by year/month.

Key design decisions baked in:
  - Idempotent: re-running the same month overwrites the same S3 key, no duplicates
  - Retry with exponential backoff + jitter on download failures
  - Control table in S3 tracks every run: timestamp, rows, file size, status
  - Quarantine path for files that fail validation (not deleted, inspectable)
  - Checksum written alongside every file for integrity verification

Interview story: "I landed the raw parquet in S3 first, never mutated it,
and tracked every run in a control table so I could answer 'what loaded when'
at any point — that's what made the pipeline auditable and replayable."
"""

import io
import json
import time
import random
import logging
import hashlib
import requests
import pandas as pd
import boto3
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from typing import Optional, Tuple
from botocore.exceptions import ClientError

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import load_aws_config, load_pipeline_config, AWSConfig, PipelineConfig

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("ingestion")


# ── S3 helpers ───────────────────────────────────────────────────────────────

def get_s3_client(cfg: AWSConfig):
    return boto3.client(
        "s3",
        aws_access_key_id=cfg.access_key_id,
        aws_secret_access_key=cfg.secret_access_key,
        region_name=cfg.region,
    )


def s3_key_exists(s3, bucket: str, key: str) -> bool:
    """Return True if an object already exists at this key."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def upload_bytes_to_s3(s3, data: bytes, bucket: str, key: str, metadata: dict = None):
    """Upload raw bytes to S3 with optional metadata."""
    extra = {"Metadata": {k: str(v) for k, v in metadata.items()}} if metadata else {}
    s3.put_object(Bucket=bucket, Key=key, Body=data, **extra)
    logger.info(f"Uploaded s3://{bucket}/{key} ({len(data):,} bytes)")


# ── Control table ────────────────────────────────────────────────────────────

def write_control_record(
    s3,
    cfg: AWSConfig,
    year: int,
    month: int,
    status: str,
    rows: int = 0,
    file_bytes: int = 0,
    error_msg: str = "",
    reject_rows: int = 0,
):
    """
    Write a JSON control record to S3 after every ingest attempt.
    This becomes the pipeline's audit log and freshness source of truth.

    Key: control/yellow_taxi/YYYY/MM/run_{timestamp}.json
    """
    record = {
        "pipeline": "nyc_yellow_taxi",
        "layer": "bronze",
        "year": year,
        "month": month,
        "status": status,           # success | skipped | failed | quarantined
        "rows_ingested": rows,
        "reject_rows": reject_rows,
        "file_size_bytes": file_bytes,
        "run_timestamp": datetime.utcnow().isoformat() + "Z",
        "error_message": error_msg,
    }
    key = (
        f"{cfg.control_prefix}/yellow_taxi/"
        f"{year:04d}/{month:02d}/"
        f"run_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.json"
    )
    s3.put_object(
        Bucket=cfg.bucket_name,
        Key=key,
        Body=json.dumps(record, indent=2).encode(),
        ContentType="application/json",
    )
    logger.info(f"Control record written: {key} | status={status}")
    return record


# ── Download with retry ───────────────────────────────────────────────────────

def download_with_retry(
    url: str,
    max_retries: int,
    base_seconds: float,
) -> Tuple[bytes, int]:
    """
    Download a URL with exponential backoff + full jitter.
    Returns (raw_bytes, http_status_code).

    Exponential backoff with jitter formula:
        sleep = random(0, base * 2^attempt)
    This prevents thundering herd if multiple pipeline instances run.

    Interview answer: "I used exponential backoff with full jitter on downloads
    so a flaky source didn't cause pile-on retries — each attempt waited
    a random interval up to 2^n * base seconds, with a hard cap of 3 retries."
    """
    for attempt in range(max_retries + 1):
        try:
            logger.info(f"Downloading {url} (attempt {attempt + 1}/{max_retries + 1})")
            resp = requests.get(url, timeout=120, stream=True)

            if resp.status_code == 200:
                data = resp.content
                logger.info(f"Downloaded {len(data):,} bytes")
                return data, 200

            elif resp.status_code == 404:
                # File doesn't exist yet (future month) — don't retry
                logger.warning(f"404 for {url} — file not published yet")
                return b"", 404

            elif resp.status_code == 429:
                # Rate limited — respect Retry-After header if present
                retry_after = int(resp.headers.get("Retry-After", base_seconds * (2 ** attempt)))
                logger.warning(f"Rate limited. Waiting {retry_after}s")
                time.sleep(retry_after)

            else:
                logger.warning(f"HTTP {resp.status_code} on attempt {attempt + 1}")

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout on attempt {attempt + 1}")
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Connection error on attempt {attempt + 1}: {e}")

        if attempt < max_retries:
            # Full jitter: sleep random amount between 0 and base * 2^attempt
            sleep_secs = random.uniform(0, base_seconds * (2 ** attempt))
            logger.info(f"Retrying in {sleep_secs:.1f}s")
            time.sleep(sleep_secs)

    return b"", -1   # -1 = exhausted all retries


# ── Validation ────────────────────────────────────────────────────────────────

def validate_parquet(
    data: bytes,
    cfg_pipeline: PipelineConfig,
) -> Tuple[pd.DataFrame, pd.DataFrame, list]:
    """
    Parse parquet bytes and split into valid rows and quarantine rows.
    Returns (valid_df, quarantine_df, issues_list).

    Validation rules:
      1. Required columns must exist
      2. Required columns must be non-null
      3. trip_distance must be >= 0
      4. fare_amount must be >= 0
      5. pickup must be before dropoff
      6. Total rows must exceed minimum threshold

    Interview answer: "I validated in layers — schema first, then nulls,
    then business rules. Bad rows went to quarantine in S3 rather than
    failing the whole batch, so I could inspect them without losing the
    good data. I alerted when the reject rate crossed 5%."
    """
    issues = []

    df = pd.read_parquet(io.BytesIO(data))
    logger.info(f"Parsed parquet: {len(df):,} rows, {len(df.columns)} columns")

    # Check 1: required columns present
    missing_cols = [c for c in cfg_pipeline.required_columns if c not in df.columns]
    if missing_cols:
        issues.append(f"Missing required columns: {missing_cols}")
        # Can't continue without schema — quarantine the whole file
        return pd.DataFrame(), df, issues

    # Check 2: row count floor
    if len(df) < cfg_pipeline.min_expected_rows:
        issues.append(
            f"Row count {len(df):,} below minimum {cfg_pipeline.min_expected_rows:,}"
        )

    # Check 3: build a boolean mask of valid rows
    valid_mask = pd.Series(True, index=df.index)

    # Null checks on required columns
    for col in cfg_pipeline.required_columns:
        null_mask = df[col].isnull()
        if null_mask.any():
            issues.append(f"Nulls in {col}: {null_mask.sum():,} rows")
        valid_mask &= ~null_mask

    # Business rule: non-negative distance and fare
    if "trip_distance" in df.columns:
        neg_dist = df["trip_distance"] < 0
        valid_mask &= ~neg_dist
        if neg_dist.any():
            issues.append(f"Negative trip_distance: {neg_dist.sum():,} rows")

    if "fare_amount" in df.columns:
        neg_fare = df["fare_amount"] < 0
        valid_mask &= ~neg_fare
        if neg_fare.any():
            issues.append(f"Negative fare_amount: {neg_fare.sum():,} rows")

    # Business rule: pickup before dropoff
    if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
        bad_time = df["tpep_pickup_datetime"] >= df["tpep_dropoff_datetime"]
        valid_mask &= ~bad_time
        if bad_time.any():
            issues.append(f"Pickup >= dropoff: {bad_time.sum():,} rows")

    valid_df = df[valid_mask].copy()
    quarantine_df = df[~valid_mask].copy()

    logger.info(
        f"Validation: {len(valid_df):,} valid | {len(quarantine_df):,} quarantined"
    )
    return valid_df, quarantine_df, issues


# ── Bronze landing ────────────────────────────────────────────────────────────

def land_to_bronze(
    s3,
    cfg_aws: AWSConfig,
    cfg_pipeline: PipelineConfig,
    year: int,
    month: int,
    force_reload: bool = False,
) -> dict:
    """
    Main ingestion function for one year/month partition.

    S3 key structure (bronze):
      bronze/yellow_taxi/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet
      bronze/yellow_taxi/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.md5

    Returns a summary dict with status and row counts.
    """
    bronze_key = (
        f"{cfg_aws.bronze_prefix}/year={year:04d}/month={month:02d}/"
        f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    )
    checksum_key = bronze_key.replace(".parquet", ".md5")
    quarantine_key = (
        f"{cfg_aws.quarantine_prefix}/year={year:04d}/month={month:02d}/"
        f"yellow_tripdata_{year:04d}-{month:02d}_quarantine.parquet"
    )

    # Idempotency check: skip if already landed and not forced
    if not force_reload and s3_key_exists(s3, cfg_aws.bucket_name, bronze_key):
        logger.info(f"Already in bronze, skipping: {bronze_key}")
        return write_control_record(s3, cfg_aws, year, month, "skipped")

    # Build TLC URL
    url = f"{cfg_pipeline.tlc_base_url}/yellow_tripdata_{year:04d}-{month:02d}.parquet"

    # Download with retry
    raw_bytes, status_code = download_with_retry(
        url, cfg_pipeline.max_retries, cfg_pipeline.retry_base_seconds
    )

    if status_code == 404:
        return write_control_record(s3, cfg_aws, year, month, "not_published")

    if status_code != 200 or not raw_bytes:
        return write_control_record(
            s3, cfg_aws, year, month, "failed",
            error_msg=f"Download failed after {cfg_pipeline.max_retries} retries"
        )

    # Compute MD5 checksum for integrity
    checksum = hashlib.md5(raw_bytes).hexdigest()

    # Validate parquet
    valid_df, quarantine_df, issues = validate_parquet(raw_bytes, cfg_pipeline)

    reject_rows = len(quarantine_df)
    total_rows = len(valid_df) + reject_rows
    reject_rate = (reject_rows / total_rows * 100) if total_rows > 0 else 0

    # Quarantine bad rows if any
    if reject_rows > 0:
        quarantine_buffer = io.BytesIO()
        quarantine_df.to_parquet(quarantine_buffer, index=False)
        upload_bytes_to_s3(
            s3, quarantine_buffer.getvalue(),
            cfg_aws.bucket_name, quarantine_key,
            metadata={"reject_count": str(reject_rows), "issues": str(issues[:3])}
        )
        logger.warning(
            f"Quarantined {reject_rows:,} rows ({reject_rate:.1f}%) → {quarantine_key}"
        )

    # Alert if reject rate is too high
    if reject_rate > cfg_pipeline.max_reject_rate_pct:
        logger.error(
            f"ALERT: Reject rate {reject_rate:.1f}% exceeds threshold "
            f"{cfg_pipeline.max_reject_rate_pct}% for {year}-{month:02d}"
        )

    # Land raw file in bronze (original bytes, immutable)
    upload_bytes_to_s3(
        s3, raw_bytes, cfg_aws.bucket_name, bronze_key,
        metadata={
            "source_url": url,
            "ingested_at": datetime.utcnow().isoformat(),
            "row_count": str(total_rows),
            "valid_rows": str(len(valid_df)),
            "reject_rows": str(reject_rows),
            "md5": checksum,
        }
    )

    # Land checksum file alongside parquet
    upload_bytes_to_s3(
        s3, checksum.encode(),
        cfg_aws.bucket_name, checksum_key,
    )

    status = "success" if issues == [] else "success_with_warnings"
    return write_control_record(
        s3, cfg_aws, year, month, status,
        rows=len(valid_df),
        file_bytes=len(raw_bytes),
        reject_rows=reject_rows,
        error_msg="; ".join(issues) if issues else "",
    )


# ── Orchestration ─────────────────────────────────────────────────────────────

def run_ingestion(
    months_back: int = None,
    force_reload: bool = False,
    specific_year_month: Optional[Tuple[int, int]] = None,
):
    """
    Entry point. Run from command line or call from Airflow PythonOperator.

    Args:
        months_back: How many months to ingest (default from pipeline config)
        force_reload: Re-download even if already in S3 (useful for reruns)
        specific_year_month: Tuple (year, month) to ingest a single month
    """
    cfg_aws = load_aws_config()
    cfg_pipeline = load_pipeline_config()
    s3 = get_s3_client(cfg_aws)

    if specific_year_month:
        targets = [specific_year_month]
    else:
        # Build list of months: today - 1 month going back N months
        # We skip current month (data not finalized by TLC yet)
        n = months_back or cfg_pipeline.backfill_months
        today = date.today()
        base = today.replace(day=1) - relativedelta(months=1)
        targets = [
            (base.year, base.month - i if base.month - i > 0 else 12,)
            for i in range(n)
        ]
        # Proper year rollback
        targets = []
        for i in range(n):
            target_date = base - relativedelta(months=i)
            targets.append((target_date.year, target_date.month))

    logger.info(f"Ingesting {len(targets)} month(s): {targets}")
    results = []

    for year, month in targets:
        logger.info(f"{'='*60}")
        logger.info(f"Processing {year}-{month:02d}")
        result = land_to_bronze(
            s3, cfg_aws, cfg_pipeline, year, month, force_reload
        )
        results.append(result)
        logger.info(f"Result: {result['status']} | rows={result.get('rows_ingested', 0):,}")

    # Summary
    statuses = [r["status"] for r in results]
    logger.info(f"{'='*60}")
    logger.info(f"Ingestion complete: {statuses}")
    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NYC Taxi Bronze Ingestion")
    parser.add_argument("--months-back", type=int, default=3, help="Months to ingest")
    parser.add_argument("--force", action="store_true", help="Re-download existing files")
    parser.add_argument("--year", type=int, help="Specific year (use with --month)")
    parser.add_argument("--month", type=int, help="Specific month (use with --year)")
    args = parser.parse_args()

    specific = (args.year, args.month) if args.year and args.month else None
    run_ingestion(
        months_back=args.months_back,
        force_reload=args.force,
        specific_year_month=specific,
    )
