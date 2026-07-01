"""
tests/test_validation.py

Unit tests for the validation logic in ingestion/ingest.py.

These are UNIT tests — no S3, no network, no AWS credentials needed.
We build synthetic pandas DataFrames and pass them directly to the
validation function, testing each rule in isolation.

Run: pytest tests/ -v
"""

import io
import sys
import os
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# Add project root to path so we can import ingestion module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.ingest import validate_parquet
from config.settings import load_pipeline_config


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def pipeline_cfg():
    """Load pipeline config — uses environment or defaults, no credentials needed."""
    # Patch env vars so settings.py doesn't fail on missing AWS keys
    with patch.dict(os.environ, {
        "AWS_ACCESS_KEY_ID": "fake",
        "AWS_SECRET_ACCESS_KEY": "fake",
        "S3_BUCKET_NAME": "fake-bucket",
    }):
        return load_pipeline_config()


@pytest.fixture
def valid_row():
    """A single completely valid taxi trip row."""
    return {
        "tpep_pickup_datetime": datetime(2024, 1, 15, 9, 0, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 15, 9, 25, 0),
        "passenger_count": 2,
        "trip_distance": 3.5,
        "PULocationID": 161,
        "DOLocationID": 234,
        "fare_amount": 15.50,
        "tip_amount": 3.10,
        "total_amount": 20.35,
        "VendorID": 1,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "payment_type": 1,
        "extra": 0.5,
        "mta_tax": 0.5,
        "tolls_amount": 0.0,
        "improvement_surcharge": 0.3,
        "congestion_surcharge": 2.5,
        "airport_fee": 0.0,
    }


def make_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Convert a DataFrame to parquet bytes — mimics what we download from TLC."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()


def make_valid_df(n_rows: int, valid_row: dict) -> pd.DataFrame:
    """Build a DataFrame of n valid rows from a single row template."""
    return pd.DataFrame([valid_row] * n_rows)


# ── Tests: Required column validation ─────────────────────────────────────────

class TestRequiredColumns:

    def test_all_required_columns_present_passes(self, pipeline_cfg, valid_row):
        """A file with all required columns and valid data should pass cleanly."""
        df = make_valid_df(pipeline_cfg.min_expected_rows + 1, valid_row)
        data = make_parquet_bytes(df)
        valid, quarantine, issues = validate_parquet(data, pipeline_cfg)

        assert len(valid) > 0, "Expected valid rows but got none"
        assert len(quarantine) == 0, f"Expected no quarantine rows but got {len(quarantine)}"
        assert not any("Missing required columns" in i for i in issues)

    def test_missing_required_column_quarantines_everything(self, pipeline_cfg, valid_row):
        """If a required column is missing entirely, the whole file goes to quarantine."""
        row_without_fare = {k: v for k, v in valid_row.items() if k != "fare_amount"}
        df = make_valid_df(10, row_without_fare)
        data = make_parquet_bytes(df)
        valid, quarantine, issues = validate_parquet(data, pipeline_cfg)

        assert len(valid) == 0, "No rows should be valid when a required column is missing"
        assert any("Missing required columns" in i for i in issues), \
            f"Expected missing column issue but got: {issues}"

    def test_missing_pickup_datetime_quarantines_everything(self, pipeline_cfg, valid_row):
        """pickup_datetime is required — missing it quarantines the whole file."""
        row = {k: v for k, v in valid_row.items() if k != "tpep_pickup_datetime"}
        df = make_valid_df(10, row)
        data = make_parquet_bytes(df)
        valid, quarantine, issues = validate_parquet(data, pipeline_cfg)

        assert len(valid) == 0


# ── Tests: Null validation ─────────────────────────────────────────────────────

class TestNullValidation:

    def test_null_pickup_datetime_quarantines_row(self, pipeline_cfg, valid_row):
        """Rows with null pickup_datetime should be quarantined."""
        rows = [valid_row.copy() for _ in range(pipeline_cfg.min_expected_rows + 10)]
        # Make 5 rows have null pickup
        for i in range(5):
            rows[i]["tpep_pickup_datetime"] = None
        df = pd.DataFrame(rows)
        data = make_parquet_bytes(df)
        valid, quarantine, issues = validate_parquet(data, pipeline_cfg)

        assert len(quarantine) == 5, \
            f"Expected 5 quarantined rows but got {len(quarantine)}"
        assert len(valid) == pipeline_cfg.min_expected_rows + 5, \
            f"Expected {pipeline_cfg.min_expected_rows + 5} valid rows"

    def test_null_fare_amount_quarantines_row(self, pipeline_cfg, valid_row):
        """Rows with null fare_amount should be quarantined."""
        rows = [valid_row.copy() for _ in range(pipeline_cfg.min_expected_rows + 3)]
        for i in range(3):
            rows[i]["fare_amount"] = None
        df = pd.DataFrame(rows)
        data = make_parquet_bytes(df)
        valid, quarantine, issues = validate_parquet(data, pipeline_cfg)

        assert len(quarantine) == 3

    def test_null_location_id_quarantines_row(self, pipeline_cfg, valid_row):
        """Rows with null PULocationID should be quarantined."""
        rows = [valid_row.copy() for _ in range(pipeline_cfg.min_expected_rows + 2)]
        for i in range(2):
            rows[i]["PULocationID"] = None
        df = pd.DataFrame(rows)
        data = make_parquet_bytes(df)
        valid, quarantine, issues = validate_parquet(data, pipeline_cfg)

        assert len(quarantine) == 2


# ── Tests: Business rule validation ───────────────────────────────────────────

class TestBusinessRules:

    def test_negative_trip_distance_quarantines_row(self, pipeline_cfg, valid_row):
        """
        Negative trip distance is physically impossible.
        This was a real data quality issue found in the Jan 2024 TLC dataset.
        """
        rows = [valid_row.copy() for _ in range(pipeline_cfg.min_expected_rows + 5)]
        for i in range(5):
            rows[i]["trip_distance"] = -1.5   # Negative distance — invalid
        df = pd.DataFrame(rows)
        data = make_parquet_bytes(df)
        valid, quarantine, issues = validate_parquet(data, pipeline_cfg)

        assert len(quarantine) == 5, \
            f"Expected 5 rows quarantined for negative distance, got {len(quarantine)}"
        assert any("trip_distance" in i for i in issues)

    def test_zero_trip_distance_passes(self, pipeline_cfg, valid_row):
        """
        Zero trip distance is valid — could be a cancelled trip or meter error.
        The rule is >= 0, not > 0. Don't over-filter.
        """
        rows = [valid_row.copy() for _ in range(pipeline_cfg.min_expected_rows + 1)]
        rows[0]["trip_distance"] = 0.0
        df = pd.DataFrame(rows)
        data = make_parquet_bytes(df)
        valid, quarantine, issues = validate_parquet(data, pipeline_cfg)

        # Zero distance should NOT be quarantined
        assert len(quarantine) == 0, \
            f"Zero distance should pass validation but {len(quarantine)} rows quarantined"

    def test_negative_fare_quarantines_row(self, pipeline_cfg, valid_row):
        """Negative fare amounts are invalid — adjustment credits handled separately."""
        rows = [valid_row.copy() for _ in range(pipeline_cfg.min_expected_rows + 3)]
        for i in range(3):
            rows[i]["fare_amount"] = -5.0
        df = pd.DataFrame(rows)
        data = make_parquet_bytes(df)
        valid, quarantine, issues = validate_parquet(data, pipeline_cfg)

        assert len(quarantine) == 3
        assert any("fare_amount" in i for i in issues)

    def test_pickup_after_dropoff_quarantines_row(self, pipeline_cfg, valid_row):
        """
        Pickup timestamp >= dropoff is a corrupt record.
        This is the most common timestamp anomaly in TLC data.
        The 2002/2008 timestamp bug we fixed in bronze_to_silver
        is a special case of this — pickup year is in the past.
        """
        rows = [valid_row.copy() for _ in range(pipeline_cfg.min_expected_rows + 4)]
        for i in range(4):
            # Swap pickup and dropoff — now pickup is AFTER dropoff
            rows[i]["tpep_pickup_datetime"] = datetime(2024, 1, 15, 10, 0, 0)
            rows[i]["tpep_dropoff_datetime"] = datetime(2024, 1, 15, 9, 0, 0)
        df = pd.DataFrame(rows)
        data = make_parquet_bytes(df)
        valid, quarantine, issues = validate_parquet(data, pipeline_cfg)

        assert len(quarantine) == 4, \
            f"Expected 4 quarantined rows for pickup >= dropoff, got {len(quarantine)}"
        assert any("Pickup" in i or "pickup" in i for i in issues)

    def test_pickup_equals_dropoff_quarantines_row(self, pipeline_cfg, valid_row):
        """Pickup == dropoff means zero-duration trip — also invalid (rule is strictly <)."""
        rows = [valid_row.copy() for _ in range(pipeline_cfg.min_expected_rows + 2)]
        same_time = datetime(2024, 1, 15, 9, 0, 0)
        for i in range(2):
            rows[i]["tpep_pickup_datetime"] = same_time
            rows[i]["tpep_dropoff_datetime"] = same_time
        df = pd.DataFrame(rows)
        data = make_parquet_bytes(df)
        valid, quarantine, issues = validate_parquet(data, pipeline_cfg)

        assert len(quarantine) == 2


# ── Tests: Row count threshold ────────────────────────────────────────────────

class TestRowCountThreshold:

    def test_file_below_minimum_rows_flags_issue(self, pipeline_cfg, valid_row):
        """
        A file with fewer than 100,000 rows is suspicious — TLC monthly files
        always have millions of rows. A tiny file likely means a truncated download
        or a partial API response.

        Note: rows still pass to valid (we don't quarantine an entire small file),
        but the issue is logged for alerting purposes.
        """
        # Only 10 rows — well below the 100K minimum
        df = make_valid_df(10, valid_row)
        data = make_parquet_bytes(df)
        valid, quarantine, issues = validate_parquet(data, pipeline_cfg)

        assert any("minimum" in i.lower() or "row count" in i.lower() for i in issues), \
            f"Expected row count issue but got: {issues}"

    def test_file_above_minimum_rows_no_count_issue(self, pipeline_cfg, valid_row):
        """A file with enough rows should not flag the row count check."""
        df = make_valid_df(pipeline_cfg.min_expected_rows + 1, valid_row)
        data = make_parquet_bytes(df)
        valid, quarantine, issues = validate_parquet(data, pipeline_cfg)

        count_issues = [i for i in issues if "minimum" in i.lower() or "row count" in i.lower()]
        assert len(count_issues) == 0, \
            f"Should not flag row count for sufficient rows but got: {count_issues}"


# ── Tests: Reject rate alert ──────────────────────────────────────────────────

class TestRejectRate:

    def test_mixed_valid_and_invalid_rows(self, pipeline_cfg, valid_row):
        """
        Verifies that valid and invalid rows are correctly split.
        This is the core of the quarantine-vs-pass logic.
        """
        n_valid = pipeline_cfg.min_expected_rows + 100
        n_invalid = 50

        valid_rows = [valid_row.copy() for _ in range(n_valid)]
        invalid_rows = [valid_row.copy() for _ in range(n_invalid)]
        for row in invalid_rows:
            row["fare_amount"] = -99.0   # All invalid

        all_rows = valid_rows + invalid_rows
        df = pd.DataFrame(all_rows)
        data = make_parquet_bytes(df)
        valid_df, quarantine_df, issues = validate_parquet(data, pipeline_cfg)

        assert len(valid_df) == n_valid, \
            f"Expected {n_valid} valid rows, got {len(valid_df)}"
        assert len(quarantine_df) == n_invalid, \
            f"Expected {n_invalid} quarantine rows, got {len(quarantine_df)}"

    def test_all_valid_rows_no_quarantine(self, pipeline_cfg, valid_row):
        """A perfectly clean file should produce zero quarantine rows."""
        df = make_valid_df(pipeline_cfg.min_expected_rows + 10, valid_row)
        data = make_parquet_bytes(df)
        valid_df, quarantine_df, issues = validate_parquet(data, pipeline_cfg)

        assert len(quarantine_df) == 0, \
            f"Expected 0 quarantine rows for clean data, got {len(quarantine_df)}"