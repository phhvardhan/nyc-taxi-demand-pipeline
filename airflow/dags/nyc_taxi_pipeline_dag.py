"""
airflow/dags/nyc_taxi_pipeline_dag.py

NYC Taxi Demand Intelligence Pipeline — Airflow DAG

Schedule: Daily at 9AM UTC
  - Ingests previous day's TLC data to S3 bronze
  - Transforms bronze → silver → gold via PySpark
  - Loads gold marts into Snowflake via COPY INTO
  - Runs dbt models and tests
  - Alerts on any failure

Design decisions documented inline — these are the answers to
interview questions about orchestration.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable


# ── Default args applied to every task ────────────────────────────────────────
# Interview answer: "I set retries at the task level, not the DAG level,
# because different tasks have different failure modes. Ingestion from a
# rate-limited API gets 3 retries with exponential backoff. Transform tasks
# that OOM'd once won't magically succeed on retry — 1 retry is enough to
# catch transient Spark issues, more than that is just delaying the alert."

def _alert_on_failure(context):
    """
    Called by Airflow when any task fails after all retries.
    In production: posts to Slack via webhook, sends email, or pages on-call.
    
    Interview answer: "I separated job health alerts (this function) from
    data health alerts (the pipeline_health mart + freshness check in dbt).
    A job can succeed while delivering garbage — you need both."
    """
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["execution_date"]
    log_url = context["task_instance"].log_url

    alert_message = (
        f"PIPELINE FAILURE\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Execution date: {execution_date}\n"
        f"Logs: {log_url}\n"
        f"Action: Downstream tasks halted. Check S3 control records for last "
        f"successful partition before investigating."
    )

    # In production: replace with Slack webhook call or email operator
    # slack_webhook_url = Variable.get("slack_webhook_url")
    # requests.post(slack_webhook_url, json={"text": alert_message})
    print(f"ALERT: {alert_message}")


default_args = {
    "owner": "harsha.peela",
    "depends_on_past": False,          # Don't wait for yesterday's run to succeed
    "email_on_failure": True,
    "email_on_retry": False,           # Don't spam on retries — only on final failure
    "retries": 1,                      # Default: 1 retry for most tasks
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _alert_on_failure,
}


# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="nyc_taxi_demand_pipeline",
    description="NYC Yellow Taxi: ingest → bronze → silver → gold → Snowflake → dbt",
    schedule_interval="0 9 * * *",     # 9AM UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,                     # Don't backfill missed runs on deploy
    max_active_runs=1,                 # Only one pipeline run at a time
    tags=["nyc_taxi", "data_engineering", "production"],
    default_args=default_args,
    doc_md="""
    ## NYC Taxi Demand Intelligence Pipeline

    Runs daily at 9AM UTC. Ingests the previous month's TLC Yellow Taxi data,
    transforms through a medallion architecture (bronze → silver → gold),
    loads to Snowflake, and models with dbt.

    **SLA:** Gold data in Snowflake by 11AM UTC (2-hour window).
    **On failure:** All downstream tasks halt automatically. Slack alert fires.
    **Replay:** Re-run any date safely — ingestion is idempotent, dbt MERGE
    handles duplicates at the mart layer.
    """,
) as dag:


    # ── TASK 1: Ingest TLC data → S3 Bronze ──────────────────────────────────
    # Retries: 3 — API is rate-limited and occasionally flaky
    # Retry delay: exponential (handled inside ingest.py with jitter)
    # If this fails: entire pipeline halts, no stale data gets transformed
    ingest_bronze = BashOperator(
        task_id="ingest_to_bronze",
        bash_command=(
            "cd /opt/airflow/nyc_taxi_pipeline && "
            "source venv/bin/activate && "
            "python3 ingestion/ingest.py "
            "--year {{ execution_date.year }} "
            "--month {{ execution_date.month }}"
        ),
        retries=3,
        retry_delay=timedelta(minutes=2),
        doc_md="""
        Downloads TLC Yellow Taxi parquet for the execution month.
        Validates rows, quarantines bad data, writes MD5 checksum.
        Idempotent: re-running the same month overwrites the same S3 key.
        """,
    )


    # ── TASK 2: Bronze → Silver (PySpark) ────────────────────────────────────
    # Retries: 1 — OOM is the main failure mode; won't fix itself on retry
    # unless it's a transient memory spike, so 1 retry catches that edge case
    transform_silver = BashOperator(
        task_id="transform_bronze_to_silver",
        bash_command=(
            "cd /opt/airflow/nyc_taxi_pipeline && "
            "source venv/bin/activate && "
            "export JAVA_HOME=/opt/homebrew/opt/openjdk@17 && "
            "export SPARK_DRIVER_MEMORY=4g && "
            "python3 transform/bronze_to_silver.py"
        ),
        retries=1,
        retry_delay=timedelta(minutes=3),
        doc_md="""
        Reads raw parquet from S3 bronze.
        Applies: type casting, dedup on natural key, year-range filter
        for corrupt timestamps, derived fields (duration, time_segment).
        Writes Delta Lake to S3 silver.
        """,
    )


    # ── TASK 3: Silver → Gold (PySpark) ──────────────────────────────────────
    transform_gold = BashOperator(
        task_id="transform_silver_to_gold",
        bash_command=(
            "cd /opt/airflow/nyc_taxi_pipeline && "
            "source venv/bin/activate && "
            "export JAVA_HOME=/opt/homebrew/opt/openjdk@17 && "
            "export SPARK_DRIVER_MEMORY=4g && "
            "python3 transform/silver_to_gold.py"
        ),
        retries=1,
        retry_delay=timedelta(minutes=3),
        doc_md="""
        Aggregates silver into 4 gold marts:
        hourly_zone_demand, daily_vendor_revenue,
        monthly_payment_mix, pipeline_health.
        Uploads to S3 gold prefix for Snowflake COPY INTO.
        """,
    )


    # ── TASK 4: Upload gold to S3 ────────────────────────────────────────────
    upload_gold_s3 = BashOperator(
        task_id="upload_gold_to_s3",
        bash_command=(
            "aws s3 cp data/gold/ "
            "s3://nyc-taxi-harsha-pipeline/gold/yellow_taxi/ "
            "--recursive "
            "--exclude '*.crc'"   # Exclude Delta internal files — not needed in S3 stage
        ),
        doc_md="""
        Syncs local gold Delta output to S3.
        Excludes .crc files (Delta internal checksums — not valid parquet,
        would cause COPY INTO to fail with 'not a parquet file' error).
        """,
    )


    # ── TASK 5: Load Snowflake via COPY INTO ─────────────────────────────────
    # Snowflake's load metadata tracks already-loaded files — re-running this
    # task is safe; it will skip files already loaded (0 files processed).
    load_snowflake = SnowflakeOperator(
        task_id="copy_into_snowflake",
        snowflake_conn_id="snowflake_nyc_taxi",   # Configured in Airflow Connections UI
        sql="load/snowflake_copy_into.sql",        # Parameterized COPY INTO statements
        warehouse="TAXI_LOAD_WH",
        database="NYC_TAXI",
        schema="RAW",
        doc_md="""
        Runs COPY INTO for all 4 gold marts.
        Snowflake tracks loaded files — idempotent by design.
        Uses TAXI_LOAD_WH (dedicated load warehouse) to avoid
        contending with Tableau/BI queries on TAXI_BI_WH.
        """,
    )


    # ── TASK 6: dbt run + test ───────────────────────────────────────────────
    # Run models first, then tests. If tests fail, the pipeline is marked
    # failed — the data is in Snowflake but hasn't passed quality gates.
    # Interview answer: "dbt tests are the last quality gate before data
    # reaches Tableau. If not_null or accepted_values fails here, I'd rather
    # the pipeline fail loudly than silently serve bad data to dashboards."
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/nyc_taxi_pipeline/dbt_project && "
            "dbt run --profiles-dir ~/.dbt"
        ),
        doc_md="Builds staging view and incremental demand mart.",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd /opt/airflow/nyc_taxi_pipeline/dbt_project && "
            "dbt test --profiles-dir ~/.dbt"
        ),
        doc_md="""
        Runs 14 data quality tests: not_null, accepted_values, uniqueness.
        Failure here halts the pipeline — stale passing data is safer
        than fresh failing data reaching downstream consumers.
        """,
    )


    # ── TASK 7: Freshness check ──────────────────────────────────────────────
    # Separate from dbt tests — this checks the pipeline health mart itself
    # to confirm the SLA was met (data landed within 2 hours of schedule).
    freshness_check = SnowflakeOperator(
        task_id="freshness_check",
        snowflake_conn_id="snowflake_nyc_taxi",
        sql="""
            SELECT
                CASE
                    WHEN DATEDIFF('hour', MAX(_loaded_at), CURRENT_TIMESTAMP()) > 2
                    THEN 1/0   -- Force a division-by-zero error to fail the task
                    ELSE 1
                END AS freshness_ok
            FROM NYC_TAXI.RAW.hourly_zone_demand;
        """,
        warehouse="TAXI_BI_WH",
        doc_md="""
        Confirms data landed within 2-hour SLA.
        Fails the task (and triggers alert) if data is stale.
        This catches the 'job succeeded but delivered stale data' scenario
        that job-level monitoring alone would miss.
        """,
    )


    # ── Task dependencies (the pipeline DAG) ─────────────────────────────────
    # Linear chain: if any task fails, all downstream tasks are SKIPPED.
    # No trigger_rule overrides — default upstream_success behavior is correct.
    #
    # Interview answer: "I deliberately kept this linear rather than
    # parallelizing the gold mart loads, because they share the same Spark
    # session and the same S3 upload step. Parallelism would require splitting
    # the transform scripts and managing concurrent S3 uploads — added
    # complexity with minimal time savings for a daily batch pipeline."

    (
        ingest_bronze
        >> transform_silver
        >> transform_gold
        >> upload_gold_s3
        >> load_snowflake
        >> dbt_run
        >> dbt_test
        >> freshness_check
    )