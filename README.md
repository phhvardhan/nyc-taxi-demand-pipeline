# NYC Taxi Demand Intelligence Pipeline

**Built by:** Hema Harsha Vardhan Peela  
**Stack:** Python · AWS S3 · Databricks (Spark) · Delta Lake · Snowflake · dbt · Tableau Public  
**Architecture:** Medallion (Bronze → Silver → Gold)

---

## The business problem

NYC taxi operators and fleet dispatchers need real-time demand signals — which zones need drivers right now, what time-of-day patterns look like, and how revenue splits across vendors and payment types. This pipeline ingests TLC's public Yellow Taxi dataset and transforms it into actionable demand intelligence dashboards.

---

## Architecture

```
NYC TLC Public S3
        │
        ▼
Python Ingestion (ingest.py)
  · Retry with exponential backoff + jitter
  · Row-level validation → quarantine bad rows
  · MD5 checksum per file
  · Control record written to S3 after every run
        │
        ▼
S3 Bronze (raw, immutable, partitioned by year/month)
  · Original parquet, never mutated
  · Replay point if transform logic changes
        │
        ▼
Databricks Spark (notebooks/01_bronze_to_silver.py)
  · Schema standardization + type casting
  · Derived fields: duration, hour, day_of_week, time_segment
  · Deduplication on surrogate key
  · Writes Delta Lake → S3 Silver
        │
        ▼
S3 Silver (clean Delta Lake, ACID, time travel enabled)
        │
        ▼
Databricks Spark (notebooks/02_silver_to_gold.py)
  · 4 aggregated marts: hourly demand, daily revenue, payment mix, pipeline health
  · Writes Delta → S3 Gold
        │
        ▼
Snowflake (COPY INTO from S3 Gold via Storage Integration)
  · Two warehouses: TAXI_LOAD_WH (ingestion) · TAXI_BI_WH (serving)
  · Auto-suspend 60s → near-zero idle cost
        │
        ▼
dbt (staging views + incremental marts)
  · Typed staging views with dbt tests (not_null, unique, accepted_values)
  · Incremental mart model with MERGE strategy
  · Schema change policy: append_new_columns (non-breaking evolution)
        │
        ▼
Tableau Public (demand + pipeline health dashboards)
```

---

## Key design decisions (what you say in interviews)

### Why S3 bronze is immutable
Raw files land once and are never mutated. If the silver transform has a bug, I replay from bronze without re-hitting the source. The source API is rate-limited — re-ingestion is expensive and fragile. Idempotency key: `year=YYYY/month=MM/filename.parquet`. Re-running the same month overwrites the same key.

### Why Databricks Spark instead of Glue or pure SQL
The pre-load work is heavy: parsing raw JSON/parquet, dedup on a composite key, SCD logic, and enrichment joins. Spark on Databricks does this cheaply in the distributed layer before touching Snowflake credits. If the logic were SQL-expressible and the data smaller, I'd flip to Snowpipe + dbt only.

### Why Delta Lake at silver
Three things you get for free: ACID transactions (no partial writes), Time Travel (roll back to any Delta version), and Schema Enforcement (new unexpected columns fail loudly rather than silently coercing types). Schema evolution is explicit: I use `mergeSchema=true` only when I've reviewed the new field.

### Why two Snowflake warehouses
Load and serve are different workloads with different performance profiles. Running both on one warehouse means dashboard queries contend with COPY INTO jobs during pipeline runs. Separate warehouses + auto-suspend keeps cost near zero and keeps Tableau snappy.

### How incremental loads work
Ingestion tracks watermark via the S3 control record — last successful run timestamp per partition. Databricks reads only the new partitions. dbt's incremental model MERGEs on `(pickup_date, pickup_hour, pickup_location_id)` — new rows insert, changed rows update. Weekly full reconciliation catches hard deletes and missed updates the incremental path can't see.

### How schema evolution is handled
Three layers:
1. Spark reads with `mergeSchema=true` — new columns land in silver without failing the job
2. dbt's `on_schema_change: append_new_columns` — new columns propagate to marts automatically
3. Breaking changes (type changes, dropped required columns) fail loudly at the ingestion validation step — I'd rather know than silently load garbage

### Data quality
- Ingestion: null checks, row count floor, non-negative distance/fare, pickup < dropoff
- Bad rows → quarantine path in S3 (inspectable, not deleted)
- Alert when reject rate > 5%
- dbt tests: not_null, unique, accepted_values on every dimension column
- Post-load: freshness check alerts if last load > 48 hours old

### Cost optimization
- Spark transform on cheap S3 compute before loading to Snowflake (biggest lever)
- Snowflake auto-suspend: 60s for load WH, 120s for BI WH
- S3 lifecycle: bronze → Standard-IA after 30 days → Glacier after 90 days
- Parquet + Snappy compression throughout (4–6x smaller than CSV)
- Partition pruning on year/month: Spark and Snowflake only scan what they need

---

## Project structure

```
nyc_taxi_pipeline/
├── config/
│   ├── .env.example          # Environment variable template
│   ├── settings.py           # Central config loaded from env
│   └── s3_lifecycle.json     # S3 cost optimization policy
├── ingestion/
│   └── ingest.py             # Python: TLC → S3 bronze with retry + validation
├── notebooks/
│   ├── 01_bronze_to_silver.py  # Databricks: raw → clean Delta
│   └── 02_silver_to_gold.py    # Databricks: clean → aggregated marts
├── load/
│   └── snowflake_setup.sql   # DDL, stage, COPY INTO, freshness check
├── dbt_project/
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       │   ├── stg_hourly_zone_demand.sql
│       │   └── schema.yml    # Sources + dbt tests
│       └── marts/
│           └── mart_demand_intelligence.sql  # Incremental MERGE model
├── scripts/
│   └── setup_day1.sh         # Mac setup script
└── README.md
```

---

## Day-by-day build log

| Day | Goal | Done |
|-----|------|------|
| 1 | AWS setup, S3 buckets, ingestion script running locally | ☐ |
| 2 | Databricks notebooks: bronze→silver→gold Delta | ☐ |
| 3 | Snowflake load, dbt staging + marts, dbt test green | ☐ |
| 4 | Tableau dashboards, README, pipeline health monitoring | ☐ |

---

## Accounts to create (free/trial)

| Service | URL | Cost |
|---------|-----|------|
| AWS | aws.amazon.com | ~$2–5 for S3 storage |
| Databricks Community | community.cloud.databricks.com | Free |
| Snowflake Trial | snowflake.com/try | $400 credits, 30 days |
| Tableau Public | public.tableau.com | Free |

---

## Running the pipeline

```bash
# 1. Setup
chmod +x scripts/setup_day1.sh && ./scripts/setup_day1.sh
source venv/bin/activate

# 2. Fill in your credentials
cp config/.env.example config/.env
# Edit config/.env with your AWS + Snowflake values

# 3. Run ingestion (downloads 1 month of TLC data → S3 bronze)
python3 ingestion/ingest.py --months-back 1

# 4. Open Databricks, paste and run notebooks/01_bronze_to_silver.py
# 5. Run notebooks/02_silver_to_gold.py
# 6. Run Snowflake DDL: load/snowflake_setup.sql
# 7. Run dbt
cd dbt_project && dbt run && dbt test

# 8. Connect Tableau Public to Snowflake → NYC_TAXI.MARTS.MART_DEMAND_INTELLIGENCE
```
