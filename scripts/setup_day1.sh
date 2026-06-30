#!/bin/bash
# scripts/setup_day1.sh
# Run this on your Mac to set up the full local environment.
# Usage: chmod +x scripts/setup_day1.sh && ./scripts/setup_day1.sh

set -e   # Exit on any error

echo "================================================"
echo " NYC Taxi Pipeline — Day 1 Mac Setup"
echo "================================================"

# ── 1. Python virtual environment ─────────────────────────────────────────────
echo ""
echo "Step 1: Creating Python virtual environment..."
python3 -m venv venv
source venv/bin/activate
echo "  ✓ venv created and activated"

# ── 2. Install Python dependencies ────────────────────────────────────────────
echo ""
echo "Step 2: Installing Python packages..."
pip install --upgrade pip --quiet

pip install \
    boto3==1.34.0 \
    pandas==2.1.4 \
    pyarrow==14.0.2 \
    requests==2.31.0 \
    python-dotenv==1.0.0 \
    python-dateutil==2.8.2 \
    dbt-core==1.7.4 \
    dbt-snowflake==1.7.1 \
    snowflake-connector-python==3.6.0 \
    great-expectations==0.18.8 \
    --quiet

echo "  ✓ Python packages installed"

# ── 3. Copy env file ───────────────────────────────────────────────────────────
echo ""
echo "Step 3: Creating .env from template..."
if [ ! -f config/.env ]; then
    cp config/.env.example config/.env
    echo "  ✓ config/.env created — FILL IN YOUR VALUES before running ingestion"
else
    echo "  ✓ config/.env already exists — skipping"
fi

# ── 4. AWS CLI check ───────────────────────────────────────────────────────────
echo ""
echo "Step 4: Checking AWS CLI..."
if command -v aws &> /dev/null; then
    echo "  ✓ AWS CLI found: $(aws --version)"
else
    echo "  ✗ AWS CLI not found."
    echo "    Install: brew install awscli"
    echo "    Then run: aws configure"
fi

# ── 5. Create S3 bucket (requires AWS CLI configured) ─────────────────────────
echo ""
echo "Step 5: S3 bucket setup instructions"
echo "  Run these AWS CLI commands after filling in your .env:"
echo ""
echo "  # Replace 'yourname' with something unique (bucket names are global)"
echo "  BUCKET=nyc-taxi-pipeline-yourname"
echo ""
echo "  aws s3 mb s3://\$BUCKET --region us-east-1"
echo "  aws s3api put-bucket-versioning \\"
echo "      --bucket \$BUCKET \\"
echo "      --versioning-configuration Status=Enabled"
echo ""
echo "  # Enable lifecycle policy (cost optimization):"
echo "  aws s3api put-bucket-lifecycle-configuration \\"
echo "      --bucket \$BUCKET \\"
echo "      --lifecycle-configuration file://config/s3_lifecycle.json"

# ── 6. dbt profile setup ──────────────────────────────────────────────────────
echo ""
echo "Step 6: Creating dbt profiles.yml..."
mkdir -p ~/.dbt

cat > ~/.dbt/profiles.yml << 'EOF'
nyc_taxi:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: SYSADMIN
      database: NYC_TAXI
      warehouse: TAXI_BI_WH
      schema: STAGING
      threads: 4
      client_session_keep_alive: false
EOF

echo "  ✓ ~/.dbt/profiles.yml created"
echo "  ✓ Set SNOWFLAKE_* env vars before running dbt"

# ── 7. Test ingestion dry run ─────────────────────────────────────────────────
echo ""
echo "Step 7: Verify ingestion script loads..."
python3 -c "
import sys
sys.path.insert(0, '.')
from config.settings import load_pipeline_config
cfg = load_pipeline_config()
print(f'  ✓ Pipeline config loaded: {cfg.name}')
print(f'  ✓ TLC base URL: {cfg.tlc_base_url}')
print(f'  ✓ Max retries: {cfg.max_retries}')
print(f'  ✓ Min rows threshold: {cfg.min_expected_rows:,}')
"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "================================================"
echo " Setup complete. Next steps:"
echo "================================================"
echo ""
echo " 1. Fill in config/.env with your AWS keys and Snowflake creds"
echo " 2. Create your S3 bucket (commands in Step 5 above)"
echo " 3. Create a Snowflake trial account at snowflake.com/try"
echo " 4. Create a Databricks Community Edition account at community.cloud.databricks.com"
echo " 5. Run first ingestion:"
echo "      source venv/bin/activate"
echo "      python3 ingestion/ingest.py --months-back 1"
echo ""
echo " Then open notebooks/01_bronze_to_silver.py in Databricks."
echo ""
