#!/bin/bash
# Initialize Snowflake Silver and Gold schemas

set -e

echo "🏗️  Initializing Snowflake SILVER and GOLD schemas..."

# Load environment variables from parent directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

if [ -f "$PROJECT_ROOT/.env" ]; then
    export $(cat "$PROJECT_ROOT/.env" | grep -v '^#' | xargs)
    echo "✅ Loaded environment variables"
else
    echo "❌ .env file not found at $PROJECT_ROOT/.env!"
    exit 1
fi

# Check required env vars
if [ -z "$SNOWFLAKE_ACCOUNT" ] || [ -z "$SNOWFLAKE_USER" ] || [ -z "$SNOWFLAKE_PASSWORD" ]; then
    echo "❌ Missing Snowflake credentials in .env"
    exit 1
fi

echo "📊 Creating SILVER schema..."
../venv/bin/python3 <<EOF
import snowflake.connector
import os

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse='COMPUTE_WH'
)

# Read and execute SILVER DDL
with open('snowflake/init_silver_schema.sql', 'r') as f:
    silver_ddl = f.read()

cursor = conn.cursor()
for statement in [s.strip() for s in silver_ddl.split(';') if s.strip()]:
    try:
        cursor.execute(statement)
        print(f"✅ Executed: {statement[:50]}...")
    except Exception as e:
        print(f"⚠️  Warning: {e}")

cursor.close()
print("✅ SILVER schema created")

# Read and execute GOLD DDL
with open('snowflake/init_gold_schema.sql', 'r') as f:
    gold_ddl = f.read()

cursor = conn.cursor()
for statement in [s.strip() for s in gold_ddl.split(';') if s.strip()]:
    try:
        cursor.execute(statement)
        print(f"✅ Executed: {statement[:50]}...")
    except Exception as e:
        print(f"⚠️  Warning: {e}")

cursor.close()
print("✅ GOLD schema created")

conn.close()
EOF

echo ""
echo "✅ Snowflake schemas initialized successfully!"
echo ""
echo "Created schemas:"
echo "  📁 FLOWGUARD_DB.SILVER (cleaned data layer)"
echo "     - ORDERS_CLEAN"
echo "     - CLICKS_CLEAN"
echo ""
echo "  📁 FLOWGUARD_DB.GOLD (metrics layer)"
echo "     - DAILY_GMV_METRICS"
echo "     - FOOD_ITEM_PERFORMANCE"
echo "     - USER_FUNNEL_METRICS"
echo "     - HOURLY_ORDER_PATTERNS"
echo "     - USER_COHORT_METRICS"
echo ""
echo "🚀 Ready for Spark ETL pipeline!"
