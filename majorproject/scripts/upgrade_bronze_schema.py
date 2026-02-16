"""Upgrade Bronze schema with production metadata columns"""
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse='COMPUTE_WH',
    database='FLOWGUARD_DB',
    schema='BRONZE'
)

cur = conn.cursor()

# Add production metadata columns to ORDERS_RAW
print('📝 Adding production metadata columns to ORDERS_RAW...')
try:
    cur.execute('ALTER TABLE ORDERS_RAW ADD COLUMN IF NOT EXISTS EVENT_TYPE VARCHAR(50)')
    cur.execute('ALTER TABLE ORDERS_RAW ADD COLUMN IF NOT EXISTS SCHEMA_VERSION VARCHAR(20)')
    cur.execute('ALTER TABLE ORDERS_RAW ADD COLUMN IF NOT EXISTS SOURCE_TOPIC VARCHAR(100)')
    cur.execute('ALTER TABLE ORDERS_RAW ADD COLUMN IF NOT EXISTS PRODUCER_SERVICE VARCHAR(100)')
    cur.execute('ALTER TABLE ORDERS_RAW ADD COLUMN IF NOT EXISTS INGESTION_ID VARCHAR(200)')
    # Add unique constraint for idempotency (if column already exists, this will fail gracefully)
    try:
        cur.execute('ALTER TABLE ORDERS_RAW ADD UNIQUE (INGESTION_ID)')
        print('✅ Added UNIQUE constraint on INGESTION_ID')
    except Exception as ue:
        print(f'ℹ️  UNIQUE constraint: {ue}')
    print('✅ ORDERS_RAW upgraded')
except Exception as e:
    print(f'⚠️  ORDERS_RAW: {e}')

# Add production metadata columns to CLICKS_RAW
print('\n📝 Adding production metadata columns to CLICKS_RAW...')
try:
    cur.execute('ALTER TABLE CLICKS_RAW ADD COLUMN IF NOT EXISTS SCHEMA_VERSION VARCHAR(20)')
    cur.execute('ALTER TABLE CLICKS_RAW ADD COLUMN IF NOT EXISTS SOURCE_TOPIC VARCHAR(100)')
    cur.execute('ALTER TABLE CLICKS_RAW ADD COLUMN IF NOT EXISTS PRODUCER_SERVICE VARCHAR(100)')
    cur.execute('ALTER TABLE CLICKS_RAW ADD COLUMN IF NOT EXISTS INGESTION_ID VARCHAR(200)')
    # Add unique constraint for idempotency
    try:
        cur.execute('ALTER TABLE CLICKS_RAW ADD UNIQUE (INGESTION_ID)')
        print('✅ Added UNIQUE constraint on INGESTION_ID')
    except Exception as ue:
        print(f'ℹ️  UNIQUE constraint: {ue}')
    print('✅ CLICKS_RAW upgraded')
except Exception as e:
    print(f'⚠️  CLICKS_RAW: {e}')

# Verify schema
print('\n📊 Current schemas:')
cur.execute('DESCRIBE TABLE ORDERS_RAW')
orders_cols = [row[0] for row in cur.fetchall()]
print(f'  ORDERS_RAW columns: {len(orders_cols)}')
print('   ', ', '.join([c for c in orders_cols if c in ['EVENT_TYPE', 'SCHEMA_VERSION', 'SOURCE_TOPIC', 'PRODUCER_SERVICE', 'INGESTION_ID']]))

cur.execute('DESCRIBE TABLE CLICKS_RAW')
clicks_cols = [row[0] for row in cur.fetchall()]
print(f'  CLICKS_RAW columns: {len(clicks_cols)}')
print('   ', ', '.join([c for c in clicks_cols if c in ['SCHEMA_VERSION', 'SOURCE_TOPIC', 'PRODUCER_SERVICE', 'INGESTION_ID']]))

cur.close()
conn.close()

print('\n✅ Schema upgrade complete!')
