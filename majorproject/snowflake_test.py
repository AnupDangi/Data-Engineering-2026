import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define connection parameters
USER = os.getenv('SNOWFLAKE_USER')
PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'FLOWGUARD_DB')
SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'BRONZE')

print(f"Connecting to Snowflake...")
print(f"Account: {ACCOUNT}")
print(f"User: {USER}")
print(f"Warehouse: {WAREHOUSE}")
print(f"Database: {DATABASE}")
print(f"Schema: {SCHEMA}")

# Establish the connection
try:
    conn = snowflake.connector.connect(
        user=USER,
        password=PASSWORD,
        account=ACCOUNT, 
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )
    print("✓ Connection successful!")

    # Create a cursor object
    cur = conn.cursor()

    # Execute a SQL statement (e.g., check the current Snowflake version)
    cur.execute("SELECT current_version()")

    # Fetch the result
    one_row = cur.fetchone()
    print(f"✓ Snowflake Version: {one_row[0]}")
    
    # Check current database and schema
    cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
    db_schema = cur.fetchone()
    print(f"✓ Current Database: {db_schema[0]}")
    print(f"✓ Current Schema: {db_schema[1]}")

except snowflake.connector.errors.ProgrammingError as e:
    print(f"✗ Connection failed: {e}")
except Exception as e:
    print(f"✗ Unexpected error: {e}")

finally:
    # Close the cursor and connection
    if 'cur' in locals() and cur is not None:
        cur.close()
    if 'conn' in locals() and conn is not None:
        conn.close()
    print("Connection closed.")
