"""Database connection for Events Gateway service."""
import os
from contextlib import contextmanager
from typing import Optional

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor


class DatabaseConfig:
    """Database configuration."""
    
    HOST = os.getenv("POSTGRES_HOST", "localhost")
    PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    DATABASE = os.getenv("POSTGRES_DB", "food_catalog")
    USER = os.getenv("POSTGRES_USER", "flowguard")
    PASSWORD = os.getenv("POSTGRES_PASSWORD", "flowguard123")


# Global connection pool
connection_pool: Optional[pool.SimpleConnectionPool] = None


def init_connection_pool():
    """Initialize database connection pool."""
    global connection_pool
    
    if connection_pool is None:
        try:
            connection_pool = psycopg2.pool.SimpleConnectionPool(
                2,  # Min connections
                10,  # Max connections
                host=DatabaseConfig.HOST,
                port=DatabaseConfig.PORT,
                database=DatabaseConfig.DATABASE,
                user=DatabaseConfig.USER,
                password=DatabaseConfig.PASSWORD
            )
            print(f"✅ Database connection pool initialized (Events Gateway)")
        except Exception as e:
            print(f"❌ Failed to initialize database connection pool: {e}")
            print(f"⚠️  Events Gateway will start but database operations will fail")
            # Don't raise - allow service to start even if DB is down


def close_connection_pool():
    """Close all database connections."""
    global connection_pool
    
    if connection_pool:
        connection_pool.closeall()
        connection_pool = None
        print("✅ Database connection pool closed")


@contextmanager
def get_db_connection():
    """Get database connection from pool."""
    if connection_pool is None:
        raise RuntimeError("Database connection pool not initialized. PostgreSQL may be down.")
    
    conn = connection_pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        connection_pool.putconn(conn)


@contextmanager
def get_db_cursor():
    """Get database cursor (returns dict results)."""
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            yield cursor
        finally:
            cursor.close()
