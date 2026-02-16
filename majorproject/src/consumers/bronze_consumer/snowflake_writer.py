"""Snowflake Writer for Bronze Layer

Handles bulk inserts to Snowflake with connection pooling and error handling.
"""

import json
import logging
from typing import List, Dict, Any
from datetime import datetime
from contextlib import contextmanager

import snowflake.connector
from snowflake.connector.errors import ProgrammingError, DatabaseError

from .config import get_config

logger = logging.getLogger(__name__)


class SnowflakeWriterError(Exception):
    """Custom exception for Snowflake writer errors"""
    pass


class SnowflakeWriter:
    """Bulk writer for Snowflake Bronze tables"""
    
    def __init__(self):
        """Initialize Snowflake connection"""
        self.config = get_config()
        self._connection = None
        self._last_connection_check = 0
        self._connection_check_interval = 60  # Check connection every 60 seconds
        self._connect()
    
    def _connect(self):
        """Establish Snowflake connection"""
        try:
            if self._connection:
                try:
                    self._connection.close()
                except:
                    pass
            
            self._connection = snowflake.connector.connect(
                **self.config.to_snowflake_config()
            )
            logger.info(
                f"Connected to Snowflake: {self.config.snowflake_database}.{self.config.snowflake_schema}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise SnowflakeWriterError(f"Connection failed: {e}")
    
    def _ensure_connection(self):
        """Ensure connection is alive, reconnect if needed"""
        import time
        current_time = time.time()
        
        # Only check periodically to avoid overhead
        if current_time - self._last_connection_check > self._connection_check_interval:
            try:
                # Quick connection test
                cursor = self._connection.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
                self._last_connection_check = current_time
            except Exception as e:
                logger.warning(f"Connection check failed, reconnecting: {e}")
                self._connect()
                self._last_connection_check = current_time
    
    @contextmanager
    def _get_cursor(self):
        """Get cursor with auto-commit"""
        cursor = None
        try:
            # Ensure connection is alive before getting cursor
            self._ensure_connection()
            
            cursor = self._connection.cursor()
            yield cursor
            self._connection.commit()
        except Exception as e:
            if self._connection:
                try:
                    self._connection.rollback()
                except:
                    pass
            # If it's a connection error, try to reconnect
            if 'connection' in str(e).lower() or 'session' in str(e).lower():
                logger.warning(f"Connection error detected, will reconnect on next operation: {e}")
                try:
                    self._connect()
                except:
                    pass
            raise e
        finally:
            if cursor:
                cursor.close()
    
    def write_orders_batch(self, events: List[tuple]) -> int:
        """Write batch of order events to ORDERS_RAW table
        
        Args:
            events: List of (event_dict, metadata_dict) tuples
            
        Returns:
            int: Number of rows inserted
            
        Raises:
            SnowflakeWriterError: If write fails
        """
        if not events:
            return 0
        
        try:
            with self._get_cursor() as cursor:
                # Use INSERT...SELECT with PARSE_JSON (only syntax that works)
                insert_query = """
                INSERT INTO ORDERS_RAW (
                    EVENT_ID,
                    ORDER_ID,
                    USER_ID,
                    ITEM_ID,
                    ITEM_NAME,
                    PRICE,
                    STATUS,
                    EVENT_TIMESTAMP,
                    RAW_EVENT,
                    EVENT_TYPE,
                    SCHEMA_VERSION,
                    SOURCE_TOPIC,
                    PRODUCER_SERVICE,
                    INGESTION_ID
                ) SELECT %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, %s, %s
                """
                
                # Execute inserts one by one
                inserted = 0
                for event, metadata in events:
                    row = (
                        event.get('event_id'),
                        event.get('order_id'),
                        event.get('user_id'),
                        event.get('item_id'),
                        event.get('item_name'),
                        event.get('price'),
                        event.get('status', 'confirmed'),
                        event.get('timestamp'),
                        json.dumps(event),  # JSON string for PARSE_JSON
                        event.get('event_type', 'order'),  # EVENT_TYPE
                        metadata.get('schema_version'),  # SCHEMA_VERSION
                        metadata.get('source_topic'),  # SOURCE_TOPIC
                        metadata.get('producer_service'),  # PRODUCER_SERVICE
                        metadata.get('ingestion_id')  # INGESTION_ID
                    )
                    cursor.execute(insert_query, row)
                    inserted += cursor.rowcount
                
                logger.info(f"Inserted {inserted} orders into Snowflake")
                return inserted
                
        except (ProgrammingError, DatabaseError) as e:
            logger.error(f"Snowflake write error: {e}")
            raise SnowflakeWriterError(f"Write failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error writing to Snowflake: {e}")
            raise SnowflakeWriterError(f"Unexpected error: {e}")
    
    def write_clicks_batch(self, events: List[tuple]) -> int:
        """Write batch of click events to CLICKS_RAW table
        
        Args:
            events: List of (event_dict, metadata_dict) tuples
            
        Returns:
            int: Number of rows inserted
            
        Raises:
            SnowflakeWriterError: If write fails
        """
        if not events:
            return 0
        
        try:
            with self._get_cursor() as cursor:
                # Use INSERT...SELECT with PARSE_JSON (only syntax that works)
                insert_query = """
                INSERT INTO CLICKS_RAW (
                    EVENT_ID,
                    USER_ID,
                    EVENT_TYPE,
                    ITEM_ID,
                    SESSION_ID,
                    EVENT_TIMESTAMP,
                    RAW_EVENT,
                    SCHEMA_VERSION,
                    SOURCE_TOPIC,
                    PRODUCER_SERVICE,
                    INGESTION_ID
                ) SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s, %s, %s, %s
                """
                
                # Execute inserts one by one
                inserted = 0
                for event, metadata in events:
                    row = (
                        event.get('event_id'),
                        event.get('user_id'),
                        event.get('event_type'),
                        event.get('item_id'),
                        event.get('session_id'),
                        event.get('timestamp'),
                        json.dumps(event),  # JSON string for PARSE_JSON
                        metadata.get('schema_version'),  # SCHEMA_VERSION
                        metadata.get('source_topic'),  # SOURCE_TOPIC
                        metadata.get('producer_service'),  # PRODUCER_SERVICE
                        metadata.get('ingestion_id')  # INGESTION_ID
                    )
                    cursor.execute(insert_query, row)
                    inserted += cursor.rowcount
                
                logger.info(f"Inserted {inserted} clicks into Snowflake")
                return inserted
                
        except (ProgrammingError, DatabaseError) as e:
            logger.error(f"Snowflake write error: {e}")
            raise SnowflakeWriterError(f"Write failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error writing to Snowflake: {e}")
            raise SnowflakeWriterError(f"Unexpected error: {e}")
    
    def test_connection(self) -> bool:
        """Test Snowflake connection
        
        Returns:
            bool: True if connection successful
        """
        try:
            with self._get_cursor() as cursor:
                cursor.execute("SELECT CURRENT_VERSION()")
                version = cursor.fetchone()[0]
                logger.info(f"Snowflake connection OK. Version: {version}")
                return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def close(self):
        """Close Snowflake connection"""
        if self._connection:
            try:
                self._connection.close()
                logger.info("Snowflake connection closed")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")


# Global writer instance
_writer_instance: SnowflakeWriter = None


def get_writer() -> SnowflakeWriter:
    """Get or create global writer instance"""
    global _writer_instance
    if _writer_instance is None:
        _writer_instance = SnowflakeWriter()
    return _writer_instance


def close_writer():
    """Close global writer instance"""
    global _writer_instance
    if _writer_instance is not None:
        _writer_instance.close()
        _writer_instance = None
