"""
Database Handler for Fraud Alerts
Handles SQLite storage with thread-safe operations
"""

import sqlite3
import json
import threading
from queue import Queue
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Database configuration
DB_FILE = "fraud_alerts.db"
BATCH_SIZE = 10  # Write in batches for efficiency

# Thread-safe queue for database writes
alert_queue = Queue()


def init_database():
    """Initialize SQLite database with fraud alerts table"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fraud_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type TEXT NOT NULL,
            user_id TEXT,
            device_id TEXT,
            order_id TEXT,
            event_id TEXT,
            amount REAL,
            severity TEXT,
            message TEXT,
            detected_at INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            alert_data TEXT
        )
    """)
    
    # Create indexes for faster queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_alert_type ON fraud_alerts(alert_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON fraud_alerts(user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_detected_at ON fraud_alerts(detected_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_severity ON fraud_alerts(severity)")
    
    conn.commit()
    conn.close()
    logger.info(f"✅ Database initialized: {DB_FILE}")


def db_writer_thread():
    """Background thread that writes alerts to database"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    batch = []
    
    logger.info("🔄 Database writer thread started")
    
    while True:
        try:
            # Get alert from queue (blocking call)
            alert = alert_queue.get()
            
            # Check for poison pill (shutdown signal)
            if alert is None:
                break
            
            # Add to batch
            batch.append(alert)
            
            # Write batch when it reaches BATCH_SIZE or queue is empty
            if len(batch) >= BATCH_SIZE or alert_queue.empty():
                for alert_data in batch:
                    cursor.execute("""
                        INSERT INTO fraud_alerts 
                        (alert_type, user_id, device_id, order_id, event_id, 
                         amount, severity, message, detected_at, alert_data)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        alert_data.get('alert_type'),
                        alert_data.get('user_id'),
                        alert_data.get('device_id'),
                        alert_data.get('order_id'),
                        alert_data.get('event_id'),
                        alert_data.get('amount'),
                        alert_data.get('severity'),
                        alert_data.get('message'),
                        alert_data.get('detected_at'),
                        json.dumps(alert_data)
                    ))
                
                conn.commit()
                logger.info(f"💾 Saved {len(batch)} alerts to database")
                batch = []
            
            alert_queue.task_done()
            
        except Exception as e:
            logger.error(f"❌ Database write error: {e}")
    
    conn.close()
    logger.info("🛑 Database writer thread stopped")


def save_alert_to_db(alert):
    """
    Queue alert for database storage (non-blocking)
    
    Args:
        alert: Dictionary containing fraud alert data
    """
    alert_queue.put(alert)


def get_stats():
    """Get statistics from database"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    stats = {}
    
    # Total alerts
    cursor.execute("SELECT COUNT(*) FROM fraud_alerts")
    stats['total_alerts'] = cursor.fetchone()[0]
    
    # Alerts by type
    cursor.execute("""
        SELECT alert_type, COUNT(*) 
        FROM fraud_alerts 
        GROUP BY alert_type
    """)
    stats['by_type'] = dict(cursor.fetchall())
    
    # Alerts by severity
    cursor.execute("""
        SELECT severity, COUNT(*) 
        FROM fraud_alerts 
        GROUP BY severity
    """)
    stats['by_severity'] = dict(cursor.fetchall())
    
    # Recent alerts (last 24 hours)
    cursor.execute("""
        SELECT COUNT(*) 
        FROM fraud_alerts 
        WHERE detected_at > ?
    """, (int((datetime.now().timestamp() - 86400) * 1000),))
    stats['last_24h'] = cursor.fetchone()[0]
    
    conn.close()
    return stats


def query_recent_alerts(limit=10):
    """Query recent fraud alerts"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT alert_type, user_id, severity, message, detected_at 
        FROM fraud_alerts 
        ORDER BY detected_at DESC 
        LIMIT ?
    """, (limit,))
    
    alerts = cursor.fetchall()
    conn.close()
    return alerts


def shutdown():
    """Gracefully shutdown database writer thread"""
    alert_queue.put(None)  # Send poison pill
    alert_queue.join()  # Wait for queue to be empty
