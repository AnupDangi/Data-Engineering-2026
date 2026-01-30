"""
Query and analyze fraud alerts from SQLite database
"""

import sqlite3
import json
from datetime import datetime
from tabulate import tabulate

DB_FILE = "fraud_alerts.db"


def query_all_alerts(limit=20):
    """Display recent fraud alerts"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, alert_type, user_id, severity, amount, message, 
               datetime(detected_at/1000, 'unixepoch') as detected_time
        FROM fraud_alerts 
        ORDER BY detected_at DESC 
        LIMIT ?
    """, (limit,))
    
    results = cursor.fetchall()
    conn.close()
    
    if results:
        headers = ['ID', 'Type', 'User', 'Severity', 'Amount', 'Message', 'Time']
        print(f"\n📋 Recent {limit} Fraud Alerts:\n")
        print(tabulate(results, headers=headers, tablefmt='grid'))
    else:
        print("No alerts found in database.")


def query_by_user(user_id):
    """Get all alerts for a specific user"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT alert_type, severity, amount, message,
               datetime(detected_at/1000, 'unixepoch') as detected_time
        FROM fraud_alerts 
        WHERE user_id = ?
        ORDER BY detected_at DESC
    """, (user_id,))
    
    results = cursor.fetchall()
    conn.close()
    
    if results:
        headers = ['Type', 'Severity', 'Amount', 'Message', 'Time']
        print(f"\n🔍 Fraud Alerts for User: {user_id}\n")
        print(tabulate(results, headers=headers, tablefmt='grid'))
    else:
        print(f"No alerts found for user: {user_id}")


def query_by_type(alert_type):
    """Get alerts by type"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, user_id, severity, amount, message,
               datetime(detected_at/1000, 'unixepoch') as detected_time
        FROM fraud_alerts 
        WHERE alert_type = ?
        ORDER BY detected_at DESC
        LIMIT 20
    """, (alert_type,))
    
    results = cursor.fetchall()
    conn.close()
    
    if results:
        headers = ['ID', 'User', 'Severity', 'Amount', 'Message', 'Time']
        print(f"\n⚡ {alert_type} Alerts:\n")
        print(tabulate(results, headers=headers, tablefmt='grid'))
    else:
        print(f"No alerts found for type: {alert_type}")


def get_statistics():
    """Display comprehensive statistics"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    print("\n📊 Fraud Detection Statistics\n")
    print("=" * 60)
    
    # Total alerts
    cursor.execute("SELECT COUNT(*) FROM fraud_alerts")
    total = cursor.fetchone()[0]
    print(f"Total Alerts: {total}")
    
    # By alert type
    cursor.execute("""
        SELECT alert_type, COUNT(*) as count
        FROM fraud_alerts 
        GROUP BY alert_type
        ORDER BY count DESC
    """)
    print("\n📈 Alerts by Type:")
    for row in cursor.fetchall():
        print(f"   • {row[0]}: {row[1]}")
    
    # By severity
    cursor.execute("""
        SELECT severity, COUNT(*) as count
        FROM fraud_alerts 
        GROUP BY severity
        ORDER BY 
            CASE severity 
                WHEN 'CRITICAL' THEN 1 
                WHEN 'HIGH' THEN 2 
                WHEN 'MEDIUM' THEN 3 
            END
    """)
    print("\n⚠️  Alerts by Severity:")
    for row in cursor.fetchall():
        print(f"   • {row[0]}: {row[1]}")
    
    # Top users
    cursor.execute("""
        SELECT user_id, COUNT(*) as alert_count
        FROM fraud_alerts 
        WHERE user_id IS NOT NULL
        GROUP BY user_id
        ORDER BY alert_count DESC
        LIMIT 5
    """)
    print("\n👤 Top 5 Users with Most Alerts:")
    for row in cursor.fetchall():
        print(f"   • {row[0]}: {row[1]} alerts")
    
    # Recent activity (last hour)
    cursor.execute("""
        SELECT COUNT(*) 
        FROM fraud_alerts 
        WHERE detected_at > ?
    """, (int((datetime.now().timestamp() - 3600) * 1000),))
    last_hour = cursor.fetchone()[0]
    print(f"\n⏰ Alerts in Last Hour: {last_hour}")
    
    conn.close()
    print("=" * 60)


def query_high_severity():
    """Display high severity alerts"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, alert_type, user_id, amount, message,
               datetime(detected_at/1000, 'unixepoch') as detected_time
        FROM fraud_alerts 
        WHERE severity IN ('CRITICAL', 'HIGH')
        ORDER BY detected_at DESC
        LIMIT 20
    """)
    
    results = cursor.fetchall()
    conn.close()
    
    if results:
        headers = ['ID', 'Type', 'User', 'Amount', 'Message', 'Time']
        print(f"\n🚨 High Priority Alerts:\n")
        print(tabulate(results, headers=headers, tablefmt='grid'))
    else:
        print("No high priority alerts found.")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("\n🔎 Fraud Alerts Database Query Tool\n")
        print("Usage:")
        print("  python query_db.py stats              - Show statistics")
        print("  python query_db.py all [limit]        - Show recent alerts (default 20)")
        print("  python query_db.py user <user_id>     - Show alerts for user")
        print("  python query_db.py type <alert_type>  - Show alerts by type")
        print("  python query_db.py high               - Show high priority alerts")
        print("\nAlert Types:")
        print("  • VELOCITY_FRAUD")
        print("  • DEVICE_SHARING_FRAUD")
        print("  • HIGH_VALUE_FRAUD")
        print("  • GEO_ANOMALY_FRAUD")
        sys.exit(0)
    
    command = sys.argv[1].lower()
    
    if command == "stats":
        get_statistics()
    elif command == "all":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        query_all_alerts(limit)
    elif command == "user" and len(sys.argv) > 2:
        query_by_user(sys.argv[2])
    elif command == "type" and len(sys.argv) > 2:
        query_by_type(sys.argv[2])
    elif command == "high":
        query_high_severity()
    else:
        print("Invalid command. Run without arguments to see usage.")
