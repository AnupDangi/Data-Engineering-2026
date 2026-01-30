"""
Real-Time Fraud Detection System for Zomato Orders
Using Apache Flink CEP (Complex Event Processing) and Kafka

Fraud Detection Patterns:
1. Velocity Detection: Multiple orders from same user in short time window
2. Device Sharing Detection: Same device used by multiple users
3. High-Value Transaction Detection: Sudden spike in order amount
4. Geographic Anomaly: Orders from different cities in quick succession
"""

import json
import time
import logging
import warnings
import threading
from datetime import datetime, timedelta
from collections import defaultdict

# Suppress harmless PyFlink/Apache Beam warnings
warnings.filterwarnings("ignore", category=UserWarning, module="apache_beam")
warnings.filterwarnings("ignore", message=".*pkg_resources.*")
warnings.filterwarnings("ignore", message=".*google.cloud.*")

# Flink imports
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema, DeliveryGuarantee
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor, ListStateDescriptor

# Import database handler
import db_handler


# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

# Configure logging to write to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('app.log', mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Direct file writing for Flink worker processes
import os
LOG_FILE = os.path.join(os.path.dirname(__file__), 'app.log')

def write_to_log(message, level='INFO'):
    """Write directly to log file (works in Flink worker processes)"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(LOG_FILE, 'a') as f:
        f.write(f"{timestamp} [{level}] {message}\n")
        f.flush()


# ============================================================================
# CONFIGURATION
# ============================================================================

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_SOURCE_TOPIC = "zomato_transactions_raw"
KAFKA_ALERTS_TOPIC = "zomato_fraud_alerts"

## Though this will be set in configuration of Flink environment
# Fraud detection thresholds
VELOCITY_THRESHOLD = 3  # max orders per user in time window
VELOCITY_WINDOW_MINUTES = 5  # time window for velocity check
HIGH_VALUE_THRESHOLD = 1500.0  # amount threshold for high-value fraud (but this will be different for each user will get from history of users order from db call)
DEVICE_SHARING_THRESHOLD = 3  # max users per device in time window
GEO_ANOMALY_WINDOW_MINUTES = 10  # time window for geo checks


# ============================================================================
# STEP 1: KAFKA SOURCE - Read streaming logs
# ============================================================================

def create_kafka_source(env):
    """
    Creates Kafka source to consume Zomato transaction events
    """
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_topics(KAFKA_SOURCE_TOPIC) \
        .set_group_id("flink-fraud-detector") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    logger.info(f"📡 Kafka Source configured:")
    logger.info(f"   • Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"   • Topic: {KAFKA_SOURCE_TOPIC}")
    logger.info(f"   • Starting from: EARLIEST (all messages)")
    
    return env.from_source(
        kafka_source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "Kafka Source"
    )


def create_kafka_sink():
    """
    Creates Kafka sink to publish fraud alerts
    """
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(KAFKA_ALERTS_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()
    
    logger.info(f"📤 Kafka Sink configured:")
    logger.info(f"   • Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"   • Topic: {KAFKA_ALERTS_TOPIC}")
    logger.info(f"   • Delivery guarantee: AT_LEAST_ONCE")
    
    return kafka_sink


# ============================================================================
# STEP 2: PARSE AND EXTRACT TRANSACTION DATA
# ============================================================================

def parse_transaction(event_json):
    """
    Parse JSON event from Kafka
    
    Example input:
    {
        'event_id': 'c4ca3710-2fd0-4133-af64-712740c69c0d',
        'user_id': 'u_49',
        'device_id': 'd_1',
        'device_type': 'web',
        'order_id': '26d87614-b4d1-4bce-88aa-aa0f702499ec',
        'amount': 413.04,
        'restaurant_id': 'r_13',
        'city': 'Bangalore',
        'payment_method': 'UPI',
        'is_cod': False,
        'event_time': 1769771797176,
        'behavior': 'normal'
    }
    """
    try:
        event = json.loads(event_json)
        return event
    except Exception as e:
        logger.error(f"Error parsing event: {e}")
        return None


# ============================================================================
# STEP 3: FRAUD DETECTION PATTERNS
# ============================================================================

class VelocityDetector(KeyedProcessFunction):
    """
    Detects velocity-based fraud: too many orders from same user in short time
    
    Pattern: User places > VELOCITY_THRESHOLD orders within VELOCITY_WINDOW_MINUTES
    """
    
    def __init__(self):
        self.order_count_state = None
        self.last_cleanup_state = None
    
    def open(self, runtime_context: RuntimeContext):
        # State to track order timestamps per user
        self.order_count_state = runtime_context.get_list_state(
            ListStateDescriptor("order_timestamps", Types.LONG())
        )
        self.last_cleanup_state = runtime_context.get_state(
            ValueStateDescriptor("last_cleanup", Types.LONG())
        )
    
    def process_element(self, event, ctx):
        current_time = event['event_time']
        user_id = event['user_id']
        
        # Get current order timestamps
        order_times = list(self.order_count_state.get())
        
        # Remove old orders outside the time window
        window_start = current_time - (VELOCITY_WINDOW_MINUTES * 60 * 1000)
        recent_orders = [t for t in order_times if t >= window_start]
        
        # Add current order
        recent_orders.append(current_time)
        
        # Update state
        self.order_count_state.clear()
        for t in recent_orders:
            self.order_count_state.add(t)
        
        # Check if velocity threshold exceeded
        if len(recent_orders) > VELOCITY_THRESHOLD:
            alert = {
                'alert_type': 'VELOCITY_FRAUD',
                'user_id': user_id,
                'order_id': event['order_id'],
                'event_id': event['event_id'],
                'order_count': len(recent_orders),
                'time_window_minutes': VELOCITY_WINDOW_MINUTES,
                'threshold': VELOCITY_THRESHOLD,
                'amount': event['amount'],
                'detected_at': current_time,
                'severity': 'HIGH',
                'message': f"User {user_id} placed {len(recent_orders)} orders in {VELOCITY_WINDOW_MINUTES} minutes"
            }
            yield alert


class DeviceSharingDetector(KeyedProcessFunction):
    """
    Detects device sharing fraud: same device used by multiple users
    
    Pattern: Single device_id associated with > DEVICE_SHARING_THRESHOLD different users
    """
    
    def __init__(self):
        self.user_set_state = None
        self.timestamps_state = None
    
    def open(self, runtime_context: RuntimeContext):
        # Track unique users per device
        self.user_set_state = runtime_context.get_map_state(
            MapStateDescriptor("users_per_device", Types.STRING(), Types.LONG())
        )
    
    def process_element(self, event, ctx):
        device_id = event['device_id']
        user_id = event['user_id']
        current_time = event['event_time']
        
        # Get users who have used this device
        users = dict(self.user_set_state.items())
        
        # Remove users from old time window
        window_start = current_time - (VELOCITY_WINDOW_MINUTES * 60 * 1000)
        recent_users = {uid: ts for uid, ts in users.items() if ts >= window_start}
        
        # Add/update current user
        recent_users[user_id] = current_time
        
        # Update state
        self.user_set_state.clear()
        for uid, ts in recent_users.items():
            self.user_set_state.put(uid, ts)
        
        # Check if device sharing threshold exceeded
        if len(recent_users) > DEVICE_SHARING_THRESHOLD:
            alert = {
                'alert_type': 'DEVICE_SHARING_FRAUD',
                'device_id': device_id,
                'user_id': user_id,
                'order_id': event['order_id'],
                'event_id': event['event_id'],
                'unique_users': len(recent_users),
                'user_ids': list(recent_users.keys()),
                'threshold': DEVICE_SHARING_THRESHOLD,
                'amount': event['amount'],
                'detected_at': current_time,
                'severity': 'CRITICAL',
                'message': f"Device {device_id} used by {len(recent_users)} different users"
            }
            yield alert


class HighValueDetector(KeyedProcessFunction):
    """
    Detects high-value fraud: sudden spike in transaction amount
    
    Pattern: Transaction amount > HIGH_VALUE_THRESHOLD
    """
    
    def process_element(self, event, ctx):
        amount = event['amount']
        user_id = event['user_id']
        
        if amount > HIGH_VALUE_THRESHOLD:
            alert = {
                'alert_type': 'HIGH_VALUE_FRAUD',
                'user_id': user_id,
                'order_id': event['order_id'],
                'event_id': event['event_id'],
                'amount': amount,
                'threshold': HIGH_VALUE_THRESHOLD,
                'payment_method': event['payment_method'],
                'detected_at': event['event_time'],
                'severity': 'MEDIUM',
                'message': f"High-value transaction of ₹{amount} from user {user_id}"
            }
            yield alert


class GeoAnomalyDetector(KeyedProcessFunction):
    """
    Detects geographic anomaly: orders from different cities in quick succession
    
    Pattern: User orders from different cities within GEO_ANOMALY_WINDOW_MINUTES
    """
    
    def __init__(self):
        self.city_history_state = None
    
    def open(self, runtime_context: RuntimeContext):
        # Track recent cities per user with timestamps
        self.city_history_state = runtime_context.get_map_state(
            MapStateDescriptor("city_history", Types.STRING(), Types.LONG())
        )
    
    def process_element(self, event, ctx):
        user_id = event['user_id']
        current_city = event['city']
        current_time = event['event_time']
        
        # Get recent city history
        city_history = dict(self.city_history_state.items())
        
        # Remove old entries outside time window
        window_start = current_time - (GEO_ANOMALY_WINDOW_MINUTES * 60 * 1000)
        recent_cities = {city: ts for city, ts in city_history.items() if ts >= window_start}
        
        # Check if user ordered from different city recently
        if recent_cities and current_city not in recent_cities:
            previous_cities = list(recent_cities.keys())
            alert = {
                'alert_type': 'GEO_ANOMALY_FRAUD',
                'user_id': user_id,
                'order_id': event['order_id'],
                'event_id': event['event_id'],
                'current_city': current_city,
                'previous_cities': previous_cities,
                'time_window_minutes': GEO_ANOMALY_WINDOW_MINUTES,
                'amount': event['amount'],
                'detected_at': current_time,
                'severity': 'HIGH',
                'message': f"User {user_id} ordered from {current_city} after ordering from {previous_cities} recently"
            }
            yield alert
        
        # Update state
        recent_cities[current_city] = current_time
        self.city_history_state.clear()
        for city, ts in recent_cities.items():
            self.city_history_state.put(city, ts)


# ============================================================================
# STEP 4: ALERT DEDUPLICATION
# ============================================================================

class AlertDeduplicator(KeyedProcessFunction):
    """
    Prevents duplicate alerts for the same fraud pattern
    Sends alert only if user hasn't been alerted recently
    """
    
    def __init__(self, cooldown_minutes=5):
        self.last_alert_state = None
        self.cooldown_ms = cooldown_minutes * 60 * 1000
    
    def open(self, runtime_context: RuntimeContext):
        self.last_alert_state = runtime_context.get_state(
            ValueStateDescriptor("last_alert_time", Types.LONG())
        )
    
    def process_element(self, alert, ctx):
        current_time = alert['detected_at']
        last_alert_time = self.last_alert_state.value()
        
        # Send alert only if cooldown period has passed
        if last_alert_time is None or (current_time - last_alert_time) > self.cooldown_ms:
            self.last_alert_state.update(current_time)
            yield alert


# ============================================================================
# STEP 5: CONSOLE OUTPUT FOR MONITORING
# ============================================================================

def print_event(event):
    """Print incoming transaction for monitoring"""
    timestamp = datetime.fromtimestamp(event['event_time'] / 1000).strftime('%H:%M:%S')
    msg = f"[{timestamp}] 📦 Transaction | User: {event['user_id']} | Amount: ₹{event['amount']} | Device: {event['device_id']} | City: {event['city']} | Behavior: {event['behavior']}"
    
    # Write to log file directly (works in Flink workers)
    write_to_log(msg, 'INFO')
    
    # Print to console
    print(msg)
    
    return event


def print_alert(alert):
    """Print fraud alert for monitoring and save to database"""
    timestamp = datetime.fromtimestamp(alert['detected_at'] / 1000).strftime('%H:%M:%S')
    severity_emoji = {'CRITICAL': '🚨', 'HIGH': '⚠️', 'MEDIUM': '⚡'}
    emoji = severity_emoji.get(alert['severity'], '❗')
    
    msg1 = f"\n{emoji} [{timestamp}] FRAUD ALERT - {alert['alert_type']} {emoji}"
    msg2 = f"   Severity: {alert['severity']}"
    msg3 = f"   Message: {alert['message']}"
    msg4 = f"   Details: {json.dumps({k: v for k, v in alert.items() if k not in ['message', 'alert_type', 'severity']}, indent=4)}\n"
    
    # Write to log file directly (works in Flink workers)
    write_to_log(msg1, 'WARNING')
    write_to_log(msg2, 'WARNING')
    write_to_log(msg3, 'WARNING')
    write_to_log(msg4, 'WARNING')
    
    # Print to console
    print(msg1)
    print(msg2)
    print(msg3)
    print(msg4)
    
    # Save to database (non-blocking)
    db_handler.save_alert_to_db(alert)
    
    return alert


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def main():
    # Initialize database
    db_handler.init_database()
    
    # Start database writer thread
    db_thread = threading.Thread(target=db_handler.db_writer_thread, daemon=True)
    db_thread.start()
    logger.info("🧵 Database writer thread started\n")
    
    # Initialize Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    
    logger.info("=" * 80)
    logger.info("🚀 Starting Real-Time Fraud Detection System for Zomato Orders")
    logger.info("=" * 80)
    logger.info(f"📊 Configuration:")
    logger.info(f"   • Velocity Threshold: {VELOCITY_THRESHOLD} orders in {VELOCITY_WINDOW_MINUTES} minutes")
    logger.info(f"   • Device Sharing Threshold: {DEVICE_SHARING_THRESHOLD} users per device")
    logger.info(f"   • High Value Threshold: ₹{HIGH_VALUE_THRESHOLD}")
    logger.info(f"   • Geo Anomaly Window: {GEO_ANOMALY_WINDOW_MINUTES} minutes")
    logger.info("=" * 80 + "\n")
    
    # Step 1: Read from Kafka
    kafka_stream = create_kafka_source(env)
    
    # Step 2: Parse JSON events
    transactions = kafka_stream.map(
        parse_transaction,
        output_type=Types.PICKLED_BYTE_ARRAY()
    ).filter(lambda x: x is not None)
    
    # Monitor incoming transactions
    transactions = transactions.map(print_event)
    
    # Step 3: Apply fraud detection patterns
    
    # Pattern 1: Velocity Detection
    velocity_alerts = transactions.key_by(lambda x: x['user_id']) \
        .process(VelocityDetector())
    
    # Pattern 2: Device Sharing Detection
    device_alerts = transactions.key_by(lambda x: x['device_id']) \
        .process(DeviceSharingDetector())
    
    # Pattern 3: High Value Detection
    high_value_alerts = transactions.key_by(lambda x: x['user_id']) \
        .process(HighValueDetector())
    
    # Pattern 4: Geographic Anomaly Detection
    geo_alerts = transactions.key_by(lambda x: x['user_id']) \
        .process(GeoAnomalyDetector())
    
    # Step 4: Combine all alerts
    all_alerts = velocity_alerts.union(device_alerts).union(high_value_alerts).union(geo_alerts)
    
    # Step 5: Deduplicate alerts
    deduplicated_alerts = all_alerts.key_by(lambda x: f"{x['alert_type']}_{x['user_id']}") \
        .process(AlertDeduplicator(cooldown_minutes=5))
    
    # Step 6: Print alerts to console and save to database
    deduplicated_alerts.map(print_alert)
    
    # Step 7: Sink to Kafka alerts topic (for downstream processing)
    kafka_sink = create_kafka_sink()
    deduplicated_alerts.map(
        lambda alert: json.dumps(alert),
        output_type=Types.STRING()
    ).sink_to(kafka_sink)
    
    # Execute the pipeline
    logger.info("🔄 Pipeline is running... Waiting for transactions...\n")
    try:
        env.execute("Zomato Fraud Detection Pipeline")
    except KeyboardInterrupt:
        logger.info("\n🛑 Shutting down gracefully...")
        db_handler.shutdown()
    except Exception as e:
        logger.error(f"❌ Pipeline error: {e}")
        db_handler.shutdown()
        raise


if __name__ == "__main__":
    main()