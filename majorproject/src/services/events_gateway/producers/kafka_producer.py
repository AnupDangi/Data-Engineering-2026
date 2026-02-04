"""Kafka Producer Wrapper

Production-ready Kafka producer with error handling, delivery confirmations,
and graceful shutdown support.
"""

import json
import logging
from typing import Optional, Dict, Any, Callable
from datetime import datetime
from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

from src.shared.kafka.config import get_kafka_config
from src.shared.kafka.topics import TopicName

logger = logging.getLogger(__name__)


class KafkaProducerError(Exception):
    """Custom exception for Kafka producer errors"""
    pass


class FlowGuardProducer:
    """Kafka producer for FlowGuard events with robust error handling"""
    
    def __init__(self, config_override: Optional[Dict[str, Any]] = None):
        """Initialize Kafka producer
        
        Args:
            config_override: Optional configuration overrides
        """
        self._kafka_config = get_kafka_config()
        self._producer_config = self._kafka_config.to_producer_config()
        
        # Apply overrides if provided
        if config_override:
            self._producer_config.update(config_override)
        
        # Statistics
        self._messages_sent = 0
        self._messages_delivered = 0
        self._messages_failed = 0
        
        try:
            self._producer = Producer(self._producer_config)
            logger.info(
                f"Kafka producer initialized successfully. "
                f"Bootstrap servers: {self._kafka_config.bootstrap_servers}"
            )
        except KafkaException as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise KafkaProducerError(f"Producer initialization failed: {e}")
    
    def _delivery_callback(self, err: Optional[KafkaError], msg) -> None:
        """Callback for message delivery confirmation
        
        Args:
            err: Error if delivery failed
            msg: Message metadata
        """
        if err:
            self._messages_failed += 1
            logger.error(
                f"Message delivery failed: {err}. "
                f"Topic: {msg.topic()}, Partition: {msg.partition()}"
            )
        else:
            self._messages_delivered += 1
            logger.debug(
                f"Message delivered: Topic={msg.topic()}, "
                f"Partition={msg.partition()}, Offset={msg.offset()}"
            )
    
    def send_event(
        self,
        topic: TopicName,
        event: Dict[str, Any],
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        callback: Optional[Callable] = None
    ) -> bool:
        """Send an event to Kafka topic
        
        Args:
            topic: Topic name from TopicName enum
            event: Event data dictionary
            key: Optional partition key (typically user_id)
            headers: Optional message headers
            callback: Optional custom delivery callback
            
        Returns:
            bool: True if message queued successfully
            
        Raises:
            KafkaProducerError: If message cannot be queued
        """
        try:
            # Serialize event to JSON
            event_json = json.dumps(event, default=str)
            
            # Prepare headers
            kafka_headers = []
            if headers:
                kafka_headers = [(k, v.encode('utf-8')) for k, v in headers.items()]
            
            # Add timestamp header
            kafka_headers.append(('timestamp', datetime.utcnow().isoformat().encode('utf-8')))
            
            # Encode key if provided
            key_bytes = key.encode('utf-8') if key else None
            
            # Produce message
            self._producer.produce(
                topic=topic.value,
                value=event_json.encode('utf-8'),
                key=key_bytes,
                headers=kafka_headers,
                callback=callback or self._delivery_callback
            )
            
            self._messages_sent += 1
            
            # Trigger delivery reports
            self._producer.poll(0)
            
            logger.debug(
                f"Event queued for delivery: Topic={topic.value}, Key={key}"
            )
            return True
            
        except BufferError as e:
            logger.error(f"Producer queue is full: {e}")
            raise KafkaProducerError(f"Producer queue full: {e}")
        except KafkaException as e:
            logger.error(f"Failed to send event: {e}")
            raise KafkaProducerError(f"Failed to send event: {e}")
        except Exception as e:
            logger.error(f"Unexpected error sending event: {e}")
            raise KafkaProducerError(f"Unexpected error: {e}")
    
    def flush(self, timeout: float = 10.0) -> int:
        """Flush all pending messages
        
        Args:
            timeout: Maximum time to wait in seconds
            
        Returns:
            int: Number of messages still in queue after timeout
        """
        logger.info(f"Flushing producer queue (timeout={timeout}s)...")
        remaining = self._producer.flush(timeout)
        
        if remaining > 0:
            logger.warning(f"{remaining} messages still in queue after flush timeout")
        else:
            logger.info("All messages flushed successfully")
        
        return remaining
    
    def close(self):
        """Close producer and flush remaining messages"""
        logger.info("Closing Kafka producer...")
        try:
            remaining = self.flush(timeout=30.0)
            if remaining > 0:
                logger.error(f"Failed to deliver {remaining} messages before shutdown")
        finally:
            logger.info(
                f"Producer statistics: Sent={self._messages_sent}, "
                f"Delivered={self._messages_delivered}, Failed={self._messages_failed}"
            )
    
    def get_stats(self) -> Dict[str, int]:
        """Get producer statistics
        
        Returns:
            Dictionary with message statistics
        """
        return {
            'messages_sent': self._messages_sent,
            'messages_delivered': self._messages_delivered,
            'messages_failed': self._messages_failed
        }
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# Singleton producer instance
_producer_instance: Optional[FlowGuardProducer] = None


def get_producer() -> FlowGuardProducer:
    """Get or create global producer instance
    
    Returns:
        FlowGuardProducer: Singleton producer instance
    """
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = FlowGuardProducer()
    return _producer_instance


def close_producer():
    """Close global producer instance"""
    global _producer_instance
    if _producer_instance is not None:
        _producer_instance.close()
        _producer_instance = None


def check_kafka_connection() -> bool:
    """Check if Kafka cluster is reachable
    
    Returns:
        bool: True if connection successful
    """
    try:
        kafka_config = get_kafka_config()
        admin_client = AdminClient({
            'bootstrap.servers': kafka_config.bootstrap_servers
        })
        
        # Try to get cluster metadata with timeout
        metadata = admin_client.list_topics(timeout=5)
        
        logger.info(f"Successfully connected to Kafka cluster. Brokers: {len(metadata.brokers)}")
        return True
        
    except KafkaException as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking Kafka connection: {e}")
        return False
