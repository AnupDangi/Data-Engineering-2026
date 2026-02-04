"""Kafka Configuration Management

This module handles Kafka connection configuration and settings
loading from environment variables and config files.
"""

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class KafkaConfig:
    """Kafka connection and client configuration"""
    
    # Connection
    bootstrap_servers: str
    client_id: str
    
    # Producer Configuration
    producer_acks: str = "all"  # Wait for all replicas
    producer_retries: int = 3
    producer_max_in_flight: int = 5
    producer_compression_type: str = "snappy"
    producer_linger_ms: int = 10
    producer_batch_size: int = 16384
    producer_request_timeout_ms: int = 30000
    
    # Consumer Configuration (for future use)
    consumer_auto_offset_reset: str = "earliest"
    consumer_enable_auto_commit: bool = False
    consumer_max_poll_records: int = 500
    consumer_session_timeout_ms: int = 10000
    
    # Security (for future use)
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    
    def to_producer_config(self) -> Dict[str, Any]:
        """Convert to confluent-kafka producer configuration"""
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': self.client_id,
            'acks': self.producer_acks,
            'retries': self.producer_retries,
            'max.in.flight.requests.per.connection': self.producer_max_in_flight,
            'compression.type': self.producer_compression_type,
            'linger.ms': self.producer_linger_ms,
            'batch.size': self.producer_batch_size,
            'request.timeout.ms': self.producer_request_timeout_ms,
            'security.protocol': self.security_protocol,
        }
    
    def to_consumer_config(self, group_id: str) -> Dict[str, Any]:
        """Convert to confluent-kafka consumer configuration"""
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': self.client_id,
            'group.id': group_id,
            'auto.offset.reset': self.consumer_auto_offset_reset,
            'enable.auto.commit': self.consumer_enable_auto_commit,
            'max.poll.records': self.consumer_max_poll_records,
            'session.timeout.ms': self.consumer_session_timeout_ms,
            'security.protocol': self.security_protocol,
        }


def load_kafka_config() -> KafkaConfig:
    """Load Kafka configuration from environment variables
    
    Returns:
        KafkaConfig: Kafka configuration instance
    
    Raises:
        ValueError: If required environment variables are missing
    """
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    if not bootstrap_servers:
        raise ValueError(
            "KAFKA_BOOTSTRAP_SERVERS environment variable is required. "
            "Example: localhost:19092,localhost:19093,localhost:19094"
        )
    
    client_id = os.getenv('KAFKA_CLIENT_ID', 'flowguard-client')
    
    return KafkaConfig(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
        producer_acks=os.getenv('KAFKA_PRODUCER_ACKS', 'all'),
        producer_retries=int(os.getenv('KAFKA_PRODUCER_RETRIES', '3')),
        producer_compression_type=os.getenv('KAFKA_PRODUCER_COMPRESSION', 'snappy'),
    )


# Global configuration instance
_kafka_config: Optional[KafkaConfig] = None


def get_kafka_config() -> KafkaConfig:
    """Get or create global Kafka configuration instance
    
    Returns:
        KafkaConfig: Singleton Kafka configuration
    """
    global _kafka_config
    if _kafka_config is None:
        _kafka_config = load_kafka_config()
    return _kafka_config


def reset_kafka_config():
    """Reset global configuration (useful for testing)"""
    global _kafka_config
    _kafka_config = None
