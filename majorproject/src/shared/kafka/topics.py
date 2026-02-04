"""Kafka Topic Constants and Definitions

Centralized topic naming to avoid hardcoding strings across the codebase.
All topic names should be defined here for consistency.
"""

from enum import Enum
from dataclasses import dataclass
from typing import Dict, List


class TopicName(str, Enum):
    """Kafka topic name constants"""
    
    # Raw Events Topics (Phase 1)
    RAW_ORDERS = "raw.orders.v1"
    RAW_CLICKS = "raw.clicks.v1"
    
    # Attributed Events Topics (Phase 2 - Future)
    ATTRIBUTED_EVENTS = "attributed.events.v1"
    
    # Incremental Updates Topics (Phase 2 - Future)
    INCREMENTAL_BILLING = "incremental.billing.v1"
    RECON_OVERWRITE = "recon.overwrite.v1"


@dataclass
class TopicConfig:
    """Topic configuration details"""
    name: str
    partitions: int
    replication_factor: int
    retention_ms: int
    description: str
    
    @property
    def retention_days(self) -> float:
        """Convert retention_ms to days"""
        return self.retention_ms / (1000 * 60 * 60 * 24)


# Topic configurations matching config/kafka/topics.yaml
TOPIC_CONFIGS: Dict[str, TopicConfig] = {
    TopicName.RAW_ORDERS: TopicConfig(
        name=TopicName.RAW_ORDERS,
        partitions=3,
        replication_factor=2,
        retention_ms=604800000,  # 7 days
        description="Raw order events from user actions"
    ),
    TopicName.RAW_CLICKS: TopicConfig(
        name=TopicName.RAW_CLICKS,
        partitions=6,
        replication_factor=2,
        retention_ms=172800000,  # 2 days
        description="Raw click/impression events for ad tracking"
    ),
}


def get_topic_config(topic_name: TopicName) -> TopicConfig:
    """Get configuration for a specific topic
    
    Args:
        topic_name: Topic name from TopicName enum
        
    Returns:
        TopicConfig: Topic configuration
        
    Raises:
        KeyError: If topic configuration not found
    """
    return TOPIC_CONFIGS[topic_name]


def get_all_topics() -> List[str]:
    """Get list of all topic names
    
    Returns:
        List of topic name strings
    """
    return [topic.value for topic in TopicName]


def get_phase1_topics() -> List[str]:
    """Get list of Phase 1 topic names
    
    Returns:
        List of Phase 1 topic names
    """
    return [
        TopicName.RAW_ORDERS.value,
        TopicName.RAW_CLICKS.value,
    ]
