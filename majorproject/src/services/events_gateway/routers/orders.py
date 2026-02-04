"""Orders Router - Handle order event submissions"""

import logging
import uuid
from datetime import datetime
from fastapi import APIRouter, HTTPException, status
from typing import Dict, Any

from src.shared.schemas.events import OrderEvent, BaseEventResponse
from src.shared.kafka.topics import TopicName
from src.services.events_gateway.producers.kafka_producer import (
    get_producer,
    KafkaProducerError
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/orders",
    tags=["orders"],
    responses={
        500: {"description": "Internal server error"},
        503: {"description": "Service unavailable - Kafka connection issue"}
    }
)


@router.post(
    "/",
    response_model=BaseEventResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit Order Event",
    description="Submit a user order event to be processed by the pipeline"
)
async def create_order_event(order: OrderEvent) -> BaseEventResponse:
    """
    Submit an order event to the events gateway.
    
    The event will be validated, enriched with metadata, and published to Kafka
    for downstream processing.
    
    **Event Flow:**
    1. Validate event schema
    2. Generate event_id if not provided
    3. Publish to raw.orders.v1 Kafka topic
    4. Return acknowledgment to client
    """
    try:
        # Generate event_id if not provided
        if not order.event_id or order.event_id == "string":
            order.event_id = f"ord_{uuid.uuid4().hex[:12]}"
        
        # Ensure timestamp is set
        if not order.timestamp:
            order.timestamp = datetime.utcnow()
        
        # Convert to dict for Kafka
        event_dict = order.model_dump(mode='json')
        
        # Get Kafka producer
        producer = get_producer()
        
        # Use user_id as partition key for ordered processing per user
        partition_key = str(order.user_id)
        
        # Add headers
        headers = {
            "event_type": "order",
            "source": "events_gateway",
            "user_id": str(order.user_id)
        }
        
        # Send to Kafka
        producer.send_event(
            topic=TopicName.RAW_ORDERS,
            event=event_dict,
            key=partition_key,
            headers=headers
        )
        
        logger.info(
            f"Order event accepted: event_id={order.event_id}, "
            f"user_id={order.user_id}, order_id={order.order_id}"
        )
        
        return BaseEventResponse(
            success=True,
            message="Order event accepted for processing",
            event_id=order.event_id
        )
        
    except KafkaProducerError as e:
        logger.error(f"Kafka producer error: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Unable to publish event to Kafka: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error processing order: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal error processing order event: {str(e)}"
        )


@router.get(
    "/health",
    summary="Orders Router Health Check",
    description="Check if orders router is operational"
)
async def health_check() -> Dict[str, Any]:
    """Health check endpoint for orders router"""
    try:
        producer = get_producer()
        stats = producer.get_stats()
        
        return {
            "status": "healthy",
            "service": "orders_router",
            "kafka_connected": True,
            "statistics": stats
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "service": "orders_router",
            "kafka_connected": False,
            "error": str(e)
        }
