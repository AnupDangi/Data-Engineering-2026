"""Clicks Router - Handle click and impression event submissions"""

import logging
import uuid
from datetime import datetime
from fastapi import APIRouter, HTTPException, status
from typing import Dict, Any

from src.shared.schemas.events import ClickEvent, BaseEventResponse
from src.shared.kafka.topics import TopicName
from src.services.events_gateway.producers.kafka_producer import (
    get_producer,
    KafkaProducerError
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/clicks",
    tags=["clicks"],
    responses={
        500: {"description": "Internal server error"},
        503: {"description": "Service unavailable - Kafka connection issue"}
    }
)


@router.post(
    "/",
    response_model=BaseEventResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit Click/Impression Event",
    description="Submit a user click or impression event for ad attribution"
)
async def create_click_event(click: ClickEvent) -> BaseEventResponse:
    """
    Submit a click or impression event to the events gateway.
    
    The event will be validated, enriched with metadata, and published to Kafka
    for real-time attribution processing via Flink.
    
    **Event Flow:**
    1. Validate event schema
    2. Generate event_id if not provided
    3. Publish to raw.clicks.v1 Kafka topic
    4. Return acknowledgment to client
    
    **Use Cases:**
    - User clicks on an ad: is_click=True
    - Ad impression: is_click=False
    """
    try:
        # Generate event_id if not provided
        if not click.event_id or click.event_id == "string":
            click.event_id = f"clk_{uuid.uuid4().hex[:12]}"
        
        # Ensure timestamp is set
        if not click.timestamp:
            click.timestamp = datetime.utcnow()
        
        # Convert to dict for Kafka
        event_dict = click.model_dump(mode='json')
        
        # Get Kafka producer
        producer = get_producer()
        
        # Use user_id as partition key for ordered processing per user
        partition_key = str(click.user_id)
        
        # Add headers
        headers = {
            "event_type": "click" if click.is_click else "impression",
            "source": "events_gateway",
            "user_id": str(click.user_id),
            "ad_id": click.ad_id
        }
        
        # Send to Kafka
        producer.send_event(
            topic=TopicName.RAW_CLICKS,
            event=event_dict,
            key=partition_key,
            headers=headers
        )
        
        event_type_str = "click" if click.is_click else "impression"
        logger.info(
            f"{event_type_str.capitalize()} event accepted: event_id={click.event_id}, "
            f"user_id={click.user_id}, ad_id={click.ad_id}"
        )
        
        return BaseEventResponse(
            success=True,
            message=f"{event_type_str.capitalize()} event accepted for processing",
            event_id=click.event_id
        )
        
    except KafkaProducerError as e:
        logger.error(f"Kafka producer error: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Unable to publish event to Kafka: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error processing click: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal error processing click event: {str(e)}"
        )


@router.post(
    "/impression",
    response_model=BaseEventResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit Impression Event",
    description="Convenience endpoint specifically for ad impressions"
)
async def create_impression_event(click: ClickEvent) -> BaseEventResponse:
    """
    Submit an ad impression event (click event with is_click=False).
    
    This is a convenience endpoint that forces is_click=False.
    """
    # Force impression mode
    click.is_click = False
    click.event_type = "impression"
    
    return await create_click_event(click)


@router.get(
    "/health",
    summary="Clicks Router Health Check",
    description="Check if clicks router is operational"
)
async def health_check() -> Dict[str, Any]:
    """Health check endpoint for clicks router"""
    try:
        producer = get_producer()
        stats = producer.get_stats()
        
        return {
            "status": "healthy",
            "service": "clicks_router",
            "kafka_connected": True,
            "statistics": stats
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "service": "clicks_router",
            "kafka_connected": False,
            "error": str(e)
        }
