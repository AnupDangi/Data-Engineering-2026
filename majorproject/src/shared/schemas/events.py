"""Event Schema Definitions

Common event schemas used across the FlowGuard pipeline.
These schemas ensure data consistency between producers and consumers.
"""

from datetime import datetime
from typing import Optional, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field, validator


class EventType(str, Enum):
    """Types of events in the system"""
    ORDER = "order"
    CLICK = "click"
    IMPRESSION = "impression"
    VIEW = "view"


class OrderEventType(str, Enum):
    """Order event subtypes"""
    PLACED = "placed"
    CONFIRMED = "confirmed"
    CANCELLED = "cancelled"
    DELIVERED = "delivered"


class OrderEvent(BaseModel):
    """Schema for order events
    
    This event is triggered when a user places an order on the platform.
    """
    event_id: str = Field(..., description="Unique event identifier")
    event_type: str = Field(default=EventType.ORDER, description="Event type")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp")
    
    # User Information
    user_id: int = Field(..., description="User identifier", gt=0)
    
    # Order Information
    order_id: str = Field(..., description="Order identifier")
    order_type: OrderEventType = Field(default=OrderEventType.PLACED, description="Order event subtype")
    
    # Item Information
    item_id: int = Field(..., description="Food item identifier", gt=0)
    item_name: Optional[str] = Field(None, description="Food item name")
    restaurant_id: Optional[int] = Field(None, description="Restaurant identifier")
    quantity: int = Field(default=1, description="Item quantity", gt=0)
    price: Optional[float] = Field(None, description="Item price", ge=0)
    
    # Location Information
    city: Optional[str] = Field(None, description="City")
    location: Optional[str] = Field(None, description="Delivery location")
    
    # Additional Metadata
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional event metadata")
    
    class Config:
        json_schema_extra = {
            "example": {
                "event_id": "evt_123456789",
                "event_type": "order",
                "timestamp": "2026-02-04T10:30:00Z",
                "user_id": 1001,
                "order_id": "ORD-2026-001",
                "order_type": "placed",
                "item_id": 501,
                "item_name": "Biryani",
                "restaurant_id": 201,
                "quantity": 1,
                "price": 299.99,
                "city": "Mumbai",
                "location": "Andheri West",
                "metadata": {"source": "mobile_app"}
            }
        }
    
    @validator('timestamp', pre=True)
    def parse_timestamp(cls, v):
        """Parse timestamp from various formats"""
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace('Z', '+00:00'))
        return v


class ClickEvent(BaseModel):
    """Schema for click/impression events
    
    This event is triggered when a user clicks on an ad or sees an impression.
    """
    event_id: str = Field(..., description="Unique event identifier")
    event_type: str = Field(default=EventType.CLICK, description="Event type")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp")
    
    # User Information
    user_id: int = Field(..., description="User identifier", gt=0)
    session_id: Optional[str] = Field(None, description="User session identifier")
    
    # Ad Information
    ad_id: str = Field(..., description="Advertisement identifier")
    ad_campaign_id: Optional[str] = Field(None, description="Ad campaign identifier")
    ad_type: Optional[str] = Field(None, description="Type of ad (banner, native, video)")
    
    # Interaction Details
    is_click: bool = Field(default=True, description="True for click, False for impression")
    page_url: Optional[str] = Field(None, description="Page URL where event occurred")
    referrer: Optional[str] = Field(None, description="Referrer URL")
    
    # Device Information
    device_type: Optional[str] = Field(None, description="Device type (mobile, desktop, tablet)")
    user_agent: Optional[str] = Field(None, description="Browser user agent")
    
    # Location Information
    city: Optional[str] = Field(None, description="User city")
    ip_address: Optional[str] = Field(None, description="User IP address")
    
    # Additional Metadata
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional event metadata")
    
    class Config:
        json_schema_extra = {
            "example": {
                "event_id": "evt_987654321",
                "event_type": "click",
                "timestamp": "2026-02-04T10:30:00Z",
                "user_id": 1001,
                "session_id": "sess_abc123",
                "ad_id": "ad_456789",
                "ad_campaign_id": "camp_winter_2026",
                "ad_type": "banner",
                "is_click": True,
                "page_url": "https://zomato.com/restaurants",
                "device_type": "mobile",
                "city": "Mumbai",
                "metadata": {"placement": "homepage_top"}
            }
        }
    
    @validator('timestamp', pre=True)
    def parse_timestamp(cls, v):
        """Parse timestamp from various formats"""
        if isinstance(v, str):
            return datetime.fromisoformat(v.replace('Z', '+00:00'))
        return v


class BaseEventResponse(BaseModel):
    """Base response for event submissions"""
    success: bool
    message: str
    event_id: Optional[str] = None


class EventBatch(BaseModel):
    """Batch of events for bulk processing"""
    events: list[Dict[str, Any]]
    batch_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    @validator('events')
    def validate_events_not_empty(cls, v):
        """Ensure batch is not empty"""
        if not v:
            raise ValueError("Event batch cannot be empty")
        return v
