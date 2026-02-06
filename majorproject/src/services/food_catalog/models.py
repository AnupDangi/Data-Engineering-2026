"""Data models for Food Catalog service."""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class FoodItem(BaseModel):
    """Food item model."""
    
    food_id: int = Field(..., description="Unique food identifier")
    name: str = Field(..., description="Food name")
    category: str = Field(..., description="Food category (e.g., Biryani, Pizza)")
    price: float = Field(..., description="Price in INR", ge=0)
    description: Optional[str] = Field(None, description="Detailed food description")
    image_url: str = Field(..., description="Image URL")
    is_available: bool = Field(default=True, description="Availability status")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    
    class Config:
        json_schema_extra = {
            "example": {
                "food_id": 1,
                "name": "Chicken Biryani",
                "category": "Biryani",
                "price": 249.0,
                "image_url": "https://images.pexels.com/...",
                "is_available": True,
                "created_at": "2026-02-06T10:30:00"
            }
        }


class FoodListResponse(BaseModel):
    """Response model for food list."""
    
    total: int = Field(..., description="Total number of food items")
    items: list[FoodItem] = Field(..., description="List of food items")
    
    class Config:
        json_schema_extra = {
            "example": {
                "total": 25,
                "items": [
                    {
                        "food_id": 1,
                        "name": "Chicken Biryani",
                        "category": "Biryani",
                        "price": 249.0,
                        "image_url": "https://images.pexels.com/...",
                        "is_available": True
                    }
                ]
            }
        }
