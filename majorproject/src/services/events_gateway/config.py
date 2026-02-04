"""Events Gateway Service Configuration"""

import os
from pydantic_settings import BaseSettings
from typing import Optional


class EventsGatewaySettings(BaseSettings):
    """Events Gateway service configuration"""
    
    # Server Configuration
    host: str = "0.0.0.0"
    port: int = 8000
    reload: bool = True
    workers: int = 1
    log_level: str = "INFO"
    
    # API Configuration
    api_title: str = "FlowGuard Events Gateway API"
    api_description: str = "Real-time event ingestion gateway for Zomato analytics pipeline"
    api_version: str = "1.0.0"
    docs_url: str = "/docs"
    redoc_url: str = "/redoc"
    
    # Environment
    environment: str = "development"
    
    class Config:
        env_prefix = "EVENTS_GATEWAY_"
        case_sensitive = False


def get_settings() -> EventsGatewaySettings:
    """Get service settings"""
    return EventsGatewaySettings()
