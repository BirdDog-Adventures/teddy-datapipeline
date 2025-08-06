"""
HubSpot to Snowflake ETL Integration

This module provides a comprehensive ETL pipeline for extracting data from HubSpot CRM
and loading it into Snowflake for analytics and reporting.

Components:
- HubSpot API Client with OAuth/API Key authentication
- Data transformers for flattening nested JSON structures
- Snowflake connectors for efficient data loading
- Orchestration utilities for scheduling and monitoring
"""

from .hubspot_client import HubSpotClient
from .data_transformer import HubSpotDataTransformer
from .snowflake_loader import SnowflakeLoader
from .etl_pipeline import HubSpotETLPipeline

__version__ = "1.0.0"
__all__ = [
    "HubSpotClient",
    "HubSpotDataTransformer", 
    "SnowflakeLoader",
    "HubSpotETLPipeline"
]
