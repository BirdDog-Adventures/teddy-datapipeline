"""
DynamoDB Cache Manager for Teddy Data Pipeline

Provides caching functionality using DynamoDB tables for:
- Parcel data caching
- API rate limiting
- Metadata storage
"""

import boto3
import json
import time
import logging
from typing import Any, Dict, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class DynamoDBCacheManager:
    """
    DynamoDB-based cache manager for the Teddy data pipeline
    """
    
    def __init__(self, environment: str = 'dev'):
        self.dynamodb = boto3.resource('dynamodb')
        self.environment = environment
        
        # Initialize table references
        self.cache_table = self.dynamodb.Table(f'teddy-parcel-cache-{environment}')
        self.rate_limit_table = self.dynamodb.Table(f'teddy-rate-limit-{environment}')
        self.metadata_table = self.dynamodb.Table(f'teddy-api-metadata-{environment}')
    
    # Parcel Data Caching Methods
    def cache_parcel_data(self, parcel_id: str, data: Dict[str, Any], ttl_hours: int = 24) -> bool:
        """
        Cache parcel data with TTL
        
        Args:
            parcel_id: Unique parcel identifier
            data: Parcel data to cache
            ttl_hours: Time to live in hours
            
        Returns:
            bool: Success status
        """
        try:
            ttl = int(time.time()) + (ttl_hours * 3600)
            
            self.cache_table.put_item(
                Item={
                    'cache_key': f"parcel:{parcel_id}",
                    'data': json.dumps(data, default=str),
                    'ttl': ttl,
                    'created_at': int(time.time()),
                    'data_type': 'parcel'
                }
            )
            return True
            
        except Exception as e:
            logger.error(f"Error caching parcel data for {parcel_id}: {e}")
            return False
    
    def get_cached_parcel(self, parcel_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached parcel data
        
        Args:
            parcel_id: Unique parcel identifier
            
        Returns:
            Dict or None: Cached parcel data
        """
        try:
            response = self.cache_table.get_item(
                Key={'cache_key': f"parcel:{parcel_id}"}
            )
            
            if 'Item' in response:
                return json.loads(response['Item']['data'])
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving cached parcel {parcel_id}: {e}")
            return None
    
    def cache_county_metadata(self, county: str, metadata: Dict[str, Any], ttl_hours: int = 168) -> bool:
        """
        Cache county metadata (7 day default TTL)
        
        Args:
            county: County name
            metadata: County metadata
            ttl_hours: Time to live in hours
            
        Returns:
            bool: Success status
        """
        try:
            ttl = int(time.time()) + (ttl_hours * 3600)
            
            self.cache_table.put_item(
                Item={
                    'cache_key': f"county:{county}",
                    'data': json.dumps(metadata, default=str),
                    'ttl': ttl,
                    'created_at': int(time.time()),
                    'data_type': 'county_metadata'
                }
            )
            return True
            
        except Exception as e:
            logger.error(f"Error caching county metadata for {county}: {e}")
            return False
    
    def get_cached_county_metadata(self, county: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached county metadata
        
        Args:
            county: County name
            
        Returns:
            Dict or None: Cached county metadata
        """
        try:
            response = self.cache_table.get_item(
                Key={'cache_key': f"county:{county}"}
            )
            
            if 'Item' in response:
                return json.loads(response['Item']['data'])
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving cached county metadata {county}: {e}")
            return None
    
    # Rate Limiting Methods
    def check_rate_limit(self, api_key: str, requests_per_hour: int = 1000) -> bool:
        """
        Check if API key is within rate limits
        
        Args:
            api_key: API key identifier
            requests_per_hour: Maximum requests per hour
            
        Returns:
            bool: True if within limits, False if rate limited
        """
        try:
            current_hour = datetime.now().strftime('%Y-%m-%d-%H')
            
            # Try to get current hour's usage
            response = self.rate_limit_table.get_item(
                Key={
                    'api_key': api_key,
                    'time_window': current_hour
                }
            )
            
            if 'Item' in response:
                current_count = response['Item'].get('request_count', 0)
                if current_count >= requests_per_hour:
                    logger.warning(f"Rate limit exceeded for {api_key}: {current_count}/{requests_per_hour}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking rate limit for {api_key}: {e}")
            return True  # Allow on error to avoid blocking
    
    def increment_rate_limit(self, api_key: str) -> int:
        """
        Increment rate limit counter for API key
        
        Args:
            api_key: API key identifier
            
        Returns:
            int: Current request count
        """
        try:
            current_hour = datetime.now().strftime('%Y-%m-%d-%H')
            ttl = int(time.time()) + 7200  # 2 hours TTL
            
            response = self.rate_limit_table.update_item(
                Key={
                    'api_key': api_key,
                    'time_window': current_hour
                },
                UpdateExpression='ADD request_count :inc SET ttl = :ttl',
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':ttl': ttl
                },
                ReturnValues='UPDATED_NEW'
            )
            
            return response['Attributes'].get('request_count', 1)
            
        except Exception as e:
            logger.error(f"Error incrementing rate limit for {api_key}: {e}")
            return 1
    
    # API Metadata Methods
    def store_api_metadata(self, key: str, metadata: Dict[str, Any], ttl_hours: int = 24) -> bool:
        """
        Store API metadata (request logs, performance metrics, etc.)
        
        Args:
            key: Metadata key
            metadata: Metadata to store
            ttl_hours: Time to live in hours
            
        Returns:
            bool: Success status
        """
        try:
            ttl = int(time.time()) + (ttl_hours * 3600)
            
            self.metadata_table.put_item(
                Item={
                    'metadata_key': key,
                    'data': json.dumps(metadata, default=str),
                    'ttl': ttl,
                    'created_at': int(time.time())
                }
            )
            return True
            
        except Exception as e:
            logger.error(f"Error storing API metadata for {key}: {e}")
            return False
    
    def get_api_metadata(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve API metadata
        
        Args:
            key: Metadata key
            
        Returns:
            Dict or None: Stored metadata
        """
        try:
            response = self.metadata_table.get_item(
                Key={'metadata_key': key}
            )
            
            if 'Item' in response:
                return json.loads(response['Item']['data'])
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving API metadata {key}: {e}")
            return None
    
    # Batch Operations
    def cache_multiple_parcels(self, parcel_data: Dict[str, Dict[str, Any]], ttl_hours: int = 24) -> int:
        """
        Cache multiple parcels in batch
        
        Args:
            parcel_data: Dictionary of parcel_id -> parcel_data
            ttl_hours: Time to live in hours
            
        Returns:
            int: Number of successfully cached parcels
        """
        success_count = 0
        ttl = int(time.time()) + (ttl_hours * 3600)
        
        # Process in batches of 25 (DynamoDB batch limit)
        parcel_items = list(parcel_data.items())
        
        for i in range(0, len(parcel_items), 25):
            batch = parcel_items[i:i + 25]
            
            try:
                with self.cache_table.batch_writer() as batch_writer:
                    for parcel_id, data in batch:
                        batch_writer.put_item(
                            Item={
                                'cache_key': f"parcel:{parcel_id}",
                                'data': json.dumps(data, default=str),
                                'ttl': ttl,
                                'created_at': int(time.time()),
                                'data_type': 'parcel'
                            }
                        )
                        success_count += 1
                        
            except Exception as e:
                logger.error(f"Error in batch caching parcels: {e}")
                continue
        
        logger.info(f"Successfully cached {success_count}/{len(parcel_data)} parcels")
        return success_count
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics
        
        Returns:
            Dict: Cache statistics
        """
        try:
            # Get approximate item counts (this is an estimate)
            cache_response = self.cache_table.describe_table()
            rate_limit_response = self.rate_limit_table.describe_table()
            metadata_response = self.metadata_table.describe_table()
            
            return {
                'cache_table_items': cache_response['Table'].get('ItemCount', 0),
                'rate_limit_table_items': rate_limit_response['Table'].get('ItemCount', 0),
                'metadata_table_items': metadata_response['Table'].get('ItemCount', 0),
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {}
    
    def clear_expired_items(self) -> Dict[str, int]:
        """
        Manually clear expired items (DynamoDB TTL should handle this automatically)
        This is mainly for monitoring/debugging purposes
        
        Returns:
            Dict: Count of items processed per table
        """
        current_time = int(time.time())
        results = {'cache_cleared': 0, 'rate_limit_cleared': 0, 'metadata_cleared': 0}
        
        # Note: In production, DynamoDB TTL handles this automatically
        # This method is mainly for debugging/monitoring
        
        try:
            # Scan cache table for expired items
            response = self.cache_table.scan(
                FilterExpression='#ttl < :current_time',
                ExpressionAttributeNames={'#ttl': 'ttl'},
                ExpressionAttributeValues={':current_time': current_time}
            )
            
            results['cache_expired_found'] = len(response.get('Items', []))
            
        except Exception as e:
            logger.error(f"Error checking expired items: {e}")
        
        return results
