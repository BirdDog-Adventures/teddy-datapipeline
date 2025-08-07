#!/usr/bin/env python3
"""
Clear DynamoDB Cache Script for Teddy Data Pipeline

This script provides options to clear different types of cached data:
- Parcel data cache
- Rate limiting data  
- API metadata
- All cache data

Usage:
    python clear_dynamodb_cache.py [options]

Examples:
    python clear_dynamodb_cache.py --all                    # Clear all cache
    python clear_dynamodb_cache.py --parcel                 # Clear only parcel cache
    python clear_dynamodb_cache.py --rate-limit             # Clear rate limit data
    python clear_dynamodb_cache.py --metadata               # Clear API metadata
    python clear_dynamodb_cache.py --env prod               # Clear prod environment cache
    python clear_dynamodb_cache.py --stats                  # Show cache statistics only
    python clear_dynamodb_cache.py --expired                # Clear only expired items
"""

import boto3
import argparse
import json
import time
import sys
from typing import Dict, Any, List
from datetime import datetime
from boto3.dynamodb.conditions import Attr

class DynamoDBCacheCleaner:
    """DynamoDB cache clearing utility"""
    
    def __init__(self, environment: str = 'dev'):
        self.dynamodb = boto3.resource('dynamodb')
        self.environment = environment
        
        # Table names
        self.cache_table_name = f'teddy-parcel-cache-{environment}'
        self.rate_limit_table_name = f'teddy-rate-limit-{environment}'
        self.metadata_table_name = f'teddy-api-metadata-{environment}'
        
        # Initialize table references
        try:
            self.cache_table = self.dynamodb.Table(self.cache_table_name)
            self.rate_limit_table = self.dynamodb.Table(self.rate_limit_table_name)
            self.metadata_table = self.dynamodb.Table(self.metadata_table_name)
            print(f"✅ Connected to DynamoDB tables for environment: {environment}")
        except Exception as e:
            print(f"❌ Error connecting to DynamoDB tables: {e}")
            sys.exit(1)
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get detailed cache statistics"""
        stats = {
            'environment': self.environment,
            'timestamp': datetime.now().isoformat(),
            'tables': {}
        }
        
        tables = [
            ('cache', self.cache_table, self.cache_table_name),
            ('rate_limit', self.rate_limit_table, self.rate_limit_table_name),
            ('metadata', self.metadata_table, self.metadata_table_name)
        ]
        
        for table_type, table_ref, table_name in tables:
            try:
                # Get table description
                response = self.dynamodb.meta.client.describe_table(TableName=table_name)
                table_info = response['Table']
                
                # Scan for actual item count and types
                scan_response = table_ref.scan(Select='COUNT')
                actual_count = scan_response['Count']
                
                # Get sample items to analyze data types
                sample_response = table_ref.scan(Limit=10)
                sample_items = sample_response.get('Items', [])
                
                # Analyze data types in cache table
                data_types = {}
                if table_type == 'cache' and sample_items:
                    for item in sample_items:
                        data_type = item.get('data_type', 'unknown')
                        data_types[data_type] = data_types.get(data_type, 0) + 1
                
                stats['tables'][table_type] = {
                    'table_name': table_name,
                    'estimated_item_count': table_info.get('ItemCount', 0),
                    'actual_item_count': actual_count,
                    'table_size_bytes': table_info.get('TableSizeBytes', 0),
                    'status': table_info.get('TableStatus', 'UNKNOWN'),
                    'data_types': data_types if data_types else None
                }
                
            except Exception as e:
                stats['tables'][table_type] = {
                    'table_name': table_name,
                    'error': str(e)
                }
        
        return stats
    
    def clear_parcel_cache(self, confirm: bool = False) -> int:
        """Clear all parcel cache data"""
        if not confirm:
            response = input(f"⚠️  Clear ALL parcel cache data from {self.cache_table_name}? (y/N): ")
            if response.lower() != 'y':
                print("❌ Operation cancelled")
                return 0
        
        print(f"🧹 Clearing parcel cache from {self.cache_table_name}...")
        return self._clear_table_items(self.cache_table, 'cache_key', 'parcel:')
    
    def clear_rate_limit_data(self, confirm: bool = False) -> int:
        """Clear all rate limiting data"""
        if not confirm:
            response = input(f"⚠️  Clear ALL rate limit data from {self.rate_limit_table_name}? (y/N): ")
            if response.lower() != 'y':
                print("❌ Operation cancelled")
                return 0
        
        print(f"🧹 Clearing rate limit data from {self.rate_limit_table_name}...")
        return self._clear_table_items(self.rate_limit_table, 'api_key')
    
    def clear_metadata(self, confirm: bool = False) -> int:
        """Clear all API metadata"""
        if not confirm:
            response = input(f"⚠️  Clear ALL API metadata from {self.metadata_table_name}? (y/N): ")
            if response.lower() != 'y':
                print("❌ Operation cancelled")
                return 0
        
        print(f"🧹 Clearing API metadata from {self.metadata_table_name}...")
        return self._clear_table_items(self.metadata_table, 'metadata_key')
    
    def clear_all_cache(self, confirm: bool = False) -> Dict[str, int]:
        """Clear all cache data from all tables"""
        if not confirm:
            print(f"⚠️  This will clear ALL cache data from environment: {self.environment}")
            print(f"   - {self.cache_table_name}")
            print(f"   - {self.rate_limit_table_name}")
            print(f"   - {self.metadata_table_name}")
            response = input("Continue? (y/N): ")
            if response.lower() != 'y':
                print("❌ Operation cancelled")
                return {}
        
        results = {}
        results['parcel_cache'] = self.clear_parcel_cache(confirm=True)
        results['rate_limit'] = self.clear_rate_limit_data(confirm=True)
        results['metadata'] = self.clear_metadata(confirm=True)
        
        return results
    
    def clear_expired_items(self) -> Dict[str, int]:
        """Clear only expired items (where TTL < current time)"""
        current_time = int(time.time())
        results = {}
        
        print(f"🕐 Clearing expired items (TTL < {current_time})...")
        
        # Clear expired cache items
        print("Checking cache table for expired items...")
        results['cache_expired'] = self._clear_expired_from_table(
            self.cache_table, 'cache_key', current_time
        )
        
        # Clear expired rate limit items
        print("Checking rate limit table for expired items...")
        results['rate_limit_expired'] = self._clear_expired_from_table(
            self.rate_limit_table, 'api_key', current_time
        )
        
        # Clear expired metadata items
        print("Checking metadata table for expired items...")
        results['metadata_expired'] = self._clear_expired_from_table(
            self.metadata_table, 'metadata_key', current_time
        )
        
        return results
    
    def clear_specific_parcel(self, parcel_keys: List[str]) -> int:
        """Clear specific parcel cache entries"""
        cleared_count = 0
        
        for parcel_key in parcel_keys:
            try:
                # Ensure parcel key has proper prefix
                if not parcel_key.startswith('parcel:'):
                    parcel_key = f'parcel:{parcel_key}'
                
                self.cache_table.delete_item(
                    Key={'cache_key': parcel_key}
                )
                cleared_count += 1
                print(f"✅ Cleared: {parcel_key}")
                
            except Exception as e:
                print(f"❌ Error clearing {parcel_key}: {e}")
        
        return cleared_count
    
    def _clear_table_items(self, table, key_attr: str, prefix: str = None) -> int:
        """Helper method to clear all items from a table"""
        cleared_count = 0
        
        try:
            # Scan table to get all items
            scan_kwargs = {}
            if prefix:
                scan_kwargs['FilterExpression'] = Attr(key_attr).begins_with(prefix)
            
            while True:
                response = table.scan(**scan_kwargs)
                items = response.get('Items', [])
                
                if not items:
                    break
                
                # Delete items in batches
                with table.batch_writer() as batch:
                    for item in items:
                        # Get the key for deletion
                        key = {key_attr: item[key_attr]}
                        
                        # Add sort key if it exists
                        if key_attr == 'api_key' and 'time_window' in item:
                            key['time_window'] = item['time_window']
                        
                        batch.delete_item(Key=key)
                        cleared_count += 1
                
                # Handle pagination
                if 'LastEvaluatedKey' in response:
                    scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
                else:
                    break
                
                print(f"   Processed {cleared_count} items...")
                
        except Exception as e:
            print(f"❌ Error clearing table items: {e}")
        
        return cleared_count
    
    def _clear_expired_from_table(self, table, key_attr: str, current_time: int) -> int:
        """Helper method to clear expired items from a table"""
        cleared_count = 0
        
        try:
            # Scan for expired items
            response = table.scan(
                FilterExpression=Attr('ttl').lt(current_time)
            )
            
            items = response.get('Items', [])
            
            if not items:
                print(f"   No expired items found in {table.table_name}")
                return 0
            
            # Delete expired items in batches
            with table.batch_writer() as batch:
                for item in items:
                    # Get the key for deletion
                    key = {key_attr: item[key_attr]}
                    
                    # Add sort key if it exists
                    if key_attr == 'api_key' and 'time_window' in item:
                        key['time_window'] = item['time_window']
                    
                    batch.delete_item(Key=key)
                    cleared_count += 1
            
            print(f"   Cleared {cleared_count} expired items from {table.table_name}")
            
        except Exception as e:
            print(f"❌ Error clearing expired items from {table.table_name}: {e}")
        
        return cleared_count

def print_stats(stats: Dict[str, Any]):
    """Print formatted cache statistics"""
    print("\n📊 DynamoDB Cache Statistics")
    print("=" * 50)
    print(f"Environment: {stats['environment']}")
    print(f"Timestamp: {stats['timestamp']}")
    print()
    
    for table_type, table_data in stats['tables'].items():
        print(f"📦 {table_type.upper()} TABLE: {table_data['table_name']}")
        
        if 'error' in table_data:
            print(f"   ❌ Error: {table_data['error']}")
        else:
            print(f"   Status: {table_data['status']}")
            print(f"   Estimated Items: {table_data['estimated_item_count']:,}")
            print(f"   Actual Items: {table_data['actual_item_count']:,}")
            print(f"   Table Size: {table_data['table_size_bytes']:,} bytes")
            
            if table_data.get('data_types'):
                print(f"   Data Types: {table_data['data_types']}")
        
        print()

def main():
    parser = argparse.ArgumentParser(
        description='Clear DynamoDB cache for Teddy Data Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python clear_dynamodb_cache.py --stats                    # Show statistics only
  python clear_dynamodb_cache.py --all                      # Clear all cache
  python clear_dynamodb_cache.py --parcel                   # Clear parcel cache only
  python clear_dynamodb_cache.py --rate-limit               # Clear rate limit data
  python clear_dynamodb_cache.py --metadata                 # Clear metadata only
  python clear_dynamodb_cache.py --expired                  # Clear expired items only
  python clear_dynamodb_cache.py --env prod --stats         # Show prod stats
  python clear_dynamodb_cache.py --specific parcel:coords:30.123:-97.456  # Clear specific parcel
        """
    )
    
    parser.add_argument('--env', default='dev', help='Environment (dev, prod)')
    parser.add_argument('--all', action='store_true', help='Clear all cache data')
    parser.add_argument('--parcel', action='store_true', help='Clear parcel cache only')
    parser.add_argument('--rate-limit', action='store_true', help='Clear rate limit data only')
    parser.add_argument('--metadata', action='store_true', help='Clear API metadata only')
    parser.add_argument('--expired', action='store_true', help='Clear expired items only')
    parser.add_argument('--stats', action='store_true', help='Show cache statistics only')
    parser.add_argument('--specific', nargs='+', help='Clear specific parcel cache keys')
    parser.add_argument('--yes', action='store_true', help='Skip confirmation prompts')
    
    args = parser.parse_args()
    
    # Initialize cache cleaner
    cleaner = DynamoDBCacheCleaner(environment=args.env)
    
    # Show stats if requested
    if args.stats:
        stats = cleaner.get_cache_stats()
        print_stats(stats)
        return
    
    # Execute requested operations
    total_cleared = 0
    
    if args.all:
        print(f"🚀 Clearing ALL cache data for environment: {args.env}")
        results = cleaner.clear_all_cache(confirm=args.yes)
        for operation, count in results.items():
            print(f"✅ {operation}: {count} items cleared")
            total_cleared += count
    
    elif args.parcel:
        count = cleaner.clear_parcel_cache(confirm=args.yes)
        print(f"✅ Parcel cache: {count} items cleared")
        total_cleared += count
    
    elif args.rate_limit:
        count = cleaner.clear_rate_limit_data(confirm=args.yes)
        print(f"✅ Rate limit data: {count} items cleared")
        total_cleared += count
    
    elif args.metadata:
        count = cleaner.clear_metadata(confirm=args.yes)
        print(f"✅ API metadata: {count} items cleared")
        total_cleared += count
    
    elif args.expired:
        results = cleaner.clear_expired_items()
        for operation, count in results.items():
            print(f"✅ {operation}: {count} items cleared")
            total_cleared += count
    
    elif args.specific:
        count = cleaner.clear_specific_parcel(args.specific)
        print(f"✅ Specific parcels: {count} items cleared")
        total_cleared += count
    
    else:
        # No operation specified, show help
        parser.print_help()
        return
    
    # Final summary
    if total_cleared > 0:
        print(f"\n🎉 Total items cleared: {total_cleared}")
        print(f"💰 Cost saved: ~${total_cleared * 0.0000125:.6f} (estimated)")
    
    print(f"\n📊 Run with --stats to see current cache statistics")

if __name__ == '__main__':
    main()