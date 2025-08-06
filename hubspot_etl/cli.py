#!/usr/bin/env python3
"""
HubSpot ETL Command Line Interface

Simple CLI for running HubSpot to Snowflake ETL operations.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from hubspot_etl import HubSpotETLPipeline


def setup_logging(log_level='INFO'):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('hubspot_etl.log'),
            logging.StreamHandler()
        ]
    )


def run_full_extraction(args):
    """Run full data extraction"""
    print("🚀 Starting full HubSpot data extraction...")
    
    config = {}
    if args.object_types:
        config['object_types'] = args.object_types.split(',')
    if args.batch_size:
        config['batch_size'] = args.batch_size
    if args.schema:
        config['schema_name'] = args.schema
    
    pipeline = HubSpotETLPipeline(config=config)
    
    try:
        results = pipeline.run_full_extraction(
            object_types=config.get('object_types'),
            max_records_per_type=args.max_records
        )
        
        print(f"✅ Extraction completed!")
        print(f"   Status: {results['status']}")
        print(f"   Total records: {results['total_records']}")
        print(f"   Duration: {results['duration']:.2f} seconds")
        
        if results['errors']:
            print(f"⚠️  Errors encountered: {len(results['errors'])}")
            for error in results['errors']:
                print(f"   - {error}")
        
        return 0 if results['status'] in ['completed', 'completed_with_errors'] else 1
        
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        return 1


def run_incremental_extraction(args):
    """Run incremental data extraction"""
    print("🔄 Starting incremental HubSpot data extraction...")
    
    # Determine since date
    if args.since_hours:
        since_date = datetime.now() - timedelta(hours=args.since_hours)
    elif args.since_days:
        since_date = datetime.now() - timedelta(days=args.since_days)
    else:
        since_date = None
    
    config = {}
    if args.object_types:
        config['object_types'] = args.object_types.split(',')
    if args.schema:
        config['schema_name'] = args.schema
    
    pipeline = HubSpotETLPipeline(config=config)
    
    try:
        results = pipeline.run_incremental_extraction(
            object_types=config.get('object_types'),
            since=since_date
        )
        
        print(f"✅ Incremental extraction completed!")
        print(f"   Status: {results['status']}")
        print(f"   Total records: {results['total_records']}")
        print(f"   Duration: {results['duration']:.2f} seconds")
        
        if since_date:
            print(f"   Since: {since_date}")
        
        if results['errors']:
            print(f"⚠️  Errors encountered: {len(results['errors'])}")
            for error in results['errors']:
                print(f"   - {error}")
        
        return 0 if results['status'] in ['completed', 'completed_with_errors'] else 1
        
    except Exception as e:
        print(f"❌ Incremental extraction failed: {e}")
        return 1


def test_connections(args):
    """Test connections to HubSpot and Snowflake"""
    print("🔍 Testing connections...")
    
    pipeline = HubSpotETLPipeline()
    
    try:
        # Test HubSpot
        print("   Testing HubSpot connection...", end=' ')
        if pipeline.hubspot_client.test_connection():
            print("✅")
        else:
            print("❌")
            return 1
        
        # Test Snowflake
        print("   Testing Snowflake connection...", end=' ')
        if pipeline.snowflake_loader.test_connection():
            print("✅")
        else:
            print("❌")
            return 1
        
        print("✅ All connections successful!")
        return 0
        
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return 1


def show_status(args):
    """Show pipeline status"""
    print("📊 Pipeline Status")
    print("=" * 50)
    
    pipeline = HubSpotETLPipeline()
    
    try:
        status = pipeline.get_pipeline_status()
        
        # Pipeline state
        state = status['pipeline_state']
        print(f"Last run: {state.get('last_run', 'Never')}")
        print(f"Last successful run: {state.get('last_successful_run', 'Never')}")
        
        # HubSpot API usage
        if status.get('hubspot_api_usage'):
            usage = status['hubspot_api_usage']
            print(f"\nHubSpot API Usage:")
            print(f"  Daily limit: {usage.get('dailyLimit', 'Unknown')}")
            print(f"  Current usage: {usage.get('currentUsage', 'Unknown')}")
        
        # Snowflake tables
        if status.get('snowflake_tables'):
            print(f"\nSnowflake Tables:")
            for object_type, table_info in status['snowflake_tables'].items():
                print(f"  {object_type}: {table_info.get('row_count', 0)} rows")
        
        return 0
        
    except Exception as e:
        print(f"❌ Failed to get status: {e}")
        return 1


def run_quality_check(args):
    """Run data quality checks"""
    print("🔍 Running data quality checks...")
    
    config = {}
    if args.object_types:
        config['object_types'] = args.object_types.split(',')
    
    pipeline = HubSpotETLPipeline(config=config)
    
    try:
        report = pipeline.run_data_quality_check(
            object_types=config.get('object_types')
        )
        
        print(f"Overall status: {report['overall_status']}")
        print("\nObject Reports:")
        
        for object_type, obj_report in report['object_reports'].items():
            status_icon = "✅" if obj_report['status'] == 'passed' else "⚠️" if obj_report['status'] == 'warning' else "❌"
            print(f"  {status_icon} {object_type}: {obj_report['status']}")
            
            if obj_report.get('row_count'):
                print(f"     Rows: {obj_report['row_count']}")
            
            if obj_report.get('issues'):
                for issue in obj_report['issues']:
                    print(f"     Issue: {issue}")
        
        return 0 if report['overall_status'] != 'failed' else 1
        
    except Exception as e:
        print(f"❌ Quality check failed: {e}")
        return 1


def cleanup_data(args):
    """Clean up old data"""
    print(f"🧹 Cleaning up data older than {args.days} days...")
    
    pipeline = HubSpotETLPipeline()
    
    try:
        results = pipeline.cleanup_old_data(days_to_keep=args.days)
        
        print(f"✅ Cleanup completed!")
        print(f"   Total rows deleted: {results['total_rows_deleted']}")
        
        for object_type, result in results['tables_processed'].items():
            if result['status'] == 'completed':
                print(f"   {object_type}: {result['rows_deleted']} rows deleted")
            else:
                print(f"   {object_type}: Failed - {result.get('error', 'Unknown error')}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        return 1


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description='HubSpot to Snowflake ETL Pipeline CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s full --object-types contacts,companies --max-records 10000
  %(prog)s incremental --since-hours 24 --object-types deals
  %(prog)s test
  %(prog)s status
  %(prog)s quality --object-types contacts
  %(prog)s cleanup --days 90
        """
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Full extraction command
    full_parser = subparsers.add_parser('full', help='Run full data extraction')
    full_parser.add_argument('--object-types', help='Comma-separated list of object types')
    full_parser.add_argument('--max-records', type=int, help='Maximum records per object type')
    full_parser.add_argument('--batch-size', type=int, help='Batch size for processing')
    full_parser.add_argument('--schema', help='Snowflake schema name')
    full_parser.set_defaults(func=run_full_extraction)
    
    # Incremental extraction command
    incr_parser = subparsers.add_parser('incremental', help='Run incremental data extraction')
    incr_parser.add_argument('--object-types', help='Comma-separated list of object types')
    incr_parser.add_argument('--since-hours', type=int, help='Extract data modified in last N hours')
    incr_parser.add_argument('--since-days', type=int, help='Extract data modified in last N days')
    incr_parser.add_argument('--schema', help='Snowflake schema name')
    incr_parser.set_defaults(func=run_incremental_extraction)
    
    # Test connections command
    test_parser = subparsers.add_parser('test', help='Test connections to HubSpot and Snowflake')
    test_parser.set_defaults(func=test_connections)
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show pipeline status')
    status_parser.set_defaults(func=show_status)
    
    # Quality check command
    quality_parser = subparsers.add_parser('quality', help='Run data quality checks')
    quality_parser.add_argument('--object-types', help='Comma-separated list of object types')
    quality_parser.set_defaults(func=run_quality_check)
    
    # Cleanup command
    cleanup_parser = subparsers.add_parser('cleanup', help='Clean up old data')
    cleanup_parser.add_argument('--days', type=int, default=90, help='Keep data newer than N days')
    cleanup_parser.set_defaults(func=cleanup_data)
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Setup logging
    setup_logging(args.log_level)
    
    # Run command
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
