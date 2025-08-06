#!/usr/bin/env python3
"""
Bulk NLCD Processor for State-Level Land Cover Data Processing

This script processes NLCD land cover data for all parcels in specified states.
It supports resumable processing, rate limiting, and progress tracking.

Usage:
    python bulk_nlcd_processor.py --state TX --batch-size 100 --year 2021
    python bulk_nlcd_processor.py --state KS --batch-size 50 --resume
    python bulk_nlcd_processor.py --state MO --dry-run

Priority states: TX (Texas), KS (Kansas), MO (Missouri)
"""

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

# Add the lambda utils to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lambda'))

from utils.nlcd_client import get_nlcd_integration
from utils.snowflake_connector import get_snowflake_connector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bulk_nlcd_processor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BulkNLCDProcessor:
    """Bulk processor for NLCD land cover data by state"""
    
    def __init__(self, environment: str = 'dev'):
        self.environment = environment
        self.nlcd_integration = get_nlcd_integration(environment)
        
        # State code mappings
        self.state_codes = {
            'TX': 'Texas',
            'KS': 'Kansas', 
            'MO': 'Missouri',
            'OK': 'Oklahoma',
            'AR': 'Arkansas',
            'LA': 'Louisiana',
            'NM': 'New Mexico',
            'CO': 'Colorado',
            'NE': 'Nebraska',
            'IA': 'Iowa'
        }
        
        # Processing statistics
        self.stats = {
            'total_parcels': 0,
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'start_time': None,
            'end_time': None
        }
    
    def get_parcels_by_state(self, state_code: str, limit: Optional[int] = None) -> List[Dict]:
        """
        Get all parcels for a specific state
        
        Args:
            state_code: Two-letter state code (e.g., 'TX')
            limit: Optional limit for testing
            
        Returns:
            List of parcel dictionaries
        """
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                # Query to get parcels by state
                query = """
                    SELECT 
                        PARCEL_ID,
                        STATE,
                        COUNTY,
                        LATITUDE,
                        LONGITUDE,
                        ACRES,
                        GEOMETRY_JSON
                    FROM TEDDY_DATA.RAW.PARCEL_PROFILES 
                    WHERE STATE = %s
                    AND LATITUDE IS NOT NULL 
                    AND LONGITUDE IS NOT NULL
                """
                
                if limit:
                    query += f" LIMIT {limit}"
                
                results = sf.execute_query(query, {'1': state_code})
                
                logger.info(f"Found {len(results)} parcels in {state_code}")
                return results
                
        except Exception as e:
            logger.error(f"Error getting parcels for state {state_code}: {e}")
            return []
    
    def get_unprocessed_parcels(self, state_code: str, max_age_days: int = 30, limit: Optional[int] = None) -> List[Dict]:
        """
        Get parcels that haven't been processed recently
        
        Args:
            state_code: Two-letter state code
            max_age_days: Consider data older than this as needing refresh
            limit: Optional limit for testing
            
        Returns:
            List of unprocessed parcel dictionaries
        """
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                cutoff_date = datetime.now() - timedelta(days=max_age_days)
                
                query = """
                    SELECT 
                        p.PARCEL_ID,
                        p.STATE,
                        p.COUNTY,
                        p.LATITUDE,
                        p.LONGITUDE,
                        p.ACRES,
                        p.GEOMETRY_JSON
                    FROM TEDDY_DATA.RAW.PARCEL_PROFILES p
                    LEFT JOIN TEDDY_DATA.LAND_COVER.PARCEL_LAND_COVER_SUMMARY s 
                        ON p.PARCEL_ID = s.PARCEL_ID
                    WHERE p.STATE = %s
                    AND p.LATITUDE IS NOT NULL 
                    AND p.LONGITUDE IS NOT NULL
                    AND (s.PARCEL_ID IS NULL OR s.LAST_PROCESSED_AT < %s)
                """
                
                if limit:
                    query += f" LIMIT {limit}"
                
                results = sf.execute_query(query, {
                    '1': state_code, 
                    '2': cutoff_date.isoformat()
                })
                
                logger.info(f"Found {len(results)} unprocessed parcels in {state_code}")
                return results
                
        except Exception as e:
            logger.error(f"Error getting unprocessed parcels for state {state_code}: {e}")
            return []
    
    def create_processing_job(self, state_code: str, total_parcels: int, batch_size: int) -> str:
        """
        Create a processing job record in the database
        
        Args:
            state_code: State being processed
            total_parcels: Total number of parcels to process
            batch_size: Batch size for processing
            
        Returns:
            Job ID
        """
        
        job_id = f"nlcd_bulk_{state_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                sf.execute_non_query("""
                    INSERT INTO TEDDY_DATA.LAND_COVER.PROCESSING_STATUS (
                        JOB_ID, STATE_CODE, STATUS, TOTAL_PARCELS, BATCH_SIZE, STARTED_AT
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, {
                    '1': job_id,
                    '2': state_code,
                    '3': 'running',
                    '4': total_parcels,
                    '5': batch_size,
                    '6': datetime.now().isoformat()
                })
                
                logger.info(f"Created processing job: {job_id}")
                return job_id
                
        except Exception as e:
            logger.error(f"Error creating processing job: {e}")
            return job_id
    
    def update_processing_job(self, job_id: str, processed: int, failed: int, 
                            last_parcel_id: str = None, status: str = 'running'):
        """
        Update processing job status
        
        Args:
            job_id: Job identifier
            processed: Number of parcels processed
            failed: Number of failed parcels
            last_parcel_id: Last processed parcel ID
            status: Job status
        """
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                # Calculate processing rate
                elapsed_hours = (datetime.now() - self.stats['start_time']).total_seconds() / 3600
                processing_rate = processed / elapsed_hours if elapsed_hours > 0 else 0
                
                # Estimate completion time
                remaining = self.stats['total_parcels'] - processed
                estimated_completion = None
                if processing_rate > 0:
                    hours_remaining = remaining / processing_rate
                    estimated_completion = datetime.now() + timedelta(hours=hours_remaining)
                
                sf.execute_non_query("""
                    UPDATE TEDDY_DATA.LAND_COVER.PROCESSING_STATUS 
                    SET PROCESSED_PARCELS = %s,
                        FAILED_PARCELS = %s,
                        LAST_PROCESSED_PARCEL_ID = %s,
                        STATUS = %s,
                        PROCESSING_RATE_PER_HOUR = %s,
                        ESTIMATED_COMPLETION = %s,
                        UPDATED_AT = %s
                    WHERE JOB_ID = %s
                """, {
                    '1': processed,
                    '2': failed,
                    '3': last_parcel_id,
                    '4': status,
                    '5': round(processing_rate, 2),
                    '6': estimated_completion.isoformat() if estimated_completion else None,
                    '7': datetime.now().isoformat(),
                    '8': job_id
                })
                
        except Exception as e:
            logger.error(f"Error updating processing job: {e}")
    
    def complete_processing_job(self, job_id: str, status: str = 'completed'):
        """
        Mark processing job as completed
        
        Args:
            job_id: Job identifier
            status: Final status ('completed', 'failed', 'paused')
        """
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                sf.execute_non_query("""
                    UPDATE TEDDY_DATA.LAND_COVER.PROCESSING_STATUS 
                    SET STATUS = %s,
                        COMPLETED_AT = %s,
                        UPDATED_AT = %s
                    WHERE JOB_ID = %s
                """, {
                    '1': status,
                    '2': datetime.now().isoformat(),
                    '3': datetime.now().isoformat(),
                    '4': job_id
                })
                
                logger.info(f"Completed processing job: {job_id} with status: {status}")
                
        except Exception as e:
            logger.error(f"Error completing processing job: {e}")
    
    def process_parcel_batch(self, parcels: List[Dict], year: int, rate_limit: float = 1.0) -> Dict[str, int]:
        """
        Process a batch of parcels
        
        Args:
            parcels: List of parcel dictionaries
            year: NLCD year to process
            rate_limit: Delay between requests in seconds
            
        Returns:
            Dictionary with processing statistics
        """
        
        batch_stats = {'successful': 0, 'failed': 0, 'skipped': 0}
        
        for parcel in parcels:
            try:
                parcel_id = parcel['PARCEL_ID']
                
                # Process the parcel
                result = self.nlcd_integration.process_parcel_land_cover(
                    parcel_id=parcel_id,
                    year=year,
                    force_refresh=False  # Don't force refresh in bulk processing
                )
                
                if result['success']:
                    if result['source'] == 'cache':
                        batch_stats['skipped'] += 1
                        logger.debug(f"Skipped {parcel_id} (cached)")
                    else:
                        batch_stats['successful'] += 1
                        logger.info(f"Processed {parcel_id} successfully")
                else:
                    batch_stats['failed'] += 1
                    logger.warning(f"Failed to process {parcel_id}: {result.get('error', 'Unknown error')}")
                
                # Rate limiting
                if rate_limit > 0:
                    time.sleep(rate_limit)
                
            except Exception as e:
                batch_stats['failed'] += 1
                logger.error(f"Error processing parcel {parcel.get('PARCEL_ID', 'unknown')}: {e}")
        
        return batch_stats
    
    def process_state(self, state_code: str, year: int = 2021, batch_size: int = 100, 
                     rate_limit: float = 1.0, dry_run: bool = False, 
                     resume: bool = False, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Process all parcels in a state
        
        Args:
            state_code: Two-letter state code
            year: NLCD year to process
            batch_size: Number of parcels to process in each batch
            rate_limit: Delay between API requests in seconds
            dry_run: If True, don't actually process parcels
            resume: If True, only process unprocessed parcels
            limit: Optional limit for testing
            
        Returns:
            Processing results dictionary
        """
        
        logger.info(f"Starting bulk NLCD processing for {state_code}")
        logger.info(f"Parameters: year={year}, batch_size={batch_size}, rate_limit={rate_limit}s")
        logger.info(f"Dry run: {dry_run}, Resume: {resume}, Limit: {limit}")
        
        self.stats['start_time'] = datetime.now()
        
        # Get parcels to process
        if resume:
            parcels = self.get_unprocessed_parcels(state_code, limit=limit)
        else:
            parcels = self.get_parcels_by_state(state_code, limit=limit)
        
        if not parcels:
            logger.warning(f"No parcels found for state {state_code}")
            return {'success': False, 'error': 'No parcels found'}
        
        self.stats['total_parcels'] = len(parcels)
        logger.info(f"Processing {len(parcels)} parcels in {state_code}")
        
        if dry_run:
            logger.info("DRY RUN - No actual processing will be performed")
            return {
                'success': True,
                'dry_run': True,
                'total_parcels': len(parcels),
                'state': state_code
            }
        
        # Create processing job
        job_id = self.create_processing_job(state_code, len(parcels), batch_size)
        
        try:
            # Process parcels in batches
            for i in range(0, len(parcels), batch_size):
                batch = parcels[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (len(parcels) + batch_size - 1) // batch_size
                
                logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} parcels)")
                
                # Process the batch
                batch_stats = self.process_parcel_batch(batch, year, rate_limit)
                
                # Update statistics
                self.stats['processed'] += len(batch)
                self.stats['successful'] += batch_stats['successful']
                self.stats['failed'] += batch_stats['failed']
                self.stats['skipped'] += batch_stats['skipped']
                
                # Update job status
                last_parcel_id = batch[-1]['PARCEL_ID'] if batch else None
                self.update_processing_job(
                    job_id, 
                    self.stats['processed'], 
                    self.stats['failed'],
                    last_parcel_id
                )
                
                # Log progress
                progress = (self.stats['processed'] / self.stats['total_parcels']) * 100
                logger.info(f"Progress: {progress:.1f}% ({self.stats['processed']}/{self.stats['total_parcels']})")
                logger.info(f"Batch stats: {batch_stats['successful']} successful, {batch_stats['failed']} failed, {batch_stats['skipped']} skipped")
        
        except KeyboardInterrupt:
            logger.info("Processing interrupted by user")
            self.complete_processing_job(job_id, 'paused')
            return {
                'success': False,
                'interrupted': True,
                'stats': self.stats,
                'job_id': job_id
            }
        
        except Exception as e:
            logger.error(f"Error during bulk processing: {e}")
            self.complete_processing_job(job_id, 'failed')
            return {
                'success': False,
                'error': str(e),
                'stats': self.stats,
                'job_id': job_id
            }
        
        # Complete the job
        self.stats['end_time'] = datetime.now()
        self.complete_processing_job(job_id, 'completed')
        
        # Calculate final statistics
        duration = self.stats['end_time'] - self.stats['start_time']
        processing_rate = self.stats['processed'] / (duration.total_seconds() / 3600)
        
        logger.info(f"Bulk processing completed for {state_code}")
        logger.info(f"Total time: {duration}")
        logger.info(f"Processing rate: {processing_rate:.1f} parcels/hour")
        logger.info(f"Results: {self.stats['successful']} successful, {self.stats['failed']} failed, {self.stats['skipped']} skipped")
        
        return {
            'success': True,
            'state': state_code,
            'job_id': job_id,
            'stats': self.stats,
            'duration': str(duration),
            'processing_rate': round(processing_rate, 1)
        }
    
    def get_processing_status(self, job_id: str = None, state_code: str = None) -> List[Dict]:
        """
        Get processing job status
        
        Args:
            job_id: Specific job ID to query
            state_code: State code to filter by
            
        Returns:
            List of job status dictionaries
        """
        
        try:
            with get_snowflake_connector(self.environment) as sf:
                query = """
                    SELECT 
                        JOB_ID,
                        STATE_CODE,
                        STATUS,
                        TOTAL_PARCELS,
                        PROCESSED_PARCELS,
                        FAILED_PARCELS,
                        BATCH_SIZE,
                        PROCESSING_RATE_PER_HOUR,
                        STARTED_AT,
                        COMPLETED_AT,
                        ESTIMATED_COMPLETION,
                        LAST_PROCESSED_PARCEL_ID
                    FROM TEDDY_DATA.LAND_COVER.PROCESSING_STATUS
                """
                
                params = {}
                conditions = []
                
                if job_id:
                    conditions.append("JOB_ID = %s")
                    params['1'] = job_id
                
                if state_code:
                    param_key = '2' if job_id else '1'
                    conditions.append("STATE_CODE = %s")
                    params[param_key] = state_code
                
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
                
                query += " ORDER BY STARTED_AT DESC"
                
                results = sf.execute_query(query, params)
                return results
                
        except Exception as e:
            logger.error(f"Error getting processing status: {e}")
            return []

def main():
    """Main function for command-line usage"""
    
    parser = argparse.ArgumentParser(description='Bulk NLCD Processor for State-Level Processing')
    parser.add_argument('--state', required=True, choices=['TX', 'KS', 'MO', 'OK', 'AR', 'LA', 'NM', 'CO', 'NE', 'IA'],
                       help='State code to process (TX, KS, MO, etc.)')
    parser.add_argument('--year', type=int, default=2021, choices=[2001, 2004, 2006, 2008, 2011, 2013, 2016, 2019, 2021],
                       help='NLCD year to process (default: 2021)')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Number of parcels to process in each batch (default: 100)')
    parser.add_argument('--rate-limit', type=float, default=1.0,
                       help='Delay between API requests in seconds (default: 1.0)')
    parser.add_argument('--environment', default='dev', choices=['dev', 'prod'],
                       help='Environment to use (default: dev)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Perform a dry run without actual processing')
    parser.add_argument('--resume', action='store_true',
                       help='Resume processing - only process unprocessed parcels')
    parser.add_argument('--limit', type=int,
                       help='Limit number of parcels for testing')
    parser.add_argument('--status', action='store_true',
                       help='Show processing status for the state')
    
    args = parser.parse_args()
    
    # Initialize processor
    processor = BulkNLCDProcessor(args.environment)
    
    # Show status if requested
    if args.status:
        status_results = processor.get_processing_status(state_code=args.state)
        if status_results:
            print(f"\nProcessing Status for {args.state}:")
            print("-" * 80)
            for job in status_results:
                print(f"Job ID: {job['JOB_ID']}")
                print(f"Status: {job['STATUS']}")
                print(f"Progress: {job['PROCESSED_PARCELS']}/{job['TOTAL_PARCELS']} parcels")
                if job['PROCESSING_RATE_PER_HOUR']:
                    print(f"Rate: {job['PROCESSING_RATE_PER_HOUR']} parcels/hour")
                print(f"Started: {job['STARTED_AT']}")
                if job['COMPLETED_AT']:
                    print(f"Completed: {job['COMPLETED_AT']}")
                elif job['ESTIMATED_COMPLETION']:
                    print(f"Estimated completion: {job['ESTIMATED_COMPLETION']}")
                print("-" * 80)
        else:
            print(f"No processing jobs found for {args.state}")
        return
    
    # Process the state
    result = processor.process_state(
        state_code=args.state,
        year=args.year,
        batch_size=args.batch_size,
        rate_limit=args.rate_limit,
        dry_run=args.dry_run,
        resume=args.resume,
        limit=args.limit
    )
    
    # Print results
    print("\n" + "="*80)
    print("BULK NLCD PROCESSING RESULTS")
    print("="*80)
    print(f"State: {args.state}")
    print(f"Success: {result['success']}")
    
    if result['success'] and not result.get('dry_run'):
        stats = result['stats']
        print(f"Total parcels: {stats['total_parcels']}")
        print(f"Processed: {stats['processed']}")
        print(f"Successful: {stats['successful']}")
        print(f"Failed: {stats['failed']}")
        print(f"Skipped (cached): {stats['skipped']}")
        print(f"Duration: {result['duration']}")
        print(f"Processing rate: {result['processing_rate']} parcels/hour")
        print(f"Job ID: {result['job_id']}")
    elif result.get('dry_run'):
        print(f"Dry run completed - would process {result['total_parcels']} parcels")
    else:
        print(f"Error: {result.get('error', 'Unknown error')}")
    
    print("="*80)

if __name__ == "__main__":
    main()
