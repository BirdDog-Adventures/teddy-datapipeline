#!/usr/bin/env python3
"""
Parcel Data Enrichment Orchestrator
Event-driven pipeline that triggers spatial data enrichment when new parcels are ingested

Flow:
1. Regrid parcel data ingestion → PARCEL_PROFILE table
2. Spatial join with preloaded SSURGO soil data
3. Trigger enrichment for: crop history, land cover, topography, climate, etc.
4. All based on parcel boundary geometry and spatial relationships

Referenced in Teddy DataPipeline Mermaid Chart as the orchestration layer
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import snowflake.connector
from dataclasses import dataclass
import boto3
import os
import asyncio
import concurrent.futures
from shapely.geometry import Point, Polygon
from shapely.wkt import loads as wkt_loads

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ParcelEnrichmentJob:
    """Data structure for parcel enrichment job"""
    parcel_id: str
    geometry_wkt: str
    latitude: float
    longitude: float
    county_fips: str
    state_code: str
    acres: float
    created_at: datetime

class ParcelEnrichmentOrchestrator:
    """
    Orchestrates the complete parcel data enrichment pipeline
    Triggered by new parcel ingestion from Regrid
    """
    
    def __init__(self, snowflake_config: Dict, aws_config: Dict):
        self.snowflake_config = snowflake_config
        self.aws_config = aws_config
        self.lambda_client = boto3.client('lambda', region_name=aws_config.get('region', 'us-east-1'))
        self.eventbridge_client = boto3.client('events', region_name=aws_config.get('region', 'us-east-1'))
        
    def detect_new_parcels(self, lookback_hours: int = 1) -> List[ParcelEnrichmentJob]:
        """
        Detect newly ingested parcels that need enrichment
        
        Args:
            lookback_hours: How far back to look for new parcels
            
        Returns:
            List of ParcelEnrichmentJob objects
        """
        try:
            conn = snowflake.connector.connect(**self.snowflake_config)
            cursor = conn.cursor()
            
            # Find parcels ingested in the last N hours that haven't been enriched
            lookback_time = datetime.now() - timedelta(hours=lookback_hours)
            
            query = """
            SELECT DISTINCT
                p.PARCEL_ID,
                p.GEOMETRY_WKT,
                p.LATITUDE,
                p.LONGITUDE,
                p.COUNTY_FIPS,
                p.STATE_CODE,
                p.ACRES,
                p.CREATED_AT
            FROM RAW.PARCEL_PROFILE p
            LEFT JOIN RAW.SOIL_PROFILE s ON p.PARCEL_ID = s.PARCEL_ID
            LEFT JOIN RAW.CROP_HISTORY c ON p.PARCEL_ID = c.PARCEL_ID
            LEFT JOIN RAW.CLIMATE_DATA cl ON p.PARCEL_ID = cl.PARCEL_ID
            WHERE p.CREATED_AT >= %s
            AND p.GEOMETRY_WKT IS NOT NULL
            AND p.LATITUDE IS NOT NULL
            AND p.LONGITUDE IS NOT NULL
            AND (s.PARCEL_ID IS NULL OR c.PARCEL_ID IS NULL OR cl.PARCEL_ID IS NULL)
            ORDER BY p.CREATED_AT DESC
            LIMIT 1000
            """
            
            cursor.execute(query, (lookback_time,))
            
            jobs = []
            for row in cursor.fetchall():
                jobs.append(ParcelEnrichmentJob(
                    parcel_id=row[0],
                    geometry_wkt=row[1],
                    latitude=float(row[2]),
                    longitude=float(row[3]),
                    county_fips=row[4],
                    state_code=row[5],
                    acres=float(row[6]) if row[6] else 0.0,
                    created_at=row[7]
                ))
            
            conn.close()
            logger.info(f"Found {len(jobs)} parcels needing enrichment")
            return jobs
            
        except Exception as e:
            logger.error(f"Error detecting new parcels: {e}")
            return []
    
    def perform_spatial_soil_join(self, parcel_job: ParcelEnrichmentJob) -> Optional[Dict]:
        """
        Perform spatial join with preloaded SSURGO soil data
        
        Args:
            parcel_job: ParcelEnrichmentJob with parcel geometry
            
        Returns:
            Dictionary with soil data or None if no match
        """
        try:
            conn = snowflake.connector.connect(**self.snowflake_config)
            cursor = conn.cursor()
            
            # Spatial join query using ST_INTERSECTS with preloaded SSURGO data
            spatial_query = """
            SELECT 
                s.MUKEY,
                s.MUSYM,
                s.MUNAME,
                s.MUKIND,
                s.MUSTATUS,
                s.SLOPEGRADDCP,
                s.SLOPEGRADWTA,
                s.BROCKDEPMIN,
                s.WTDEPANNMIN,
                s.WTDEPAPRJUNMIN,
                s.FLODFREQDCD,
                s.FLODFREQMAX,
                s.PONDFREQPRS,
                s.AWS025WTA,
                s.AWS050WTA,
                s.AWS0100WTA,
                s.AWS0150WTA,
                s.DRCLASSDCD,
                s.DRCLASSWETTEST,
                s.HYDGRPDCD,
                s.ICCDCD,
                s.ICCDCDPCT,
                s.NICCDCD,
                s.NICCDCDPCT,
                s.ENGDWOBDCD,
                s.ENGDWBDCD,
                s.ENGDWBLL,
                s.ENGDWBML,
                s.ENGSTAFDCD,
                s.ENGSTAFLL,
                s.ENGSTAFML,
                s.ENGSLDCD,
                s.ENGSLDCP,
                s.ENGLRSDCD,
                s.ENGCMSSDCD,
                s.ENGCMSSMP,
                s.URBRECPTDCD,
                s.URBRECPTWTA,
                s.FORPEHRTDCP,
                s.HYDCLPRS,
                s.AWMMFPWWTA,
                ST_AREA(ST_INTERSECTION(ST_GEOMFROMWKT(%s), s.GEOMETRY)) / ST_AREA(ST_GEOMFROMWKT(%s)) as OVERLAP_RATIO
            FROM RAW.SSURGO_MAPUNIT s
            WHERE ST_INTERSECTS(ST_GEOMFROMWKT(%s), s.GEOMETRY)
            AND s.STATE_CODE = %s
            ORDER BY OVERLAP_RATIO DESC
            LIMIT 5
            """
            
            cursor.execute(spatial_query, (
                parcel_job.geometry_wkt,
                parcel_job.geometry_wkt,
                parcel_job.geometry_wkt,
                parcel_job.state_code
            ))
            
            soil_data = []
            for row in cursor.fetchall():
                soil_record = {
                    'mukey': row[0],
                    'musym': row[1],
                    'muname': row[2],
                    'mukind': row[3],
                    'mustatus': row[4],
                    'slope_grad_dcp': row[5],
                    'slope_grad_wta': row[6],
                    'bedrock_depth_min': row[7],
                    'water_table_depth_annual_min': row[8],
                    'water_table_depth_april_june_min': row[9],
                    'flood_freq_dcd': row[10],
                    'flood_freq_max': row[11],
                    'pond_freq_prs': row[12],
                    'aws_025_wta': row[13],
                    'aws_050_wta': row[14],
                    'aws_0100_wta': row[15],
                    'aws_0150_wta': row[16],
                    'drainage_class_dcd': row[17],
                    'drainage_class_wettest': row[18],
                    'hydrologic_group_dcd': row[19],
                    'irrigated_capability_class_dcd': row[20],
                    'irrigated_capability_class_pct': row[21],
                    'non_irrigated_capability_class_dcd': row[22],
                    'non_irrigated_capability_class_pct': row[23],
                    'eng_dwellings_wo_basements_dcd': row[24],
                    'eng_dwellings_w_basements_dcd': row[25],
                    'eng_dwellings_w_basements_ll': row[26],
                    'eng_dwellings_w_basements_ml': row[27],
                    'eng_septic_tank_absorption_fields_dcd': row[28],
                    'eng_septic_tank_absorption_fields_ll': row[29],
                    'eng_septic_tank_absorption_fields_ml': row[30],
                    'eng_sewage_lagoons_dcd': row[31],
                    'eng_sewage_lagoons_cp': row[32],
                    'eng_local_roads_streets_dcd': row[33],
                    'eng_construction_materials_sand_dcd': row[34],
                    'eng_construction_materials_sand_mp': row[35],
                    'urban_reclamation_potential_dcd': row[36],
                    'urban_reclamation_potential_wta': row[37],
                    'forest_productivity_dcp': row[38],
                    'hydric_classification_prs': row[39],
                    'available_water_monthly_mean_fall_winter_wta': row[40],
                    'overlap_ratio': float(row[41])
                }
                soil_data.append(soil_record)
            
            conn.close()
            
            if soil_data:
                # Return the soil unit with highest overlap
                primary_soil = soil_data[0]
                primary_soil['all_intersecting_soils'] = soil_data
                return primary_soil
            else:
                logger.warning(f"No soil data found for parcel {parcel_job.parcel_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error in spatial soil join for parcel {parcel_job.parcel_id}: {e}")
            return None
    
    def save_soil_enrichment(self, parcel_job: ParcelEnrichmentJob, soil_data: Dict):
        """
        Save soil enrichment data to SOIL_PROFILE table
        
        Args:
            parcel_job: ParcelEnrichmentJob
            soil_data: Dictionary with soil data from spatial join
        """
        try:
            conn = snowflake.connector.connect(**self.snowflake_config)
            cursor = conn.cursor()
            
            insert_sql = """
            INSERT INTO RAW.SOIL_PROFILE (
                PARCEL_ID,
                MUKEY,
                MUSYM,
                MUNAME,
                MUKIND,
                MUSTATUS,
                SLOPE_GRAD_DCP,
                SLOPE_GRAD_WTA,
                BEDROCK_DEPTH_MIN,
                WATER_TABLE_DEPTH_ANNUAL_MIN,
                WATER_TABLE_DEPTH_APRIL_JUNE_MIN,
                FLOOD_FREQ_DCD,
                FLOOD_FREQ_MAX,
                POND_FREQ_PRS,
                AWS_025_WTA,
                AWS_050_WTA,
                AWS_0100_WTA,
                AWS_0150_WTA,
                DRAINAGE_CLASS_DCD,
                DRAINAGE_CLASS_WETTEST,
                HYDROLOGIC_GROUP_DCD,
                IRRIGATED_CAPABILITY_CLASS_DCD,
                IRRIGATED_CAPABILITY_CLASS_PCT,
                NON_IRRIGATED_CAPABILITY_CLASS_DCD,
                NON_IRRIGATED_CAPABILITY_CLASS_PCT,
                OVERLAP_RATIO,
                ALL_INTERSECTING_SOILS,
                CREATED_AT
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP())
            """
            
            cursor.execute(insert_sql, (
                parcel_job.parcel_id,
                soil_data.get('mukey'),
                soil_data.get('musym'),
                soil_data.get('muname'),
                soil_data.get('mukind'),
                soil_data.get('mustatus'),
                soil_data.get('slope_grad_dcp'),
                soil_data.get('slope_grad_wta'),
                soil_data.get('bedrock_depth_min'),
                soil_data.get('water_table_depth_annual_min'),
                soil_data.get('water_table_depth_april_june_min'),
                soil_data.get('flood_freq_dcd'),
                soil_data.get('flood_freq_max'),
                soil_data.get('pond_freq_prs'),
                soil_data.get('aws_025_wta'),
                soil_data.get('aws_050_wta'),
                soil_data.get('aws_0100_wta'),
                soil_data.get('aws_0150_wta'),
                soil_data.get('drainage_class_dcd'),
                soil_data.get('drainage_class_wettest'),
                soil_data.get('hydrologic_group_dcd'),
                soil_data.get('irrigated_capability_class_dcd'),
                soil_data.get('irrigated_capability_class_pct'),
                soil_data.get('non_irrigated_capability_class_dcd'),
                soil_data.get('non_irrigated_capability_class_pct'),
                soil_data.get('overlap_ratio'),
                json.dumps(soil_data.get('all_intersecting_soils', []))
            ))
            
            conn.commit()
            conn.close()
            logger.info(f"Saved soil enrichment for parcel {parcel_job.parcel_id}")
            
        except Exception as e:
            logger.error(f"Error saving soil enrichment for parcel {parcel_job.parcel_id}: {e}")
    
    def trigger_enrichment_lambdas(self, parcel_job: ParcelEnrichmentJob, soil_data: Dict):
        """
        Trigger downstream enrichment Lambda functions
        
        Args:
            parcel_job: ParcelEnrichmentJob
            soil_data: Dictionary with soil data
        """
        enrichment_payload = {
            'parcel_id': parcel_job.parcel_id,
            'latitude': parcel_job.latitude,
            'longitude': parcel_job.longitude,
            'county_fips': parcel_job.county_fips,
            'state_code': parcel_job.state_code,
            'acres': parcel_job.acres,
            'geometry_wkt': parcel_job.geometry_wkt,
            'soil_mukey': soil_data.get('mukey') if soil_data else None,
            'trigger_timestamp': datetime.now().isoformat()
        }
        
        # List of enrichment Lambda functions to trigger
        enrichment_functions = [
            'teddy-crop-history-enrichment',
            'teddy-climate-data-enrichment', 
            'teddy-topography-enrichment',
            'teddy-land-cover-enrichment',
            'teddy-landid-maps-enrichment'
        ]
        
        for function_name in enrichment_functions:
            try:
                response = self.lambda_client.invoke(
                    FunctionName=function_name,
                    InvocationType='Event',  # Async invocation
                    Payload=json.dumps(enrichment_payload)
                )
                logger.info(f"Triggered {function_name} for parcel {parcel_job.parcel_id}")
                
            except Exception as e:
                logger.error(f"Error triggering {function_name} for parcel {parcel_job.parcel_id}: {e}")
    
    def send_enrichment_event(self, parcel_job: ParcelEnrichmentJob, enrichment_status: str):
        """
        Send enrichment completion event to EventBridge
        
        Args:
            parcel_job: ParcelEnrichmentJob
            enrichment_status: Status of enrichment (success/failure)
        """
        try:
            event_detail = {
                'parcel_id': parcel_job.parcel_id,
                'enrichment_status': enrichment_status,
                'timestamp': datetime.now().isoformat(),
                'county_fips': parcel_job.county_fips,
                'state_code': parcel_job.state_code
            }
            
            response = self.eventbridge_client.put_events(
                Entries=[
                    {
                        'Source': 'teddy.datapipeline',
                        'DetailType': 'Parcel Enrichment Complete',
                        'Detail': json.dumps(event_detail),
                        'EventBusName': 'default'
                    }
                ]
            )
            
            logger.info(f"Sent enrichment event for parcel {parcel_job.parcel_id}")
            
        except Exception as e:
            logger.error(f"Error sending enrichment event for parcel {parcel_job.parcel_id}: {e}")
    
    def enrich_single_parcel(self, parcel_job: ParcelEnrichmentJob) -> bool:
        """
        Enrich a single parcel with all spatial data
        
        Args:
            parcel_job: ParcelEnrichmentJob to process
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Starting enrichment for parcel {parcel_job.parcel_id}")
            
            # Step 1: Perform spatial join with SSURGO soil data
            soil_data = self.perform_spatial_soil_join(parcel_job)
            
            if soil_data:
                # Step 2: Save soil enrichment data
                self.save_soil_enrichment(parcel_job, soil_data)
                
                # Step 3: Trigger downstream enrichment Lambda functions
                self.trigger_enrichment_lambdas(parcel_job, soil_data)
                
                # Step 4: Send completion event
                self.send_enrichment_event(parcel_job, 'success')
                
                logger.info(f"Successfully enriched parcel {parcel_job.parcel_id}")
                return True
            else:
                logger.warning(f"No soil data found for parcel {parcel_job.parcel_id}")
                self.send_enrichment_event(parcel_job, 'no_soil_data')
                return False
                
        except Exception as e:
            logger.error(f"Error enriching parcel {parcel_job.parcel_id}: {e}")
            self.send_enrichment_event(parcel_job, 'failure')
            return False
    
    def run_enrichment_batch(self, max_workers: int = 5, lookback_hours: int = 1):
        """
        Run enrichment for a batch of newly ingested parcels
        
        Args:
            max_workers: Maximum number of concurrent workers
            lookback_hours: How far back to look for new parcels
        """
        logger.info("Starting parcel enrichment batch process")
        
        # Detect new parcels needing enrichment
        parcel_jobs = self.detect_new_parcels(lookback_hours)
        
        if not parcel_jobs:
            logger.info("No parcels found needing enrichment")
            return
        
        # Process parcels concurrently
        successful_enrichments = 0
        failed_enrichments = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_parcel = {
                executor.submit(self.enrich_single_parcel, job): job 
                for job in parcel_jobs
            }
            
            for future in concurrent.futures.as_completed(future_to_parcel):
                parcel_job = future_to_parcel[future]
                try:
                    success = future.result()
                    if success:
                        successful_enrichments += 1
                    else:
                        failed_enrichments += 1
                except Exception as e:
                    logger.error(f"Exception in enrichment for parcel {parcel_job.parcel_id}: {e}")
                    failed_enrichments += 1
        
        logger.info(f"Enrichment batch complete: {successful_enrichments} successful, {failed_enrichments} failed")

def lambda_handler(event, context):
    """
    AWS Lambda handler for parcel enrichment orchestration
    Can be triggered by EventBridge, S3 events, or scheduled execution
    """
    try:
        # Configuration from environment variables
        snowflake_config = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': 'BIRDDOG_DATA',
            'schema': 'RAW'
        }
        
        aws_config = {
            'region': os.getenv('AWS_REGION', 'us-east-1')
        }
        
        # Initialize orchestrator
        orchestrator = ParcelEnrichmentOrchestrator(snowflake_config, aws_config)
        
        # Get lookback hours from event or default to 1
        lookback_hours = event.get('lookback_hours', 1)
        max_workers = event.get('max_workers', 5)
        
        # Run enrichment batch
        orchestrator.run_enrichment_batch(max_workers, lookback_hours)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Parcel enrichment orchestration completed successfully',
                'timestamp': datetime.now().isoformat()
            })
        }
        
    except Exception as e:
        logger.error(f"Error in lambda handler: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
        }

def main():
    """Main execution for local testing"""
    # Configuration for local testing
    snowflake_config = {
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': 'BIRDDOG_DATA',
        'schema': 'RAW'
    }
    
    aws_config = {
        'region': 'us-east-1'
    }
    
    # Initialize and run orchestrator
    orchestrator = ParcelEnrichmentOrchestrator(snowflake_config, aws_config)
    orchestrator.run_enrichment_batch(max_workers=3, lookback_hours=24)

if __name__ == "__main__":
    main()
