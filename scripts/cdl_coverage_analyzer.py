#!/usr/bin/env python3
"""
CDL Coverage Analyzer

This script analyzes which parcels in your database are likely to have
CDL (Cropland Data Layer) coverage and provides alternative data sources
for parcels outside CDL coverage.
"""

import os
import sys
import json
import logging
import requests
import snowflake.connector
from datetime import datetime
from typing import List, Dict, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CDLCoverageAnalyzer:
    """Analyze CDL coverage for parcels and suggest alternatives"""
    
    def __init__(self):
        self.snowflake_config = self._get_snowflake_config()
        
        # CDL Coverage Information
        self.cdl_coverage = {
            # States with full CDL coverage (agricultural focus)
            'full_coverage': [
                'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'IA', 
                'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 
                'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 
                'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 
                'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY'
            ],
            # States with limited or no CDL coverage
            'limited_coverage': ['AK', 'HI'],
            # Years with best CDL coverage
            'best_years': [2019, 2020, 2021, 2022, 2023],
            # CDL resolution (30 meters)
            'resolution_meters': 30
        }
        
        # Alternative data sources for non-CDL areas
        self.alternative_sources = {
            'NLCD': {
                'name': 'National Land Cover Database',
                'coverage': 'All US states',
                'resolution': '30 meters',
                'years': [2001, 2004, 2006, 2008, 2011, 2013, 2016, 2019],
                'api': 'https://www.mrlc.gov/data-services-page',
                'good_for': ['Land cover classification', 'Urban/forest/water identification']
            },
            'NASS_CENSUS': {
                'name': 'NASS Agricultural Census',
                'coverage': 'County-level US',
                'resolution': 'County aggregated',
                'years': [2007, 2012, 2017, 2022],
                'api': 'https://quickstats.nass.usda.gov/api',
                'good_for': ['County-level crop statistics', 'Agricultural trends']
            },
            'MODIS': {
                'name': 'MODIS Land Cover',
                'coverage': 'Global',
                'resolution': '500 meters',
                'years': list(range(2001, 2024)),
                'api': 'https://modis.gsfc.nasa.gov/',
                'good_for': ['Large-scale land cover', 'Time series analysis']
            },
            'LANDSAT': {
                'name': 'Landsat Analysis Ready Data',
                'coverage': 'Global',
                'resolution': '30 meters',
                'years': list(range(1985, 2024)),
                'api': 'https://www.usgs.gov/landsat-missions',
                'good_for': ['Custom land cover analysis', 'Change detection']
            }
        }
    
    def _get_snowflake_config(self):
        """Get Snowflake configuration from environment variables"""
        return {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'BIRDDOG_INGESTION_WH'),
            'database': os.getenv('SNOWFLAKE_DATABASE', 'BIRDDOG_DATA'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'CURATED')
        }
    
    def get_snowflake_connection(self):
        """Get Snowflake connection with authentication"""
        try:
            # Try private key authentication first
            private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
            if private_key_path and os.path.exists(private_key_path):
                from cryptography.hazmat.primitives import serialization
                from cryptography.hazmat.primitives.serialization import load_pem_private_key
                from cryptography.hazmat.backends import default_backend
                
                with open(private_key_path, 'rb') as key_file:
                    private_key = load_pem_private_key(
                        key_file.read(),
                        password=None,
                        backend=default_backend()
                    )
                
                private_key_bytes = private_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
                
                config = self.snowflake_config.copy()
                config['private_key'] = private_key_bytes
                
                return snowflake.connector.connect(**config)
            else:
                # Fall back to password authentication
                password = os.getenv('SNOWFLAKE_PASSWORD')
                if password:
                    config = self.snowflake_config.copy()
                    config['password'] = password
                    return snowflake.connector.connect(**config)
                else:
                    raise ValueError("No Snowflake authentication method available")
                    
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def analyze_parcel_coverage(self, limit: Optional[int] = None) -> Dict:
        """Analyze CDL coverage potential for parcels in database"""
        logger.info("Analyzing CDL coverage for parcels...")
        
        try:
            conn = self.get_snowflake_connection()
            cursor = conn.cursor()
            
            # Get parcel distribution by state
            query = """
            SELECT 
                STATE_CODE,
                COUNT(*) as parcel_count,
                COUNT(CASE WHEN LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL THEN 1 END) as with_coords,
                AVG(ACRES) as avg_acres,
                MIN(ACRES) as min_acres,
                MAX(ACRES) as max_acres,
                MIN(LATITUDE) as min_lat,
                MAX(LATITUDE) as max_lat,
                MIN(LONGITUDE) as min_lon,
                MAX(LONGITUDE) as max_lon
            FROM PARCEL_PROFILE
            WHERE STATE_CODE IS NOT NULL
            GROUP BY STATE_CODE
            ORDER BY parcel_count DESC
            """
            
            if limit:
                query = f"SELECT * FROM ({query}) LIMIT {limit}"
            
            cursor.execute(query)
            state_data = cursor.fetchall()
            
            # Analyze coverage by state
            coverage_analysis = {
                'full_cdl_coverage': [],
                'limited_cdl_coverage': [],
                'no_cdl_coverage': [],
                'total_parcels': 0,
                'parcels_with_coords': 0,
                'cdl_eligible_parcels': 0,
                'alternative_needed_parcels': 0
            }
            
            for row in state_data:
                state_code = row[0]
                parcel_count = row[1]
                with_coords = row[2]
                avg_acres = float(row[3]) if row[3] else 0
                min_acres = float(row[4]) if row[4] else 0
                max_acres = float(row[5]) if row[5] else 0
                min_lat = float(row[6]) if row[6] else 0
                max_lat = float(row[7]) if row[7] else 0
                min_lon = float(row[8]) if row[8] else 0
                max_lon = float(row[9]) if row[9] else 0
                
                coverage_analysis['total_parcels'] += parcel_count
                coverage_analysis['parcels_with_coords'] += with_coords
                
                state_info = {
                    'state': state_code,
                    'parcel_count': parcel_count,
                    'with_coordinates': with_coords,
                    'avg_acres': avg_acres,
                    'min_acres': min_acres,
                    'max_acres': max_acres,
                    'lat_range': [min_lat, max_lat],
                    'lon_range': [min_lon, max_lon],
                    'cdl_coverage_likelihood': self._assess_cdl_likelihood(state_code, min_lat, max_lat, min_lon, max_lon, avg_acres)
                }
                
                if state_code in self.cdl_coverage['full_coverage']:
                    coverage_analysis['full_cdl_coverage'].append(state_info)
                    coverage_analysis['cdl_eligible_parcels'] += with_coords
                elif state_code in self.cdl_coverage['limited_coverage']:
                    coverage_analysis['limited_cdl_coverage'].append(state_info)
                    coverage_analysis['alternative_needed_parcels'] += with_coords
                else:
                    coverage_analysis['no_cdl_coverage'].append(state_info)
                    coverage_analysis['alternative_needed_parcels'] += with_coords
            
            return coverage_analysis
            
        except Exception as e:
            logger.error(f"Error analyzing parcel coverage: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def _assess_cdl_likelihood(self, state_code: str, min_lat: float, max_lat: float, 
                              min_lon: float, max_lon: float, avg_acres: float) -> Dict:
        """Assess likelihood of CDL coverage for parcels in this state/region"""
        
        likelihood = {
            'overall_score': 0,  # 0-100
            'factors': [],
            'recommendations': []
        }
        
        # State coverage factor
        if state_code in self.cdl_coverage['full_coverage']:
            likelihood['overall_score'] += 40
            likelihood['factors'].append(f"State {state_code} has full CDL coverage")
        elif state_code in self.cdl_coverage['limited_coverage']:
            likelihood['overall_score'] += 10
            likelihood['factors'].append(f"State {state_code} has limited CDL coverage")
        else:
            likelihood['factors'].append(f"State {state_code} not in CDL coverage list")
        
        # Geographic factors
        # CDL focuses on agricultural regions - check if coordinates are in agricultural areas
        if state_code in ['IA', 'IL', 'IN', 'NE', 'KS', 'MN', 'MO', 'OH', 'WI']:
            likelihood['overall_score'] += 30
            likelihood['factors'].append("Located in major agricultural region")
        elif state_code in ['TX', 'CA', 'FL', 'GA', 'NC', 'AR', 'MS', 'AL']:
            likelihood['overall_score'] += 20
            likelihood['factors'].append("Located in mixed agricultural region")
        
        # Parcel size factor (CDL works better for larger agricultural parcels)
        if avg_acres > 100:
            likelihood['overall_score'] += 20
            likelihood['factors'].append(f"Large average parcel size ({avg_acres:.1f} acres)")
        elif avg_acres > 10:
            likelihood['overall_score'] += 10
            likelihood['factors'].append(f"Medium average parcel size ({avg_acres:.1f} acres)")
        else:
            likelihood['factors'].append(f"Small average parcel size ({avg_acres:.1f} acres) - CDL may be less accurate")
        
        # Latitude factor (CDL coverage is best in continental US)
        if 25 <= max_lat <= 49 and -125 <= min_lon <= -66:
            likelihood['overall_score'] += 10
            likelihood['factors'].append("Coordinates within continental US CDL coverage area")
        else:
            likelihood['factors'].append("Coordinates may be outside optimal CDL coverage area")
        
        # Generate recommendations
        if likelihood['overall_score'] >= 70:
            likelihood['recommendations'].append("High CDL success probability - proceed with CDL loader")
            likelihood['recommendations'].append("Expected success rate: 80-95%")
        elif likelihood['overall_score'] >= 40:
            likelihood['recommendations'].append("Moderate CDL success probability - try CDL but prepare alternatives")
            likelihood['recommendations'].append("Expected success rate: 40-80%")
            likelihood['recommendations'].append("Consider NLCD for non-agricultural parcels")
        else:
            likelihood['recommendations'].append("Low CDL success probability - use alternative data sources")
            likelihood['recommendations'].append("Recommended: NLCD for land cover, NASS Census for agricultural data")
            likelihood['recommendations'].append("Consider Landsat/MODIS for custom analysis")
        
        return likelihood
    
    def get_alternative_recommendations(self, coverage_analysis: Dict) -> Dict:
        """Get recommendations for alternative data sources"""
        
        recommendations = {
            'primary_strategy': '',
            'data_sources': [],
            'implementation_priority': [],
            'expected_coverage': {}
        }
        
        total_parcels = coverage_analysis['parcels_with_coords']
        cdl_eligible = coverage_analysis['cdl_eligible_parcels']
        alternative_needed = coverage_analysis['alternative_needed_parcels']
        
        if cdl_eligible / total_parcels > 0.8:
            recommendations['primary_strategy'] = 'CDL_FOCUSED'
            recommendations['data_sources'].append({
                'source': 'USDA_CDL',
                'coverage_percent': (cdl_eligible / total_parcels) * 100,
                'use_case': 'Primary crop history data',
                'priority': 1
            })
            recommendations['data_sources'].append({
                'source': 'NLCD',
                'coverage_percent': 100,
                'use_case': 'Fallback for non-agricultural parcels',
                'priority': 2
            })
        elif cdl_eligible / total_parcels > 0.4:
            recommendations['primary_strategy'] = 'HYBRID_APPROACH'
            recommendations['data_sources'].append({
                'source': 'USDA_CDL',
                'coverage_percent': (cdl_eligible / total_parcels) * 100,
                'use_case': 'Agricultural parcels in covered states',
                'priority': 1
            })
            recommendations['data_sources'].append({
                'source': 'NLCD',
                'coverage_percent': 100,
                'use_case': 'All parcels for land cover classification',
                'priority': 1
            })
            recommendations['data_sources'].append({
                'source': 'NASS_CENSUS',
                'coverage_percent': 100,
                'use_case': 'County-level agricultural statistics',
                'priority': 2
            })
        else:
            recommendations['primary_strategy'] = 'ALTERNATIVE_FOCUSED'
            recommendations['data_sources'].append({
                'source': 'NLCD',
                'coverage_percent': 100,
                'use_case': 'Primary land cover data',
                'priority': 1
            })
            recommendations['data_sources'].append({
                'source': 'NASS_CENSUS',
                'coverage_percent': 100,
                'use_case': 'Agricultural statistics by county',
                'priority': 1
            })
            recommendations['data_sources'].append({
                'source': 'MODIS',
                'coverage_percent': 100,
                'use_case': 'Time series land cover analysis',
                'priority': 2
            })
        
        # Implementation priority
        if recommendations['primary_strategy'] == 'CDL_FOCUSED':
            recommendations['implementation_priority'] = [
                'Fix CDL API issues and run crop history loader',
                'Implement NLCD fallback for failed CDL queries',
                'Add county-level NASS data for context'
            ]
        elif recommendations['primary_strategy'] == 'HYBRID_APPROACH':
            recommendations['implementation_priority'] = [
                'Implement NLCD land cover loader (immediate value)',
                'Run CDL loader for agricultural states',
                'Add NASS county-level agricultural statistics',
                'Create unified land use classification system'
            ]
        else:
            recommendations['implementation_priority'] = [
                'Implement NLCD land cover loader (primary)',
                'Add NASS county-level agricultural data',
                'Consider custom Landsat analysis for detailed crop detection',
                'Skip CDL loader (low success probability)'
            ]
        
        return recommendations
    
    def generate_coverage_report(self, output_file: str = 'cdl_coverage_analysis.json'):
        """Generate comprehensive CDL coverage analysis report"""
        logger.info("Generating CDL coverage analysis report...")
        
        try:
            # Analyze coverage
            coverage_analysis = self.analyze_parcel_coverage()
            
            # Get recommendations
            recommendations = self.get_alternative_recommendations(coverage_analysis)
            
            # Create comprehensive report
            report = {
                'analysis_date': datetime.now().isoformat(),
                'summary': {
                    'total_parcels': coverage_analysis['total_parcels'],
                    'parcels_with_coordinates': coverage_analysis['parcels_with_coords'],
                    'cdl_eligible_parcels': coverage_analysis['cdl_eligible_parcels'],
                    'cdl_coverage_percentage': (coverage_analysis['cdl_eligible_parcels'] / coverage_analysis['parcels_with_coords']) * 100 if coverage_analysis['parcels_with_coords'] > 0 else 0,
                    'alternative_needed_parcels': coverage_analysis['alternative_needed_parcels'],
                    'recommended_strategy': recommendations['primary_strategy']
                },
                'coverage_by_state': {
                    'full_cdl_coverage': coverage_analysis['full_cdl_coverage'],
                    'limited_cdl_coverage': coverage_analysis['limited_cdl_coverage'],
                    'no_cdl_coverage': coverage_analysis['no_cdl_coverage']
                },
                'recommendations': recommendations,
                'alternative_data_sources': self.alternative_sources,
                'cdl_coverage_info': self.cdl_coverage
            }
            
            # Save report
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            logger.info(f"Coverage analysis report saved to {output_file}")
            return report
            
        except Exception as e:
            logger.error(f"Error generating coverage report: {e}")
            raise


def main():
    """Main function for command-line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='CDL Coverage Analyzer')
    parser.add_argument('--output', type=str, default='cdl_coverage_analysis.json', 
                       help='Output file for analysis report')
    parser.add_argument('--limit-states', type=int, help='Limit analysis to top N states by parcel count')
    
    args = parser.parse_args()
    
    try:
        analyzer = CDLCoverageAnalyzer()
        report = analyzer.generate_coverage_report(args.output)
        
        # Print summary
        print("\n" + "="*80)
        print("CDL COVERAGE ANALYSIS SUMMARY")
        print("="*80)
        print(f"Total Parcels: {report['summary']['total_parcels']:,}")
        print(f"Parcels with Coordinates: {report['summary']['parcels_with_coordinates']:,}")
        print(f"CDL Eligible Parcels: {report['summary']['cdl_eligible_parcels']:,}")
        print(f"CDL Coverage: {report['summary']['cdl_coverage_percentage']:.1f}%")
        print(f"Recommended Strategy: {report['summary']['recommended_strategy']}")
        
        print(f"\nTop States by Parcel Count:")
        all_states = (report['coverage_by_state']['full_cdl_coverage'] + 
                     report['coverage_by_state']['limited_cdl_coverage'] + 
                     report['coverage_by_state']['no_cdl_coverage'])
        all_states.sort(key=lambda x: x['parcel_count'], reverse=True)
        
        for state in all_states[:10]:
            cdl_status = "✅ Full CDL" if state['state'] in analyzer.cdl_coverage['full_coverage'] else "⚠️ Limited/No CDL"
            print(f"  {state['state']}: {state['parcel_count']:,} parcels ({state['with_coordinates']:,} with coords) - {cdl_status}")
        
        print(f"\nRecommended Implementation Priority:")
        for i, priority in enumerate(report['recommendations']['implementation_priority'], 1):
            print(f"  {i}. {priority}")
        
        print(f"\nDetailed analysis saved to: {args.output}")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    main()
