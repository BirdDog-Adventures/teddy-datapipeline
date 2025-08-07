"""
HTTP-Based Snowflake Connector - Zero Cryptography Dependencies
Uses Snowflake REST API with password authentication
"""

import json
import logging
import os
import time
from typing import Dict, Any, Optional, List
import boto3
import requests
import urllib.parse

logger = logging.getLogger(__name__)

class SnowflakeConnector:
    """HTTP-based Snowflake connector using REST API"""
    
    def __init__(self, environment: str = 'dev'):
        self.environment = environment
        self.session_token = None
        self.master_token = None
        self.secrets = None
        self.base_url = None
        self._load_secrets()
    
    def _load_secrets(self):
        """Load secrets from AWS Secrets Manager"""
        try:
            secrets_client = boto3.client('secretsmanager')
            secret_name = f'teddy-data-pipeline-secrets-{self.environment}'
            
            response = secrets_client.get_secret_value(SecretId=secret_name)
            self.secrets = json.loads(response['SecretString'])
            
            # Build base URL
            account = self.secrets['snowflake_account']
            self.base_url = f"https://{account}.snowflakecomputing.com"
            
            logger.info(f"Successfully loaded secrets for environment: {self.environment}")
            
        except Exception as e:
            logger.error(f"Error loading secrets: {e}")
            raise
    
    def connect(self) -> bool:
        """Establish connection to Snowflake using REST API"""
        try:
            if self.session_token:
                return True
            
            # Login request
            login_url = f"{self.base_url}/session/v1/login-request"
            
            login_data = {
                "data": {
                    "ACCOUNT_NAME": self.secrets['snowflake_account'],
                    "LOGIN_NAME": self.secrets['snowflake_user'],
                    "PASSWORD": self.secrets['snowflake_password'],
                    "CLIENT_APP_ID": "PythonConnector",
                    "CLIENT_APP_VERSION": "1.0.0"
                }
            }
            
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'User-Agent': 'TeddyDataPipeline/1.0'
            }
            
            response = requests.post(login_url, json=login_data, headers=headers, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    self.session_token = result['data']['token']
                    self.master_token = result['data']['masterToken']
                    logger.info("Successfully connected to Snowflake via REST API")
                    return True
                else:
                    logger.error(f"Login failed: {result.get('message', 'Unknown error')}")
                    return False
            else:
                logger.error(f"HTTP error during login: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            return False
    
    def _execute_query(self, query: str, bindings: List = None) -> Dict:
        """Execute SQL query via REST API"""
        if not self.connect():
            raise Exception("Failed to connect to Snowflake")
        
        query_url = f"{self.base_url}/queries/v1/query-request"
        
        query_data = {
            "sqlText": query,
            "warehouse": "TEDDY_INGESTION_WH",
            "database": "TEDDY_DATA",
            "schema": "RAW",
            "bindings": bindings or []
        }
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Snowflake Token="{self.session_token}"',
            'User-Agent': 'TeddyDataPipeline/1.0'
        }
        
        response = requests.post(query_url, json=query_data, headers=headers, timeout=60)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Query failed: {response.status_code} - {response.text}")
    
    def insert_raw_data(self, s3_key: str, county: str, state: str, data: Dict[str, Any]) -> int:
        """Insert raw data into Snowflake"""
        try:
            # Create table if not exists
            create_table_query = """
            CREATE TABLE IF NOT EXISTS PARCEL_DATA_RAW (
                FILE_NAME VARCHAR(500),
                JSON_PAYLOAD VARIANT,
                INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                S3_BUCKET VARCHAR(100),
                S3_KEY VARCHAR(500),
                COUNTY VARCHAR(100),
                STATE VARCHAR(100)
            )
            """
            
            self._execute_query(create_table_query)
            
            # Insert data
            insert_query = """
            INSERT INTO PARCEL_DATA_RAW (FILE_NAME, JSON_PAYLOAD, S3_KEY, COUNTY, STATE, INGESTION_TIMESTAMP)
            VALUES (?, PARSE_JSON(?), ?, ?, ?, CURRENT_TIMESTAMP())
            """
            
            bindings = [s3_key, json.dumps(data), s3_key, county, state]
            result = self._execute_query(insert_query, bindings)
            
            if result.get('success'):
                logger.info("Successfully inserted raw data")
                return 1
            else:
                raise Exception(f"Insert failed: {result.get('message', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"Error inserting raw data: {e}")
            raise
    
    def insert_staging_data(self, county: str, state: str, parcels: List[Dict[str, Any]]) -> int:
        """Insert staging data into Snowflake"""
        try:
            # Create staging table if not exists
            create_table_query = """
            CREATE TABLE IF NOT EXISTS PARCEL_STAGING (
                PARCEL_ID VARCHAR(100),
                COUNTY VARCHAR(100),
                STATE VARCHAR(100),
                PARCEL_DATA VARIANT,
                INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
            
            self._execute_query(create_table_query)
            
            # Insert parcels
            inserted_count = 0
            for parcel in parcels:
                parcel_id = parcel.get('id', parcel.get('parcel_id', 'unknown'))
                
                insert_query = """
                INSERT INTO PARCEL_STAGING (PARCEL_ID, COUNTY, STATE, PARCEL_DATA, INGESTION_TIMESTAMP)
                VALUES (?, ?, ?, PARSE_JSON(?), CURRENT_TIMESTAMP())
                """
                
                bindings = [parcel_id, county, state, json.dumps(parcel)]
                result = self._execute_query(insert_query, bindings)
                
                if result.get('success'):
                    inserted_count += 1
                else:
                    logger.warning(f"Failed to insert parcel {parcel_id}")
            
            logger.info(f"Successfully inserted {inserted_count} staging records")
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error inserting staging data: {e}")
            raise
    
    def close(self):
        """Close the Snowflake connection"""
        try:
            if self.session_token:
                # Logout request
                logout_url = f"{self.base_url}/session/logout-request"
                headers = {
                    'Authorization': f'Snowflake Token="{self.session_token}"',
                    'User-Agent': 'TeddyDataPipeline/1.0'
                }
                
                requests.post(logout_url, headers=headers, timeout=10)
                self.session_token = None
                self.master_token = None
                logger.info("Snowflake connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
