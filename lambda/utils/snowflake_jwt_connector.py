"""
Native Snowflake JWT Connector
Based on the working pattern from birddog-geodata-viewer project
"""

import json
import logging
import os
from typing import Dict, Any, Optional, List
import boto3
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector

logger = logging.getLogger(__name__)

class SnowflakeJWTConnector:
    """Native Snowflake connector using JWT authentication"""
    
    def __init__(self, environment: str = 'dev'):
        self.environment = environment
        self.connection = None
        self.secrets = None
        self._load_secrets()
    
    def _load_secrets(self):
        """Load secrets from AWS Secrets Manager"""
        try:
            secrets_client = boto3.client('secretsmanager')
            secret_name = f'teddy-data-pipeline-secrets-{self.environment}'
            
            response = secrets_client.get_secret_value(SecretId=secret_name)
            self.secrets = json.loads(response['SecretString'])
            
            logger.info(f"Successfully loaded secrets for environment: {self.environment}")
            
        except Exception as e:
            logger.error(f"Error loading secrets: {e}")
            raise
    
    def _load_private_key(self):
        """Load and parse RSA private key"""
        try:
            # Use AWS Secrets Manager (works in both Lambda and local environments)
            private_key_base64 = self.secrets.get('snowflake_private_key')
            if private_key_base64:
                import base64
                private_key_data = base64.b64decode(private_key_base64)
            else:
                # Fall back to local file only for development
                private_key_path = 'keys/snowflake_rsa_key.p8'
                if os.path.exists(private_key_path):
                    with open(private_key_path, 'rb') as key_file:
                        private_key_data = key_file.read()
                else:
                    raise Exception("No private key found in secrets or local file")
            
            # Parse the private key
            private_key = serialization.load_pem_private_key(
                private_key_data,
                password=None,
                backend=default_backend()
            )
            
            # Convert to DER format for Snowflake
            pkcs8_key = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            return pkcs8_key
            
        except Exception as e:
            logger.error(f"Error loading private key: {e}")
            raise
    
    def connect(self) -> bool:
        """Establish connection to Snowflake using JWT authentication"""
        try:
            if self.connection:
                return True
            
            # Load private key
            private_key = self._load_private_key()
            
            # Connection configuration - following geodata-viewer pattern
            config = {
                'account': self.secrets['snowflake_account'],
                'user': self.secrets['snowflake_user'],
                'warehouse': self.secrets.get('snowflake_warehouse', 'TEDDY_INGESTION_WH'),
                'database': self.secrets.get('snowflake_database', 'TEDDY_DATA'),
                'schema': self.secrets.get('snowflake_schema', 'RAW'),
                'authenticator': 'SNOWFLAKE_JWT',  # Critical setting
                'private_key': private_key,        # DER format key
                'password': None                   # Explicit None
            }
            
            logger.info(f"Connecting to Snowflake with JWT auth for user: {config['user']}")
            
            self.connection = snowflake.connector.connect(**config)
            
            logger.info("Successfully connected to Snowflake via JWT authentication")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            return False
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries"""
        try:
            if not self.connect():
                raise Exception("Failed to connect to Snowflake")
            
            cursor = self.connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch results
            rows = cursor.fetchall()
            
            # Convert to dictionaries
            results = []
            for row in rows:
                row_dict = dict(zip(columns, row))
                results.append(row_dict)
            
            cursor.close()
            
            logger.info(f"Successfully executed query, returned {len(results)} rows")
            return results
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    def test_connection(self) -> Dict[str, Any]:
        """Test the Snowflake connection and return connection info"""
        try:
            if self.connect():
                # Test query to get current user and warehouse info
                results = self.execute_query("""
                    SELECT 
                        CURRENT_USER() as current_user,
                        CURRENT_WAREHOUSE() as current_warehouse,
                        CURRENT_DATABASE() as current_database,
                        CURRENT_SCHEMA() as current_schema,
                        CURRENT_TIMESTAMP() as connection_time
                """)
                
                if results:
                    logger.info("Snowflake JWT connection test successful")
                    return {
                        'status': 'success',
                        'connection_info': results[0],
                        'authentication_method': 'JWT'
                    }
                else:
                    return {
                        'status': 'failed',
                        'error': 'No results from test query',
                        'authentication_method': 'JWT'
                    }
            else:
                return {
                    'status': 'failed',
                    'error': 'Failed to establish connection',
                    'authentication_method': 'JWT'
                }
                
        except Exception as e:
            logger.error(f"Snowflake JWT connection test failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'authentication_method': 'JWT'
            }
    
    def insert_raw_data(self, s3_key: str, county: str, state: str, data: Any) -> int:
        """Insert raw data into Snowflake using the exact schema structure"""
        try:
            # Store the complete API response data (now that we have working VARIANT insertion)
            complete_data = data
            
            # Use the working pattern from birddog-datascraping-service: SELECT with PARSE_JSON
            json_data = json.dumps(complete_data, default=str)
            file_name = s3_key.split('/')[-1]
            county_clean = (county.upper() if county else 'UNKNOWN')
            
            cursor = self.connection.cursor()
            
            # Use SELECT with PARSE_JSON (this works unlike VALUES with PARSE_JSON)
            insert_sql = """
            INSERT INTO PARCEL_DATA_RAW (
                FILE_NAME, 
                FILE_ROW_NUMBER, 
                INGESTION_TYPE, 
                COUNTY, 
                CHUNK_INDEX, 
                PARCEL_COUNT, 
                RAW_DATA
            ) 
            SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
            """
            
            cursor.execute(insert_sql, (
                file_name,
                1,
                'api',
                county_clean, 
                1,
                1,
                json_data
            ))
            cursor.close()
            
            logger.info("Successfully inserted raw data via JWT connector")
            return 1
            
        except Exception as e:
            logger.error(f"Error inserting raw data: {e}")
            raise
    
    def close(self):
        """Close the connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Snowflake JWT connection closed")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

# Convenience function for Lambda usage
def get_snowflake_jwt_connector(environment: str = None) -> SnowflakeJWTConnector:
    """Get a Snowflake JWT connector instance"""
    if environment is None:
        environment = os.environ.get('ENVIRONMENT', 'dev')
    
    return SnowflakeJWTConnector(environment=environment)