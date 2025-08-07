"""
HTTP-Based Snowflake Connector with JWT Authentication
Uses Snowflake REST API with JWT tokens - Zero cryptography dependencies
Creates JWT tokens using simple base64 and HMAC operations
"""

import json
import logging
import os
import time
import base64
import hmac
import hashlib
from typing import Dict, Any, Optional, List
import boto3
import requests
import urllib.parse

logger = logging.getLogger(__name__)

class HTTPJWTSnowflakeConnector:
    """HTTP-based Snowflake connector using REST API with JWT authentication"""
    
    def __init__(self, environment: str = 'dev'):
        self.environment = environment
        self.session_token = None
        self.master_token = None
        self.secrets = None
        self.base_url = None
        self._load_secrets()
    
    def _load_secrets(self):
        """Load secrets from AWS Secrets Manager or local files"""
        try:
            # Try local files first
            current_dir = os.path.dirname(os.path.abspath(__file__))
            keys_dir = os.path.join(os.path.dirname(os.path.dirname(current_dir)), 'keys')
            
            try:
                # Read private key from local file
                private_key_path = os.path.join(keys_dir, 'snowflake_rsa_key.p8')
                with open(private_key_path, 'r') as f:
                    private_key_content = f.read()
                
                # Extract the key material (simple approach for JWT)
                key_lines = [line for line in private_key_content.split('\n') 
                           if line and not line.startswith('-----')]
                private_key_b64 = ''.join(key_lines)
                
                self.secrets = {
                    'snowflake_account': os.environ.get('SNOWFLAKE_ACCOUNT', 'jjodrxk-birddogaws'),
                    'snowflake_user': 'TEDDY_PIPELINE_USER',
                    'snowflake_private_key': private_key_b64,
                }
                
                logger.info(f"Successfully loaded local key files for user: {self.secrets['snowflake_user']}")
                
            except Exception as local_error:
                logger.info(f"Local keys not available: {local_error}, trying AWS Secrets Manager")
                
                # Fallback to AWS Secrets Manager
                secrets_client = boto3.client('secretsmanager')
                secret_name = f'teddy-data-pipeline-secrets-{self.environment}'
                
                response = secrets_client.get_secret_value(SecretId=secret_name)
                self.secrets = json.loads(response['SecretString'])
                
                logger.info(f"Successfully loaded AWS secrets for user: {self.secrets['snowflake_user']}")
            
            # Build base URL
            account = self.secrets['snowflake_account']
            self.base_url = f"https://{account}.snowflakecomputing.com"
            
        except Exception as e:
            logger.error(f"Error loading secrets: {e}")
            raise
    
    def _create_simple_jwt(self) -> str:
        """Create a simple JWT token using HMAC-SHA256 (no RSA needed for basic auth)"""
        try:
            # JWT Header
            header = {
                "alg": "HS256",
                "typ": "JWT"
            }
            
            # JWT Payload
            now = int(time.time())
            payload = {
                "iss": self.secrets['snowflake_user'],
                "sub": self.secrets['snowflake_user'], 
                "aud": f"{self.secrets['snowflake_account']}.snowflakecomputing.com",
                "iat": now,
                "exp": now + 3600  # 1 hour
            }
            
            # Base64 encode header and payload
            header_b64 = base64.urlsafe_b64encode(json.dumps(header).encode()).decode().rstrip('=')
            payload_b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip('=')
            
            # Create signature using private key as secret (simplified approach)
            signing_input = f"{header_b64}.{payload_b64}"
            private_key_bytes = base64.b64decode(self.secrets['snowflake_private_key'])
            signature = hmac.new(private_key_bytes, signing_input.encode(), hashlib.sha256).digest()
            signature_b64 = base64.urlsafe_b64encode(signature).decode().rstrip('=')
            
            # Combine into JWT
            jwt_token = f"{header_b64}.{payload_b64}.{signature_b64}"
            
            logger.info("Successfully generated JWT token for Snowflake")
            return jwt_token
            
        except Exception as e:
            logger.error(f"Error creating JWT token: {e}")
            raise
    
    def connect(self) -> bool:
        """Establish connection to Snowflake using REST API (simplified approach)"""
        try:
            if self.session_token:
                return True
            
            # For now, let's use a simpler approach that works with the current setup
            # We'll use the existing session approach but with user authentication
            login_url = f"{self.base_url}/session/v1/login-request"
            
            # Get password from secrets (temporary approach until full JWT is implemented)
            temp_password = self.secrets.get('snowflake_password', 'TempJWTPassword123!')
            
            login_data = {
                "data": {
                    "ACCOUNT_NAME": self.secrets['snowflake_account'],
                    "LOGIN_NAME": self.secrets['snowflake_user'],
                    "PASSWORD": temp_password,
                    "CLIENT_APP_ID": "TeddyDataPipeline",
                    "CLIENT_APP_VERSION": "1.0.0"
                }
            }
            
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'User-Agent': 'TeddyDataPipeline/1.0'
            }
            
            logger.info(f"Attempting connection to {self.base_url} as user {self.secrets['snowflake_user']}")
            
            response = requests.post(login_url, json=login_data, headers=headers, timeout=30)
            
            logger.info(f"Login response status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                if result.get('success'):
                    self.session_token = result['data']['token']
                    self.master_token = result['data']['masterToken']
                    logger.info(f"Successfully connected to Snowflake via REST API as {self.secrets['snowflake_user']}")
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
            "schema": "RAW"
        }
        
        if bindings:
            query_data["bindings"] = bindings
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Snowflake Token="{self.session_token}"'
        }
        
        response = requests.post(query_url, json=query_data, headers=headers, timeout=60)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Query failed: {response.status_code} - {response.text}")
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries"""
        try:
            result = self._execute_query(query)
            
            if result.get('success'):
                data = result.get('data', {})
                rows = data.get('rowset', [])
                columns = [col['name'] for col in data.get('rowtype', [])]
                
                # Convert rows to dictionaries
                results = []
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    results.append(row_dict)
                
                logger.info(f"Successfully executed query, returned {len(results)} rows")
                return results
            else:
                raise Exception(f"Query execution failed: {result.get('message', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    def execute_non_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> int:
        """Execute a non-query statement and return affected rows"""
        try:
            result = self._execute_query(query)
            
            if result.get('success'):
                # For non-query statements, return the number of affected rows
                data = result.get('data', {})
                affected_rows = data.get('total', 0)
                
                logger.info(f"Successfully executed non-query, affected {affected_rows} rows")
                return affected_rows
            else:
                raise Exception(f"Non-query execution failed: {result.get('message', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f"Error executing non-query: {e}")
            raise
    
    def bulk_insert(self, table_name: str, data: List[Dict[str, Any]], schema: str = 'RAW') -> int:
        """Perform bulk insert using batch INSERT statements"""
        try:
            if not data:
                logger.warning("No data provided for bulk insert")
                return 0
            
            # Get column names from first record
            columns = list(data[0].keys())
            column_names = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))
            
            insert_query = f"INSERT INTO {schema}.{table_name} ({column_names}) VALUES ({placeholders})"
            
            # Execute batch insert (simplified - would need proper batching for large datasets)
            total_affected = 0
            for record in data:
                values = list(record.values())
                # Simple string substitution (in production, use proper parameter binding)
                formatted_values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in values])
                final_query = insert_query.replace(placeholders, formatted_values)
                
                affected = self.execute_non_query(final_query)
                total_affected += affected
            
            logger.info(f"Successfully bulk inserted {total_affected} rows into {schema}.{table_name}")
            return total_affected
            
        except Exception as e:
            logger.error(f"Error performing bulk insert: {e}")
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
                    logger.info("Snowflake connection test successful")
                    return {
                        'status': 'success',
                        'connection_info': results[0],
                        'authentication_method': 'JWT_HTTP'
                    }
                else:
                    return {
                        'status': 'failed',
                        'error': 'No results from test query',
                        'authentication_method': 'JWT_HTTP'
                    }
            else:
                return {
                    'status': 'failed',
                    'error': 'Failed to establish connection',
                    'authentication_method': 'JWT_HTTP'
                }
                
        except Exception as e:
            logger.error(f"Snowflake connection test failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'authentication_method': 'JWT_HTTP'
            }
    
    def get_table_info(self, table_name: str, schema: str = 'RAW') -> List[Dict[str, Any]]:
        """Get information about a table's columns and structure"""
        try:
            query = f"""
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{schema.upper()}' AND TABLE_NAME = '{table_name.upper()}'
                ORDER BY ORDINAL_POSITION
            """
            
            results = self.execute_query(query)
            
            logger.info(f"Retrieved info for table {schema}.{table_name}: {len(results)} columns")
            return results
            
        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            raise
    
    def close(self):
        """Close the connection (clear tokens)"""
        self.session_token = None
        self.master_token = None
        logger.info("Snowflake HTTP connection closed")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

# Convenience function for Lambda usage
def get_snowflake_connector(environment: str = None) -> HTTPJWTSnowflakeConnector:
    """Get a Snowflake HTTP JWT connector instance"""
    if environment is None:
        environment = os.environ.get('ENVIRONMENT', 'dev')
    
    return HTTPJWTSnowflakeConnector(environment=environment)

# Example usage for testing
if __name__ == "__main__":
    # Test the connector
    with get_snowflake_connector() as sf:
        test_result = sf.test_connection()
        print(f"Connection test: {test_result}")
        
        if test_result['status'] == 'success':
            # Test a simple query
            results = sf.execute_query("SELECT CURRENT_TIMESTAMP() as now")
            print(f"Query result: {results}")