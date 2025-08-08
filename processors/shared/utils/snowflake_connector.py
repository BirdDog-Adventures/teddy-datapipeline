"""
Snowflake Connector with JWT Authentication

This module provides a secure connection to Snowflake using JWT authentication
with RSA private keys instead of password-based authentication.
"""

import json
import base64
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import boto3
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
import jwt
import snowflake.connector
from snowflake.connector import DictCursor

logger = logging.getLogger(__name__)

class SnowflakeJWTConnector:
    """
    Snowflake connector using JWT authentication with private key
    """
    
    def __init__(self, environment: str = 'dev'):
        self.environment = environment
        self.connection = None
        self.secrets = None
        self._load_secrets()
    
    def _load_secrets(self):
        """Load secrets from local key files and environment variables"""
        try:
            # Get the directory containing this script
            current_dir = os.path.dirname(os.path.abspath(__file__))
            keys_dir = os.path.join(os.path.dirname(os.path.dirname(current_dir)), 'keys')
            
            # Read the private key from local file
            private_key_path = os.path.join(keys_dir, 'snowflake_rsa_key.p8')
            with open(private_key_path, 'r') as f:
                private_key_content = f.read()
            
            # Base64 encode the private key for consistency with existing code
            private_key_b64 = base64.b64encode(private_key_content.encode('utf-8')).decode('utf-8')
            
            # Set up secrets with local configuration
            self.secrets = {
                'snowflake_account': os.environ.get('SNOWFLAKE_ACCOUNT', 'jjodrxk-birddogaws'),
                'snowflake_user': 'TEDDY_PIPELINE_USER',  # Changed to TEDDY_PIPELINE_USER as requested
                'snowflake_private_key': private_key_b64,
                'snowflake_private_key_passphrase': None  # No passphrase for this key
            }
            
            logger.info(f"Successfully loaded local key files for user: {self.secrets['snowflake_user']}")
            
        except Exception as e:
            logger.error(f"Error loading local key files: {e}")
            # Fallback to AWS Secrets Manager if local files not available
            try:
                secrets_client = boto3.client('secretsmanager')
                secret_name = f'teddy-data-pipeline-secrets-{self.environment}'
                
                response = secrets_client.get_secret_value(SecretId=secret_name)
                secrets_data = json.loads(response['SecretString'])
                
                # Update the user to TEDDY_PIPELINE_USER
                secrets_data['snowflake_user'] = 'TEDDY_PIPELINE_USER'
                self.secrets = secrets_data
                
                logger.info(f"Fallback to AWS Secrets Manager successful, using user: {self.secrets['snowflake_user']}")
                
            except Exception as fallback_error:
                logger.error(f"Both local keys and AWS Secrets Manager failed: {fallback_error}")
                raise
    
    def _generate_jwt_token(self) -> str:
        """
        Generate JWT token for Snowflake authentication using private key
        """
        try:
            # Decode the base64 encoded private key
            private_key_data = base64.b64decode(self.secrets['snowflake_private_key'])
            
            # Get passphrase if provided
            passphrase = self.secrets.get('snowflake_private_key_passphrase')
            passphrase_bytes = passphrase.encode('utf-8') if passphrase else None
            
            # Load the private key
            private_key = serialization.load_pem_private_key(
                private_key_data,
                password=passphrase_bytes,
                backend=default_backend()
            )
            
            # Get the public key and calculate fingerprint using Snowflake's method
            public_key = private_key.public_key()
            public_key_der = public_key.public_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )
            
            # Create JWT payload with correct Snowflake format
            account = self.secrets['snowflake_account']
            user = self.secrets['snowflake_user']
            
            # Construct the fully qualified username (ACCOUNT.USER format)
            qualified_username = f"{account.upper()}.{user.upper()}"
            
            # Calculate public key fingerprint using Snowflake's method:
            # SHA256 hash of DER-encoded public key, then base64 encode
            import hashlib
            public_key_hash = hashlib.sha256(public_key_der).digest()
            public_key_fp = base64.b64encode(public_key_hash).decode('utf-8')
            
            # Current time in UTC - use current timestamp
            now = datetime.utcnow()
            iat = int(now.timestamp())
            exp = int((now + timedelta(minutes=59)).timestamp())
            
            # JWT payload following Snowflake's exact specification
            payload = {
                'iss': f"{qualified_username}.SHA256:{public_key_fp}",
                'sub': qualified_username,
                'iat': iat,
                'exp': exp
            }
            
            # Generate JWT token with explicit headers
            headers = {
                'alg': 'RS256',
                'typ': 'JWT'
            }
            
            token = jwt.encode(
                payload,
                private_key,
                algorithm='RS256',
                headers=headers
            )
            
            logger.info("Successfully generated JWT token for Snowflake authentication")
            logger.debug(f"JWT payload: iss={payload['iss'][:50]}..., sub={payload['sub']}")
            return token
            
        except Exception as e:
            logger.error(f"Error generating JWT token: {e}")
            raise
    
    def _get_public_key_fingerprint(self, public_key_der: bytes) -> str:
        """
        Calculate SHA256 fingerprint of the public key
        """
        import hashlib
        
        digest = hashlib.sha256(public_key_der).digest()
        return base64.b64encode(digest).decode('utf-8')
    
    def connect(self) -> snowflake.connector.SnowflakeConnection:
        """
        Establish connection to Snowflake using JWT authentication
        """
        try:
            if self.connection and not self.connection.is_closed():
                return self.connection
            
            # Load the private key for direct authentication
            private_key_data = base64.b64decode(self.secrets['snowflake_private_key'])
            passphrase = self.secrets.get('snowflake_private_key_passphrase')
            passphrase_bytes = passphrase.encode('utf-8') if passphrase else None
            
            private_key = serialization.load_pem_private_key(
                private_key_data,
                password=passphrase_bytes,
                backend=default_backend()
            )
            
            # Connection parameters - using SNOWFLAKE_JWT authenticator like working implementation
            connection_params = {
                'account': self.secrets['snowflake_account'],
                'user': self.secrets['snowflake_user'],
                'authenticator': 'SNOWFLAKE_JWT',  # Use SNOWFLAKE_JWT instead of oauth
                'private_key': private_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                ),
                'warehouse': 'TEDDY_INGESTION_WH',  # User's default warehouse
                'database': 'TEDDY_DATA',
                'schema': 'RAW',
                'client_session_keep_alive': True,
                'client_session_keep_alive_heartbeat_frequency': 3600  # 1 hour
            }
            
            # Establish connection
            self.connection = snowflake.connector.connect(**connection_params)
            
            logger.info(f"Successfully connected to Snowflake account: {self.secrets['snowflake_account']}")
            return self.connection
            
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            raise
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a query and return results as list of dictionaries
        """
        try:
            connection = self.connect()
            cursor = connection.cursor(DictCursor)
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            results = cursor.fetchall()
            cursor.close()
            
            logger.info(f"Successfully executed query, returned {len(results)} rows")
            return results
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    def execute_non_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> int:
        """
        Execute a non-query statement (INSERT, UPDATE, DELETE) and return affected rows
        """
        try:
            connection = self.connect()
            cursor = connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            affected_rows = cursor.rowcount
            connection.commit()
            cursor.close()
            
            logger.info(f"Successfully executed non-query, affected {affected_rows} rows")
            return affected_rows
            
        except Exception as e:
            logger.error(f"Error executing non-query: {e}")
            raise
    
    def bulk_insert(self, table_name: str, data: List[Dict[str, Any]], 
                   schema: str = 'RAW') -> int:
        """
        Perform bulk insert using Snowflake's PUT and COPY commands
        """
        try:
            if not data:
                logger.warning("No data provided for bulk insert")
                return 0
            
            connection = self.connect()
            cursor = connection.cursor()
            
            # Create temporary stage for bulk loading
            stage_name = f"temp_stage_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Create temporary file format
            cursor.execute(f"""
                CREATE OR REPLACE TEMPORARY FILE FORMAT temp_json_format
                TYPE = 'JSON'
                STRIP_OUTER_ARRAY = TRUE
            """)
            
            # Create temporary stage
            cursor.execute(f"""
                CREATE OR REPLACE TEMPORARY STAGE {stage_name}
                FILE_FORMAT = temp_json_format
            """)
            
            # Convert data to JSON string
            json_data = '\n'.join([json.dumps(record) for record in data])
            
            # Put data to stage (this would typically involve writing to a file first)
            # For now, we'll use a direct INSERT approach for smaller datasets
            
            # Get column names from first record
            if data:
                columns = list(data[0].keys())
                placeholders = ', '.join(['%s'] * len(columns))
                column_names = ', '.join(columns)
                
                insert_query = f"""
                    INSERT INTO {schema}.{table_name} ({column_names})
                    VALUES ({placeholders})
                """
                
                # Execute batch insert
                values_list = [list(record.values()) for record in data]
                cursor.executemany(insert_query, values_list)
                
                affected_rows = cursor.rowcount
                connection.commit()
                cursor.close()
                
                logger.info(f"Successfully bulk inserted {affected_rows} rows into {schema}.{table_name}")
                return affected_rows
            
        except Exception as e:
            logger.error(f"Error performing bulk insert: {e}")
            raise
    
    def test_connection(self) -> Dict[str, Any]:
        """
        Test the Snowflake connection and return connection info
        """
        try:
            connection = self.connect()
            cursor = connection.cursor(DictCursor)
            
            # Test query to get current user and warehouse info
            cursor.execute("""
                SELECT 
                    CURRENT_USER() as current_user,
                    CURRENT_WAREHOUSE() as current_warehouse,
                    CURRENT_DATABASE() as current_database,
                    CURRENT_SCHEMA() as current_schema,
                    CURRENT_TIMESTAMP() as connection_time
            """)
            
            result = cursor.fetchone()
            cursor.close()
            
            logger.info("Snowflake connection test successful")
            return {
                'status': 'success',
                'connection_info': result,
                'authentication_method': 'JWT'
            }
            
        except Exception as e:
            logger.error(f"Snowflake connection test failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'authentication_method': 'JWT'
            }
    
    def get_table_info(self, table_name: str, schema: str = 'RAW') -> List[Dict[str, Any]]:
        """
        Get information about a table's columns and structure
        """
        try:
            query = """
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """
            
            results = self.execute_query(query, {'1': schema.upper(), '2': table_name.upper()})
            
            logger.info(f"Retrieved info for table {schema}.{table_name}: {len(results)} columns")
            return results
            
        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            raise
    
    def close(self):
        """
        Close the Snowflake connection
        """
        try:
            if self.connection and not self.connection.is_closed():
                self.connection.close()
                logger.info("Snowflake connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()

# Convenience function for Lambda usage
def get_snowflake_connector(environment: str = None) -> SnowflakeJWTConnector:
    """
    Get a Snowflake connector instance
    """
    if environment is None:
        environment = os.environ.get('ENVIRONMENT', 'dev')
    
    return SnowflakeJWTConnector(environment=environment)

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
