"""
Simple Snowflake Connector with Password Authentication

This module provides a simple connection to Snowflake using username/password
authentication for the TEDDY_PIPELINE_SERVICE account.
"""

import json
import logging
import os
from typing import Dict, Any, Optional, List
import boto3
import snowflake.connector
from snowflake.connector import DictCursor

logger = logging.getLogger(__name__)

class SimpleSnowflakeConnector:
    """
    Simple Snowflake connector using password authentication
    """
    
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
    
    def connect(self) -> snowflake.connector.SnowflakeConnection:
        """
        Establish connection to Snowflake using password authentication
        """
        try:
            if self.connection and not self.connection.is_closed():
                return self.connection
            
            # Connection parameters for password authentication
            connection_params = {
                'account': self.secrets['SNOWFLAKE_ACCOUNT'],
                'user': self.secrets['SNOWFLAKE_USER'],
                'password': self.secrets['SNOWFLAKE_PASSWORD'],
                'warehouse': self.secrets['SNOWFLAKE_WAREHOUSE'],
                'database': self.secrets['SNOWFLAKE_DATABASE'],
                'schema': self.secrets['SNOWFLAKE_SCHEMA'],
                'role': self.secrets['SNOWFLAKE_ROLE'],
                'client_session_keep_alive': True,
                'client_session_keep_alive_heartbeat_frequency': 3600  # 1 hour
            }
            
            # Establish connection
            self.connection = snowflake.connector.connect(**connection_params)
            
            logger.info(f"Successfully connected to Snowflake account: {self.secrets['SNOWFLAKE_ACCOUNT']}")
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
                   schema: str = None) -> int:
        """
        Perform bulk insert using batch INSERT statements
        """
        try:
            if not data:
                logger.warning("No data provided for bulk insert")
                return 0
            
            if schema is None:
                schema = self.secrets['SNOWFLAKE_SCHEMA']
            
            connection = self.connect()
            cursor = connection.cursor()
            
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
                'authentication_method': 'password'
            }
            
        except Exception as e:
            logger.error(f"Snowflake connection test failed: {e}")
            return {
                'status': 'failed',
                'error': str(e),
                'authentication_method': 'password'
            }
    
    def get_table_info(self, table_name: str, schema: str = None) -> List[Dict[str, Any]]:
        """
        Get information about a table's columns and structure
        """
        try:
            if schema is None:
                schema = self.secrets['SNOWFLAKE_SCHEMA']
                
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
def get_snowflake_connector(environment: str = None) -> SimpleSnowflakeConnector:
    """
    Get a simple Snowflake connector instance
    """
    if environment is None:
        environment = os.environ.get('ENVIRONMENT', 'dev')
    
    return SimpleSnowflakeConnector(environment=environment)

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
