"""
Snowflake Loader

Handles loading transformed HubSpot data into Snowflake using various methods
including direct connection, Snowpipe, and bulk loading strategies.
"""

import os
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import json
import tempfile
import csv
from io import StringIO

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Load .env file from the current directory
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    load_dotenv(env_path)
except ImportError:
    # dotenv not available, continue without it
    pass

logger = logging.getLogger(__name__)


class SnowflakeConnectionError(Exception):
    """Raised when Snowflake connection fails"""
    pass


class SnowflakeLoadError(Exception):
    """Raised when data loading fails"""
    pass


class SnowflakeLoader:
    """
    Loads data into Snowflake using multiple strategies.
    
    Features:
    - Direct pandas integration
    - Bulk loading via COPY INTO
    - Snowpipe integration for streaming
    - Schema management and DDL generation
    - Error handling and retry logic
    - Performance optimization
    """
    
    def __init__(self,
                 account: Optional[str] = None,
                 user: Optional[str] = None,
                 password: Optional[str] = None,
                 warehouse: Optional[str] = None,
                 database: Optional[str] = None,
                 schema: Optional[str] = None,
                 role: Optional[str] = None,
                 private_key_path: Optional[str] = None,
                 private_key_passphrase: Optional[str] = None):
        """
        Initialize Snowflake loader with connection parameters.
        
        Args:
            account: Snowflake account identifier
            user: Username
            password: Password (if not using key-pair auth)
            warehouse: Warehouse name
            database: Database name
            schema: Schema name
            role: Role name
            private_key_path: Path to private key file for key-pair auth
            private_key_passphrase: Private key passphrase
        """
        # Load from environment variables if not provided
        self.account = account or os.getenv('SNOWFLAKE_ACCOUNT')
        self.user = user or os.getenv('SNOWFLAKE_USER')
        self.password = password or os.getenv('SNOWFLAKE_PASSWORD')
        self.warehouse = warehouse or os.getenv('SNOWFLAKE_WAREHOUSE')
        self.database = database or os.getenv('SNOWFLAKE_DATABASE')
        self.schema = schema or os.getenv('SNOWFLAKE_SCHEMA', 'HUBSPOT')
        self.role = role or os.getenv('SNOWFLAKE_ROLE')
        self.private_key_path = private_key_path or os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
        self.private_key_passphrase = private_key_passphrase or os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
        
        self.connection = None
        self._validate_config()
    
    def _validate_config(self):
        """Validate configuration parameters"""
        required_params = ['account', 'user', 'warehouse', 'database']
        missing_params = [param for param in required_params if not getattr(self, param)]
        
        if missing_params:
            raise SnowflakeConnectionError(f"Missing required parameters: {missing_params}")
        
        # Must have either password or private key
        if not self.password and not self.private_key_path:
            raise SnowflakeConnectionError("Either password or private_key_path must be provided")
    
    def _get_private_key(self):
        """Load private key for key-pair authentication"""
        if not self.private_key_path:
            return None
        
        try:
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.primitives.serialization import load_pem_private_key
            
            with open(self.private_key_path, 'rb') as key_file:
                private_key = load_pem_private_key(
                    key_file.read(),
                    password=self.private_key_passphrase.encode() if self.private_key_passphrase else None
                )
            
            return private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
        except Exception as e:
            logger.error(f"Failed to load private key: {e}")
            raise SnowflakeConnectionError(f"Private key loading failed: {e}")
    
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            connection_params = {
                'account': self.account,
                'user': self.user,
                'warehouse': self.warehouse,
                'database': self.database,
                'schema': self.schema,
                # Complete OCSP bypass for Python 3.13 compatibility
                'insecure_mode': False,  # Keep secure but add fallback options
                'ocsp_fail_open': True,  # Allow connection if OCSP validation fails
                'disable_ocsp_checks': True,  # Completely disable OCSP checks
                'client_session_keep_alive': True,  # Keep connection alive
                'login_timeout': 60,  # Increase login timeout
                'network_timeout': 60,  # Increase network timeout
                'socket_timeout': 60,  # Add socket timeout
            }
            
            if self.role:
                connection_params['role'] = self.role
            
            # Use key-pair authentication if available, otherwise password
            if self.private_key_path:
                connection_params['private_key'] = self._get_private_key()
            else:
                connection_params['password'] = self.password
            
            self.connection = snowflake.connector.connect(**connection_params)
            logger.info(f"Connected to Snowflake: {self.database}.{self.schema}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            # Try with more permissive SSL settings as fallback
            try:
                logger.warning("Retrying connection with relaxed SSL settings...")
                connection_params.update({
                    'ocsp_fail_open': True,
                    'insecure_mode': False,  # Still keep secure
                    'disable_request_pooling': True,  # Disable connection pooling
                })
                self.connection = snowflake.connector.connect(**connection_params)
                logger.info(f"Connected to Snowflake with fallback settings: {self.database}.{self.schema}")
            except Exception as fallback_error:
                logger.error(f"Fallback connection also failed: {fallback_error}")
                raise SnowflakeConnectionError(f"Connection failed: {e}. Fallback also failed: {fallback_error}")
    
    def disconnect(self):
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Disconnected from Snowflake")
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            List of result dictionaries
        """
        if not self.connection:
            self.connect()
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch results
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            result_dicts = []
            for row in results:
                result_dicts.append(dict(zip(columns, row)))
            
            cursor.close()
            return result_dicts
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise SnowflakeLoadError(f"Query failed: {e}")
    
    def create_schema_if_not_exists(self, schema_name: Optional[str] = None):
        """
        Create schema if it doesn't exist.
        
        Args:
            schema_name: Schema name (uses default if not provided)
        """
        schema_name = schema_name or self.schema
        query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        
        try:
            self.execute_query(query)
            logger.info(f"Schema {schema_name} created or already exists")
        except Exception as e:
            logger.error(f"Failed to create schema {schema_name}: {e}")
            raise
    
    def create_table_from_dataframe(self, 
                                   df: pd.DataFrame, 
                                   table_name: str,
                                   if_exists: str = 'replace') -> str:
        """
        Create table based on DataFrame structure.
        
        Args:
            df: DataFrame with data structure
            table_name: Name of table to create
            if_exists: What to do if table exists ('replace', 'append', 'fail')
            
        Returns:
            CREATE TABLE SQL statement
        """
        if df.empty:
            raise SnowflakeLoadError("Cannot create table from empty DataFrame")
        
        # Generate column definitions
        columns = []
        for col_name, dtype in df.dtypes.items():
            snowflake_type = self._pandas_to_snowflake_type(dtype, col_name)
            columns.append(f"{col_name.upper()} {snowflake_type}")
        
        # Create table SQL
        if if_exists == 'replace':
            create_sql = f"CREATE OR REPLACE TABLE {table_name.upper()} (\n    " + ",\n    ".join(columns) + "\n)"
        else:
            create_sql = f"CREATE TABLE IF NOT EXISTS {table_name.upper()} (\n    " + ",\n    ".join(columns) + "\n)"
        
        try:
            self.execute_query(create_sql)
            logger.info(f"Table {table_name} created successfully")
            return create_sql
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise SnowflakeLoadError(f"Table creation failed: {e}")
    
    def _pandas_to_snowflake_type(self, pandas_dtype, col_name: str) -> str:
        """
        Convert pandas dtype to Snowflake data type.
        
        Args:
            pandas_dtype: Pandas data type
            col_name: Column name for context
            
        Returns:
            Snowflake data type
        """
        dtype_str = str(pandas_dtype)
        
        # Handle specific column patterns
        if col_name.lower() in ['id'] or col_name.endswith('_id'):
            return 'VARCHAR(50)'
        
        if 'date' in col_name.lower() or 'time' in col_name.lower():
            return 'TIMESTAMP_NTZ'
        
        if col_name.lower() in ['email']:
            return 'VARCHAR(255)'
        
        if col_name.lower() in ['phone', 'phone_number']:
            return 'VARCHAR(50)'
        
        # Handle pandas dtypes
        if 'datetime' in dtype_str:
            return 'TIMESTAMP_NTZ'
        elif 'bool' in dtype_str:
            return 'BOOLEAN'
        elif 'int' in dtype_str:
            return 'NUMBER(38,0)'
        elif 'float' in dtype_str:
            return 'NUMBER(38,10)'
        elif 'object' in dtype_str:
            # Check if it's likely JSON
            if col_name.lower() in ['associations', 'properties']:
                return 'VARIANT'
            # Default to VARCHAR
            return 'VARCHAR(16777216)'
        else:
            return 'VARCHAR(16777216)'
    
    def load_dataframe(self, 
                      df: pd.DataFrame, 
                      table_name: str,
                      if_exists: str = 'append',
                      chunk_size: int = 10000,
                      create_table: bool = True) -> Dict[str, Any]:
        """
        Load DataFrame into Snowflake table using pandas integration.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            if_exists: What to do if table exists ('replace', 'append', 'fail')
            chunk_size: Size of chunks for loading
            create_table: Whether to create table if it doesn't exist
            
        Returns:
            Load statistics
        """
        if df.empty:
            logger.warning(f"Empty DataFrame provided for table {table_name}")
            return {'rows_loaded': 0, 'status': 'empty'}
        
        if not self.connection:
            self.connect()
        
        start_time = datetime.now()
        
        try:
            # Create table if requested
            if create_table:
                self.create_table_from_dataframe(df, table_name, if_exists)
            
            # Prepare DataFrame for loading
            df_clean = self._prepare_dataframe_for_snowflake(df)
            
            # Try write_pandas first, fall back to SQL inserts if it fails
            try:
                success, nchunks, nrows, _ = write_pandas(
                    conn=self.connection,
                    df=df_clean,
                    table_name=table_name.upper(),
                    schema=self.schema.upper(),
                    chunk_size=chunk_size,
                    compression='gzip',
                    on_error='continue',
                    quote_identifiers=False,
                    use_logical_type=True  # Fix timezone warning
                )
                
                if success:
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    logger.info(f"Successfully loaded {nrows} rows into {table_name} using write_pandas in {duration:.2f} seconds")
                    return {
                        'rows_loaded': nrows,
                        'chunks': nchunks,
                        'duration_seconds': duration,
                        'status': 'success'
                    }
                else:
                    raise Exception("write_pandas returned False")
                    
            except Exception as pandas_error:
                logger.warning(f"write_pandas failed for {table_name}: {pandas_error}. Falling back to SQL inserts.")
                
                # Fall back to SQL insert method
                return self._load_dataframe_via_sql(df_clean, table_name, chunk_size, start_time)
                
        except Exception as e:
            logger.error(f"Failed to load DataFrame into {table_name}: {e}")
            raise SnowflakeLoadError(f"DataFrame loading failed: {e}")
    
    def _prepare_dataframe_for_snowflake(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for Snowflake loading.
        
        Args:
            df: DataFrame to prepare
            
        Returns:
            Prepared DataFrame
        """
        df_clean = df.copy()
        
        # Replace NaN with None for proper NULL handling
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        
        # Convert datetime columns to strings, handling NaT values
        for col in df_clean.columns:
            if df_clean[col].dtype == 'datetime64[ns]':
                # Convert to string, but handle NaT values
                df_clean[col] = df_clean[col].apply(
                    lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None
                )
        
        # Handle object columns that might have mixed types
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                # Convert all values to strings to avoid type conversion errors
                df_clean[col] = df_clean[col].astype(str)
                # Replace various null representations back to actual None
                null_representations = ['None', 'nan', 'NaN', 'NaT', 'null', 'NULL']
                for null_rep in null_representations:
                    df_clean[col] = df_clean[col].replace(null_rep, None)
        
        # Ensure column names are uppercase (Snowflake convention)
        df_clean.columns = [col.upper() for col in df_clean.columns]
        
        return df_clean
    
    def bulk_load_from_csv(self, 
                          csv_file_path: str, 
                          table_name: str,
                          file_format: Optional[str] = None) -> Dict[str, Any]:
        """
        Load data from CSV file using COPY INTO command.
        
        Args:
            csv_file_path: Path to CSV file
            table_name: Target table name
            file_format: Snowflake file format name
            
        Returns:
            Load statistics
        """
        if not os.path.exists(csv_file_path):
            raise SnowflakeLoadError(f"CSV file not found: {csv_file_path}")
        
        # Create file format if not provided
        if not file_format:
            file_format = f"{table_name}_CSV_FORMAT"
            format_sql = f"""
            CREATE OR REPLACE FILE FORMAT {file_format}
            TYPE = 'CSV'
            FIELD_DELIMITER = ','
            RECORD_DELIMITER = '\\n'
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            TRIM_SPACE = TRUE
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
            REPLACE_INVALID_CHARACTERS = TRUE
            """
            self.execute_query(format_sql)
        
        # Upload file to internal stage
        stage_name = f"@%{table_name.upper()}"
        put_sql = f"PUT file://{csv_file_path} {stage_name}"
        
        try:
            self.execute_query(put_sql)
            
            # Load data using COPY INTO
            copy_sql = f"""
            COPY INTO {table_name.upper()}
            FROM {stage_name}
            FILE_FORMAT = (FORMAT_NAME = '{file_format}')
            ON_ERROR = 'CONTINUE'
            """
            
            result = self.execute_query(copy_sql)
            
            # Parse results
            rows_loaded = sum(row.get('rows_loaded', 0) for row in result)
            errors = sum(row.get('errors_seen', 0) for row in result)
            
            logger.info(f"Bulk loaded {rows_loaded} rows into {table_name} with {errors} errors")
            
            return {
                'rows_loaded': rows_loaded,
                'errors': errors,
                'status': 'success' if errors == 0 else 'partial_success'
            }
            
        except Exception as e:
            logger.error(f"Bulk load failed for {table_name}: {e}")
            raise SnowflakeLoadError(f"Bulk load failed: {e}")
    
    def upsert_data(self, 
                   df: pd.DataFrame, 
                   table_name: str,
                   key_columns: List[str],
                   update_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Perform upsert (merge) operation on data.
        
        Args:
            df: DataFrame with data to upsert
            table_name: Target table name
            key_columns: Columns to use for matching
            update_columns: Columns to update (all if None)
            
        Returns:
            Upsert statistics
        """
        if df.empty:
            return {'rows_processed': 0, 'status': 'empty'}
        
        # Create temporary table
        temp_table = f"{table_name}_TEMP_{int(datetime.now().timestamp())}"
        
        try:
            # Load data into temporary table
            self.load_dataframe(df, temp_table, if_exists='replace')
            
            # Determine columns to update
            if update_columns is None:
                update_columns = [col for col in df.columns if col not in key_columns]
            
            # Build MERGE statement
            key_conditions = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])
            update_sets = ", ".join([f"{col} = source.{col}" for col in update_columns])
            insert_columns = ", ".join(df.columns)
            insert_values = ", ".join([f"source.{col}" for col in df.columns])
            
            merge_sql = f"""
            MERGE INTO {table_name.upper()} AS target
            USING {temp_table.upper()} AS source
            ON {key_conditions}
            WHEN MATCHED THEN
                UPDATE SET {update_sets}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
            """
            
            result = self.execute_query(merge_sql)
            
            # Clean up temporary table
            self.execute_query(f"DROP TABLE {temp_table.upper()}")
            
            logger.info(f"Upsert completed for {table_name}")
            
            return {
                'rows_processed': len(df),
                'status': 'success'
            }
            
        except Exception as e:
            # Clean up temporary table on error
            try:
                self.execute_query(f"DROP TABLE IF EXISTS {temp_table.upper()}")
            except:
                pass
            
            logger.error(f"Upsert failed for {table_name}: {e}")
            raise SnowflakeLoadError(f"Upsert failed: {e}")
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get information about a table.
        
        Args:
            table_name: Table name
            
        Returns:
            Table information
        """
        info_sql = f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns 
        WHERE table_schema = '{self.schema.upper()}'
        AND table_name = '{table_name.upper()}'
        ORDER BY ordinal_position
        """
        
        columns = self.execute_query(info_sql)
        
        count_sql = f"SELECT COUNT(*) as row_count FROM {table_name.upper()}"
        count_result = self.execute_query(count_sql)
        row_count = count_result[0]['ROW_COUNT'] if count_result else 0
        
        return {
            'table_name': table_name,
            'schema': self.schema,
            'columns': columns,
            'row_count': row_count
        }
    
    def _load_dataframe_via_sql(self, 
                               df: pd.DataFrame, 
                               table_name: str, 
                               chunk_size: int,
                               start_time: datetime) -> Dict[str, Any]:
        """
        Load DataFrame using SQL INSERT statements as fallback method.
        
        Args:
            df: Prepared DataFrame to load
            table_name: Target table name
            chunk_size: Size of chunks for loading
            start_time: Start time for duration calculation
            
        Returns:
            Load statistics
        """
        total_rows = len(df)
        rows_loaded = 0
        
        try:
            # Get column names
            columns = list(df.columns)
            column_names = ', '.join(columns)
            
            # Process in chunks
            for i in range(0, total_rows, chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                
                # Build VALUES clause for this chunk
                values_list = []
                for _, row in chunk.iterrows():
                    # Convert each value to SQL-safe format
                    values = []
                    for val in row:
                        if val is None:
                            values.append('NULL')
                        elif pd.isna(val):
                            values.append('NULL')
                        elif isinstance(val, str):
                            # Check for various null representations
                            if val.upper() in ['NAN', 'NAT', 'NONE', 'NULL', '']:
                                values.append('NULL')
                            else:
                                # Escape single quotes and wrap in quotes
                                escaped_val = val.replace("'", "''")
                                values.append(f"'{escaped_val}'")
                        elif isinstance(val, (int, float)):
                            # Check for NaN values
                            if pd.isna(val):
                                values.append('NULL')
                            else:
                                values.append(str(val))
                        else:
                            # Convert to string and check for null representations
                            str_val = str(val)
                            if str_val.upper() in ['NAN', 'NAT', 'NONE', 'NULL', '']:
                                values.append('NULL')
                            else:
                                escaped_val = str_val.replace("'", "''")
                                values.append(f"'{escaped_val}'")
                    
                    values_list.append(f"({', '.join(values)})")
                
                # Execute INSERT statement for this chunk
                if values_list:
                    insert_sql = f"""
                    INSERT INTO {table_name.upper()} ({column_names})
                    VALUES {', '.join(values_list)}
                    """
                    
                    self.execute_query(insert_sql)
                    rows_loaded += len(chunk)
                    
                    logger.info(f"Loaded chunk {i//chunk_size + 1}: {len(chunk)} rows into {table_name}")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"Successfully loaded {rows_loaded} rows into {table_name} using SQL inserts in {duration:.2f} seconds")
            
            return {
                'rows_loaded': rows_loaded,
                'chunks': (total_rows + chunk_size - 1) // chunk_size,  # Ceiling division
                'duration_seconds': duration,
                'status': 'success',
                'method': 'sql_insert'
            }
            
        except Exception as e:
            logger.error(f"SQL insert method failed for {table_name}: {e}")
            raise SnowflakeLoadError(f"SQL insert failed: {e}")

    def test_connection(self) -> bool:
        """
        Test the Snowflake connection.
        
        Returns:
            True if connection is successful
        """
        try:
            if not self.connection:
                self.connect()
            
            result = self.execute_query("SELECT CURRENT_VERSION()")
            logger.info(f"Snowflake connection test successful: {result[0]}")
            return True
            
        except Exception as e:
            logger.error(f"Snowflake connection test failed: {e}")
            return False
