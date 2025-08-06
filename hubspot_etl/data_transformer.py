"""
HubSpot Data Transformer

Transforms nested HubSpot JSON data into flat structures suitable for Snowflake loading.
Handles property flattening, data type conversion, and schema normalization.
"""

import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import pandas as pd
import json
from decimal import Decimal

logger = logging.getLogger(__name__)


class HubSpotDataTransformer:
    """
    Transforms HubSpot CRM data into flat, Snowflake-compatible structures.
    
    Features:
    - Flattens nested JSON properties
    - Handles HubSpot-specific data types
    - Normalizes timestamps and dates
    - Converts data types for Snowflake compatibility
    - Handles associations and relationships
    """
    
    # HubSpot property types and their Snowflake equivalents
    PROPERTY_TYPE_MAPPING = {
        'string': 'VARCHAR',
        'number': 'NUMBER',
        'bool': 'BOOLEAN',
        'datetime': 'TIMESTAMP_NTZ',
        'date': 'DATE',
        'enumeration': 'VARCHAR',
        'phone_number': 'VARCHAR',
        'json': 'VARIANT'
    }
    
    # Properties that should be treated as timestamps (milliseconds since epoch)
    TIMESTAMP_PROPERTIES = {
        'createdate', 'hs_lastmodifieddate', 'lastmodifieddate', 'closedate',
        'hs_createdate', 'hs_date_entered_*', 'hs_date_exited_*'
    }
    
    def __init__(self):
        """Initialize the data transformer"""
        self.schema_cache = {}
    
    def transform_objects(self, 
                         objects: List[Dict[str, Any]], 
                         object_type: str) -> pd.DataFrame:
        """
        Transform a list of HubSpot objects into a pandas DataFrame.
        
        Args:
            objects: List of HubSpot objects
            object_type: Type of objects (contacts, companies, deals, etc.)
            
        Returns:
            Pandas DataFrame with flattened data
        """
        if not objects:
            return pd.DataFrame()
        
        transformed_data = []
        
        for obj in objects:
            flattened_obj = self._flatten_object(obj, object_type)
            transformed_data.append(flattened_obj)
        
        df = pd.DataFrame(transformed_data)
        
        # Apply data type conversions
        df = self._apply_data_types(df, object_type)
        
        # Add metadata columns
        df = self._add_metadata_columns(df)
        
        logger.info(f"Transformed {len(objects)} {object_type} objects into DataFrame with shape {df.shape}")
        
        return df
    
    def _flatten_object(self, obj: Dict[str, Any], object_type: str) -> Dict[str, Any]:
        """
        Flatten a single HubSpot object.
        
        Args:
            obj: HubSpot object
            object_type: Type of object
            
        Returns:
            Flattened dictionary
        """
        flattened = {}
        
        # Add object ID and basic metadata
        flattened['id'] = obj.get('id')
        flattened['object_type'] = object_type
        
        # Flatten properties
        properties = obj.get('properties', {})
        for prop_name, prop_value in properties.items():
            flattened[prop_name] = self._transform_property_value(prop_name, prop_value)
        
        # Handle associations
        associations = obj.get('associations', {})
        if associations:
            flattened['associations'] = json.dumps(associations)
            
            # Create separate columns for association counts
            for assoc_type, assoc_data in associations.items():
                if isinstance(assoc_data, dict) and 'results' in assoc_data:
                    flattened[f'{assoc_type}_count'] = len(assoc_data['results'])
                    # Store first few association IDs
                    assoc_ids = [result.get('id') for result in assoc_data['results'][:5]]
                    flattened[f'{assoc_type}_ids'] = ','.join(filter(None, assoc_ids))
        
        # Handle archived status
        flattened['archived'] = obj.get('archived', False)
        flattened['archived_at'] = obj.get('archivedAt')
        
        # Add created and updated timestamps from object level
        if 'createdAt' in obj:
            flattened['created_at'] = obj['createdAt']
        if 'updatedAt' in obj:
            flattened['updated_at'] = obj['updatedAt']
        
        return flattened
    
    def _transform_property_value(self, prop_name: str, prop_value: Any) -> Any:
        """
        Transform a property value based on its type and name.
        
        Args:
            prop_name: Property name
            prop_value: Property value
            
        Returns:
            Transformed value
        """
        if prop_value is None or prop_value == '':
            return None
        
        # Handle timestamp properties (HubSpot uses milliseconds since epoch)
        if self._is_timestamp_property(prop_name):
            try:
                if isinstance(prop_value, str) and prop_value.isdigit():
                    timestamp_ms = int(prop_value)
                    return datetime.fromtimestamp(timestamp_ms / 1000.0)
                elif isinstance(prop_value, (int, float)):
                    return datetime.fromtimestamp(prop_value / 1000.0)
            except (ValueError, OSError) as e:
                logger.warning(f"Failed to convert timestamp property {prop_name}: {prop_value}, error: {e}")
                return str(prop_value) if prop_value is not None else None
        
        # Handle boolean values
        if isinstance(prop_value, str) and prop_value.lower() in ('true', 'false'):
            return prop_value.lower() == 'true'
        
        # Handle numeric values - be more careful with conversion
        if isinstance(prop_value, str):
            # Check if it's a pure number (no mixed content)
            cleaned_value = prop_value.strip()
            if cleaned_value.replace('.', '').replace('-', '').replace('+', '').isdigit():
                try:
                    if '.' in cleaned_value:
                        return float(cleaned_value)
                    else:
                        return int(cleaned_value)
                except ValueError:
                    pass
        
        # Handle JSON strings
        if isinstance(prop_value, str) and (prop_value.startswith('{') or prop_value.startswith('[')):
            try:
                return json.loads(prop_value)
            except json.JSONDecodeError:
                pass
        
        # Return as string for everything else - ensure it's always a string
        return str(prop_value) if prop_value is not None else None
    
    def _is_timestamp_property(self, prop_name: str) -> bool:
        """
        Check if a property should be treated as a timestamp.
        
        Args:
            prop_name: Property name
            
        Returns:
            True if property is a timestamp
        """
        # Direct matches
        if prop_name in self.TIMESTAMP_PROPERTIES:
            return True
        
        # Pattern matches
        timestamp_patterns = [
            'createdate', 'date', 'time', 'lastmodified', 'closedate',
            'hs_date_', 'hs_time_', '_date', '_time'
        ]
        
        prop_lower = prop_name.lower()
        return any(pattern in prop_lower for pattern in timestamp_patterns)
    
    def _apply_data_types(self, df: pd.DataFrame, object_type: str) -> pd.DataFrame:
        """
        Apply appropriate data types to DataFrame columns.
        
        Args:
            df: DataFrame to process
            object_type: Type of objects
            
        Returns:
            DataFrame with proper data types
        """
        if df.empty:
            return df
        
        # Convert timestamp columns
        timestamp_columns = [col for col in df.columns if self._is_timestamp_property(col)]
        for col in timestamp_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Convert boolean columns
        boolean_columns = [col for col in df.columns if col.endswith('_bool') or 
                          col in ['archived', 'hs_is_unworked', 'hs_is_closed']]
        for col in boolean_columns:
            if col in df.columns:
                df[col] = df[col].astype('boolean')
        
        # Convert numeric columns
        numeric_patterns = ['amount', 'revenue', 'value', 'count', 'number', 'score', 'probability']
        for col in df.columns:
            if any(pattern in col.lower() for pattern in numeric_patterns):
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Ensure ID columns are strings
        id_columns = [col for col in df.columns if col.endswith('_id') or col == 'id']
        for col in id_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
        
        return df
    
    def _add_metadata_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add metadata columns for tracking and auditing.
        
        Args:
            df: DataFrame to process
            
        Returns:
            DataFrame with metadata columns
        """
        if df.empty:
            return df
        
        # Add ETL metadata
        df['etl_loaded_at'] = datetime.now()
        df['etl_source'] = 'hubspot_api'
        df['etl_version'] = '1.0'
        
        return df
    
    def generate_snowflake_schema(self, 
                                 df: pd.DataFrame, 
                                 table_name: str,
                                 object_type: str) -> str:
        """
        Generate Snowflake CREATE TABLE statement based on DataFrame structure.
        
        Args:
            df: DataFrame with data
            table_name: Name for the table
            object_type: Type of HubSpot object
            
        Returns:
            SQL CREATE TABLE statement
        """
        if df.empty:
            return f"-- No data available for {table_name}"
        
        columns = []
        
        for col_name, dtype in df.dtypes.items():
            snowflake_type = self._pandas_to_snowflake_type(dtype, col_name)
            columns.append(f"    {col_name.upper()} {snowflake_type}")
        
        schema_sql = f"""
CREATE OR REPLACE TABLE {table_name.upper()} (
{','.join(columns)}
);

-- Add comments
COMMENT ON TABLE {table_name.upper()} IS 'HubSpot {object_type} data loaded via ETL pipeline';
"""
        
        return schema_sql
    
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
        
        if self._is_timestamp_property(col_name):
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
            # Default to VARCHAR with reasonable length
            return 'VARCHAR(16777216)'  # Snowflake max VARCHAR length
        else:
            return 'VARCHAR(16777216)'
    
    def prepare_for_snowflake(self, 
                            df: pd.DataFrame, 
                            chunk_size: int = 10000) -> List[pd.DataFrame]:
        """
        Prepare DataFrame for Snowflake loading by chunking and final formatting.
        
        Args:
            df: DataFrame to prepare
            chunk_size: Size of chunks for loading
            
        Returns:
            List of DataFrame chunks ready for loading
        """
        if df.empty:
            return []
        
        # Replace NaN values with None for proper NULL handling
        df = df.where(pd.notnull(df), None)
        
        # Convert datetime columns to strings for Snowflake compatibility
        for col in df.columns:
            if df[col].dtype == 'datetime64[ns]':
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Split into chunks
        chunks = []
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size].copy()
            chunks.append(chunk)
        
        logger.info(f"Prepared {len(chunks)} chunks of data for Snowflake loading")
        return chunks
    
    def validate_data_quality(self, df: pd.DataFrame, object_type: str) -> Dict[str, Any]:
        """
        Validate data quality and generate a report.
        
        Args:
            df: DataFrame to validate
            object_type: Type of objects
            
        Returns:
            Data quality report
        """
        if df.empty:
            return {'status': 'empty', 'message': 'No data to validate'}
        
        report = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'null_counts': df.isnull().sum().to_dict(),
            'duplicate_ids': 0,
            'data_types': df.dtypes.astype(str).to_dict(),
            'issues': []
        }
        
        # Check for duplicate IDs
        if 'id' in df.columns:
            duplicate_ids = df['id'].duplicated().sum()
            report['duplicate_ids'] = duplicate_ids
            if duplicate_ids > 0:
                report['issues'].append(f"Found {duplicate_ids} duplicate IDs")
        
        # Check for completely null columns
        completely_null_cols = [col for col, null_count in report['null_counts'].items() 
                               if null_count == len(df)]
        if completely_null_cols:
            report['issues'].append(f"Completely null columns: {completely_null_cols}")
        
        # Check for required fields based on object type
        required_fields = {
            'contacts': ['email', 'firstname', 'lastname'],
            'companies': ['name'],
            'deals': ['dealname', 'amount'],
            'tickets': ['subject']
        }
        
        if object_type in required_fields:
            for field in required_fields[object_type]:
                if field in df.columns:
                    null_count = df[field].isnull().sum()
                    if null_count > len(df) * 0.5:  # More than 50% null
                        report['issues'].append(f"High null rate in required field {field}: {null_count}/{len(df)}")
        
        report['status'] = 'passed' if not report['issues'] else 'warning'
        
        return report
