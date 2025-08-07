#!/bin/bash
# Fix API Lambda with Snowflake Connector - Password Authentication
# This script uses the official Snowflake connector with password auth (no JWT/cryptography)

set -e

echo "🔧 Fixing API Lambda with Snowflake Connector (Password Auth)..."

# Create build directory
BUILD_DIR="lambda-build-connector"
if [ -d "$BUILD_DIR" ]; then
    rm -rf "$BUILD_DIR"
fi
mkdir "$BUILD_DIR"
cd "$BUILD_DIR"

echo "📦 Creating deployment package with Snowflake connector..."

# Copy Lambda function code
if [ -d "../lambda" ]; then
    cp -r ../lambda/* .
else
    echo "❌ Lambda directory not found. Please run from teddy-datapipeline root."
    exit 1
fi

# Create requirements.txt with compatible Snowflake connector
cat > requirements.txt << 'EOF'
snowflake-connector-python>=1.9.1,<2.0.0
requests>=2.20.0
boto3>=1.4.4,<1.10.0
urllib3>=1.21.1,<2.0
botocore>=1.5.0,<1.13.0
EOF

echo "📥 Installing dependencies for Lambda architecture..."

# Install packages for Lambda Linux x86_64 architecture
pip install --platform linux_x86_64 --implementation cp --python-version 3.11 --only-binary=:all: --upgrade --target . -r requirements.txt

# Create Snowflake connector with password authentication
cat > utils/snowflake_connector.py << 'EOF'
"""
Snowflake Connector - Password Authentication (No JWT/Cryptography)
Uses official snowflake-connector-python with password auth
"""

import json
import logging
import os
from typing import Dict, Any, Optional, List
import boto3
import snowflake.connector
from snowflake.connector import DictCursor

logger = logging.getLogger(__name__)

class SnowflakeConnector:
    """Snowflake connector using password authentication"""
    
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
    
    def connect(self) -> bool:
        """Establish connection to Snowflake using password authentication"""
        try:
            if self.connection and not self.connection.is_closed():
                return True
            
            # Connection parameters using password authentication (no JWT)
            connection_params = {
                'account': self.secrets['snowflake_account'],
                'user': 'TEDDY_PIPELINE_USER_PWD',
                'password': 'TeddyPipelineUserPwd',
                'warehouse': 'TEDDY_INGESTION_WH',
                'database': 'TEDDY_DATA',
                'schema': 'RAW',
                'client_session_keep_alive': True,
                'client_session_keep_alive_heartbeat_frequency': 3600
            }
            
            self.connection = snowflake.connector.connect(**connection_params)
            logger.info(f"Successfully connected to Snowflake account: {self.secrets['snowflake_account']}")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            return False
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Execute a query and return results"""
        try:
            if not self.connect():
                raise Exception("Failed to connect to Snowflake")
            
            cursor = self.connection.cursor(DictCursor)
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            results = cursor.fetchall()
            cursor.close()
            
            return results
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
    
    def insert_raw_data(self, s3_key: str, county: str, state: str, data: Dict[str, Any]) -> int:
        """Insert raw data into Snowflake"""
        try:
            if not self.connect():
                raise Exception("Failed to connect to Snowflake")
            
            cursor = self.connection.cursor()
            
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
            cursor.execute(create_table_query)
            
            # Insert data
            insert_query = """
            INSERT INTO PARCEL_DATA_RAW (FILE_NAME, JSON_PAYLOAD, S3_KEY, COUNTY, STATE, INGESTION_TIMESTAMP)
            VALUES (%s, PARSE_JSON(%s), %s, %s, %s, CURRENT_TIMESTAMP())
            """
            
            cursor.execute(insert_query, (
                s3_key, json.dumps(data), s3_key, county, state
            ))
            
            affected_rows = cursor.rowcount
            self.connection.commit()
            cursor.close()
            
            logger.info(f"Successfully inserted {affected_rows} raw records")
            return affected_rows
            
        except Exception as e:
            logger.error(f"Error inserting raw data: {e}")
            raise
    
    def insert_staging_data(self, county: str, state: str, parcels: List[Dict[str, Any]]) -> int:
        """Insert staging data into Snowflake"""
        try:
            if not self.connect():
                raise Exception("Failed to connect to Snowflake")
            
            cursor = self.connection.cursor()
            
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
            cursor.execute(create_table_query)
            
            # Insert parcels
            insert_query = """
            INSERT INTO PARCEL_STAGING (PARCEL_ID, COUNTY, STATE, PARCEL_DATA, INGESTION_TIMESTAMP)
            VALUES (%s, %s, %s, PARSE_JSON(%s), CURRENT_TIMESTAMP())
            """
            
            inserted_count = 0
            for parcel in parcels:
                parcel_id = parcel.get('id', parcel.get('parcel_id', 'unknown'))
                cursor.execute(insert_query, (
                    parcel_id, county, state, json.dumps(parcel)
                ))
                inserted_count += 1
            
            self.connection.commit()
            cursor.close()
            
            logger.info(f"Successfully inserted {inserted_count} staging records")
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error inserting staging data: {e}")
            raise
    
    def close(self):
        """Close the Snowflake connection"""
        try:
            if self.connection and not self.connection.is_closed():
                self.connection.close()
                logger.info("Snowflake connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
EOF

echo "📦 Creating deployment package..."

# Create deployment zip
zip -r ../lambda-deployment-connector.zip . -x "*.pyc" "*__pycache__*" "*.git*"

cd ..

echo "✅ Snowflake connector Lambda deployment package created: lambda-deployment-connector.zip"
echo ""
echo "🚀 To deploy the fix:"
echo "aws lambda update-function-code --function-name teddy-api-parcel-ingestion-dev --zip-file fileb://lambda-deployment-connector.zip"
echo ""
echo "📋 This solution uses:"
echo "   • Official Snowflake connector (snowflake-connector-python)"
echo "   • Password authentication (no JWT/cryptography)"
echo "   • Compatible dependency versions"
echo "   • Full Snowflake connector functionality"
