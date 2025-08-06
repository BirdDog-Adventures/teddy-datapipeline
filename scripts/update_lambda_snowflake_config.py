#!/usr/bin/env python3
"""
Update Lambda function environment variables to use correct Teddy Snowflake configuration
"""

import boto3
import json
import sys

def update_lambda_env_vars():
    """Update Lambda function environment variables"""
    
    # Initialize Lambda client
    lambda_client = boto3.client('lambda')
    
    # Correct Teddy Snowflake configuration
    teddy_env_vars = {
        "SNOWFLAKE_ACCOUNT": "JJODRXK-BIRDDOGAWS",
        "SNOWFLAKE_DATABASE": "TEDDY_DATA",
        "SNOWFLAKE_SCHEMA": "RAW", 
        "ENVIRONMENT": "dev",
        "SNOWFLAKE_WAREHOUSE": "TEDDY_INGESTION_WH",
        "SNOWFLAKE_PRIVATE_KEY_SECRET": "teddy-pipeline/snowflake-private-key",
        "DATA_BUCKET": "teddy-data-lake-dev",
        "SNOWFLAKE_USER": "TEDDY_PIPELINE_USER"
    }
    
    # Lambda functions to update
    functions = [
        "teddy-s3-to-snowflake-loader-dev",
        "teddy-api-parcel-ingestion-dev", 
        "teddy-bulk-parcel-ingestion-dev"
    ]
    
    print("🔧 Updating Lambda function Snowflake configurations...")
    print("=" * 60)
    
    for function_name in functions:
        try:
            print(f"📝 Updating {function_name}...")
            
            # Update environment variables
            response = lambda_client.update_function_configuration(
                FunctionName=function_name,
                Environment={
                    'Variables': teddy_env_vars
                }
            )
            
            print(f"✅ Successfully updated {function_name}")
            print(f"   📊 Last Modified: {response['LastModified']}")
            
        except Exception as e:
            print(f"❌ Failed to update {function_name}: {str(e)}")
    
    print("\n🎉 Lambda function configuration update complete!")
    print("\n📋 Updated Configuration:")
    for key, value in teddy_env_vars.items():
        print(f"   {key}: {value}")

if __name__ == "__main__":
    update_lambda_env_vars()
