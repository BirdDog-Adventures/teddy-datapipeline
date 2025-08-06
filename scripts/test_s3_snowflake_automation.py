#!/usr/bin/env python3
"""
Test S3 to Snowflake Automation
Tests the automated S3-triggered Lambda function for loading data into Snowflake
"""

import json
import boto3
import time
from datetime import datetime

def test_s3_snowflake_automation():
    """
    Test the S3 to Snowflake automation by triggering the Lambda function
    """
    
    print("🧪 Testing S3 to Snowflake Automation")
    print("=" * 50)
    
    # Initialize AWS clients
    lambda_client = boto3.client('lambda')
    s3_client = boto3.client('s3')
    
    # Test with existing Shackelford data
    bucket_name = 'teddy-data-lake-dev'
    
    try:
        # List existing Shackelford files
        print("🔍 Looking for existing Shackelford data...")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix='raw/parcel/shackelford/'
        )
        
        if 'Contents' not in response:
            print("❌ No Shackelford data found in S3")
            return False
        
        files = [obj for obj in response['Contents'] if obj['Key'].endswith('.json')]
        if not files:
            print("❌ No JSON files found in Shackelford directory")
            return False
        
        print(f"📁 Found {len(files)} JSON files to test with")
        
        # Test with the first file
        test_file = files[0]
        s3_key = test_file['Key']
        
        print(f"🎯 Testing with file: {s3_key}")
        
        # Create a mock S3 event to trigger the Lambda function
        mock_s3_event = {
            "Records": [
                {
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "awsRegion": "us-east-1",
                    "eventTime": datetime.utcnow().isoformat() + "Z",
                    "eventName": "ObjectCreated:Put",
                    "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "test-config",
                        "bucket": {
                            "name": bucket_name,
                            "arn": f"arn:aws:s3:::{bucket_name}"
                        },
                        "object": {
                            "key": s3_key,
                            "size": test_file['Size']
                        }
                    }
                }
            ]
        }
        
        # Find the S3 to Snowflake Lambda function
        print("🔍 Looking for S3 to Snowflake Lambda function...")
        
        try:
            functions_response = lambda_client.list_functions()
            s3_snowflake_function = None
            
            for func in functions_response['Functions']:
                if 's3-to-snowflake' in func['FunctionName'].lower() or 'snowflake-loader' in func['FunctionName'].lower():
                    s3_snowflake_function = func['FunctionName']
                    break
            
            if not s3_snowflake_function:
                print("⚠️  S3 to Snowflake Lambda function not found")
                print("   The function needs to be deployed first")
                return False
            
            print(f"✅ Found Lambda function: {s3_snowflake_function}")
            
            # Invoke the Lambda function with the mock S3 event
            print("🚀 Invoking Lambda function with S3 event...")
            
            response = lambda_client.invoke(
                FunctionName=s3_snowflake_function,
                InvocationType='RequestResponse',
                Payload=json.dumps(mock_s3_event)
            )
            
            # Parse the response
            response_payload = json.loads(response['Payload'].read())
            
            print(f"📊 Lambda Response Status: {response['StatusCode']}")
            print(f"📋 Response Payload: {json.dumps(response_payload, indent=2)}")
            
            if response['StatusCode'] == 200:
                print("✅ Lambda function executed successfully!")
                
                # Check if data was processed
                if 'body' in response_payload:
                    body = response_payload['body']
                    if isinstance(body, str):
                        body = json.loads(body)
                    
                    processed_files = body.get('processed_files', 0)
                    results = body.get('results', [])
                    
                    print(f"📈 Processed {processed_files} files")
                    
                    for result in results:
                        if result.get('status') == 'success':
                            print(f"✅ Successfully loaded: {result.get('s3_key')}")
                            print(f"   County: {result.get('county')}")
                            print(f"   Raw records: {result.get('raw_records')}")
                            print(f"   Staging records: {result.get('staging_records')}")
                        else:
                            print(f"❌ Failed to load: {result.get('s3_key')}")
                            print(f"   Error: {result.get('error')}")
                
                return True
            else:
                print(f"❌ Lambda function failed with status: {response['StatusCode']}")
                return False
                
        except Exception as e:
            print(f"❌ Error invoking Lambda function: {e}")
            return False
        
    except Exception as e:
        print(f"❌ Error testing S3 to Snowflake automation: {e}")
        return False

def verify_data_in_snowflake():
    """
    Verify that data was loaded into Snowflake
    """
    print("\n🔍 Verifying data in Snowflake...")
    
    try:
        # This would require Snowflake connection
        # For now, just provide instructions
        print("📋 To verify data in Snowflake, run these queries:")
        print()
        print("-- Check raw data count")
        print("SELECT COUNT(*) FROM BIRDDOG_DATA.RAW.PARCEL_DATA_RAW WHERE COUNTY = 'shackelford';")
        print()
        print("-- Check staging data count")
        print("SELECT COUNT(*) FROM BIRDDOG_DATA.RAW.PARCEL_DATA_STAGING WHERE COUNTY = 'shackelford';")
        print()
        print("-- View sample data")
        print("SELECT PARCEL_ID, ADDRESS, OWNER_NAME, PROPERTY_TYPE, ACRES, ASSESSED_VALUE")
        print("FROM BIRDDOG_DATA.RAW.PARCEL_DATA_STAGING")
        print("WHERE COUNTY = 'shackelford'")
        print("LIMIT 5;")
        
        return True
        
    except Exception as e:
        print(f"❌ Error verifying Snowflake data: {e}")
        return False

def main():
    """
    Main test function
    """
    print("🧪 S3 to Snowflake Automation Test")
    print("=" * 60)
    
    # Test the automation
    if test_s3_snowflake_automation():
        print("\n🎉 Automation test completed successfully!")
        
        # Provide verification instructions
        verify_data_in_snowflake()
        
        print("\n📋 Next Steps:")
        print("1. Check the Lambda function logs in CloudWatch")
        print("2. Verify data in Snowflake using the provided queries")
        print("3. Test with new file uploads to S3")
        
        return 0
    else:
        print("\n❌ Automation test failed")
        print("\n📋 Troubleshooting:")
        print("1. Ensure the S3 to Snowflake Lambda function is deployed")
        print("2. Check Lambda function permissions for S3 and Snowflake")
        print("3. Verify Snowflake credentials in AWS Secrets Manager")
        
        return 1

if __name__ == "__main__":
    exit(main())
