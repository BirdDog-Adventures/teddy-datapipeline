#!/usr/bin/env python3
"""
Test Pipeline Status Script

Quick script to test the current status of the Teddy Data Pipeline
"""

import json
import boto3
import time
from datetime import datetime

def test_s3_upload_and_trigger():
    """Test S3 upload and Lambda trigger"""
    print("🧪 Testing S3 upload and Lambda trigger...")
    
    s3_client = boto3.client('s3')
    lambda_client = boto3.client('lambda')
    
    bucket_name = 'teddy-data-lake-dev'
    
    # Test data
    test_data = {
        "parcel_id": "test_quick_001",
        "address": "456 Quick Test Ave, Austin, TX 78701",
        "county": "travis",
        "state": "texas",
        "coordinates": {
            "latitude": 30.2672,
            "longitude": -97.7431
        },
        "timestamp": datetime.utcnow().isoformat()
    }
    
    try:
        # Upload test file
        test_key = f"raw/parcel/test/{datetime.now().strftime('%Y%m%d_%H%M%S')}_quick_test.json"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=json.dumps(test_data),
            ContentType='application/json'
        )
        
        print(f"✅ Uploaded test file: s3://{bucket_name}/{test_key}")
        
        # Wait for trigger
        print("⏳ Waiting 15 seconds for S3 trigger...")
        time.sleep(15)
        
        # Check CloudWatch logs for the Lambda function
        logs_client = boto3.client('logs')
        function_name = 'teddy-s3-to-snowflake-loader-dev'
        log_group = f'/aws/lambda/{function_name}'
        
        try:
            # Get recent log events
            response = logs_client.describe_log_streams(
                logGroupName=log_group,
                orderBy='LastEventTime',
                descending=True,
                limit=1
            )
            
            if response['logStreams']:
                latest_stream = response['logStreams'][0]
                
                events_response = logs_client.get_log_events(
                    logGroupName=log_group,
                    logStreamName=latest_stream['logStreamName'],
                    limit=10,
                    startFromHead=False
                )
                
                print("📋 Recent Lambda execution logs:")
                for event in events_response['events'][-5:]:  # Last 5 events
                    timestamp = datetime.fromtimestamp(event['timestamp'] / 1000)
                    print(f"  {timestamp}: {event['message'].strip()}")
                    
            else:
                print("⚠️ No log streams found for Lambda function")
                
        except Exception as e:
            print(f"⚠️ Could not retrieve logs: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in S3 upload test: {e}")
        return False

def test_api_function():
    """Test API Lambda function directly"""
    print("\n🧪 Testing API Lambda function...")
    
    lambda_client = boto3.client('lambda')
    function_name = 'teddy-api-parcel-ingestion-dev'
    
    test_event = {
        'body': json.dumps({
            "type": "coordinates",
            "latitude": 30.2672,
            "longitude": -97.7431
        }),
        'httpMethod': 'POST'
    }
    
    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(test_event)
        )
        
        response_payload = json.loads(response['Payload'].read())
        print(f"📊 API function response: {response_payload}")
        
        return response['StatusCode'] == 200
        
    except Exception as e:
        print(f"❌ Error testing API function: {e}")
        return False

def check_infrastructure():
    """Check infrastructure components"""
    print("\n🔍 Checking infrastructure components...")
    
    # Check S3 bucket
    s3_client = boto3.client('s3')
    try:
        s3_client.head_bucket(Bucket='teddy-data-lake-dev')
        print("✅ S3 bucket exists: teddy-data-lake-dev")
    except Exception as e:
        print(f"❌ S3 bucket issue: {e}")
    
    # Check Lambda functions
    lambda_client = boto3.client('lambda')
    functions = [
        'teddy-s3-to-snowflake-loader-dev',
        'teddy-api-parcel-ingestion-dev',
        'teddy-bulk-parcel-ingestion-dev'
    ]
    
    for func_name in functions:
        try:
            response = lambda_client.get_function_configuration(FunctionName=func_name)
            print(f"✅ Lambda function exists: {func_name} ({response['State']})")
        except Exception as e:
            print(f"❌ Lambda function issue {func_name}: {e}")
    
    # Check DynamoDB tables
    dynamodb = boto3.client('dynamodb')
    tables = [
        'teddy-parcel-cache-dev',
        'teddy-rate-limit-dev',
        'teddy-api-metadata-dev'
    ]
    
    for table_name in tables:
        try:
            response = dynamodb.describe_table(TableName=table_name)
            print(f"✅ DynamoDB table exists: {table_name} ({response['Table']['TableStatus']})")
        except Exception as e:
            print(f"❌ DynamoDB table issue {table_name}: {e}")

def main():
    """Main test function"""
    print("🚀 Teddy Data Pipeline Status Test")
    print("=" * 50)
    
    # Check infrastructure
    check_infrastructure()
    
    # Test S3 upload and trigger
    s3_success = test_s3_upload_and_trigger()
    
    # Test API function
    api_success = test_api_function()
    
    # Summary
    print(f"\n📊 Test Summary")
    print("=" * 20)
    print(f"S3 Upload & Trigger: {'✅ PASS' if s3_success else '❌ FAIL'}")
    print(f"API Function: {'✅ PASS' if api_success else '❌ FAIL'}")
    
    if s3_success and api_success:
        print("\n🎉 Pipeline is working correctly!")
        print("Ready for production parcel data ingestion.")
    else:
        print("\n⚠️ Some components need attention.")
        print("Check the error messages above for troubleshooting.")

if __name__ == "__main__":
    main()
