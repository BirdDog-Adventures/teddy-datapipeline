#!/usr/bin/env python3
"""
Shackelford County Parcel Data Ingestion Script
Uploads JSON data to S3 and triggers the Teddy Data Pipeline
"""

import json
import boto3
import os
import sys
from datetime import datetime
from pathlib import Path
import argparse

class ShackelfordDataIngester:
    def __init__(self, bucket_name="teddy-data-lake-dev", aws_region="us-east-1"):
        self.bucket_name = bucket_name
        self.aws_region = aws_region
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.stepfunctions_client = boto3.client('stepfunctions', region_name=aws_region)
        
    def validate_json_file(self, file_path):
        """Validate that the JSON file exists and is readable"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"JSON file not found: {file_path}")
        
        file_size = os.path.getsize(file_path)
        print(f"📊 File size: {file_size / (1024*1024):.2f} MB")
        
        # Test if we can read the JSON structure
        try:
            with open(file_path, 'r') as f:
                # Read just the first few characters to validate JSON format
                first_chars = f.read(100)
                if not (first_chars.strip().startswith('{') or first_chars.strip().startswith('[')):
                    raise ValueError("File doesn't appear to be valid JSON")
            print("✅ JSON file validation passed")
            return True
        except Exception as e:
            print(f"❌ JSON validation failed: {e}")
            return False
    
    def upload_to_s3(self, file_path, s3_key=None):
        """Upload the JSON file to S3"""
        if not s3_key:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = Path(file_path).name
            s3_key = f"raw/parcel/shackelford/{timestamp}_{filename}"
        
        try:
            print(f"🚀 Uploading to S3: s3://{self.bucket_name}/{s3_key}")
            
            # Upload with metadata
            self.s3_client.upload_file(
                file_path, 
                self.bucket_name, 
                s3_key,
                ExtraArgs={
                    'Metadata': {
                        'county': 'shackelford',
                        'state': 'texas',
                        'upload_timestamp': datetime.now().isoformat(),
                        'data_type': 'parcel_data',
                        'source': 'regrid_api'
                    },
                    'ContentType': 'application/json'
                }
            )
            
            print(f"✅ Successfully uploaded to S3: s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            print(f"❌ S3 upload failed: {e}")
            raise
    
    def trigger_step_functions(self, s3_key):
        """Trigger the Step Functions workflow to process the uploaded data"""
        try:
            # Get the Step Functions state machine ARN
            response = self.stepfunctions_client.list_state_machines()
            state_machine_arn = None
            
            for sm in response['stateMachines']:
                if 'teddy-data-pipeline-dev' in sm['name']:
                    state_machine_arn = sm['stateMachineArn']
                    break
            
            if not state_machine_arn:
                print("⚠️  Step Functions state machine not found. Data uploaded but workflow not triggered.")
                return None
            
            # Start execution
            execution_input = {
                "counties": ["shackelford"],
                "batch_size": 1000,
                "chunk_size": 500,
                "s3_key": s3_key,
                "trigger_source": "manual_upload",
                "timestamp": datetime.now().isoformat()
            }
            
            execution_name = f"shackelford-ingestion-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
            
            response = self.stepfunctions_client.start_execution(
                stateMachineArn=state_machine_arn,
                name=execution_name,
                input=json.dumps(execution_input)
            )
            
            print(f"✅ Step Functions execution started:")
            print(f"   Execution ARN: {response['executionArn']}")
            print(f"   Start Date: {response['startDate']}")
            
            return response['executionArn']
            
        except Exception as e:
            print(f"⚠️  Step Functions trigger failed: {e}")
            print("   Data was uploaded successfully, but automated processing may not start.")
            return None
    
    def send_api_notification(self, s3_key):
        """Send a notification through the API Gateway endpoint"""
        try:
            import requests
            
            api_url = "https://ycxdfjf2kl.execute-api.us-east-1.amazonaws.com/dev/parcel"
            
            payload = {
                "event_type": "bulk_upload_complete",
                "county": "shackelford",
                "state": "texas",
                "s3_key": s3_key,
                "timestamp": datetime.now().isoformat(),
                "data_source": "manual_upload"
            }
            
            response = requests.post(api_url, json=payload, timeout=30)
            
            if response.status_code == 200:
                print(f"✅ API notification sent successfully")
                print(f"   Response: {response.json()}")
            else:
                print(f"⚠️  API notification failed: {response.status_code}")
                
        except ImportError:
            print("⚠️  requests library not available. Skipping API notification.")
        except Exception as e:
            print(f"⚠️  API notification failed: {e}")
    
    def ingest_data(self, file_path, trigger_pipeline=True, send_notification=True):
        """Main ingestion workflow"""
        print("🚀 Starting Shackelford County Parcel Data Ingestion")
        print("=" * 60)
        
        # Step 1: Validate file
        if not self.validate_json_file(file_path):
            return False
        
        # Step 2: Upload to S3
        try:
            s3_key = self.upload_to_s3(file_path)
        except Exception as e:
            print(f"❌ Ingestion failed during S3 upload: {e}")
            return False
        
        # Step 3: Trigger processing pipeline
        if trigger_pipeline:
            execution_arn = self.trigger_step_functions(s3_key)
        
        # Step 4: Send API notification
        if send_notification:
            self.send_api_notification(s3_key)
        
        print("\n" + "=" * 60)
        print("🎉 Shackelford Data Ingestion Complete!")
        print(f"📍 S3 Location: s3://{self.bucket_name}/{s3_key}")
        
        if trigger_pipeline and execution_arn:
            print(f"⚡ Processing Pipeline: Started")
            print(f"🔗 Monitor at: AWS Console → Step Functions → Executions")
        
        print("\n📋 Next Steps:")
        print("1. Monitor the Step Functions execution in AWS Console")
        print("2. Check CloudWatch logs for Lambda function execution")
        print("3. Verify data appears in Snowflake (once pipeline is implemented)")
        print("4. Use the API endpoint to query processed data")
        
        return True

def main():
    parser = argparse.ArgumentParser(description='Ingest Shackelford County parcel data into Teddy Pipeline')
    parser.add_argument('json_file', help='Path to the Shackelford JSON file')
    parser.add_argument('--bucket', default='teddy-data-lake-dev', help='S3 bucket name')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    parser.add_argument('--no-trigger', action='store_true', help='Skip triggering Step Functions')
    parser.add_argument('--no-notification', action='store_true', help='Skip API notification')
    
    args = parser.parse_args()
    
    # Initialize ingester
    ingester = ShackelfordDataIngester(
        bucket_name=args.bucket,
        aws_region=args.region
    )
    
    # Run ingestion
    success = ingester.ingest_data(
        file_path=args.json_file,
        trigger_pipeline=not args.no_trigger,
        send_notification=not args.no_notification
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
