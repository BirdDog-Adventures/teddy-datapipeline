#!/usr/bin/env python3
"""
Deploy Lambda Function with S3 Trigger Setup
Alternative deployment without Docker dependency
"""

import json
import boto3
import zipfile
import os
import tempfile
import shutil
from datetime import datetime

def create_deployment_package():
    """Create ZIP deployment package for Lambda"""
    print("📦 Creating Lambda deployment package...")
    
    # Get the script directory and project root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # Create temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        package_dir = os.path.join(temp_dir, "package")
        os.makedirs(package_dir)
        
        # Copy Lambda function
        lambda_source = os.path.join(project_root, "lambda", "s3_to_snowflake_loader.py")
        if os.path.exists(lambda_source):
            shutil.copy2(lambda_source, os.path.join(package_dir, "lambda_function.py"))
            print(f"✅ Copied Lambda function from {lambda_source}")
        else:
            print(f"❌ Lambda function not found at {lambda_source}")
            return None
        
        # Copy utils
        utils_source = os.path.join(project_root, "lambda", "utils")
        if os.path.exists(utils_source):
            shutil.copytree(utils_source, os.path.join(package_dir, "utils"))
            print(f"✅ Copied utils from {utils_source}")
        
        # Create ZIP file
        zip_path = os.path.join(project_root, "s3_to_snowflake_deployment.zip")
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(package_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, package_dir)
                    zipf.write(file_path, arcname)
        
        print(f"✅ Created deployment package: {zip_path}")
        return zip_path

def create_lambda_function():
    """Create new Lambda function with ZIP deployment"""
    print("🔄 Creating Lambda function...")
    
    lambda_client = boto3.client('lambda')
    function_name = "teddy-s3-to-snowflake-loader-dev"
    
    try:
        # Check if function already exists
        try:
            lambda_client.get_function(FunctionName=function_name)
            print(f"📝 Function {function_name} already exists, updating instead...")
            return update_existing_function()
        except lambda_client.exceptions.ResourceNotFoundException:
            print(f"🆕 Creating new function: {function_name}")
        
        # Create deployment package
        zip_path = create_deployment_package()
        
        # Create function
        with open(zip_path, 'rb') as zip_file:
            response = lambda_client.create_function(
                FunctionName=function_name,
                Runtime='python3.11',
                Role='arn:aws:iam::551565094761:role/teddy-data-pipeline-dev-LambdaExecutionRole',
                Handler='lambda_function.lambda_handler',
                Code={'ZipFile': zip_file.read()},
                Description='S3 to Snowflake data loader for parcel data',
                Timeout=300,
                MemorySize=1024,
                Environment={
                    'Variables': {
                        'SNOWFLAKE_ACCOUNT': 'JJODRXK-BIRDDOGAWS',
                        'SNOWFLAKE_USER': 'TEDDY_PIPELINE_USER',
                        'SNOWFLAKE_WAREHOUSE': 'COMPUTE_WH',
                        'SNOWFLAKE_DATABASE': 'BIRDDOG_DATA',
                        'SNOWFLAKE_SCHEMA': 'RAW',
                        'SNOWFLAKE_PRIVATE_KEY_SECRET': 'teddy-pipeline/snowflake-private-key',
                        'DATA_BUCKET': 'teddy-data-lake-dev',
                        'ENVIRONMENT': 'dev'
                    }
                }
            )
        
        print(f"✅ Created function: {function_name}")
        
        # Clean up
        os.remove(zip_path)
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating Lambda function: {e}")
        return False

def update_existing_function():
    """Update existing Lambda function with new code"""
    lambda_client = boto3.client('lambda')
    function_name = "teddy-s3-to-snowflake-loader-dev"
    
    try:
        # Create deployment package
        zip_path = create_deployment_package()
        
        # Update function code
        with open(zip_path, 'rb') as zip_file:
            response = lambda_client.update_function_code(
                FunctionName=function_name,
                ZipFile=zip_file.read()
            )
        
        print(f"✅ Updated function: {function_name}")
        
        # Update environment variables
        lambda_client.update_function_configuration(
            FunctionName=function_name,
            Environment={
                'Variables': {
                    'SNOWFLAKE_ACCOUNT': 'JJODRXK-BIRDDOGAWS',
                    'SNOWFLAKE_USER': 'TEDDY_PIPELINE_USER',
                    'SNOWFLAKE_WAREHOUSE': 'COMPUTE_WH',
                    'SNOWFLAKE_DATABASE': 'BIRDDOG_DATA',
                    'SNOWFLAKE_SCHEMA': 'RAW',
                    'SNOWFLAKE_PRIVATE_KEY_SECRET': 'teddy-pipeline/snowflake-private-key',
                    'DATA_BUCKET': 'teddy-data-lake-dev',
                    'ENVIRONMENT': 'dev'
                }
            },
            Timeout=300,
            MemorySize=1024
        )
        
        print("✅ Updated function configuration")
        
        # Clean up
        os.remove(zip_path)
        
        return True
        
    except Exception as e:
        print(f"❌ Error updating Lambda function: {e}")
        return False

def setup_s3_trigger():
    """Set up S3 trigger for Lambda function"""
    print("🔗 Setting up S3 trigger...")
    
    lambda_client = boto3.client('lambda')
    s3_client = boto3.client('s3')
    
    function_name = "teddy-s3-to-snowflake-loader-dev"
    bucket_name = "teddy-data-lake-dev"
    
    try:
        # Get function ARN
        function_response = lambda_client.get_function(FunctionName=function_name)
        function_arn = function_response['Configuration']['FunctionArn']
        
        # Add permission for S3 to invoke Lambda
        try:
            lambda_client.add_permission(
                FunctionName=function_name,
                StatementId='s3-trigger-permission',
                Action='lambda:InvokeFunction',
                Principal='s3.amazonaws.com',
                SourceArn=f'arn:aws:s3:::{bucket_name}'
            )
            print("✅ Added S3 invoke permission")
        except lambda_client.exceptions.ResourceConflictException:
            print("📝 S3 invoke permission already exists")
        
        # Configure S3 bucket notification
        notification_config = {
            'LambdaFunctionConfigurations': [
                {
                    'Id': 's3-to-snowflake-trigger',
                    'LambdaFunctionArn': function_arn,
                    'Events': ['s3:ObjectCreated:*'],
                    'Filter': {
                        'Key': {
                            'FilterRules': [
                                {
                                    'Name': 'prefix',
                                    'Value': 'raw/parcel/'
                                },
                                {
                                    'Name': 'suffix',
                                    'Value': '.json'
                                }
                            ]
                        }
                    }
                }
            ]
        }
        
        s3_client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration=notification_config
        )
        
        print("✅ Configured S3 bucket notification")
        return True
        
    except Exception as e:
        print(f"❌ Error setting up S3 trigger: {e}")
        return False

def test_existing_file():
    """Test the Lambda function with existing S3 file"""
    print("🧪 Testing Lambda function with existing file...")
    
    lambda_client = boto3.client('lambda')
    function_name = "teddy-s3-to-snowflake-loader-dev"
    
    # Create test event for existing file
    test_event = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": datetime.utcnow().isoformat() + "Z",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {
                        "name": "teddy-data-lake-dev"
                    },
                    "object": {
                        "key": "raw/parcel/shackelford/20250804_210154_tx_shackelford.json"
                    }
                }
            }
        ]
    }
    
    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(test_event)
        )
        
        response_payload = json.loads(response['Payload'].read())
        
        if response['StatusCode'] == 200:
            print("✅ Lambda function test successful")
            print(f"📊 Response: {json.dumps(response_payload, indent=2)}")
            return True
        else:
            print(f"❌ Lambda function test failed: {response_payload}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing Lambda function: {e}")
        return False

def verify_deployment():
    """Verify the deployment is working"""
    print("🔍 Verifying deployment...")
    
    # Check S3 bucket notification
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_bucket_notification_configuration(
            Bucket='teddy-data-lake-dev'
        )
        
        if 'LambdaFunctionConfigurations' in response:
            print("✅ S3 bucket notification configured")
        else:
            print("❌ S3 bucket notification not found")
            
    except Exception as e:
        print(f"❌ Error checking S3 notification: {e}")
    
    # Check Lambda function
    lambda_client = boto3.client('lambda')
    try:
        response = lambda_client.get_function_configuration(
            FunctionName='teddy-s3-to-snowflake-loader-dev'
        )
        
        print(f"✅ Lambda function exists: {response['FunctionName']}")
        print(f"📊 Runtime: {response['Runtime']}")
        print(f"📊 Memory: {response['MemorySize']} MB")
        print(f"📊 Timeout: {response['Timeout']} seconds")
        
    except Exception as e:
        print(f"❌ Error checking Lambda function: {e}")

def main():
    """Main deployment function"""
    print("🚀 Lambda Deployment with S3 Trigger Setup")
    print("=" * 50)
    
    success_count = 0
    total_steps = 4
    
    # Step 1: Create Lambda function
    if create_lambda_function():
        success_count += 1
        print("✅ Step 1/4: Lambda function created")
    else:
        print("❌ Step 1/4: Lambda function creation failed")
    
    # Step 2: Setup S3 trigger
    if setup_s3_trigger():
        success_count += 1
        print("✅ Step 2/4: S3 trigger configured")
    else:
        print("❌ Step 2/4: S3 trigger setup failed")
    
    # Step 3: Test with existing file
    if test_existing_file():
        success_count += 1
        print("✅ Step 3/4: Function test successful")
    else:
        print("❌ Step 3/4: Function test failed")
    
    # Step 4: Verify deployment
    verify_deployment()
    success_count += 1
    print("✅ Step 4/4: Deployment verification complete")
    
    # Summary
    print(f"\n🎉 Deployment Summary")
    print("=" * 30)
    print(f"✅ Successful steps: {success_count}/{total_steps}")
    
    if success_count == total_steps:
        print("🎉 Deployment completed successfully!")
        print("\n📋 Next Steps:")
        print("1. Check CloudWatch logs for function execution")
        print("2. Upload a new file to test automatic processing")
        print("3. Query Snowflake to verify data loading")
        print("4. Monitor function performance")
    else:
        print("⚠️ Deployment completed with some issues")
        print("Check the error messages above for troubleshooting")
    
    return 0 if success_count == total_steps else 1

if __name__ == "__main__":
    exit(main())
