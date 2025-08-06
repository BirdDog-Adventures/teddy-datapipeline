#!/usr/bin/env python3
"""
Deploy S3 to Snowflake Lambda Function
Creates and deploys the automated S3-triggered Lambda function
"""

import json
import boto3
import zipfile
import os
import time
from datetime import datetime

def create_lambda_deployment_package():
    """
    Create a deployment package for the S3 to Snowflake Lambda function
    """
    print("📦 Creating Lambda deployment package...")
    
    # Create a temporary directory for the package
    package_dir = "temp_lambda_package"
    os.makedirs(package_dir, exist_ok=True)
    
    try:
        # Copy the Lambda function code
        lambda_code = """
"""
        
        # Read the actual Lambda function code
        with open('lambda/s3_to_snowflake_loader.py', 'r') as f:
            lambda_code = f.read()
        
        # Write to package directory
        with open(f"{package_dir}/lambda_function.py", 'w') as f:
            f.write(lambda_code)
        
        # Create requirements.txt for the Lambda layer
        requirements = """
snowflake-connector-python==3.7.0
boto3==1.34.0
"""
        
        with open(f"{package_dir}/requirements.txt", 'w') as f:
            f.write(requirements)
        
        # Create the ZIP file
        zip_filename = "s3_to_snowflake_lambda.zip"
        
        with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(f"{package_dir}/lambda_function.py", "lambda_function.py")
            zipf.write(f"{package_dir}/requirements.txt", "requirements.txt")
        
        print(f"✅ Created deployment package: {zip_filename}")
        return zip_filename
        
    finally:
        # Clean up temporary directory
        import shutil
        if os.path.exists(package_dir):
            shutil.rmtree(package_dir)

def deploy_lambda_function():
    """
    Deploy the S3 to Snowflake Lambda function
    """
    print("🚀 Deploying S3 to Snowflake Lambda function...")
    
    lambda_client = boto3.client('lambda')
    iam_client = boto3.client('iam')
    
    function_name = "teddy-s3-to-snowflake-loader-dev"
    
    try:
        # Create deployment package
        zip_filename = create_lambda_deployment_package()
        
        # Read the ZIP file
        with open(zip_filename, 'rb') as f:
            zip_content = f.read()
        
        # Check if function already exists
        function_exists = False
        try:
            lambda_client.get_function(FunctionName=function_name)
            function_exists = True
            print(f"📝 Function {function_name} already exists, updating...")
        except lambda_client.exceptions.ResourceNotFoundException:
            print(f"🆕 Creating new function {function_name}...")
        
        # Lambda function configuration
        function_config = {
            'FunctionName': function_name,
            'Runtime': 'python3.9',
            'Role': 'arn:aws:iam::551565094761:role/teddy-lambda-execution-role-dev',  # Use existing role
            'Handler': 'lambda_function.lambda_handler',
            'Code': {'ZipFile': zip_content},
            'Description': 'Automatically loads parcel data from S3 to Snowflake',
            'Timeout': 300,  # 5 minutes
            'MemorySize': 1024,
            'Environment': {
                'Variables': {
                    'SNOWFLAKE_ACCOUNT': 'JJODRXK-BIRDDOGAWS',
                    'SNOWFLAKE_USER': 'TEDDY_PIPELINE_USER',
                    'SNOWFLAKE_WAREHOUSE': 'COMPUTE_WH',
                    'SNOWFLAKE_DATABASE': 'TEDDY_DATA',
                    'SNOWFLAKE_SCHEMA': 'RAW',
                    'SNOWFLAKE_PRIVATE_KEY_SECRET': 'teddy-pipeline/snowflake-private-key'
                }
            }
        }
        
        if function_exists:
            # Update existing function
            lambda_client.update_function_code(
                FunctionName=function_name,
                ZipFile=zip_content
            )
            
            lambda_client.update_function_configuration(
                FunctionName=function_name,
                Runtime=function_config['Runtime'],
                Role=function_config['Role'],
                Handler=function_config['Handler'],
                Description=function_config['Description'],
                Timeout=function_config['Timeout'],
                MemorySize=function_config['MemorySize'],
                Environment=function_config['Environment']
            )
        else:
            # Create new function
            lambda_client.create_function(**function_config)
        
        print(f"✅ Lambda function {function_name} deployed successfully!")
        
        # Clean up ZIP file
        os.remove(zip_filename)
        
        return function_name
        
    except Exception as e:
        print(f"❌ Error deploying Lambda function: {e}")
        return None

def configure_s3_trigger(function_name):
    """
    Configure S3 bucket to trigger the Lambda function
    """
    print("🔗 Configuring S3 trigger for Lambda function...")
    
    s3_client = boto3.client('s3')
    lambda_client = boto3.client('lambda')
    
    bucket_name = 'teddy-data-lake-dev'
    
    try:
        # Add permission for S3 to invoke Lambda
        try:
            lambda_client.add_permission(
                FunctionName=function_name,
                StatementId='s3-trigger-permission',
                Action='lambda:InvokeFunction',
                Principal='s3.amazonaws.com',
                SourceArn=f'arn:aws:s3:::{bucket_name}'
            )
            print("✅ Added S3 invoke permission to Lambda function")
        except lambda_client.exceptions.ResourceConflictException:
            print("📝 S3 invoke permission already exists")
        
        # Configure S3 bucket notification
        notification_config = {
            'LambdaConfigurations': [
                {
                    'Id': 'parcel-data-processing',
                    'LambdaFunctionArn': f'arn:aws:lambda:us-east-1:551565094761:function:{function_name}',
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
        
        # Get existing notification configuration
        try:
            existing_config = s3_client.get_bucket_notification_configuration(Bucket=bucket_name)
            
            # Merge with existing configurations
            if 'LambdaConfigurations' in existing_config:
                # Remove any existing configuration for our function
                existing_config['LambdaConfigurations'] = [
                    config for config in existing_config['LambdaConfigurations']
                    if config.get('Id') != 'parcel-data-processing'
                ]
                # Add our new configuration
                existing_config['LambdaConfigurations'].extend(notification_config['LambdaConfigurations'])
                notification_config = existing_config
            
        except s3_client.exceptions.NoSuchConfiguration:
            # No existing configuration
            pass
        
        # Set the notification configuration
        s3_client.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration=notification_config
        )
        
        print(f"✅ Configured S3 bucket {bucket_name} to trigger Lambda function")
        return True
        
    except Exception as e:
        print(f"❌ Error configuring S3 trigger: {e}")
        return False

def test_deployment(function_name):
    """
    Test the deployed Lambda function
    """
    print("🧪 Testing deployed Lambda function...")
    
    lambda_client = boto3.client('lambda')
    
    # Create a test S3 event
    test_event = {
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
                        "name": "teddy-data-lake-dev",
                        "arn": "arn:aws:s3:::teddy-data-lake-dev"
                    },
                    "object": {
                        "key": "raw/parcel/shackelford/20250804_185035_tx_shackelford.json",
                        "size": 30000000
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
        
        print(f"📊 Test Response Status: {response['StatusCode']}")
        print(f"📋 Response: {json.dumps(response_payload, indent=2)}")
        
        if response['StatusCode'] == 200:
            print("✅ Lambda function test successful!")
            return True
        else:
            print("❌ Lambda function test failed")
            return False
            
    except Exception as e:
        print(f"❌ Error testing Lambda function: {e}")
        return False

def main():
    """
    Main deployment function
    """
    print("🚀 S3 to Snowflake Lambda Deployment")
    print("=" * 50)
    
    # Deploy the Lambda function
    function_name = deploy_lambda_function()
    if not function_name:
        print("❌ Lambda deployment failed")
        return 1
    
    # Wait a moment for the function to be ready
    print("⏳ Waiting for function to be ready...")
    time.sleep(10)
    
    # Configure S3 trigger
    if not configure_s3_trigger(function_name):
        print("❌ S3 trigger configuration failed")
        return 1
    
    # Test the deployment
    if not test_deployment(function_name):
        print("❌ Deployment test failed")
        return 1
    
    print("\n🎉 Deployment completed successfully!")
    print(f"📋 Function Name: {function_name}")
    print(f"🔗 S3 Trigger: Configured for bucket teddy-data-lake-dev")
    print(f"📁 Trigger Path: raw/parcel/*.json")
    
    print("\n📋 Next Steps:")
    print("1. Your existing Shackelford data will be automatically processed")
    print("2. Any new JSON files uploaded to raw/parcel/ will trigger processing")
    print("3. Check CloudWatch logs for processing details")
    print("4. Verify data in Snowflake tables")
    
    return 0

if __name__ == "__main__":
    exit(main())
