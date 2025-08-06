#!/usr/bin/env python3
"""
Build and Deploy Docker-based Lambda Functions
Uses existing ECR repository and Lambda execution role from successful deployment
"""

import json
import boto3
import subprocess
import os
import time
from datetime import datetime

# Configuration from DEPLOYMENT_SUCCESS_SUMMARY.md
ECR_REPOSITORY = "551565094761.dkr.ecr.us-east-1.amazonaws.com/teddy-data-pipeline-dev"
LAMBDA_EXECUTION_ROLE = "arn:aws:iam::551565094761:role/teddy-data-pipeline-dev-LambdaExecutionRole"
EXISTING_FUNCTIONS = {
    "s3_to_snowflake": "teddy-s3-to-snowflake-loader-dev",
    "bulk_ingestion": "teddy-bulk-parcel-ingestion-dev", 
    "api_ingestion": "teddy-api-parcel-ingestion-dev"
}

def run_command(command, description):
    """Run shell command with error handling"""
    print(f"🔧 {description}")
    print(f"   Command: {command}")
    
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        if result.stdout:
            print(f"   Output: {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error: {e}")
        if e.stdout:
            print(f"   Stdout: {e.stdout}")
        if e.stderr:
            print(f"   Stderr: {e.stderr}")
        return False

def docker_login():
    """Login to ECR"""
    print("🔐 Logging into ECR...")
    
    # Get ECR login token
    ecr_client = boto3.client('ecr', region_name='us-east-1')
    
    try:
        response = ecr_client.get_authorization_token()
        token = response['authorizationData'][0]['authorizationToken']
        endpoint = response['authorizationData'][0]['proxyEndpoint']
        
        # Decode token (it's base64 encoded)
        import base64
        username, password = base64.b64decode(token).decode().split(':')
        
        # Docker login
        login_cmd = f"echo {password} | docker login --username {username} --password-stdin {endpoint}"
        return run_command(login_cmd, "Docker login to ECR")
        
    except Exception as e:
        print(f"❌ ECR login failed: {e}")
        return False

def build_docker_images():
    """Build Docker images for each Lambda function"""
    print("🏗️ Building Docker images...")
    
    images_built = {}
    
    # Build S3 to Snowflake loader
    s3_image = f"{ECR_REPOSITORY}:s3-to-snowflake-latest"
    if run_command(
        f"docker build -t {s3_image} --build-arg HANDLER=s3_to_snowflake_loader.lambda_handler .",
        "Building S3 to Snowflake image"
    ):
        images_built["s3_to_snowflake"] = s3_image
    
    # Build bulk ingestion
    bulk_image = f"{ECR_REPOSITORY}:bulk-ingestion-latest"
    if run_command(
        f"docker build -t {bulk_image} --build-arg HANDLER=bulk_parcel_ingestion.lambda_handler .",
        "Building bulk ingestion image"
    ):
        images_built["bulk_ingestion"] = bulk_image
    
    # Build API ingestion
    api_image = f"{ECR_REPOSITORY}:api-ingestion-latest"
    if run_command(
        f"docker build -t {api_image} --build-arg HANDLER=api_parcel_ingestion.lambda_handler .",
        "Building API ingestion image"
    ):
        images_built["api_ingestion"] = api_image
    
    return images_built

def push_docker_images(images):
    """Push Docker images to ECR"""
    print("📤 Pushing Docker images to ECR...")
    
    pushed_images = {}
    
    for function_type, image_uri in images.items():
        if run_command(f"docker push {image_uri}", f"Pushing {function_type} image"):
            pushed_images[function_type] = image_uri
            print(f"✅ Successfully pushed {function_type}: {image_uri}")
    
    return pushed_images

def update_lambda_functions(images):
    """Update existing Lambda functions to use new container images"""
    print("🔄 Updating Lambda functions with new container images...")
    
    lambda_client = boto3.client('lambda', region_name='us-east-1')
    updated_functions = {}
    
    for function_type, image_uri in images.items():
        function_name = EXISTING_FUNCTIONS.get(function_type)
        if not function_name:
            print(f"⚠️ No existing function found for {function_type}")
            continue
        
        try:
            print(f"🔄 Updating {function_name} with image {image_uri}")
            
            # Update function code to use container image
            response = lambda_client.update_function_code(
                FunctionName=function_name,
                ImageUri=image_uri
            )
            
            # Wait for update to complete
            print(f"⏳ Waiting for {function_name} update to complete...")
            waiter = lambda_client.get_waiter('function_updated')
            waiter.wait(FunctionName=function_name)
            
            # Update function configuration if needed
            lambda_client.update_function_configuration(
                FunctionName=function_name,
                Timeout=300,  # 5 minutes
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
            
            updated_functions[function_type] = {
                'function_name': function_name,
                'image_uri': image_uri,
                'status': 'updated'
            }
            
            print(f"✅ Successfully updated {function_name}")
            
        except Exception as e:
            print(f"❌ Error updating {function_name}: {e}")
            updated_functions[function_type] = {
                'function_name': function_name,
                'image_uri': image_uri,
                'status': 'failed',
                'error': str(e)
            }
    
    return updated_functions

def create_new_s3_trigger_function(image_uri):
    """Create new S3-triggered Lambda function if it doesn't exist"""
    print("🆕 Creating new S3-triggered Lambda function...")
    
    lambda_client = boto3.client('lambda', region_name='us-east-1')
    
    function_name = "teddy-s3-to-snowflake-loader-dev"
    
    try:
        # Check if function exists
        try:
            lambda_client.get_function(FunctionName=function_name)
            print(f"📝 Function {function_name} already exists, will update instead")
            return None
        except lambda_client.exceptions.ResourceNotFoundException:
            pass
        
        # Create new function
        response = lambda_client.create_function(
            FunctionName=function_name,
            Role=LAMBDA_EXECUTION_ROLE,
            Code={'ImageUri': image_uri},
            PackageType='Image',
            Description='S3 to Snowflake data loader with real Snowflake integration',
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
        
        print(f"✅ Created new function: {function_name}")
        return response
        
    except Exception as e:
        print(f"❌ Error creating function {function_name}: {e}")
        return None

def test_lambda_functions(updated_functions):
    """Test the updated Lambda functions"""
    print("🧪 Testing updated Lambda functions...")
    
    lambda_client = boto3.client('lambda', region_name='us-east-1')
    test_results = {}
    
    for function_type, function_info in updated_functions.items():
        if function_info['status'] != 'updated':
            continue
            
        function_name = function_info['function_name']
        
        try:
            print(f"🧪 Testing {function_name}...")
            
            # Create test event based on function type
            if function_type == "s3_to_snowflake":
                test_event = {
                    "Records": [{
                        "eventVersion": "2.1",
                        "eventSource": "aws:s3",
                        "awsRegion": "us-east-1",
                        "eventTime": datetime.utcnow().isoformat() + "Z",
                        "eventName": "ObjectCreated:Put",
                        "s3": {
                            "bucket": {"name": "teddy-data-lake-dev"},
                            "object": {"key": "raw/parcel/shackelford/20250804_185035_tx_shackelford.json"}
                        }
                    }]
                }
            elif function_type == "bulk_ingestion":
                test_event = {
                    "counties": ["shackelford"],
                    "batch_size": 100,
                    "chunk_size": 50
                }
            else:  # api_ingestion
                test_event = {
                    "body": json.dumps({
                        "type": "address",
                        "address": "123 Main St, Austin, TX"
                    })
                }
            
            # Invoke function
            response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(test_event)
            )
            
            response_payload = json.loads(response['Payload'].read())
            
            test_results[function_type] = {
                'function_name': function_name,
                'status_code': response['StatusCode'],
                'response': response_payload,
                'success': response['StatusCode'] == 200
            }
            
            if response['StatusCode'] == 200:
                print(f"✅ {function_name} test successful")
            else:
                print(f"❌ {function_name} test failed: {response_payload}")
                
        except Exception as e:
            print(f"❌ Error testing {function_name}: {e}")
            test_results[function_type] = {
                'function_name': function_name,
                'success': False,
                'error': str(e)
            }
    
    return test_results

def main():
    """Main deployment function"""
    print("🚀 Docker-based Lambda Deployment")
    print("=" * 50)
    print(f"📋 ECR Repository: {ECR_REPOSITORY}")
    print(f"📋 Lambda Role: {LAMBDA_EXECUTION_ROLE}")
    print(f"📋 Functions to update: {list(EXISTING_FUNCTIONS.values())}")
    print()
    
    # Step 1: Docker login
    if not docker_login():
        print("❌ Docker login failed")
        return 1
    
    # Step 2: Build Docker images
    images = build_docker_images()
    if not images:
        print("❌ No images built successfully")
        return 1
    
    print(f"✅ Built {len(images)} Docker images")
    
    # Step 3: Push images to ECR
    pushed_images = push_docker_images(images)
    if not pushed_images:
        print("❌ No images pushed successfully")
        return 1
    
    print(f"✅ Pushed {len(pushed_images)} images to ECR")
    
    # Step 4: Update Lambda functions
    updated_functions = update_lambda_functions(pushed_images)
    
    # Step 5: Create S3 trigger function if needed
    if "s3_to_snowflake" in pushed_images:
        create_new_s3_trigger_function(pushed_images["s3_to_snowflake"])
    
    # Step 6: Test functions
    test_results = test_lambda_functions(updated_functions)
    
    # Summary
    print("\n🎉 Deployment Summary")
    print("=" * 30)
    
    for function_type, result in updated_functions.items():
        status = "✅" if result['status'] == 'updated' else "❌"
        print(f"{status} {result['function_name']}: {result['status']}")
        if result['status'] == 'failed':
            print(f"   Error: {result.get('error', 'Unknown error')}")
    
    print(f"\n📊 Results:")
    print(f"   Images built: {len(images)}")
    print(f"   Images pushed: {len(pushed_images)}")
    print(f"   Functions updated: {len([f for f in updated_functions.values() if f['status'] == 'updated'])}")
    
    print(f"\n📋 Next Steps:")
    print("1. Check CloudWatch logs for function execution")
    print("2. Test S3 upload to trigger automatic processing")
    print("3. Verify data appears in Snowflake tables")
    print("4. Monitor function performance and errors")
    
    return 0

if __name__ == "__main__":
    exit(main())
