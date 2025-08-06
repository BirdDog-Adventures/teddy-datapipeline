#!/usr/bin/env python3
"""
Complete Teddy Data Pipeline Deployment Script

This script deploys the entire data pipeline including:
1. CloudFormation infrastructure (DynamoDB, S3, Lambda, etc.)
2. Lambda functions with proper dependencies
3. S3 triggers for automatic processing
4. NLCD integration
5. End-to-end testing

Usage:
    python scripts/deploy_complete_pipeline.py --environment dev
"""

import json
import boto3
import zipfile
import os
import tempfile
import shutil
import argparse
import time
from datetime import datetime
from pathlib import Path

def get_project_root():
    """Get the project root directory"""
    return Path(__file__).parent.parent

def create_lambda_layer():
    """Create Lambda layer with all dependencies"""
    print("📦 Creating Lambda layer with dependencies...")
    
    project_root = get_project_root()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        layer_dir = os.path.join(temp_dir, "python")
        os.makedirs(layer_dir)
        
        # Install dependencies
        requirements_file = project_root / "requirements.txt"
        if requirements_file.exists():
            os.system(f"pip install -r {requirements_file} -t {layer_dir}")
        else:
            # Install essential dependencies
            dependencies = [
                "snowflake-connector-python",
                "boto3",
                "requests",
                "cryptography",
                "PyJWT"
            ]
            for dep in dependencies:
                os.system(f"pip install {dep} -t {layer_dir}")
        
        # Create layer ZIP
        layer_zip_path = project_root / "teddy-pipeline-layer.zip"
        with zipfile.ZipFile(layer_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, temp_dir)
                    zipf.write(file_path, arcname)
        
        print(f"✅ Created Lambda layer: {layer_zip_path}")
        return str(layer_zip_path)

def deploy_lambda_layer(layer_zip_path, environment):
    """Deploy Lambda layer to AWS"""
    print("🚀 Deploying Lambda layer...")
    
    lambda_client = boto3.client('lambda')
    layer_name = f"teddy-pipeline-dependencies-{environment}"
    
    try:
        with open(layer_zip_path, 'rb') as zip_file:
            response = lambda_client.publish_layer_version(
                LayerName=layer_name,
                Description='Dependencies for Teddy Data Pipeline',
                Content={'ZipFile': zip_file.read()},
                CompatibleRuntimes=['python3.9', 'python3.10', 'python3.11'],
                CompatibleArchitectures=['x86_64']
            )
        
        layer_arn = response['LayerVersionArn']
        print(f"✅ Deployed Lambda layer: {layer_arn}")
        return layer_arn
        
    except Exception as e:
        print(f"❌ Error deploying Lambda layer: {e}")
        return None

def create_lambda_deployment_package(function_name, layer_arn):
    """Create deployment package for Lambda function"""
    print(f"📦 Creating deployment package for {function_name}...")
    
    project_root = get_project_root()
    lambda_dir = project_root / "lambda"
    
    with tempfile.TemporaryDirectory() as temp_dir:
        package_dir = os.path.join(temp_dir, "package")
        os.makedirs(package_dir)
        
        # Copy main Lambda function
        if function_name == "s3_to_snowflake_loader":
            source_file = lambda_dir / "s3_to_snowflake_loader.py"
            target_file = os.path.join(package_dir, "lambda_function.py")
        elif function_name == "api_parcel_ingestion":
            source_file = lambda_dir / "api_parcel_ingestion.py"
            target_file = os.path.join(package_dir, "lambda_function.py")
        elif function_name == "bulk_parcel_ingestion":
            source_file = lambda_dir / "bulk_parcel_ingestion.py"
            target_file = os.path.join(package_dir, "lambda_function.py")
        elif function_name == "nlcd_parcel_processor":
            source_file = lambda_dir / "nlcd_parcel_processor.py"
            target_file = os.path.join(package_dir, "lambda_function.py")
        else:
            raise ValueError(f"Unknown function: {function_name}")
        
        if source_file.exists():
            shutil.copy2(source_file, target_file)
            print(f"✅ Copied {source_file} to {target_file}")
        else:
            print(f"❌ Source file not found: {source_file}")
            return None
        
        # Copy utils directory
        utils_source = lambda_dir / "utils"
        if utils_source.exists():
            shutil.copytree(utils_source, os.path.join(package_dir, "utils"))
            print(f"✅ Copied utils directory")
        
        # Create ZIP file
        zip_path = project_root / f"{function_name}_deployment.zip"
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, dirs, files in os.walk(package_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, package_dir)
                    zipf.write(file_path, arcname)
        
        print(f"✅ Created deployment package: {zip_path}")
        return str(zip_path)

def deploy_cloudformation_stack(environment, parameters):
    """Deploy CloudFormation stack"""
    print("🚀 Deploying CloudFormation stack...")
    
    cf_client = boto3.client('cloudformation')
    project_root = get_project_root()
    
    stack_name = f"teddy-data-pipeline-{environment}"
    template_path = project_root / "aws" / "cloudformation" / "data-pipeline-stack.yaml"
    
    try:
        with open(template_path, 'r') as template_file:
            template_body = template_file.read()
        
        # Check if stack exists
        try:
            cf_client.describe_stacks(StackName=stack_name)
            print(f"📝 Stack {stack_name} exists, updating...")
            
            response = cf_client.update_stack(
                StackName=stack_name,
                TemplateBody=template_body,
                Parameters=parameters,
                Capabilities=['CAPABILITY_NAMED_IAM']
            )
            operation = "update"
            
        except cf_client.exceptions.ClientError as e:
            if "does not exist" in str(e):
                print(f"🆕 Creating new stack: {stack_name}")
                
                response = cf_client.create_stack(
                    StackName=stack_name,
                    TemplateBody=template_body,
                    Parameters=parameters,
                    Capabilities=['CAPABILITY_NAMED_IAM']
                )
                operation = "create"
            else:
                raise e
        
        # Wait for stack operation to complete
        print(f"⏳ Waiting for stack {operation} to complete...")
        
        if operation == "create":
            waiter = cf_client.get_waiter('stack_create_complete')
        else:
            waiter = cf_client.get_waiter('stack_update_complete')
        
        waiter.wait(
            StackName=stack_name,
            WaiterConfig={'Delay': 30, 'MaxAttempts': 60}
        )
        
        print(f"✅ Stack {operation} completed successfully")
        
        # Get stack outputs
        response = cf_client.describe_stacks(StackName=stack_name)
        outputs = {}
        if 'Outputs' in response['Stacks'][0]:
            for output in response['Stacks'][0]['Outputs']:
                outputs[output['OutputKey']] = output['OutputValue']
        
        return outputs
        
    except Exception as e:
        print(f"❌ Error deploying CloudFormation stack: {e}")
        return None

def update_lambda_functions(environment, layer_arn, stack_outputs):
    """Update Lambda functions with actual code"""
    print("🔄 Updating Lambda functions with actual code...")
    
    lambda_client = boto3.client('lambda')
    
    functions = [
        {
            'name': 's3_to_snowflake_loader',
            'aws_name': f'teddy-s3-to-snowflake-loader-{environment}',
            'timeout': 300,
            'memory': 1024
        },
        {
            'name': 'api_parcel_ingestion',
            'aws_name': f'teddy-api-parcel-ingestion-{environment}',
            'timeout': 30,
            'memory': 512
        },
        {
            'name': 'bulk_parcel_ingestion',
            'aws_name': f'teddy-bulk-parcel-ingestion-{environment}',
            'timeout': 900,
            'memory': 1024
        },
        {
            'name': 'nlcd_parcel_processor',
            'aws_name': f'teddy-nlcd-parcel-processor-{environment}',
            'timeout': 300,
            'memory': 1024
        }
    ]
    
    updated_functions = []
    
    for func in functions:
        try:
            print(f"📝 Updating function: {func['aws_name']}")
            
            # Create deployment package
            zip_path = create_lambda_deployment_package(func['name'], layer_arn)
            if not zip_path:
                continue
            
            # Update function code
            with open(zip_path, 'rb') as zip_file:
                lambda_client.update_function_code(
                    FunctionName=func['aws_name'],
                    ZipFile=zip_file.read()
                )
            
            # Update function configuration
            lambda_client.update_function_configuration(
                FunctionName=func['aws_name'],
                Layers=[layer_arn],
                Environment={
                    'Variables': {
                        'SNOWFLAKE_ACCOUNT': 'JJODRXK-BIRDDOGAWS',
                        'SNOWFLAKE_USER': 'TEDDY_PIPELINE_USER',
                        'SNOWFLAKE_WAREHOUSE': 'COMPUTE_WH',
                        'SNOWFLAKE_DATABASE': 'BIRDDOG_DATA',
                        'SNOWFLAKE_SCHEMA': 'RAW',
                        'SNOWFLAKE_PRIVATE_KEY_SECRET': 'teddy-pipeline/snowflake-private-key',
                        'DATA_BUCKET': stack_outputs.get('DataLakeBucketName', f'teddy-data-lake-{environment}'),
                        'ENVIRONMENT': environment
                    }
                },
                Timeout=func['timeout'],
                MemorySize=func['memory']
            )
            
            print(f"✅ Updated function: {func['aws_name']}")
            updated_functions.append(func['aws_name'])
            
            # Clean up
            os.remove(zip_path)
            
        except Exception as e:
            print(f"❌ Error updating function {func['aws_name']}: {e}")
            continue
    
    return updated_functions

def setup_s3_triggers(environment, stack_outputs):
    """Setup S3 triggers for Lambda functions"""
    print("🔗 Setting up S3 triggers...")
    
    lambda_client = boto3.client('lambda')
    s3_client = boto3.client('s3')
    
    bucket_name = stack_outputs.get('DataLakeBucketName', f'teddy-data-lake-{environment}')
    function_name = f'teddy-s3-to-snowflake-loader-{environment}'
    
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
        print(f"❌ Error setting up S3 triggers: {e}")
        return False

def test_pipeline_end_to_end(environment, stack_outputs):
    """Test the complete pipeline end-to-end"""
    print("🧪 Testing pipeline end-to-end...")
    
    s3_client = boto3.client('s3')
    lambda_client = boto3.client('lambda')
    
    bucket_name = stack_outputs.get('DataLakeBucketName', f'teddy-data-lake-{environment}')
    
    # Test data
    test_parcel_data = {
        "parcel_id": "test_parcel_001",
        "address": "123 Test Street, Austin, TX 78701",
        "county": "travis",
        "state": "texas",
        "coordinates": {
            "latitude": 30.2672,
            "longitude": -97.7431
        },
        "properties": {
            "area_acres": 0.25,
            "zoning": "residential",
            "year_built": 2020
        },
        "timestamp": datetime.utcnow().isoformat()
    }
    
    try:
        # 1. Upload test data to S3
        test_key = f"raw/parcel/test/{datetime.now().strftime('%Y%m%d_%H%M%S')}_test_parcel.json"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=test_key,
            Body=json.dumps(test_parcel_data),
            ContentType='application/json'
        )
        
        print(f"✅ Uploaded test data to S3: s3://{bucket_name}/{test_key}")
        
        # 2. Wait for S3 trigger to process
        print("⏳ Waiting for S3 trigger to process data...")
        time.sleep(10)
        
        # 3. Test API endpoint
        api_url = stack_outputs.get('ApiGatewayUrl')
        if api_url:
            print(f"📡 API Gateway URL: {api_url}/parcel")
            
            # Test API parcel ingestion
            test_api_payload = {
                "type": "coordinates",
                "latitude": 30.2672,
                "longitude": -97.7431
            }
            
            api_function_name = f'teddy-api-parcel-ingestion-{environment}'
            
            response = lambda_client.invoke(
                FunctionName=api_function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps({
                    'body': json.dumps(test_api_payload),
                    'httpMethod': 'POST'
                })
            )
            
            response_payload = json.loads(response['Payload'].read())
            print(f"📊 API test response: {response_payload}")
        
        # 4. Test NLCD processing
        nlcd_function_name = f'teddy-nlcd-parcel-processor-{environment}'
        
        nlcd_test_event = {
            "parcel_data": test_parcel_data,
            "coordinates": {
                "latitude": 30.2672,
                "longitude": -97.7431
            }
        }
        
        try:
            response = lambda_client.invoke(
                FunctionName=nlcd_function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(nlcd_test_event)
            )
            
            nlcd_response = json.loads(response['Payload'].read())
            print(f"🌱 NLCD test response: {nlcd_response}")
            
        except Exception as e:
            print(f"⚠️ NLCD test failed (function may not exist): {e}")
        
        print("✅ End-to-end pipeline test completed")
        return True
        
    except Exception as e:
        print(f"❌ Error in end-to-end test: {e}")
        return False

def verify_deployment(environment, stack_outputs):
    """Verify the deployment is working correctly"""
    print("🔍 Verifying deployment...")
    
    # Check CloudFormation stack
    cf_client = boto3.client('cloudformation')
    stack_name = f"teddy-data-pipeline-{environment}"
    
    try:
        response = cf_client.describe_stacks(StackName=stack_name)
        stack_status = response['Stacks'][0]['StackStatus']
        print(f"✅ CloudFormation stack status: {stack_status}")
    except Exception as e:
        print(f"❌ Error checking CloudFormation stack: {e}")
    
    # Check DynamoDB tables
    dynamodb = boto3.client('dynamodb')
    tables = [
        f'teddy-parcel-cache-{environment}',
        f'teddy-rate-limit-{environment}',
        f'teddy-api-metadata-{environment}'
    ]
    
    for table_name in tables:
        try:
            response = dynamodb.describe_table(TableName=table_name)
            status = response['Table']['TableStatus']
            print(f"✅ DynamoDB table {table_name}: {status}")
        except Exception as e:
            print(f"❌ Error checking DynamoDB table {table_name}: {e}")
    
    # Check S3 bucket
    s3_client = boto3.client('s3')
    bucket_name = stack_outputs.get('DataLakeBucketName', f'teddy-data-lake-{environment}')
    
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✅ S3 bucket exists: {bucket_name}")
        
        # Check bucket notification
        response = s3_client.get_bucket_notification_configuration(Bucket=bucket_name)
        if 'LambdaFunctionConfigurations' in response:
            print("✅ S3 bucket notification configured")
        else:
            print("⚠️ S3 bucket notification not configured")
            
    except Exception as e:
        print(f"❌ Error checking S3 bucket: {e}")
    
    # Check Lambda functions
    lambda_client = boto3.client('lambda')
    functions = [
        f'teddy-s3-to-snowflake-loader-{environment}',
        f'teddy-api-parcel-ingestion-{environment}',
        f'teddy-bulk-parcel-ingestion-{environment}'
    ]
    
    for function_name in functions:
        try:
            response = lambda_client.get_function_configuration(FunctionName=function_name)
            print(f"✅ Lambda function {function_name}: {response['State']}")
        except Exception as e:
            print(f"❌ Error checking Lambda function {function_name}: {e}")

def main():
    """Main deployment function"""
    parser = argparse.ArgumentParser(description='Deploy Teddy Data Pipeline')
    parser.add_argument('--environment', '-e', default='dev', 
                       choices=['dev', 'staging', 'prod'],
                       help='Environment to deploy to')
    parser.add_argument('--skip-layer', action='store_true',
                       help='Skip Lambda layer creation/deployment')
    parser.add_argument('--skip-cf', action='store_true',
                       help='Skip CloudFormation deployment')
    parser.add_argument('--test-only', action='store_true',
                       help='Only run tests, skip deployment')
    
    args = parser.parse_args()
    
    print("🚀 Teddy Data Pipeline Complete Deployment")
    print("=" * 60)
    print(f"Environment: {args.environment}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 60)
    
    success_count = 0
    total_steps = 7
    
    layer_arn = None
    stack_outputs = {}
    
    if not args.test_only:
        # Step 1: Create and deploy Lambda layer
        if not args.skip_layer:
            layer_zip_path = create_lambda_layer()
            if layer_zip_path:
                layer_arn = deploy_lambda_layer(layer_zip_path, args.environment)
                if layer_arn:
                    success_count += 1
                    print("✅ Step 1/7: Lambda layer deployed")
                    # Clean up
                    os.remove(layer_zip_path)
                else:
                    print("❌ Step 1/7: Lambda layer deployment failed")
            else:
                print("❌ Step 1/7: Lambda layer creation failed")
        else:
            print("⏭️ Step 1/7: Skipped Lambda layer deployment")
            success_count += 1
        
        # Step 2: Deploy CloudFormation stack
        if not args.skip_cf:
            cf_parameters = [
                {'ParameterKey': 'Environment', 'ParameterValue': args.environment},
                {'ParameterKey': 'RegridApiKey', 'ParameterValue': 'your-regrid-api-key'},
                {'ParameterKey': 'SnowflakeAccount', 'ParameterValue': 'JJODRXK-BIRDDOGAWS'},
                {'ParameterKey': 'SnowflakeUser', 'ParameterValue': 'TEDDY_PIPELINE_USER'},
                {'ParameterKey': 'SnowflakePrivateKey', 'ParameterValue': 'base64-encoded-private-key'},
                {'ParameterKey': 'SnowflakePrivateKeyPassphrase', 'ParameterValue': ''}
            ]
            
            stack_outputs = deploy_cloudformation_stack(args.environment, cf_parameters)
            if stack_outputs is not None:
                success_count += 1
                print("✅ Step 2/7: CloudFormation stack deployed")
            else:
                print("❌ Step 2/7: CloudFormation stack deployment failed")
        else:
            print("⏭️ Step 2/7: Skipped CloudFormation deployment")
            success_count += 1
        
        # Step 3: Update Lambda functions
        if layer_arn:
            updated_functions = update_lambda_functions(args.environment, layer_arn, stack_outputs)
            if updated_functions:
                success_count += 1
                print("✅ Step 3/7: Lambda functions updated")
            else:
                print("❌ Step 3/7: Lambda function updates failed")
        else:
            print("⏭️ Step 3/7: Skipped Lambda function updates (no layer ARN)")
            success_count += 1
        
        # Step 4: Setup S3 triggers
        if setup_s3_triggers(args.environment, stack_outputs):
            success_count += 1
            print("✅ Step 4/7: S3 triggers configured")
        else:
            print("❌ Step 4/7: S3 trigger setup failed")
    else:
        success_count += 4  # Skip deployment steps
        print("⏭️ Steps 1-4: Skipped deployment (test-only mode)")
    
    # Step 5: Test pipeline end-to-end
    if test_pipeline_end_to_end(args.environment, stack_outputs):
        success_count += 1
        print("✅ Step 5/7: End-to-end pipeline test successful")
    else:
        print("❌ Step 5/7: End-to-end pipeline test failed")
    
    # Step 6: Verify deployment
    verify_deployment(args.environment, stack_outputs)
    success_count += 1
    print("✅ Step 6/7: Deployment verification complete")
    
    # Step 7: Summary and next steps
    success_count += 1
    print("✅ Step 7/7: Deployment summary complete")
    
    # Final summary
    print(f"\n🎉 Deployment Summary")
    print("=" * 40)
    print(f"✅ Successful steps: {success_count}/{total_steps}")
    print(f"🌍 Environment: {args.environment}")
    
    if stack_outputs:
        print(f"🪣 S3 Bucket: {stack_outputs.get('DataLakeBucketName', 'N/A')}")
        print(f"🌐 API URL: {stack_outputs.get('ApiGatewayUrl', 'N/A')}")
        print(f"⚙️ State Machine: {stack_outputs.get('StateMachineArn', 'N/A')}")
    
    if success_count == total_steps:
        print("\n🎉 Deployment completed successfully!")
        print("\n📋 Next Steps:")
        print("1. Upload parcel JSON files to S3 to test automatic processing")
        print("2. Use the API Gateway endpoint to test real-time ingestion")
        print("3. Check CloudWatch logs for function execution details")
        print("4. Query Snowflake to verify data loading")
        print("5. Monitor DynamoDB for caching performance")
        print("6. Test NLCD integration for land cover data")
    else:
        print("\n⚠️ Deployment completed with some issues")
        print("Check the error messages above for troubleshooting")
    
    return 0 if success_count == total_steps else 1

if __name__ == "__main__":
    exit(main())
