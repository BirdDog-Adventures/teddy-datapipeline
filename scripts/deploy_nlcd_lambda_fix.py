#!/usr/bin/env python3
"""
Deploy NLCD Lambda Function Fix

This script specifically deploys the updated Lambda functions with the NLCD client fix.
It focuses on updating the existing Lambda functions without touching the infrastructure.

Usage:
    python scripts/deploy_nlcd_lambda_fix.py --environment dev
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

def create_lambda_deployment_package(function_name):
    """Create deployment package for Lambda function with NLCD fix"""
    print(f"📦 Creating deployment package for {function_name}...")
    
    project_root = get_project_root()
    lambda_dir = project_root / "lambda"
    
    with tempfile.TemporaryDirectory() as temp_dir:
        package_dir = os.path.join(temp_dir, "package")
        os.makedirs(package_dir)
        
        # Copy main Lambda function
        if function_name == "nlcd_parcel_processor":
            source_file = lambda_dir / "nlcd_parcel_processor.py"
            target_file = os.path.join(package_dir, "lambda_function.py")
        elif function_name == "s3_to_snowflake_loader":
            source_file = lambda_dir / "s3_to_snowflake_loader.py"
            target_file = os.path.join(package_dir, "lambda_function.py")
        elif function_name == "api_parcel_ingestion":
            source_file = lambda_dir / "api_parcel_ingestion.py"
            target_file = os.path.join(package_dir, "lambda_function.py")
        elif function_name == "bulk_parcel_ingestion":
            source_file = lambda_dir / "bulk_parcel_ingestion.py"
            target_file = os.path.join(package_dir, "lambda_function.py")
        else:
            raise ValueError(f"Unknown function: {function_name}")
        
        if source_file.exists():
            shutil.copy2(source_file, target_file)
            print(f"✅ Copied {source_file} to {target_file}")
        else:
            print(f"❌ Source file not found: {source_file}")
            return None
        
        # Copy utils directory with NLCD fix
        utils_source = lambda_dir / "utils"
        if utils_source.exists():
            shutil.copytree(utils_source, os.path.join(package_dir, "utils"))
            print(f"✅ Copied utils directory with NLCD fix")
        
        # Copy clients directory if it exists
        clients_source = lambda_dir / "clients"
        if clients_source.exists():
            shutil.copytree(clients_source, os.path.join(package_dir, "clients"))
            print(f"✅ Copied clients directory")
        
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

def update_lambda_function(function_name, aws_function_name, environment):
    """Update a specific Lambda function with the NLCD fix"""
    print(f"🚀 Updating Lambda function: {aws_function_name}")
    
    lambda_client = boto3.client('lambda')
    
    try:
        # Check if function exists
        try:
            lambda_client.get_function(FunctionName=aws_function_name)
            print(f"✅ Function {aws_function_name} exists")
        except lambda_client.exceptions.ResourceNotFoundException:
            print(f"❌ Function {aws_function_name} not found")
            return False
        
        # Create deployment package
        zip_path = create_lambda_deployment_package(function_name)
        if not zip_path:
            return False
        
        # Update function code
        with open(zip_path, 'rb') as zip_file:
            response = lambda_client.update_function_code(
                FunctionName=aws_function_name,
                ZipFile=zip_file.read()
            )
        
        print(f"✅ Updated function code for {aws_function_name}")
        print(f"   📊 Code SHA256: {response['CodeSha256']}")
        print(f"   📊 Last Modified: {response['LastModified']}")
        
        # Update environment variables to ensure NLCD fix is enabled
        try:
            current_config = lambda_client.get_function_configuration(FunctionName=aws_function_name)
            current_env = current_config.get('Environment', {}).get('Variables', {})
            
            # Add/update environment variables for NLCD fix
            updated_env = current_env.copy()
            updated_env.update({
                'NLCD_FIX_ENABLED': 'true',
                'NLCD_API_VERSION': '2021',
                'ENVIRONMENT': environment,
                'DEPLOYMENT_TIMESTAMP': datetime.now().isoformat()
            })
            
            lambda_client.update_function_configuration(
                FunctionName=aws_function_name,
                Environment={'Variables': updated_env}
            )
            
            print(f"✅ Updated environment variables for {aws_function_name}")
            
        except Exception as e:
            print(f"⚠️  Warning: Could not update environment variables: {e}")
        
        # Clean up
        os.remove(zip_path)
        
        return True
        
    except Exception as e:
        print(f"❌ Error updating function {aws_function_name}: {e}")
        return False

def test_nlcd_function(aws_function_name):
    """Test the NLCD function with our fix"""
    print(f"🧪 Testing NLCD function: {aws_function_name}")
    
    lambda_client = boto3.client('lambda')
    
    # Test event with coordinates that should work with our fix
    test_event = {
        "parcel_id": "test_nlcd_fix_001",
        "year": 2021,
        "force_refresh": True,
        "coordinates": {
            "latitude": 34.4535,
            "longitude": -95.0526
        }
    }
    
    try:
        response = lambda_client.invoke(
            FunctionName=aws_function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(test_event)
        )
        
        response_payload = json.loads(response['Payload'].read())
        
        if response['StatusCode'] == 200:
            print(f"✅ Function test successful")
            
            # Parse the response
            if isinstance(response_payload, dict):
                if response_payload.get('statusCode') == 200:
                    body = json.loads(response_payload.get('body', '{}'))
                    if body.get('success'):
                        print(f"   🎯 NLCD processing successful!")
                        print(f"   🎯 NLCD Code: {body.get('nlcd_code', 'N/A')}")
                        print(f"   🎯 Land Cover: {body.get('land_cover_name', 'N/A')}")
                        return True
                    else:
                        print(f"   ⚠️  NLCD processing failed: {body.get('error', 'Unknown error')}")
                else:
                    print(f"   ⚠️  HTTP Status: {response_payload.get('statusCode')}")
                    print(f"   ⚠️  Response: {response_payload}")
            else:
                print(f"   📊 Raw response: {response_payload}")
                
        else:
            print(f"❌ Function invocation failed with status: {response['StatusCode']}")
            
    except Exception as e:
        print(f"❌ Error testing function: {e}")
    
    return False

def verify_nlcd_fix_deployment(environment):
    """Verify that the NLCD fix has been deployed correctly"""
    print("🔍 Verifying NLCD fix deployment...")
    
    lambda_client = boto3.client('lambda')
    
    # Functions that should have the NLCD fix
    functions_to_check = [
        f'teddy-nlcd-parcel-processor-{environment}',
        f'teddy-s3-to-snowflake-loader-{environment}',
        f'teddy-api-parcel-ingestion-{environment}',
        f'teddy-bulk-parcel-ingestion-{environment}'
    ]
    
    verified_functions = []
    
    for function_name in functions_to_check:
        try:
            config = lambda_client.get_function_configuration(FunctionName=function_name)
            env_vars = config.get('Environment', {}).get('Variables', {})
            
            print(f"📋 Checking {function_name}:")
            print(f"   📊 Runtime: {config['Runtime']}")
            print(f"   📊 Last Modified: {config['LastModified']}")
            print(f"   📊 Code Size: {config['CodeSize']} bytes")
            
            # Check for NLCD fix indicators
            if env_vars.get('NLCD_FIX_ENABLED') == 'true':
                print(f"   ✅ NLCD fix enabled")
                verified_functions.append(function_name)
            else:
                print(f"   ⚠️  NLCD fix not explicitly enabled")
            
            if 'DEPLOYMENT_TIMESTAMP' in env_vars:
                print(f"   📅 Deployment: {env_vars['DEPLOYMENT_TIMESTAMP']}")
            
        except lambda_client.exceptions.ResourceNotFoundException:
            print(f"   ❌ Function not found: {function_name}")
        except Exception as e:
            print(f"   ❌ Error checking function: {e}")
    
    print(f"\n📊 Verification Summary:")
    print(f"   ✅ Functions verified: {len(verified_functions)}")
    print(f"   📋 Total functions checked: {len(functions_to_check)}")
    
    return verified_functions

def main():
    """Main deployment function"""
    parser = argparse.ArgumentParser(description='Deploy NLCD Lambda Function Fix')
    parser.add_argument('--environment', '-e', default='dev', 
                       choices=['dev', 'staging', 'prod'],
                       help='Environment to deploy to')
    parser.add_argument('--function', '-f', 
                       choices=['nlcd_parcel_processor', 's3_to_snowflake_loader', 
                               'api_parcel_ingestion', 'bulk_parcel_ingestion', 'all'],
                       default='all',
                       help='Specific function to deploy (default: all)')
    parser.add_argument('--test', '-t', action='store_true',
                       help='Run tests after deployment')
    parser.add_argument('--verify-only', action='store_true',
                       help='Only verify deployment, skip actual deployment')
    
    args = parser.parse_args()
    
    print("🚀 NLCD Lambda Function Fix Deployment")
    print("=" * 50)
    print(f"Environment: {args.environment}")
    print(f"Function: {args.function}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 50)
    
    if args.verify_only:
        verified_functions = verify_nlcd_fix_deployment(args.environment)
        if verified_functions:
            print("✅ NLCD fix verification completed")
            return 0
        else:
            print("❌ NLCD fix verification failed")
            return 1
    
    # Define functions to deploy
    if args.function == 'all':
        functions_to_deploy = [
            ('nlcd_parcel_processor', f'teddy-nlcd-parcel-processor-{args.environment}'),
            ('s3_to_snowflake_loader', f'teddy-s3-to-snowflake-loader-{args.environment}'),
            ('api_parcel_ingestion', f'teddy-api-parcel-ingestion-{args.environment}'),
            ('bulk_parcel_ingestion', f'teddy-bulk-parcel-ingestion-{args.environment}')
        ]
    else:
        functions_to_deploy = [
            (args.function, f'teddy-{args.function.replace("_", "-")}-{args.environment}')
        ]
    
    # Deploy functions
    successful_deployments = []
    failed_deployments = []
    
    for function_name, aws_function_name in functions_to_deploy:
        print(f"\n🔄 Deploying {function_name}...")
        
        if update_lambda_function(function_name, aws_function_name, args.environment):
            successful_deployments.append(aws_function_name)
            print(f"✅ Successfully deployed {aws_function_name}")
        else:
            failed_deployments.append(aws_function_name)
            print(f"❌ Failed to deploy {aws_function_name}")
    
    # Test functions if requested
    if args.test and successful_deployments:
        print(f"\n🧪 Testing deployed functions...")
        
        # Test NLCD function specifically
        nlcd_function = f'teddy-nlcd-parcel-processor-{args.environment}'
        if nlcd_function in successful_deployments:
            test_nlcd_function(nlcd_function)
    
    # Verify deployment
    print(f"\n🔍 Verifying deployment...")
    verified_functions = verify_nlcd_fix_deployment(args.environment)
    
    # Summary
    print(f"\n🎉 Deployment Summary")
    print("=" * 30)
    print(f"✅ Successful: {len(successful_deployments)}")
    print(f"❌ Failed: {len(failed_deployments)}")
    print(f"🔍 Verified: {len(verified_functions)}")
    
    if successful_deployments:
        print(f"\n✅ Successfully deployed functions:")
        for func in successful_deployments:
            print(f"   - {func}")
    
    if failed_deployments:
        print(f"\n❌ Failed deployments:")
        for func in failed_deployments:
            print(f"   - {func}")
    
    print(f"\n📋 Next Steps:")
    print("1. Test the NLCD functionality with real parcel data")
    print("2. Monitor CloudWatch logs for any errors")
    print("3. Verify NLCD data is being processed correctly")
    print("4. Check Snowflake for updated land cover data")
    
    if len(successful_deployments) == len(functions_to_deploy):
        print("\n🎉 All functions deployed successfully with NLCD fix!")
        return 0
    else:
        print("\n⚠️ Some deployments failed. Check the logs above.")
        return 1

if __name__ == "__main__":
    exit(main())
