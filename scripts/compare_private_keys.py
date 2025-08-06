#!/usr/bin/env python3
"""
Compare the private key in AWS Secrets Manager with the local private key file
"""

import sys
import os
import json
import base64
import boto3
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def load_secrets(environment='dev'):
    """Load secrets from AWS Secrets Manager"""
    secrets_client = boto3.client('secretsmanager')
    secret_name = f'teddy-data-pipeline-secrets-{environment}'
    
    response = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def compare_keys():
    """Compare AWS Secrets Manager private key with local file"""
    
    print("🔍 Comparing Private Keys")
    print("=" * 50)
    
    try:
        # Load secrets from AWS
        secrets = load_secrets('dev')
        aws_private_key_b64 = secrets['snowflake_private_key']
        
        # Decode AWS private key
        aws_private_key_data = base64.b64decode(aws_private_key_b64)
        aws_private_key = serialization.load_pem_private_key(
            aws_private_key_data,
            password=None,
            backend=default_backend()
        )
        
        # Get public key from AWS private key
        aws_public_key = aws_private_key.public_key()
        aws_public_key_der = aws_public_key.public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        aws_public_key_b64 = base64.b64encode(aws_public_key_der).decode('utf-8')
        
        # The public key registered in Snowflake
        snowflake_public_key_b64 = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmraeFvqhiYCM/50SWWia3elqAleTfl46v7RP7clzfnE1wQZVZ59Uq8fCQotvkJSPtdlaYmhgXA0dXzvA5qTdspg5cMs9zjT1MhyvyU9HfJUQ5UDDyKTd16s4GUHQzLpfcsy8bnjavX34a+rwZ8Eql05O4rQGY8JcTh1BEtFU71LQZ7XM3OiVSXK1Y7dH4dlUQU9otP4rymTKk+oI3kY3Lrp/c5LN10RJUTMorgo5BV/T4sutxS3svwfQfK/7b7hZ1Qtqe0xgTZEazPJ+XBVIYHrJBioWVhpmS6QMicSLlQFvZpjg2dPhtRPHiQca7+gqqddg9No5x/YaDOPMU3LXWwIDAQAB'
        
        print(f"AWS private key -> public key (first 50 chars): {aws_public_key_b64[:50]}...")
        print(f"Snowflake registered public key (first 50 chars): {snowflake_public_key_b64[:50]}...")
        
        if aws_public_key_b64 == snowflake_public_key_b64:
            print("✅ KEYS MATCH: AWS private key corresponds to Snowflake public key")
            return True
        else:
            print("❌ KEYS DO NOT MATCH!")
            print("The private key in AWS Secrets Manager does NOT correspond to the public key registered in Snowflake")
            
            # Check if there's a local private key file
            local_key_paths = [
                'snowflake_private_key.pem',
                '../snowflake_private_key.pem',
                'snowflake_rsa_key.pem',
                '../snowflake_rsa_key.pem'
            ]
            
            for key_path in local_key_paths:
                if os.path.exists(key_path):
                    print(f"\n🔍 Found local private key file: {key_path}")
                    try:
                        with open(key_path, 'rb') as f:
                            local_private_key_data = f.read()
                        
                        local_private_key = serialization.load_pem_private_key(
                            local_private_key_data,
                            password=None,
                            backend=default_backend()
                        )
                        
                        local_public_key = local_private_key.public_key()
                        local_public_key_der = local_public_key.public_bytes(
                            encoding=serialization.Encoding.DER,
                            format=serialization.PublicFormat.SubjectPublicKeyInfo
                        )
                        local_public_key_b64 = base64.b64encode(local_public_key_der).decode('utf-8')
                        
                        print(f"Local private key -> public key (first 50 chars): {local_public_key_b64[:50]}...")
                        
                        if local_public_key_b64 == snowflake_public_key_b64:
                            print(f"✅ MATCH FOUND: Local key {key_path} corresponds to Snowflake public key!")
                            
                            # Encode the correct private key for AWS Secrets Manager
                            correct_private_key_b64 = base64.b64encode(local_private_key_data).decode('utf-8')
                            
                            print(f"\n🔧 SOLUTION:")
                            print(f"Update AWS Secrets Manager with the correct private key from {key_path}")
                            print(f"Base64 encoded private key (first 50 chars): {correct_private_key_b64[:50]}...")
                            
                            return False
                        else:
                            print(f"❌ Local key {key_path} does not match Snowflake public key")
                            
                    except Exception as e:
                        print(f"❌ Error reading local key {key_path}: {e}")
            
            print(f"\n💡 RECOMMENDATION:")
            print(f"1. Find the correct private key file that corresponds to the Snowflake public key")
            print(f"2. Update AWS Secrets Manager with the correct private key")
            print(f"3. The correct private key should generate this public key: {snowflake_public_key_b64}")
            
            return False
            
    except Exception as e:
        print(f"❌ Error during comparison: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    compare_keys()
