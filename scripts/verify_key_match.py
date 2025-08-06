#!/usr/bin/env python3
"""
Verify that our private key matches the public key registered in Snowflake
"""

import sys
import os
import json
import base64
import hashlib
from datetime import datetime, timedelta
import boto3
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import jwt

# Add the lambda utils to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lambda'))

def load_secrets(environment='dev'):
    """Load secrets from AWS Secrets Manager"""
    secrets_client = boto3.client('secretsmanager')
    secret_name = f'teddy-data-pipeline-secrets-{environment}'
    
    response = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def verify_key_match():
    """Verify that our private key matches the Snowflake public key"""
    
    print("🔍 Verifying Key Match")
    print("=" * 50)
    
    try:
        # Load secrets
        secrets = load_secrets('dev')
        
        # Decode private key
        private_key_data = base64.b64decode(secrets['snowflake_private_key'])
        private_key = serialization.load_pem_private_key(
            private_key_data,
            password=None,
            backend=default_backend()
        )
        
        # Get public key from private key
        public_key = private_key.public_key()
        
        # Convert to DER format
        public_key_der = public_key.public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        
        # Convert to base64 (same format as Snowflake registration)
        our_public_key_b64 = base64.b64encode(public_key_der).decode('utf-8')
        
        # The public key registered in Snowflake
        snowflake_public_key_b64 = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmraeFvqhiYCM/50SWWia3elqAleTfl46v7RP7clzfnE1wQZVZ59Uq8fCQotvkJSPtdlaYmhgXA0dXzvA5qTdspg5cMs9zjT1MhyvyU9HfJUQ5UDDyKTd16s4GUHQzLpfcsy8bnjavX34a+rwZ8Eql05O4rQGY8JcTh1BEtFU71LQZ7XM3OiVSXK1Y7dH4dlUQU9otP4rymTKk+oI3kY3Lrp/c5LN10RJUTMorgo5BV/T4sutxS3svwfQfK/7b7hZ1Qtqe0xgTZEazPJ+XBVIYHrJBioWVhpmS6QMicSLlQFvZpjg2dPhtRPHiQca7+gqqddg9No5x/YaDOPMU3LXWwIDAQAB'
        
        print(f"Our public key (first 50 chars): {our_public_key_b64[:50]}...")
        print(f"Snowflake public key (first 50 chars): {snowflake_public_key_b64[:50]}...")
        
        if our_public_key_b64 == snowflake_public_key_b64:
            print("✅ KEYS MATCH! Our private key corresponds to the Snowflake public key")
            
            # Now calculate the correct fingerprint using the Snowflake public key
            snowflake_public_key_der = base64.b64decode(snowflake_public_key_b64)
            
            # Method 1: SHA256 -> Base64
            hash1 = hashlib.sha256(snowflake_public_key_der).digest()
            fp1 = base64.b64encode(hash1).decode('utf-8')
            print(f"Correct fingerprint (Base64): {fp1}")
            
            # Method 2: SHA256 -> Hex (uppercase)
            fp2 = hashlib.sha256(snowflake_public_key_der).hexdigest().upper()
            print(f"Correct fingerprint (Hex): {fp2}")
            
            # Test JWT with correct fingerprint
            account = secrets['snowflake_account']
            user = secrets['snowflake_user']
            qualified_username = f"{account.upper()}.{user.upper()}"
            
            now = datetime.utcnow()
            iat = int(now.timestamp())
            exp = int((now + timedelta(minutes=59)).timestamp())
            
            # Test with base64 fingerprint
            payload = {
                'iss': f"{qualified_username}.SHA256:{fp1}",
                'sub': qualified_username,
                'iat': iat,
                'exp': exp
            }
            
            token = jwt.encode(payload, private_key, algorithm='RS256')
            print(f"✅ Generated JWT with correct fingerprint")
            print(f"   Issuer: {payload['iss'][:60]}...")
            
            return {
                'keys_match': True,
                'base64_fingerprint': fp1,
                'hex_fingerprint': fp2,
                'jwt_token': token
            }
            
        else:
            print("❌ KEYS DO NOT MATCH!")
            print("The private key in AWS Secrets Manager does not correspond to the public key registered in Snowflake")
            return {'keys_match': False}
            
    except Exception as e:
        print(f"❌ Error during verification: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    result = verify_key_match()
    if result and result.get('keys_match'):
        print("\n🎯 SOLUTION FOUND!")
        print("Use the base64 fingerprint method with the exact Snowflake public key")
