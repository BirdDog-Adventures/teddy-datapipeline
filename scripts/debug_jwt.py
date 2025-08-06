#!/usr/bin/env python3
"""
Debug JWT Token Generation for Snowflake

This script helps debug JWT token generation issues by testing different approaches.
"""

import sys
import os
import json
import base64
import logging
from datetime import datetime, timedelta
import boto3
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import jwt
import hashlib

# Add the lambda utils to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lambda'))

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def load_secrets(environment='dev'):
    """Load secrets from AWS Secrets Manager"""
    secrets_client = boto3.client('secretsmanager')
    secret_name = f'teddy-data-pipeline-secrets-{environment}'
    
    response = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

def debug_jwt_generation():
    """Debug JWT token generation with different approaches"""
    
    print("🔍 Starting JWT Debug Session")
    print("=" * 50)
    
    try:
        # Load secrets
        secrets = load_secrets('dev')
        print(f"✅ Loaded secrets successfully")
        print(f"   Account: {secrets['snowflake_account']}")
        print(f"   User: {secrets['snowflake_user']}")
        
        # Decode private key
        private_key_data = base64.b64decode(secrets['snowflake_private_key'])
        print(f"✅ Decoded private key (length: {len(private_key_data)} bytes)")
        
        # Load private key
        private_key = serialization.load_pem_private_key(
            private_key_data,
            password=None,
            backend=default_backend()
        )
        print(f"✅ Loaded private key successfully")
        
        # Get public key
        public_key = private_key.public_key()
        public_key_der = public_key.public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        print(f"✅ Generated public key DER (length: {len(public_key_der)} bytes)")
        
        # Test different fingerprint methods
        print("\n🔬 Testing Fingerprint Methods:")
        
        # Method 1: SHA256 -> Base64
        hash1 = hashlib.sha256(public_key_der).digest()
        fp1 = base64.b64encode(hash1).decode('utf-8')
        print(f"   Method 1 (SHA256->Base64): {fp1[:20]}...")
        
        # Method 2: SHA256 -> Hex (uppercase)
        fp2 = hashlib.sha256(public_key_der).hexdigest().upper()
        print(f"   Method 2 (SHA256->Hex): {fp2[:20]}...")
        
        # Method 3: SHA256 -> Hex (lowercase)
        fp3 = hashlib.sha256(public_key_der).hexdigest().lower()
        print(f"   Method 3 (SHA256->hex): {fp3[:20]}...")
        
        # Create qualified username
        account = secrets['snowflake_account']
        user = secrets['snowflake_user']
        qualified_username = f"{account.upper()}.{user.upper()}"
        print(f"\n✅ Qualified Username: {qualified_username}")
        
        # Test JWT generation with Method 1 (Base64)
        print(f"\n🔧 Testing JWT Generation (Method 1 - Base64):")
        
        now = datetime.utcnow()
        iat = int(now.timestamp())
        exp = int((now + timedelta(minutes=59)).timestamp())
        
        payload1 = {
            'iss': f"{qualified_username}.SHA256:{fp1}",
            'sub': qualified_username,
            'iat': iat,
            'exp': exp
        }
        
        token1 = jwt.encode(payload1, private_key, algorithm='RS256')
        print(f"   JWT Token (first 50 chars): {token1[:50]}...")
        print(f"   Issuer: {payload1['iss'][:60]}...")
        
        # Test JWT generation with Method 2 (Hex uppercase)
        print(f"\n🔧 Testing JWT Generation (Method 2 - Hex Upper):")
        
        payload2 = {
            'iss': f"{qualified_username}.SHA256:{fp2}",
            'sub': qualified_username,
            'iat': iat,
            'exp': exp
        }
        
        token2 = jwt.encode(payload2, private_key, algorithm='RS256')
        print(f"   JWT Token (first 50 chars): {token2[:50]}...")
        print(f"   Issuer: {payload2['iss'][:60]}...")
        
        print(f"\n✅ JWT Debug Complete - Generated {2} test tokens")
        
        return {
            'method1_token': token1,
            'method2_token': token2,
            'method1_fingerprint': fp1,
            'method2_fingerprint': fp2,
            'qualified_username': qualified_username
        }
        
    except Exception as e:
        print(f"❌ Error during JWT debug: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    debug_jwt_generation()
