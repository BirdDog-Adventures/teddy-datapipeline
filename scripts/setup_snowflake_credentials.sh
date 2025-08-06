#!/bin/bash

# Snowflake Credentials Setup Script for AWS Secrets Manager
# This script helps you configure Snowflake JWT authentication for the Teddy data pipeline

set -e

echo "🔐 Snowflake Credentials Setup for Teddy Data Pipeline"
echo "======================================================"

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "❌ AWS CLI is not installed. Please install it first."
    exit 1
fi

# Check if OpenSSL is installed
if ! command -v openssl &> /dev/null; then
    echo "❌ OpenSSL is not installed. Please install it first."
    exit 1
fi

# Function to generate RSA key pair
generate_keys() {
    echo "🔑 Generating RSA key pair for Snowflake JWT authentication..."
    
    # Generate private key
    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_rsa_key.p8 -nocrypt
    
    # Generate public key
    openssl rsa -in snowflake_rsa_key.p8 -pubout -out snowflake_rsa_key.pub
    
    echo "✅ RSA key pair generated:"
    echo "   - Private key: snowflake_rsa_key.p8"
    echo "   - Public key: snowflake_rsa_key.pub"
    echo ""
}

# Function to display public key for Snowflake configuration
show_public_key() {
    echo "📋 Public Key for Snowflake Configuration:"
    echo "=========================================="
    echo "Copy the following public key (without headers/footers) and use it in Snowflake:"
    echo ""
    
    # Extract public key content without headers
    PUBLIC_KEY_CONTENT=$(openssl rsa -in snowflake_rsa_key.p8 -pubout -outform DER | base64 | tr -d '\n')
    echo "$PUBLIC_KEY_CONTENT"
    echo ""
    
    echo "📝 Run this SQL in Snowflake as ACCOUNTADMIN:"
    echo "============================================="
    cat << EOF
-- Connect as ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Create user if it doesn't exist
CREATE USER IF NOT EXISTS TEDDY_PIPELINE_USER
  DEFAULT_ROLE = 'PUBLIC'
  DEFAULT_WAREHOUSE = 'COMPUTE_WH'
  MUST_CHANGE_PASSWORD = FALSE;

-- Set the public key for JWT authentication
ALTER USER TEDDY_PIPELINE_USER SET RSA_PUBLIC_KEY='$PUBLIC_KEY_CONTENT';

-- Grant necessary privileges
GRANT ROLE SYSADMIN TO USER TEDDY_PIPELINE_USER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO USER TEDDY_PIPELINE_USER;
GRANT USAGE ON DATABASE BIRDDOG_DATA TO USER TEDDY_PIPELINE_USER;
GRANT USAGE ON SCHEMA BIRDDOG_DATA.RAW TO USER TEDDY_PIPELINE_USER;
GRANT CREATE TABLE ON SCHEMA BIRDDOG_DATA.RAW TO USER TEDDY_PIPELINE_USER;
GRANT INSERT, SELECT, UPDATE, DELETE ON ALL TABLES IN SCHEMA BIRDDOG_DATA.RAW TO USER TEDDY_PIPELINE_USER;
GRANT INSERT, SELECT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA BIRDDOG_DATA.RAW TO USER TEDDY_PIPELINE_USER;
EOF
    echo ""
}

# Function to create AWS secret
create_aws_secret() {
    echo "☁️ Creating AWS Secrets Manager secret..."
    
    # Read private key content and escape for JSON
    PRIVATE_KEY=$(cat snowflake_rsa_key.p8 | sed ':a;N;$!ba;s/\n/\\n/g')
    
    # Create secret in AWS Secrets Manager
    aws secretsmanager create-secret \
      --name "teddy-pipeline/snowflake-private-key" \
      --description "Snowflake JWT private key for Teddy data pipeline" \
      --secret-string "{\"private_key\":\"$PRIVATE_KEY\"}" \
      --region us-east-1
    
    if [ $? -eq 0 ]; then
        echo "✅ Successfully created Snowflake credentials in AWS Secrets Manager"
        echo "   Secret name: teddy-pipeline/snowflake-private-key"
        echo "   Region: us-east-1"
    else
        echo "❌ Failed to create secret in AWS Secrets Manager"
        exit 1
    fi
}

# Function to test the setup
test_lambda_function() {
    echo "🧪 Testing Lambda function..."
    
    # Create test payload
    cat > test-payload.json << EOF
{
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "us-east-1",
      "eventTime": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
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
EOF

    # Invoke the Lambda function
    aws lambda invoke \
      --function-name teddy-s3-to-snowflake-loader-dev \
      --payload file://test-payload.json \
      response.json
    
    echo "📊 Lambda function response:"
    cat response.json
    echo ""
    
    # Clean up test files
    rm -f test-payload.json response.json
}

# Function to show CloudWatch logs
show_logs() {
    echo "📋 Recent CloudWatch logs:"
    echo "=========================="
    
    # Get the latest log stream
    LOG_STREAM=$(aws logs describe-log-streams \
      --log-group-name "/aws/lambda/teddy-s3-to-snowflake-loader-dev" \
      --order-by LastEventTime \
      --descending \
      --max-items 1 \
      --query 'logStreams[0].logStreamName' \
      --output text 2>/dev/null)
    
    if [ "$LOG_STREAM" != "None" ] && [ -n "$LOG_STREAM" ]; then
        aws logs get-log-events \
          --log-group-name "/aws/lambda/teddy-s3-to-snowflake-loader-dev" \
          --log-stream-name "$LOG_STREAM" \
          --limit 10 \
          --query 'events[].message' \
          --output text
    else
        echo "No recent logs found. The Lambda function may not have been invoked yet."
    fi
}

# Main execution
main() {
    echo "Choose an option:"
    echo "1. Generate new RSA key pair"
    echo "2. Use existing RSA key pair"
    echo "3. Show public key for Snowflake configuration"
    echo "4. Create AWS Secrets Manager secret"
    echo "5. Test Lambda function"
    echo "6. Show CloudWatch logs"
    echo "7. Complete setup (all steps)"
    echo ""
    read -p "Enter your choice (1-7): " choice
    
    case $choice in
        1)
            generate_keys
            show_public_key
            ;;
        2)
            if [ ! -f "snowflake_rsa_key.p8" ]; then
                echo "❌ snowflake_rsa_key.p8 not found. Please generate keys first."
                exit 1
            fi
            show_public_key
            ;;
        3)
            if [ ! -f "snowflake_rsa_key.p8" ]; then
                echo "❌ snowflake_rsa_key.p8 not found. Please generate keys first."
                exit 1
            fi
            show_public_key
            ;;
        4)
            if [ ! -f "snowflake_rsa_key.p8" ]; then
                echo "❌ snowflake_rsa_key.p8 not found. Please generate keys first."
                exit 1
            fi
            create_aws_secret
            ;;
        5)
            test_lambda_function
            ;;
        6)
            show_logs
            ;;
        7)
            echo "🚀 Running complete setup..."
            generate_keys
            show_public_key
            echo ""
            echo "⏸️  PAUSE: Please configure the public key in Snowflake using the SQL above."
            read -p "Press Enter after you've configured Snowflake..."
            create_aws_secret
            echo ""
            test_lambda_function
            echo ""
            show_logs
            ;;
        *)
            echo "❌ Invalid choice. Please run the script again."
            exit 1
            ;;
    esac
}

# Check if running in script directory
if [ ! -f "$(dirname "$0")/setup_snowflake_credentials.sh" ]; then
    echo "⚠️  Please run this script from the teddy-datapipeline directory:"
    echo "   cd teddy-datapipeline"
    echo "   ./scripts/setup_snowflake_credentials.sh"
    exit 1
fi

# Run main function
main

echo ""
echo "🎉 Setup complete! Your Snowflake credentials are now configured."
echo ""
echo "📋 Next steps:"
echo "1. Upload parcel data files to S3 bucket: teddy-data-lake-dev/raw/parcel/"
echo "2. Monitor CloudWatch logs: /aws/lambda/teddy-s3-to-snowflake-loader-dev"
echo "3. Query data in Snowflake: SELECT * FROM BIRDDOG_DATA.RAW.PARCEL_DATA_RAW;"
echo ""
echo "📖 For detailed documentation, see:"
echo "   teddy-datapipeline/docs/SNOWFLAKE_CREDENTIALS_SETUP_GUIDE.md"
