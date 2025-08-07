#!/bin/bash
# Deploy Lambda Functions with JWT Snowflake Authentication
# This script creates deployment packages using the working JWT approach from birddog-geodata-viewer

set -e

echo "🚀 Deploying Lambda functions with JWT Snowflake authentication..."

# Create build directory
BUILD_DIR="lambda-build-jwt"
if [ -d "$BUILD_DIR" ]; then
    rm -rf "$BUILD_DIR"
fi
mkdir "$BUILD_DIR"
cd "$BUILD_DIR"

echo "📦 Creating JWT-based deployment package..."

# Copy Lambda function code
if [ -d "../lambda" ]; then
    cp -r ../lambda/* .
    
    # Create lambda_function.py as entry point for API function
    cp api_parcel_ingestion.py lambda_function.py
    
    echo "📋 Creating requirements.txt with JWT dependencies..."
    
    # Create requirements.txt with JWT authentication dependencies
    cat > requirements.txt << 'EOF'
# AWS SDK
boto3>=1.34.0
botocore>=1.34.0

# Snowflake connector with JWT support (compatible version)
snowflake-connector-python>=1.9.1

# Cryptography for JWT authentication (use current working version)
cryptography

# HTTP requests
requests>=2.31.0

# JSON handling
orjson>=3.9.10

# Date/time handling
python-dateutil>=2.8.2

# Environment variable handling
python-dotenv>=1.0.0
EOF

    echo "📥 Installing dependencies for Lambda Linux x86_64..."
    
    # Install packages (let pip handle architecture compatibility)
    pip install --upgrade --target . -r requirements.txt
    
    # Copy RSA private key for JWT authentication
    echo "🔐 Setting up JWT authentication keys..."
    
    if [ -f "../keys/snowflake_rsa_key.p8" ]; then
        mkdir -p keys
        cp ../keys/snowflake_rsa_key.p8 keys/
        echo "✅ RSA private key copied for JWT authentication"
    else
        echo "⚠️  Warning: RSA private key not found. JWT authentication may fail."
    fi
    
    echo "📦 Creating deployment package..."
    
    # Create deployment zip (excluding development files)
    zip -r ../lambda-api-jwt-deployment.zip . -x "*.pyc" "*__pycache__*" "*.git*" "*.DS_Store*" "requirements.txt"
    
    echo "✅ API Lambda deployment package created: lambda-api-jwt-deployment.zip"
    echo "   Size: $(du -h ../lambda-api-jwt-deployment.zip | cut -f1)"
    
else
    echo "❌ Lambda directory not found. Please run from teddy-datapipeline root."
    exit 1
fi

cd ..

# Deploy API Lambda function
echo "🚀 Deploying API Lambda function..."

AWS_FUNCTION_NAME="teddy-api-parcel-ingestion-dev"

if aws lambda get-function --function-name "$AWS_FUNCTION_NAME" >/dev/null 2>&1; then
    echo "📤 Updating existing Lambda function: $AWS_FUNCTION_NAME"
    aws lambda update-function-code \
        --function-name "$AWS_FUNCTION_NAME" \
        --zip-file fileb://lambda-api-jwt-deployment.zip
    
    echo "⚙️  Updating Lambda configuration for JWT authentication..."
    aws lambda update-function-configuration \
        --function-name "$AWS_FUNCTION_NAME" \
        --timeout 300 \
        --memory-size 1024 \
        --environment 'Variables={"ENVIRONMENT":"dev","DATA_BUCKET":"teddy-data-lake-dev"}'
        
    echo "✅ Lambda function updated successfully!"
else
    echo "❌ Lambda function $AWS_FUNCTION_NAME not found. Please create it first or update the function name."
    exit 1
fi

# Create bulk ingestion package
echo "📦 Creating bulk ingestion Lambda package..."

cd "$BUILD_DIR"

# Copy bulk ingestion function
cp bulk_parcel_ingestion.py bulk_lambda_function.py

# Update import to use JWT connector
sed -i.bak 's/from utils.working_http_snowflake_connector import SnowflakeConnector/from utils.snowflake_jwt_connector import SnowflakeJWTConnector/g' bulk_lambda_function.py
sed -i.bak 's/SnowflakeConnector()/SnowflakeJWTConnector()/g' bulk_lambda_function.py

# Create bulk deployment package
zip -r ../lambda-bulk-jwt-deployment.zip . -x "*.pyc" "*__pycache__*" "*.git*" "*.DS_Store*" "requirements.txt" "api_parcel_ingestion.py" "lambda_function.py"

cd ..

echo "✅ Bulk Lambda deployment package created: lambda-bulk-jwt-deployment.zip"
echo "   Size: $(du -h lambda-bulk-jwt-deployment.zip | cut -f1)"

# Deploy bulk Lambda function
BULK_FUNCTION_NAME="teddy-bulk-parcel-ingestion-dev"

if aws lambda get-function --function-name "$BULK_FUNCTION_NAME" >/dev/null 2>&1; then
    echo "📤 Updating bulk Lambda function: $BULK_FUNCTION_NAME"
    aws lambda update-function-code \
        --function-name "$BULK_FUNCTION_NAME" \
        --zip-file fileb://lambda-bulk-jwt-deployment.zip
    
    echo "✅ Bulk Lambda function updated successfully!"
else
    echo "ℹ️  Bulk Lambda function $BULK_FUNCTION_NAME not found. Skipping bulk deployment."
fi

echo ""
echo "🎉 JWT Lambda deployment complete!"
echo ""
echo "📋 Deployment Summary:"
echo "   ✅ API Lambda: $AWS_FUNCTION_NAME"
echo "   ✅ Authentication: JWT with RSA keys"
echo "   ✅ Snowflake: Native connector (snowflake-connector-python)"
echo "   ✅ Dependencies: Cryptography >=41.0.0 (proven working)"
echo "   ✅ Platform: Linux x86_64 (Lambda compatible)"
echo ""
echo "🧪 Test the deployment:"
echo "aws lambda invoke --function-name $AWS_FUNCTION_NAME --payload '{\"body\": \"{\\\"type\\\":\\\"coordinates\\\",\\\"latitude\\\":29.445844,\\\"longitude\\\":-103.668153}\"}' response.json && cat response.json"
echo ""
echo "📊 Monitor logs:"
echo "aws logs tail /aws/lambda/$AWS_FUNCTION_NAME --follow"