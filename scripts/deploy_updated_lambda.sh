#!/bin/bash
# Deploy Updated Lambda Function with Simple Snowflake Connector

set -e

echo "🚀 Deploying Updated Lambda Function with Simple Snowflake Connector..."

# Function name
FUNCTION_NAME="teddy-api-parcel-ingestion-dev"

# Create deployment package
echo "📦 Creating deployment package..."

# Store the current directory (teddy-datapipeline root)
TEDDY_ROOT=$(pwd)

cd lambda

# Create a temporary directory for the package
rm -rf /tmp/lambda-package
mkdir -p /tmp/lambda-package

# Copy Lambda function files
cp -r . /tmp/lambda-package/

# Install dependencies in the package directory
cd /tmp/lambda-package
pip install -r $TEDDY_ROOT/requirements.txt -t .

# Create the zip file
zip -r ../lambda-deployment.zip . -x "*.pyc" "__pycache__/*" "*.git*"

cd ..

echo "✅ Deployment package created: lambda-deployment.zip"

# Update the Lambda function
echo "🔄 Updating Lambda function: $FUNCTION_NAME"

aws lambda update-function-code \
    --function-name $FUNCTION_NAME \
    --zip-file fileb://lambda-deployment.zip

echo "✅ Lambda function updated successfully!"

# Update environment variables to ensure they're correct
echo "🔧 Updating environment variables..."

aws lambda update-function-configuration \
    --function-name $FUNCTION_NAME \
    --environment Variables='{
        "ENVIRONMENT": "dev",
        "REGRID_API_KEY": "'$(aws secretsmanager get-secret-value --secret-id teddy-data-pipeline-secrets-dev --query SecretString --output text | jq -r .REGRID_API_KEY)'",
        "DATA_BUCKET": "teddy-data-pipeline-dev"
    }'

echo "✅ Environment variables updated!"

# Test the function
echo "🧪 Testing the updated function..."

# Create test payload
cat > test-payload.json << 'EOF'
{
  "body": "{\"type\": \"coordinates\", \"latitude\": 32.7767, \"longitude\": -96.7970}"
}
EOF

# Invoke the function
echo "📞 Invoking Lambda function with test coordinates..."
aws lambda invoke \
    --function-name $FUNCTION_NAME \
    --payload file://test-payload.json \
    --cli-binary-format raw-in-base64-out \
    response.json

echo "📋 Lambda Response:"
cat response.json | jq .

# Check logs
echo "📊 Recent logs:"
aws logs filter-log-events \
    --log-group-name "/aws/lambda/$FUNCTION_NAME" \
    --start-time $(date -d '5 minutes ago' +%s)000 \
    --query 'events[*].message' \
    --output text | tail -20

# Clean up
rm -f test-payload.json response.json lambda-deployment.zip
rm -rf /tmp/lambda-package

echo ""
echo "🎉 Lambda function deployment complete!"
echo "   Function: $FUNCTION_NAME"
echo "   Status: Updated with Simple Snowflake Connector"
echo "   Snowflake: Using password authentication (no MFA)"
echo ""
echo "🔍 Next steps:"
echo "   1. Check the logs above for any errors"
echo "   2. Test with your own coordinates/address"
echo "   3. Verify Snowflake connection in the logs"
