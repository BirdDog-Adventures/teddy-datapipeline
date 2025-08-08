#!/bin/bash

# Deploy Topography Lambda Function Script
# This script builds a Docker image and deploys it as a Lambda function

set -e

# Configuration
REGION="us-east-1"
FUNCTION_NAME="teddy-topography-parcel-processor-dev"
ECR_REPO_NAME="teddy-topography-processor"
IMAGE_TAG="latest"
DOCKERFILE="processors/topography/Dockerfile.topography"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🏔️  Starting Topography Lambda Deployment${NC}"
echo "Function: $FUNCTION_NAME"
echo "Region: $REGION"
echo "Docker file: $DOCKERFILE"
echo ""

# Get AWS account ID
echo -e "${YELLOW}Getting AWS account ID...${NC}"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
if [ -z "$ACCOUNT_ID" ]; then
    echo -e "${RED}❌ Error: Could not get AWS account ID${NC}"
    exit 1
fi
echo "Account ID: $ACCOUNT_ID"

# ECR repository URI
ECR_URI="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$ECR_REPO_NAME"

echo ""
echo -e "${YELLOW}🏗️  Step 1: Creating ECR repository if it doesn't exist...${NC}"
aws ecr describe-repositories --repository-names $ECR_REPO_NAME --region $REGION 2>/dev/null || {
    echo "Creating ECR repository: $ECR_REPO_NAME"
    aws ecr create-repository --repository-name $ECR_REPO_NAME --region $REGION
}

echo ""
echo -e "${YELLOW}🔐 Step 2: Authenticating Docker with ECR...${NC}"
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ECR_URI

echo ""
echo -e "${YELLOW}🐳 Step 3: Building Docker image...${NC}"
docker build --platform linux/amd64 -t $ECR_REPO_NAME:$IMAGE_TAG -f $DOCKERFILE .

echo ""
echo -e "${YELLOW}🏷️  Step 4: Tagging Docker image...${NC}"
docker tag $ECR_REPO_NAME:$IMAGE_TAG $ECR_URI:$IMAGE_TAG

echo ""
echo -e "${YELLOW}📤 Step 5: Pushing Docker image to ECR...${NC}"
docker push $ECR_URI:$IMAGE_TAG

echo ""
echo -e "${YELLOW}☁️  Step 6: Checking if Lambda function exists...${NC}"
if aws lambda get-function --function-name $FUNCTION_NAME --region $REGION 2>/dev/null; then
    echo "Function exists, updating..."
    
    echo ""
    echo -e "${YELLOW}🔄 Step 7: Updating Lambda function code...${NC}"
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --image-uri $ECR_URI:$IMAGE_TAG \
        --region $REGION

    echo ""
    echo -e "${YELLOW}⚙️  Step 8: Updating Lambda function configuration...${NC}"
    # Create environment variables JSON file
    cat > /tmp/topography-env-vars.json << EOF
{
    "Variables": {
        "ENVIRONMENT": "dev",
        "FORCE_REFRESH": "false"
    }
}
EOF

    aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --timeout 900 \
        --memory-size 1024 \
        --environment file:///tmp/topography-env-vars.json \
        --region $REGION

    # Clean up temp file
    rm /tmp/topography-env-vars.json

else
    echo "Function doesn't exist, creating..."
    
    echo ""
    echo -e "${YELLOW}🆕 Step 7: Creating Lambda function...${NC}"
    
    # Create environment variables JSON file
    cat > /tmp/topography-env-vars.json << EOF
{
    "Variables": {
        "ENVIRONMENT": "dev",
        "FORCE_REFRESH": "false"
    }
}
EOF

    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --package-type Image \
        --code ImageUri=$ECR_URI:$IMAGE_TAG \
        --role arn:aws:iam::$ACCOUNT_ID:role/TeddyDataPipeline-Lambda-dev \
        --timeout 900 \
        --memory-size 1024 \
        --environment file:///tmp/topography-env-vars.json \
        --region $REGION

    # Clean up temp file
    rm /tmp/topography-env-vars.json
fi

echo ""
echo -e "${YELLOW}⏳ Step 9: Waiting for function to be ready...${NC}"
aws lambda wait function-updated --function-name $FUNCTION_NAME --region $REGION

echo ""
echo -e "${YELLOW}🧪 Step 10: Testing Lambda function...${NC}"
# Create test event for single parcel processing
cat > /tmp/topography-test-event.json << EOF
{
    "parcel_id": "test_topography_001",
    "force_refresh": false
}
EOF

echo "Invoking function with test event..."
aws lambda invoke \
    --function-name $FUNCTION_NAME \
    --payload file:///tmp/topography-test-event.json \
    --region $REGION \
    /tmp/topography-response.json

echo ""
echo -e "${BLUE}📄 Test Response:${NC}"
cat /tmp/topography-response.json | jq '.'

# Clean up temp files
rm /tmp/topography-test-event.json /tmp/topography-response.json

echo ""
echo -e "${GREEN}✅ Topography Lambda deployment completed successfully!${NC}"
echo ""
echo -e "${BLUE}📋 Function Details:${NC}"
echo "Function Name: $FUNCTION_NAME"
echo "Function ARN: arn:aws:lambda:$REGION:$ACCOUNT_ID:function:$FUNCTION_NAME"
echo "ECR Image: $ECR_URI:$IMAGE_TAG"
echo ""
echo -e "${BLUE}🔧 Next Steps:${NC}"
echo "1. Set up SNS subscription for automatic topography processing"
echo "2. Test with real parcel data"
echo "3. Configure API Gateway endpoint if needed"
echo "4. Monitor CloudWatch logs: /aws/lambda/$FUNCTION_NAME"
echo ""
echo -e "${GREEN}🏔️  Topography processing is ready to roll!${NC}"