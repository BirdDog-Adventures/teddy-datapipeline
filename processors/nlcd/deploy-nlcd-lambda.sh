#!/bin/bash

# Deploy NLCD Lambda function to AWS
set -e

FUNCTION_NAME="teddy-nlcd-parcel-processor-dev"
REGION="us-east-1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REPOSITORY_NAME="teddy-nlcd-processor"
IMAGE_TAG="latest"

echo "🚀 Deploying NLCD Lambda function..."
echo "Function: $FUNCTION_NAME"
echo "Region: $REGION"
echo "Account: $ACCOUNT_ID"

# Check if ECR repository exists, create if not
if ! aws ecr describe-repositories --repository-names $REPOSITORY_NAME --region $REGION >/dev/null 2>&1; then
    echo "📦 Creating ECR repository: $REPOSITORY_NAME"
    aws ecr create-repository \
        --repository-name $REPOSITORY_NAME \
        --region $REGION \
        --image-scanning-configuration scanOnPush=true
fi

# Get ECR login token
echo "🔐 Logging into ECR..."
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

# Build Docker image
echo "🔨 Building Docker image..."
docker build -t $REPOSITORY_NAME:$IMAGE_TAG -f processors/nlcd/Dockerfile.nlcd .

# Tag and push image
ECR_URI="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPOSITORY_NAME:$IMAGE_TAG"
echo "🏷️  Tagging image: $ECR_URI"
docker tag $REPOSITORY_NAME:$IMAGE_TAG $ECR_URI

echo "⬆️  Pushing image to ECR..."
docker push $ECR_URI

# Update or create Lambda function
if aws lambda get-function --function-name $FUNCTION_NAME --region $REGION >/dev/null 2>&1; then
    echo "🔄 Updating existing Lambda function..."
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --image-uri $ECR_URI \
        --region $REGION
    
    # Update function configuration
    aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --timeout 300 \
        --memory-size 512 \
        --environment Variables='{"ENVIRONMENT":"dev","NLCD_YEAR":"2021","FORCE_REFRESH":"false"}' \
        --region $REGION
else
    echo "✨ Creating new Lambda function..."
    
    # Get the IAM role ARN (assumes it exists from CloudFormation)
    ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/TeddyDataPipeline-Lambda-dev"
    
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --role $ROLE_ARN \
        --code ImageUri=$ECR_URI \
        --package-type Image \
        --timeout 300 \
        --memory-size 512 \
        --environment Variables='{"ENVIRONMENT":"dev","NLCD_YEAR":"2021","FORCE_REFRESH":"false"}' \
        --region $REGION
fi

echo "✅ NLCD Lambda function deployed successfully!"
echo "Function ARN: arn:aws:lambda:$REGION:$ACCOUNT_ID:function:$FUNCTION_NAME"
echo "Image URI: $ECR_URI"