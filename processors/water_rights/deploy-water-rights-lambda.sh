#!/bin/bash

# Deploy Water Rights Lambda function to AWS (Dataset 5)
# Function name matches the orchestrator's enrichment_functions entry.
set -e

FUNCTION_NAME="teddy-water-rights-enrichment"
REGION="us-east-1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REPOSITORY_NAME="teddy-water-rights-processor"
IMAGE_TAG="latest"

echo "🚀 Deploying Water Rights Lambda function..."
echo "Function: $FUNCTION_NAME"

if ! aws ecr describe-repositories --repository-names $REPOSITORY_NAME --region $REGION >/dev/null 2>&1; then
    echo "📦 Creating ECR repository: $REPOSITORY_NAME"
    aws ecr create-repository --repository-name $REPOSITORY_NAME --region $REGION \
        --image-scanning-configuration scanOnPush=true
fi

echo "🔐 Logging into ECR..."
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

echo "🔨 Building Docker image..."
docker build -t $REPOSITORY_NAME:$IMAGE_TAG -f processors/water_rights/Dockerfile.water_rights .

ECR_URI="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPOSITORY_NAME:$IMAGE_TAG"
docker tag $REPOSITORY_NAME:$IMAGE_TAG $ECR_URI
echo "⬆️  Pushing image to ECR..."
docker push $ECR_URI

if aws lambda get-function --function-name $FUNCTION_NAME --region $REGION >/dev/null 2>&1; then
    echo "🔄 Updating existing Lambda function..."
    aws lambda update-function-code --function-name $FUNCTION_NAME --image-uri $ECR_URI --region $REGION
    aws lambda update-function-configuration --function-name $FUNCTION_NAME \
        --timeout 300 --memory-size 512 \
        --environment Variables='{"ENVIRONMENT":"dev","WATER_RIGHTS_RADIUS_MI":"0.25","FORCE_REFRESH":"false"}' \
        --region $REGION
else
    echo "✨ Creating new Lambda function..."
    ROLE_ARN="arn:aws:iam::$ACCOUNT_ID:role/TeddyDataPipeline-Lambda-dev"
    aws lambda create-function --function-name $FUNCTION_NAME --role $ROLE_ARN \
        --code ImageUri=$ECR_URI --package-type Image --timeout 300 --memory-size 512 \
        --environment Variables='{"ENVIRONMENT":"dev","WATER_RIGHTS_RADIUS_MI":"0.25","FORCE_REFRESH":"false"}' \
        --region $REGION
fi

echo "✅ Water Rights Lambda function deployed successfully!"
echo "Function ARN: arn:aws:lambda:$REGION:$ACCOUNT_ID:function:$FUNCTION_NAME"
