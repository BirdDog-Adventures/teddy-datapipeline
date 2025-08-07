#!/bin/bash
# Deploy Lambda using Docker container (fixes cryptography architecture issues)
# Based on the working approach from birddog-geodata-viewer

set -e

echo "🐳 Deploying Lambda functions using Docker container approach..."

# Configuration
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION="us-east-1"
REPOSITORY_NAME="teddy-data-pipeline"
IMAGE_TAG="latest"
FUNCTION_NAME="teddy-api-parcel-ingestion-container-dev"

# Full image URI
IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${REPOSITORY_NAME}:${IMAGE_TAG}"

echo "📋 Configuration:"
echo "   • AWS Account: $AWS_ACCOUNT_ID"
echo "   • Region: $AWS_REGION"
echo "   • Repository: $REPOSITORY_NAME"
echo "   • Function: $FUNCTION_NAME"
echo "   • Image URI: $IMAGE_URI"

# Create ECR repository if it doesn't exist
echo "🏗️  Setting up ECR repository..."
if ! aws ecr describe-repositories --repository-names $REPOSITORY_NAME >/dev/null 2>&1; then
    echo "Creating ECR repository: $REPOSITORY_NAME"
    aws ecr create-repository --repository-name $REPOSITORY_NAME
else
    echo "ECR repository already exists: $REPOSITORY_NAME"
fi

# Get ECR login token
echo "🔐 Logging into ECR..."
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

# Copy requirements for Docker build
cp requirements-docker.txt requirements.txt

# Build Docker image with platform targeting (critical for cryptography)
echo "🔨 Building Docker image with linux/amd64 platform..."
docker build --platform=linux/amd64 -t $REPOSITORY_NAME:$IMAGE_TAG .

# Tag for ECR
echo "🏷️  Tagging image for ECR..."
docker tag $REPOSITORY_NAME:$IMAGE_TAG $IMAGE_URI

# Push to ECR
echo "📤 Pushing image to ECR..."
docker push $IMAGE_URI

# Update Lambda function to use container image
echo "🚀 Updating Lambda function with container image..."

if aws lambda get-function --function-name $FUNCTION_NAME >/dev/null 2>&1; then
    echo "Updating existing Lambda function: $FUNCTION_NAME"
    
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --image-uri $IMAGE_URI
    
    # Update configuration for container deployment
    aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --timeout 300 \
        --memory-size 1024 \
        --environment 'Variables={"ENVIRONMENT":"dev","DATA_BUCKET":"teddy-data-lake-dev"}'
        
    echo "✅ Lambda function updated with container image!"
    
else
    echo "Creating new Lambda function: $FUNCTION_NAME"
    
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --package-type Image \
        --code ImageUri=$IMAGE_URI \
        --role arn:aws:iam::$AWS_ACCOUNT_ID:role/teddy-data-pipeline-dev-LambdaExecutionRole \
        --timeout 300 \
        --memory-size 1024 \
        --environment 'Variables={"ENVIRONMENT":"dev","DATA_BUCKET":"teddy-data-lake-dev"}'
        
    echo "✅ Lambda function created with container image!"
fi

echo ""
echo "🎉 Docker Lambda deployment complete!"
echo ""
echo "📋 Deployment Summary:"
echo "   ✅ Platform: linux/amd64 (Lambda compatible)"
echo "   ✅ Base Image: public.ecr.aws/lambda/python:3.11"
echo "   ✅ Authentication: JWT with RSA keys"
echo "   ✅ Cryptography: >=41.0.0 (properly compiled for x86_64)"
echo "   ✅ Snowflake: Native connector with JWT support"
echo ""
echo "🧪 Test the deployment:"
echo 'aws lambda invoke --function-name '$FUNCTION_NAME' --payload '\''{"body":"{\"type\":\"coordinates\",\"latitude\":29.445844,\"longitude\":-103.668153}"}'\'' response.json && cat response.json'
echo ""
echo "📊 Monitor logs:"
echo "aws logs tail /aws/lambda/$FUNCTION_NAME --follow"

echo ""
echo "🎯 Function deployed successfully: $FUNCTION_NAME"
echo "   Image: $IMAGE_URI"
echo "   SHA: $(docker inspect $IMAGE_URI --format='{{.RepoDigests}}')"

# Cleanup
rm requirements.txt