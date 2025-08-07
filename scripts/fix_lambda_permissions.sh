#!/bin/bash
# Fix Lambda IAM Permissions for Secrets Manager and S3 Access

set -e

echo "🔧 Fixing Lambda IAM Permissions for Snowflake Integration..."

# Get the Lambda function's execution role ARN
LAMBDA_ROLE_ARN=$(aws lambda get-function --function-name teddy-api-parcel-ingestion-dev --query 'Configuration.Role' --output text)
ROLE_NAME=$(echo $LAMBDA_ROLE_ARN | cut -d'/' -f2)

echo "📋 Lambda Role: $ROLE_NAME"

# Create IAM policy for Secrets Manager access
cat > lambda-secrets-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret"
            ],
            "Resource": [
                "arn:aws:secretsmanager:*:*:secret:teddy-data-pipeline-secrets-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::teddy-data-pipeline-bucket-*/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::teddy-data-pipeline-bucket-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:PutItem",
                "dynamodb:GetItem",
                "dynamodb:UpdateItem",
                "dynamodb:DeleteItem",
                "dynamodb:Query",
                "dynamodb:Scan"
            ],
            "Resource": [
                "arn:aws:dynamodb:*:*:table/teddy-parcel-cache-*"
            ]
        }
    ]
}
EOF

# Create the policy
echo "📝 Creating IAM policy..."
POLICY_ARN=$(aws iam create-policy \
    --policy-name TeddyLambdaSecretsPolicy \
    --policy-document file://lambda-secrets-policy.json \
    --query 'Policy.Arn' \
    --output text 2>/dev/null || \
    aws iam get-policy \
    --policy-arn "arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):policy/TeddyLambdaSecretsPolicy" \
    --query 'Policy.Arn' \
    --output text)

echo "📎 Policy ARN: $POLICY_ARN"

# Attach policy to Lambda role
echo "🔗 Attaching policy to Lambda role..."
aws iam attach-role-policy \
    --role-name $ROLE_NAME \
    --policy-arn $POLICY_ARN

echo "✅ IAM permissions updated successfully!"

# Clean up
rm -f lambda-secrets-policy.json

echo ""
echo "🎯 Lambda function now has permissions for:"
echo "   • Secrets Manager (Snowflake credentials)"
echo "   • S3 bucket access (data storage)"
echo "   • DynamoDB (caching)"
echo ""
echo "🚀 Your Lambda function should now work with full Snowflake integration!"
echo ""
echo "📋 Test the Lambda function again - it should now:"
echo "   1. Fetch parcel data from Regrid API ✅"
echo "   2. Store data to S3 ✅"
echo "   3. Load data to Snowflake ✅"
echo "   4. Cache results in DynamoDB ✅"
