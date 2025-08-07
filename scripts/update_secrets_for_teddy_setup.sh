#!/bin/bash
# Update AWS Secrets Manager with Correct Teddy Database Configuration

set -e

echo "🔧 Updating AWS Secrets Manager with Teddy Database Configuration..."

# Update the secrets in AWS Secrets Manager to match your Snowflake setup
aws secretsmanager update-secret \
    --secret-id teddy-data-pipeline-secrets-dev \
    --secret-string '{
        "SNOWFLAKE_ACCOUNT": "jjodrxk-birddogaws",
        "SNOWFLAKE_USER": "TEDDY_PIPELINE_SERVICE",
        "SNOWFLAKE_PASSWORD": "TeddyPipeline2024!Service",
        "SNOWFLAKE_DATABASE": "TEDDY_DATA",
        "SNOWFLAKE_SCHEMA": "RAW",
        "SNOWFLAKE_WAREHOUSE": "TEDDY_INGESTION_WH",
        "SNOWFLAKE_ROLE": "TEDDY_PIPELINE_ROLE"
    }' \
    --description "Updated with correct TEDDY_DATA database and TEDDY_INGESTION_WH warehouse"

echo "✅ AWS Secrets Manager updated successfully!"
echo ""
echo "📋 Updated Configuration:"
echo "   • Database: TEDDY_DATA (was BIRDDOG_DATA)"
echo "   • Warehouse: TEDDY_INGESTION_WH (was COMPUTE_WH)"
echo "   • User: TEDDY_PIPELINE_SERVICE"
echo "   • Role: TEDDY_PIPELINE_ROLE"
echo "   • Schema: RAW"
echo ""
echo "🎯 Your Lambda function will now connect to:"
echo "   TEDDY_DATA.RAW using TEDDY_INGESTION_WH warehouse"
echo ""
echo "🚀 Test your Lambda function - it should now work with full Snowflake integration!"
echo ""
echo "📝 Note: I noticed a small typo in your SQL script:"
echo "   Line: 'GRANT SELECT ON ALL TABLES IN SCHEMA BIRDDOG_DATA.CURATED TO ROLE TEDDY_PIPELINE_ROLE;'"
echo "   Should be: 'GRANT SELECT ON ALL TABLES IN SCHEMA TEDDY_DATA.CURATED TO ROLE TEDDY_PIPELINE_ROLE;'"
echo "   (BIRDDOG_DATA should be TEDDY_DATA to match your setup)"
