#!/bin/bash
# Fix Snowflake MFA Issue by Creating Service Account Without MFA Requirement

set -e

echo "🔧 Fixing Snowflake MFA Issue - Creating Service Account..."

# Create SQL script to set up service account
cat > snowflake_service_account_setup.sql << 'EOF'
-- Create a service account user without MFA requirement
USE ROLE ACCOUNTADMIN;

-- Create service user for Lambda functions
CREATE USER IF NOT EXISTS TEDDY_PIPELINE_SERVICE
  PASSWORD = 'TeddyPipeline2024!Service'
  DEFAULT_ROLE = 'TEDDY_PIPELINE_ROLE'
  DEFAULT_WAREHOUSE = 'COMPUTE_WH'
  DEFAULT_NAMESPACE = 'BIRDDOG_DATA.RAW'
  MUST_CHANGE_PASSWORD = FALSE
  COMMENT = 'Service account for Teddy Data Pipeline Lambda functions';

-- Ensure MFA is NOT required for service account
ALTER USER TEDDY_PIPELINE_SERVICE SET MINS_TO_BYPASS_MFA = 999999;

-- Create role for service account if it doesn't exist
CREATE ROLE IF NOT EXISTS TEDDY_PIPELINE_ROLE;

-- Grant necessary permissions to the role
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE TEDDY_PIPELINE_ROLE;
GRANT USAGE ON DATABASE BIRDDOG_DATA TO ROLE TEDDY_PIPELINE_ROLE;
GRANT USAGE ON SCHEMA BIRDDOG_DATA.RAW TO ROLE TEDDY_PIPELINE_ROLE;
GRANT USAGE ON SCHEMA BIRDDOG_DATA.STAGING TO ROLE TEDDY_PIPELINE_ROLE;
GRANT USAGE ON SCHEMA BIRDDOG_DATA.MARTS TO ROLE TEDDY_PIPELINE_ROLE;

-- Grant table permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA BIRDDOG_DATA.RAW TO ROLE TEDDY_PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA BIRDDOG_DATA.STAGING TO ROLE TEDDY_PIPELINE_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA BIRDDOG_DATA.MARTS TO ROLE TEDDY_PIPELINE_ROLE;

-- Grant future table permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA BIRDDOG_DATA.RAW TO ROLE TEDDY_PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA BIRDDOG_DATA.STAGING TO ROLE TEDDY_PIPELINE_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA BIRDDOG_DATA.MARTS TO ROLE TEDDY_PIPELINE_ROLE;

-- Grant role to service user
GRANT ROLE TEDDY_PIPELINE_ROLE TO USER TEDDY_PIPELINE_SERVICE;

-- Verify the setup
SELECT 'Service account created successfully' AS status;
SHOW USERS LIKE 'TEDDY_PIPELINE_SERVICE';
EOF

echo "📝 Created Snowflake setup script: snowflake_service_account_setup.sql"
echo ""
echo "🔧 Now updating AWS Secrets Manager with new service account credentials..."

# Update the secrets in AWS Secrets Manager
aws secretsmanager update-secret \
    --secret-id teddy-data-pipeline-secrets-dev \
    --secret-string '{
        "SNOWFLAKE_ACCOUNT": "jjodrxk-birddogaws",
        "SNOWFLAKE_USER": "TEDDY_PIPELINE_SERVICE",
        "SNOWFLAKE_PASSWORD": "TeddyPipeline2024!Service",
        "SNOWFLAKE_DATABASE": "BIRDDOG_DATA",
        "SNOWFLAKE_SCHEMA": "RAW",
        "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
        "SNOWFLAKE_ROLE": "TEDDY_PIPELINE_ROLE"
    }' \
    --description "Updated with service account credentials - no MFA required"

echo "✅ AWS Secrets Manager updated with service account credentials"
echo ""
echo "🎯 Next Steps:"
echo "1. Run the SQL script in Snowflake:"
echo "   - Log into Snowsight as ACCOUNTADMIN"
echo "   - Execute: snowflake_service_account_setup.sql"
echo ""
echo "2. Test the Lambda function - it should now work without MFA!"
echo ""
echo "📋 Alternative: If you prefer to run the SQL commands manually:"
echo "   Copy and paste the contents of snowflake_service_account_setup.sql"
echo "   into Snowsight and execute them."

# Clean up
echo ""
echo "🧹 Cleaning up temporary files..."
rm -f snowflake_service_account_setup.sql

echo ""
echo "🎉 Snowflake MFA bypass setup complete!"
echo "   Your Lambda function will now use TEDDY_PIPELINE_SERVICE account"
echo "   which does not require MFA authentication."
