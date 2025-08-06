#!/bin/bash

# Setup Test Environment for NLCD Client Testing
# This script installs dependencies and prepares the environment for testing

set -e  # Exit on any error

echo "🚀 Setting up NLCD Client Test Environment"
echo "=========================================="

# Check if we're in the right directory
if [ ! -f "requirements.txt" ]; then
    echo "❌ Error: requirements.txt not found. Please run this script from the teddy-datapipeline directory."
    exit 1
fi

# Check Python version
python_version=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1,2)
echo "🐍 Python version: $python_version"

if [[ "$python_version" < "3.8" ]]; then
    echo "❌ Error: Python 3.8 or higher is required"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📥 Installing dependencies from requirements.txt..."
pip install -r requirements.txt

# Install additional testing dependencies
echo "📥 Installing additional testing dependencies..."
pip install pytest pytest-cov

# Verify key installations
echo "✅ Verifying installations..."

# Check Snowflake connector
python3 -c "import snowflake.connector; print('✅ Snowflake connector installed successfully')" || {
    echo "❌ Failed to import snowflake.connector"
    exit 1
}

# Check other key dependencies
python3 -c "import pandas; print('✅ Pandas installed successfully')" || {
    echo "❌ Failed to import pandas"
    exit 1
}

python3 -c "import requests; print('✅ Requests installed successfully')" || {
    echo "❌ Failed to import requests"
    exit 1
}

python3 -c "import boto3; print('✅ Boto3 installed successfully')" || {
    echo "❌ Failed to import boto3"
    exit 1
}

# Check if lambda modules can be imported
echo "🔍 Checking lambda module imports..."
python3 -c "
import sys
import os
sys.path.append(os.path.join(os.getcwd(), 'lambda'))
try:
    from utils.snowflake_connector import get_snowflake_connector
    print('✅ Snowflake connector module imported successfully')
except ImportError as e:
    print(f'⚠️  Warning: Could not import snowflake_connector: {e}')

try:
    from utils.nlcd_client import NLCDClient
    print('✅ NLCD client module imported successfully')
except ImportError as e:
    print(f'⚠️  Warning: Could not import nlcd_client: {e}')
"

echo ""
echo "🎉 Environment setup complete!"
echo ""
echo "📋 Next steps:"
echo "1. Set up your Snowflake environment variables:"
echo "   export SNOWFLAKE_ACCOUNT='your-account'"
echo "   export SNOWFLAKE_USER='your-username'"
echo "   export SNOWFLAKE_PASSWORD='your-password'"
echo "   export SNOWFLAKE_DATABASE='TEDDY_DATA'"
echo "   export SNOWFLAKE_WAREHOUSE='your-warehouse'"
echo "   export SNOWFLAKE_ROLE='your-role'"
echo ""
echo "2. Run the database schema setup:"
echo "   # Execute scripts/setup_land_cover_schema.sql in Snowflake"
echo ""
echo "3. Run the NLCD client tests:"
echo "   source venv/bin/activate"
echo "   python scripts/test_nlcd_client.py --environment dev"
echo ""
echo "4. For detailed testing instructions, see:"
echo "   docs/NLCD_CLIENT_TESTING_GUIDE.md"
echo ""
echo "🔧 To activate the virtual environment in the future:"
echo "   source venv/bin/activate"
