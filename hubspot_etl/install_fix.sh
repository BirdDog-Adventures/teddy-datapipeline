#!/bin/bash

# Snowflake Connection Fix Installation Script
# This script applies the Python 3.13 compatibility fixes for Snowflake connection issues

set -e  # Exit on any error

echo "=============================================="
echo "Snowflake Connection Fix Installation Script"
echo "=============================================="
echo

# Check if we're in the right directory
if [[ ! -f "requirements.txt" ]] || [[ ! -f "snowflake_loader.py" ]]; then
    echo "❌ Error: This script must be run from the hubspot_etl directory"
    echo "Please navigate to: /Users/ganeshbirddog/Projects/birddog-datascraping-service/scraper/hubspot_etl"
    exit 1
fi

echo "✅ Found hubspot_etl directory"

# Check Python version
PYTHON_VERSION=$(python --version 2>&1 | cut -d' ' -f2)
echo "📍 Current Python version: $PYTHON_VERSION"

if [[ $PYTHON_VERSION == 3.13* ]]; then
    echo "⚠️  Warning: You're using Python 3.13. This fix should resolve compatibility issues."
    echo "   For best results, consider using Python 3.11 or 3.12."
elif [[ $PYTHON_VERSION == 3.12* ]] || [[ $PYTHON_VERSION == 3.11* ]]; then
    echo "✅ Good: You're using a recommended Python version."
else
    echo "⚠️  Warning: Untested Python version. This fix is designed for Python 3.11-3.13."
fi

echo

# Backup existing requirements.txt
if [[ -f "requirements.txt" ]]; then
    echo "📦 Backing up existing requirements.txt..."
    cp requirements.txt requirements.txt.backup.$(date +%Y%m%d_%H%M%S)
    echo "   Backup created: requirements.txt.backup.$(date +%Y%m%d_%H%M%S)"
fi

# Check if virtual environment is active
if [[ -z "$VIRTUAL_ENV" ]] && [[ -z "$CONDA_DEFAULT_ENV" ]]; then
    echo "⚠️  Warning: No virtual environment detected."
    echo "   It's recommended to use a virtual environment to avoid conflicts."
    echo
    read -p "Do you want to continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Installation cancelled. Please activate a virtual environment and try again."
        exit 1
    fi
else
    if [[ -n "$VIRTUAL_ENV" ]]; then
        echo "✅ Virtual environment detected: $(basename $VIRTUAL_ENV)"
    elif [[ -n "$CONDA_DEFAULT_ENV" ]]; then
        echo "✅ Conda environment detected: $CONDA_DEFAULT_ENV"
    fi
fi

echo

# Install/upgrade dependencies
echo "📦 Installing compatible dependencies..."
echo "   This may take a few minutes..."

# Uninstall potentially conflicting packages first
echo "   Uninstalling potentially conflicting packages..."
pip uninstall -y snowflake-connector-python cryptography pyOpenSSL cffi 2>/dev/null || true

# Install the fixed versions
echo "   Installing fixed versions..."
pip install -r requirements.txt

echo "✅ Dependencies installed successfully"
echo

# Test the connection
echo "🔍 Testing Snowflake connection..."
if python test_snowflake_connection.py; then
    echo
    echo "🎉 SUCCESS! Snowflake connection is working correctly."
    echo
    echo "Next steps:"
    echo "1. You can now run your AI model training scripts"
    echo "2. Try running: python -c \"from ai.model_trainer import ModelTrainer; print('AI modules loaded successfully')\""
    echo "3. Check the SNOWFLAKE_CONNECTION_FIX.md file for more details"
else
    echo
    echo "❌ Connection test failed. Please check the following:"
    echo
    echo "1. Verify your .env file configuration:"
    echo "   - SNOWFLAKE_ACCOUNT"
    echo "   - SNOWFLAKE_USER"
    echo "   - SNOWFLAKE_DATABASE"
    echo "   - SNOWFLAKE_SCHEMA"
    echo "   - SNOWFLAKE_WAREHOUSE"
    echo "   - SNOWFLAKE_PRIVATE_KEY_PATH (if using key-pair auth)"
    echo
    echo "2. Check that your private key file exists and is readable:"
    if [[ -n "${SNOWFLAKE_PRIVATE_KEY_PATH}" ]]; then
        if [[ -f "${SNOWFLAKE_PRIVATE_KEY_PATH}" ]]; then
            echo "   ✅ Private key file found: ${SNOWFLAKE_PRIVATE_KEY_PATH}"
        else
            echo "   ❌ Private key file not found: ${SNOWFLAKE_PRIVATE_KEY_PATH}"
        fi
    else
        echo "   ⚠️  SNOWFLAKE_PRIVATE_KEY_PATH not set in environment"
    fi
    echo
    echo "3. Verify network connectivity to Snowflake"
    echo "4. Check the detailed troubleshooting guide in SNOWFLAKE_CONNECTION_FIX.md"
    echo
    echo "For more help, run: python test_snowflake_connection.py"
fi

echo
echo "=============================================="
echo "Installation script completed"
echo "=============================================="
