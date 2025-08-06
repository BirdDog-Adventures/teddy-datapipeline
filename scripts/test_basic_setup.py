#!/usr/bin/env python3
"""
Basic Setup Test Script

This script tests the basic setup without requiring all dependencies.
Use this to verify your environment before installing heavy dependencies.

Usage:
    python scripts/test_basic_setup.py
"""

import os
import sys
import subprocess
from pathlib import Path

def test_python_version():
    """Test Python version compatibility"""
    print("🐍 Testing Python version...")
    version = sys.version_info
    print(f"   Python version: {version.major}.{version.minor}.{version.micro}")
    
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("   ❌ Python 3.8 or higher is required")
        return False
    else:
        print("   ✅ Python version is compatible")
        return True

def test_directory_structure():
    """Test if we're in the right directory with correct structure"""
    print("\n📁 Testing directory structure...")
    
    required_files = [
        "requirements.txt",
        "lambda/utils/snowflake_connector.py",
        "lambda/utils/nlcd_client.py",
        "scripts/test_nlcd_client.py"
    ]
    
    missing_files = []
    for file_path in required_files:
        if not Path(file_path).exists():
            missing_files.append(file_path)
    
    if missing_files:
        print(f"   ❌ Missing required files: {missing_files}")
        print("   Make sure you're running this from the teddy-datapipeline directory")
        return False
    else:
        print("   ✅ All required files found")
        return True

def test_virtual_environment():
    """Test if virtual environment exists and is activated"""
    print("\n🔧 Testing virtual environment...")
    
    # Check if venv directory exists
    venv_path = Path("venv")
    if not venv_path.exists():
        print("   ⚠️  Virtual environment not found")
        print("   Run: python3 -m venv venv")
        return False
    
    # Check if we're in a virtual environment
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("   ✅ Virtual environment is activated")
        return True
    else:
        print("   ⚠️  Virtual environment not activated")
        print("   Run: source venv/bin/activate")
        return False

def test_pip_and_requirements():
    """Test pip and requirements.txt"""
    print("\n📦 Testing pip and requirements...")
    
    try:
        # Check pip version
        result = subprocess.run([sys.executable, "-m", "pip", "--version"], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print(f"   ✅ Pip available: {result.stdout.strip()}")
        else:
            print("   ❌ Pip not available")
            return False
            
        # Check requirements.txt
        with open("requirements.txt", "r") as f:
            requirements = f.read()
            if "snowflake-connector-python" in requirements:
                print("   ✅ Requirements.txt contains snowflake-connector-python")
            else:
                print("   ❌ Requirements.txt missing snowflake-connector-python")
                return False
                
        return True
        
    except Exception as e:
        print(f"   ❌ Error testing pip: {e}")
        return False

def test_environment_variables():
    """Test if environment variables are set"""
    print("\n🔐 Testing environment variables...")
    
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER", 
        "SNOWFLAKE_DATABASE"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"   ⚠️  Missing environment variables: {missing_vars}")
        print("   These are required for full testing but not for basic setup")
        return False
    else:
        print("   ✅ All required environment variables are set")
        return True

def main():
    """Run all basic setup tests"""
    print("🚀 Running Basic Setup Tests")
    print("=" * 50)
    
    tests = [
        ("Python Version", test_python_version),
        ("Directory Structure", test_directory_structure),
        ("Virtual Environment", test_virtual_environment),
        ("Pip and Requirements", test_pip_and_requirements),
        ("Environment Variables", test_environment_variables)
    ]
    
    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"   ❌ Error in {test_name}: {e}")
            results[test_name] = False
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 BASIC SETUP TEST SUMMARY")
    print("=" * 50)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{status} {test_name}")
    
    print(f"\nResults: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 Basic setup is complete!")
        print("\n📋 Next steps:")
        print("1. Install dependencies: pip install -r requirements.txt")
        print("2. Set up Snowflake environment variables")
        print("3. Run full tests: python scripts/test_nlcd_client.py")
    else:
        print("\n⚠️  Some setup issues found. Please address them before proceeding.")
        
        if not results.get("Virtual Environment", True):
            print("\n🔧 To create and activate virtual environment:")
            print("   python3 -m venv venv")
            print("   source venv/bin/activate")
            
        if not results.get("Environment Variables", True):
            print("\n🔐 To set environment variables:")
            print("   export SNOWFLAKE_ACCOUNT='your-account'")
            print("   export SNOWFLAKE_USER='your-username'")
            print("   export SNOWFLAKE_PASSWORD='your-password'")
            print("   export SNOWFLAKE_DATABASE='TEDDY_DATA'")
            print("   export SNOWFLAKE_WAREHOUSE='your-warehouse'")
            print("   export SNOWFLAKE_ROLE='your-role'")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
