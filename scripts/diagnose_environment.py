#!/usr/bin/env python3
"""
Environment Diagnostic Script

This script helps diagnose Python environment and dependency issues.
Run this to understand what's happening with your setup.

Usage:
    python scripts/diagnose_environment.py
"""

import sys
import os
import subprocess
from pathlib import Path

def print_section(title):
    """Print a section header"""
    print(f"\n{'='*60}")
    print(f"🔍 {title}")
    print('='*60)

def run_command(cmd, description):
    """Run a command and show the result"""
    print(f"\n📋 {description}")
    print(f"Command: {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            print(f"✅ Success:")
            print(result.stdout.strip())
        else:
            print(f"❌ Failed (exit code {result.returncode}):")
            print(result.stderr.strip())
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print("⏰ Command timed out")
        return False
    except Exception as e:
        print(f"🚨 Error: {e}")
        return False

def check_python_info():
    """Check Python environment information"""
    print_section("PYTHON ENVIRONMENT")
    
    print(f"🐍 Python executable: {sys.executable}")
    print(f"🐍 Python version: {sys.version}")
    print(f"🐍 Python path: {sys.path[:3]}...")  # Show first 3 paths
    
    # Check if we're in a virtual environment
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("✅ Virtual environment is ACTIVE")
        print(f"   Virtual env prefix: {sys.prefix}")
        if hasattr(sys, 'base_prefix'):
            print(f"   Base prefix: {sys.base_prefix}")
    else:
        print("❌ Virtual environment is NOT active")
        print("   You're using the system Python")

def check_directory_structure():
    """Check if we're in the right directory"""
    print_section("DIRECTORY STRUCTURE")
    
    cwd = Path.cwd()
    print(f"📁 Current directory: {cwd}")
    
    required_files = [
        "requirements.txt",
        "lambda/utils/snowflake_connector.py", 
        "lambda/utils/nlcd_client.py",
        "scripts/test_nlcd_client.py",
        "venv"
    ]
    
    for file_path in required_files:
        path = Path(file_path)
        if path.exists():
            if path.is_dir():
                print(f"✅ Directory exists: {file_path}")
            else:
                print(f"✅ File exists: {file_path}")
        else:
            print(f"❌ Missing: {file_path}")

def check_installed_packages():
    """Check what packages are installed"""
    print_section("INSTALLED PACKAGES")
    
    # Check pip list
    run_command("pip list", "Listing all installed packages")
    
    # Check specific packages
    important_packages = [
        "snowflake-connector-python",
        "pandas", 
        "numpy",
        "boto3",
        "requests"
    ]
    
    print(f"\n🔍 Checking specific packages:")
    for package in important_packages:
        try:
            result = subprocess.run([sys.executable, "-c", f"import {package.replace('-', '_')}; print(f'{package}: OK')"], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ {package}: Importable")
            else:
                print(f"❌ {package}: Import failed - {result.stderr.strip()}")
        except Exception as e:
            print(f"🚨 {package}: Error checking - {e}")

def check_snowflake_specifically():
    """Check Snowflake connector specifically"""
    print_section("SNOWFLAKE CONNECTOR DIAGNOSIS")
    
    # Try different import methods
    import_tests = [
        ("import snowflake", "Basic snowflake import"),
        ("import snowflake.connector", "Snowflake connector import"),
        ("from snowflake.connector import connect", "Snowflake connect import"),
    ]
    
    for import_cmd, description in import_tests:
        print(f"\n🧪 Testing: {description}")
        try:
            result = subprocess.run([sys.executable, "-c", import_cmd], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ Success: {import_cmd}")
            else:
                print(f"❌ Failed: {import_cmd}")
                print(f"   Error: {result.stderr.strip()}")
        except Exception as e:
            print(f"🚨 Exception: {e}")

def check_environment_variables():
    """Check environment variables"""
    print_section("ENVIRONMENT VARIABLES")
    
    snowflake_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD", 
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_ROLE"
    ]
    
    for var in snowflake_vars:
        value = os.getenv(var)
        if value:
            # Mask sensitive values
            if "PASSWORD" in var or "SECRET" in var:
                masked_value = "*" * len(value)
                print(f"✅ {var}: {masked_value}")
            else:
                print(f"✅ {var}: {value}")
        else:
            print(f"❌ {var}: Not set")

def test_simple_imports():
    """Test simple imports that should work"""
    print_section("BASIC IMPORT TESTS")
    
    basic_imports = [
        "os",
        "sys", 
        "json",
        "pathlib",
        "subprocess"
    ]
    
    for module in basic_imports:
        try:
            __import__(module)
            print(f"✅ {module}: OK")
        except ImportError as e:
            print(f"❌ {module}: Failed - {e}")

def main():
    """Run all diagnostic checks"""
    print("🚀 TEDDY DATA PIPELINE - ENVIRONMENT DIAGNOSTICS")
    print("=" * 60)
    print("This script will help diagnose your Python environment setup.")
    
    # Run all checks
    check_python_info()
    check_directory_structure()
    test_simple_imports()
    check_installed_packages()
    check_snowflake_specifically()
    check_environment_variables()
    
    # Summary and recommendations
    print_section("SUMMARY & RECOMMENDATIONS")
    
    # Check if virtual environment is active
    venv_active = hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)
    
    if not venv_active:
        print("🚨 CRITICAL: Virtual environment is not active!")
        print("   Solution:")
        print("   1. cd teddy-datapipeline")
        print("   2. source venv/bin/activate")
        print("   3. Re-run this diagnostic")
        return
    
    # Check if snowflake can be imported
    try:
        import snowflake.connector
        print("✅ Snowflake connector is working!")
        print("   You should be able to run the NLCD tests now.")
    except ImportError as e:
        print("❌ Snowflake connector import failed!")
        print(f"   Error: {e}")
        print("   Solution:")
        print("   1. Make sure virtual environment is active")
        print("   2. pip install --upgrade pip")
        print("   3. pip install -r requirements.txt")
        print("   4. Re-run this diagnostic")
    
    print(f"\n🎯 Next steps:")
    print("1. Fix any issues shown above")
    print("2. Re-run: python scripts/diagnose_environment.py")
    print("3. When all checks pass, run: python scripts/test_nlcd_client.py --environment dev")

if __name__ == "__main__":
    main()
