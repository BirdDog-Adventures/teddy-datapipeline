#!/usr/bin/env python3
"""
Test AI Models with Snowflake Data

This script tests the AI model training system with your HubSpot CRM data
loaded in Snowflake. It provides a quick way to verify everything is working
and demonstrates the key capabilities.
"""

import logging
import sys
import os
from datetime import datetime
import pandas as pd

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_snowflake_connection():
    """Test basic Snowflake connection and data availability."""
    print("🔍 Testing Snowflake Connection...")
    
    try:
        from snowflake_loader import SnowflakeLoader
        
        with SnowflakeLoader() as loader:
            # Test basic connection
            result = loader.execute_query("SELECT CURRENT_TIMESTAMP() as NOW")
            print(f"✅ Snowflake connection successful: {result[0]['NOW']}")
            
            # Check available tables
            tables_query = """
            SELECT TABLE_NAME, ROW_COUNT 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
            AND TABLE_NAME LIKE 'HUBSPOT_%'
            ORDER BY TABLE_NAME
            """
            
            tables = loader.execute_query(tables_query)
            print(f"📊 Available HubSpot tables:")
            for table in tables:
                print(f"   - {table['TABLE_NAME']}: {table['ROW_COUNT']:,} rows")
            
            return True
            
    except Exception as e:
        print(f"❌ Snowflake connection failed: {e}")
        return False

def test_data_processor():
    """Test the data processor with actual Snowflake data."""
    print("\n🔧 Testing Data Processor...")
    
    try:
        from ai.data_processor import HubSpotDataProcessor
        
        processor = HubSpotDataProcessor()
        
        # Extract sample data
        print("   Extracting lead data (last 30 days)...")
        lead_data = processor.extract_lead_data(days_back=30, include_deals=True, include_companies=True)
        
        if lead_data.empty:
            print("⚠️  No recent lead data found. Trying last 365 days...")
            lead_data = processor.extract_lead_data(days_back=365, include_deals=True, include_companies=True)
        
        if not lead_data.empty:
            print(f"✅ Extracted {len(lead_data)} leads")
            print(f"   Columns: {list(lead_data.columns)}")
            
            # Test feature engineering
            print("   Engineering features...")
            engineered_data = processor.engineer_features(lead_data)
            print(f"✅ Feature engineering complete: {len(engineered_data.columns)} features")
            
            # Test conversion target creation
            print("   Creating conversion targets...")
            target_data = processor.create_conversion_target(engineered_data)
            conversion_rate = target_data['CONVERTED'].mean()
            print(f"✅ Conversion rate: {conversion_rate:.2%}")
            
            return target_data
        else:
            print("❌ No lead data available")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Data processor test failed: {e}")
        return pd.DataFrame()

def test_lead_scoring(data):
    """Test lead scoring model."""
    print("\n🎯 Testing Lead Scoring Model...")
    
    if data.empty:
        print("⚠️  Skipping lead scoring - no data available")
        return
    
    try:
        from ai.lead_scoring import LeadScoringModel
        
        # Initialize model
        lead_scorer = LeadScoringModel('random_forest')
        
        # Check if we have enough data for training
        if len(data) < 50:
            print(f"⚠️  Only {len(data)} records available. Need at least 50 for training.")
            return
        
        # Check conversion rate
        conversion_rate = data['CONVERTED'].mean()
        if conversion_rate == 0 or conversion_rate == 1:
            print(f"⚠️  Conversion rate is {conversion_rate:.2%}. Need mixed outcomes for training.")
            return
        
        print(f"   Training with {len(data)} records, {conversion_rate:.2%} conversion rate")
        
        # Prepare training data
        X_train, X_test, y_train, y_test = lead_scorer.prepare_training_data(days_back=365)
        
        if len(X_train) > 0:
            # Train model
            training_result = lead_scorer.train(X_train, y_train)
            print(f"✅ Lead scoring model trained")
            print(f"   Cross-validation AUC: {training_result.get('cv_auc_mean', 0):.3f}")
            
            # Evaluate on test set
            if len(X_test) > 0:
                evaluation = lead_scorer.evaluate(X_test, y_test)
                print(f"   Test AUC: {evaluation.get('auc_score', 0):.3f}")
            
            return True
        else:
            print("❌ No training data available")
            return False
            
    except Exception as e:
        print(f"❌ Lead scoring test failed: {e}")
        return False

def test_customer_segmentation(data):
    """Test customer segmentation."""
    print("\n👥 Testing Customer Segmentation...")
    
    if data.empty:
        print("⚠️  Skipping segmentation - no data available")
        return
    
    try:
        from ai.customer_segmentation import CustomerSegmentation
        
        segmentation = CustomerSegmentation('kmeans')
        
        # Prepare segmentation data
        seg_data = segmentation.prepare_segmentation_data(days_back=365)
        
        if not seg_data.empty and len(seg_data) >= 10:
            print(f"   Segmenting {len(seg_data)} customers...")
            
            # Perform clustering
            n_clusters = min(5, len(seg_data) // 2)  # Ensure reasonable cluster count
            clustered_data = segmentation.perform_clustering(seg_data, n_clusters=n_clusters)
            
            # Profile segments
            profiles = segmentation.profile_segments(clustered_data)
            
            print(f"✅ Created {len(profiles)} customer segments:")
            for segment_id, profile in profiles.items():
                print(f"   Segment {segment_id}: {profile['size']} customers")
            
            return True
        else:
            print(f"⚠️  Not enough data for segmentation: {len(seg_data) if not seg_data.empty else 0} records")
            return False
            
    except Exception as e:
        print(f"❌ Customer segmentation test failed: {e}")
        return False

def test_model_trainer():
    """Test the integrated model trainer."""
    print("\n🚀 Testing Model Trainer...")
    
    try:
        from ai.model_trainer import ModelTrainer
        
        trainer = ModelTrainer(models_directory="test_models")
        
        print("   Starting automated training (this may take a few minutes)...")
        
        # Train with smaller dataset for testing
        training_results = trainer.train_all_models(
            days_back=180,  # 6 months of data
            test_size=0.3,  # Larger test set for small datasets
            optimize_hyperparameters=False  # Faster training
        )
        
        session_id = training_results['session_id']
        duration = training_results.get('duration', 0)
        
        print(f"✅ Training completed in {duration:.1f} seconds")
        print(f"   Session ID: {session_id}")
        
        # Check results
        results = training_results.get('results', {})
        successful_models = 0
        
        for model_type, model_results in results.items():
            if isinstance(model_results, dict):
                for model_name, result in model_results.items():
                    if result.get('status') == 'success':
                        successful_models += 1
                        print(f"   ✅ {model_type}.{model_name}")
                    else:
                        print(f"   ❌ {model_type}.{model_name}: {result.get('error', 'Unknown error')}")
            else:
                if model_results.get('status') == 'success':
                    successful_models += 1
                    print(f"   ✅ {model_type}")
                else:
                    print(f"   ❌ {model_type}: {model_results.get('error', 'Unknown error')}")
        
        print(f"   {successful_models} models trained successfully")
        
        # Generate report
        if successful_models > 0:
            try:
                report = trainer.generate_training_report(session_id)
                print("\n📊 Training Report Summary:")
                lines = report.split('\n')
                for line in lines[:20]:  # First 20 lines
                    if line.strip():
                        print(f"   {line}")
                if len(lines) > 20:
                    print("   ... (truncated)")
            except Exception as e:
                print(f"   ⚠️  Could not generate report: {e}")
        
        return successful_models > 0
        
    except Exception as e:
        print(f"❌ Model trainer test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🧪 AI Model Testing Suite")
    print("=" * 50)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Test results tracking
    tests_passed = 0
    total_tests = 5
    
    # Test 1: Snowflake Connection
    if test_snowflake_connection():
        tests_passed += 1
    
    # Test 2: Data Processor
    sample_data = test_data_processor()
    if not sample_data.empty:
        tests_passed += 1
    
    # Test 3: Lead Scoring
    if test_lead_scoring(sample_data):
        tests_passed += 1
    
    # Test 4: Customer Segmentation
    if test_customer_segmentation(sample_data):
        tests_passed += 1
    
    # Test 5: Model Trainer
    if test_model_trainer():
        tests_passed += 1
    
    # Summary
    print("\n" + "=" * 50)
    print("🏁 Test Summary")
    print(f"Tests passed: {tests_passed}/{total_tests}")
    print(f"Success rate: {tests_passed/total_tests:.1%}")
    
    if tests_passed == total_tests:
        print("🎉 All tests passed! Your AI system is ready to use.")
        print("\nNext steps:")
        print("1. Run full training: python ai/example_ai_usage.py")
        print("2. Review the AI_MODEL_TRAINING_GUIDE.md for detailed usage")
        print("3. Set up automated daily recommendations")
    elif tests_passed >= 3:
        print("✅ Most tests passed. System is functional with some limitations.")
        print("Check the failed tests above for specific issues.")
    else:
        print("⚠️  Multiple tests failed. Please check:")
        print("1. Snowflake connection and credentials")
        print("2. Data availability in HubSpot tables")
        print("3. Review error messages above")
    
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
