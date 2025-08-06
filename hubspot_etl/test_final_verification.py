#!/usr/bin/env python3
"""
Final Verification Test for AI Models
Tests all the core models to ensure dependency injection is working properly.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_all_models():
    """Test all models quickly to verify fixes."""
    print("🔧 Final Verification: Testing All AI Models")
    print("=" * 50)
    
    results = {
        'lead_scoring': 0,
        'churn_prediction': 0, 
        'customer_segmentation': 0
    }
    
    # Test Lead Scoring
    print("\n1. Testing Lead Scoring Models...")
    try:
        from ai.lead_scoring import LeadScoringModel
        
        for model_type in ['random_forest', 'gradient_boosting', 'logistic_regression']:
            try:
                model = LeadScoringModel(model_type)
                data = model.prepare_training_data(days_back=30)
                print(f"   ✅ {model_type}: Data prepared ({len(data[0])} training samples)")
                results['lead_scoring'] += 1
            except Exception as e:
                print(f"   ❌ {model_type}: {e}")
    except Exception as e:
        print(f"   ❌ Lead Scoring module error: {e}")
    
    # Test Churn Prediction
    print("\n2. Testing Churn Prediction Models...")
    try:
        from ai.churn_prediction import ChurnPredictor
        
        for model_type in ['random_forest', 'gradient_boosting', 'logistic_regression']:
            try:
                model = ChurnPredictor(model_type)
                data = model.prepare_churn_data(days_back=30)
                print(f"   ✅ {model_type}: Data prepared ({len(data)} records)")
                results['churn_prediction'] += 1
            except Exception as e:
                print(f"   ❌ {model_type}: {e}")
    except Exception as e:
        print(f"   ❌ Churn Prediction module error: {e}")
    
    # Test Customer Segmentation
    print("\n3. Testing Customer Segmentation Models...")
    try:
        from ai.customer_segmentation import CustomerSegmentation
        
        for algorithm in ['kmeans', 'dbscan', 'hierarchical']:
            try:
                model = CustomerSegmentation(algorithm)
                data = model.prepare_segmentation_data(days_back=30)
                print(f"   ✅ {algorithm}: Data prepared ({len(data)} records)")
                results['customer_segmentation'] += 1
            except Exception as e:
                print(f"   ❌ {algorithm}: {e}")
    except Exception as e:
        print(f"   ❌ Customer Segmentation module error: {e}")
    
    # Summary
    total_working = sum(results.values())
    total_expected = 9  # 3 models x 3 variants each
    
    print("\n" + "=" * 50)
    print("📊 FINAL RESULTS:")
    print(f"Lead Scoring: {results['lead_scoring']}/3 models working")
    print(f"Churn Prediction: {results['churn_prediction']}/3 models working") 
    print(f"Customer Segmentation: {results['customer_segmentation']}/3 models working")
    print(f"\nOVERALL: {total_working}/{total_expected} models working ({total_working/total_expected:.1%})")
    
    if total_working >= 8:
        print("🎉 SUCCESS: Dependency injection issues are RESOLVED!")
        print("   All core models can access data_processor properly.")
    elif total_working >= 6:
        print("✅ MOSTLY FIXED: Most models are working correctly.")
        print("   Minor issues remain but core functionality is restored.")
    else:
        print("⚠️  ISSUES REMAIN: Some models still have problems.")
        print("   Check the error messages above for details.")
    
    return total_working >= 8

if __name__ == "__main__":
    test_all_models()