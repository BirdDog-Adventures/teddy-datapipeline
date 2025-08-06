"""
Example Usage of HubSpot AI/ML Module

This script demonstrates how to use all the AI/ML components
for HubSpot data analysis and insights.
"""

import logging
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ai.model_trainer import ModelTrainer
from ai.lead_scoring import LeadScoringModel
from ai.predictive_analytics import PredictiveAnalytics
from ai.customer_segmentation import CustomerSegmentation
from ai.churn_prediction import ChurnPredictor
from ai.recommendation_engine import RecommendationEngine
from ai.data_processor import HubSpotDataProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main example workflow."""
    print("HubSpot AI/ML Module Example")
    print("=" * 40)
    
    try:
        # Example 1: Automated Model Training
        print("\n1. AUTOMATED MODEL TRAINING")
        print("-" * 30)
        
        trainer = ModelTrainer(models_directory="example_models")
        
        # Train all models with sample parameters
        training_results = trainer.train_all_models(
            days_back=180,  # Use last 6 months of data
            test_size=0.2,
            optimize_hyperparameters=False  # Set to True for better performance
        )
        
        session_id = training_results['session_id']
        print(f"Training session: {session_id}")
        print(f"Training duration: {training_results.get('duration', 0):.2f} seconds")
        
        # Generate training report
        training_report = trainer.generate_training_report(session_id)
        print("\nTraining Report:")
        print(training_report)
        
        # Example 2: Individual Model Usage
        print("\n2. INDIVIDUAL MODEL EXAMPLES")
        print("-" * 30)
        
        # Lead Scoring Example
        print("\nLead Scoring:")
        lead_scorer = LeadScoringModel('random_forest')
        
        try:
            # Prepare training data
            X_train, X_test, y_train, y_test = lead_scorer.prepare_training_data(days_back=180)
            
            # Train model
            training_result = lead_scorer.train(X_train, y_train)
            print(f"Lead scoring model trained with AUC: {training_result.get('cv_auc_mean', 0):.3f}")
            
            # Evaluate model
            evaluation = lead_scorer.evaluate(X_test, y_test)
            print(f"Test AUC: {evaluation.get('auc_score', 0):.3f}")
            
        except Exception as e:
            print(f"Lead scoring example failed: {e}")
        
        # Customer Segmentation Example
        print("\nCustomer Segmentation:")
        segmentation = CustomerSegmentation('kmeans')
        
        try:
            # Prepare segmentation data
            seg_data = segmentation.prepare_segmentation_data(days_back=180)
            
            if not seg_data.empty:
                # Perform clustering
                clustered_data = segmentation.perform_clustering(seg_data, n_clusters=5)
                
                # Profile segments
                profiles = segmentation.profile_segments(clustered_data)
                print(f"Created {len(profiles)} customer segments")
                
                # Generate segmentation report
                seg_report = segmentation.generate_segmentation_report(clustered_data)
                print("Segmentation Report (first 500 chars):")
                print(seg_report[:500] + "...")
            else:
                print("No data available for segmentation")
                
        except Exception as e:
            print(f"Segmentation example failed: {e}")
        
        # Churn Prediction Example
        print("\nChurn Prediction:")
        churn_predictor = ChurnPredictor('random_forest')
        
        try:
            # Prepare churn data
            churn_data = churn_predictor.prepare_churn_data(days_back=180)
            
            if not churn_data.empty:
                # Train churn model
                churn_results = churn_predictor.train_churn_model(churn_data)
                print(f"Churn model trained with AUC: {churn_results.get('cv_auc_mean', 0):.3f}")
                print(f"Churn rate: {churn_results.get('churn_rate', 0):.2%}")
            else:
                print("No data available for churn prediction")
                
        except Exception as e:
            print(f"Churn prediction example failed: {e}")
        
        # Predictive Analytics Example
        print("\nPredictive Analytics:")
        analytics = PredictiveAnalytics()
        
        try:
            # Prepare deal data
            deal_data = analytics.prepare_deal_data(days_back=180)
            
            if not deal_data.empty:
                # Train closure prediction
                closure_results = analytics.train_closure_prediction_model(deal_data)
                print(f"Deal closure model trained with AUC: {closure_results.get('cv_auc_mean', 0):.3f}")
                
                # Generate revenue forecast
                forecast = analytics.generate_revenue_forecast(months_ahead=6)
                if forecast:
                    total_forecast = forecast.get('total_forecasted_revenue', 0)
                    print(f"6-month revenue forecast: ${total_forecast:,.2f}")
            else:
                print("No deal data available for predictive analytics")
                
        except Exception as e:
            print(f"Predictive analytics example failed: {e}")
        
        # Example 3: Integrated Recommendation Engine
        print("\n3. RECOMMENDATION ENGINE")
        print("-" * 30)
        
        try:
            # Create recommendation engine with trained models
            recommendation_engine = trainer.create_recommendation_engine(session_id)
            
            # Prepare sample data for recommendations
            data_processor = HubSpotDataProcessor()
            sample_data = data_processor.extract_lead_data(days_back=30)
            
            if not sample_data.empty:
                # Limit to first 100 records for example
                sample_data = sample_data.head(100)
                
                # Generate recommendations
                recommendations = recommendation_engine.generate_recommendations(sample_data)
                
                print(f"Generated recommendations for {len(recommendations)} customers")
                
                # Prioritize customers
                prioritized = recommendation_engine.prioritize_customers(recommendations)
                
                # Show top 5 priority customers
                print("\nTop 5 Priority Customers:")
                top_5 = prioritized.head(5)
                for idx, customer in top_5.iterrows():
                    email = customer.get('EMAIL', 'N/A')
                    strategy = customer.get('PRIMARY_STRATEGY', 'N/A')
                    priority = customer.get('PRIORITY', 'N/A')
                    print(f"  {email} - {strategy} ({priority})")
                
                # Generate action plan
                action_plan = recommendation_engine.generate_action_plan(recommendations, days_ahead=7)
                print(f"\n7-day action plan created for {action_plan['summary']['total_customers']} customers")
                print(f"Daily capacity: {action_plan['summary']['daily_capacity']} customers")
                
                # Generate recommendations report
                rec_report = recommendation_engine.generate_recommendations_report(recommendations)
                print("\nRecommendations Report (first 800 chars):")
                print(rec_report[:800] + "...")
            else:
                print("No recent data available for recommendations")
                
        except Exception as e:
            print(f"Recommendation engine example failed: {e}")
        
        # Example 4: Model Performance Evaluation
        print("\n4. MODEL PERFORMANCE EVALUATION")
        print("-" * 30)
        
        try:
            performance = trainer.evaluate_model_performance(session_id)
            print(f"Overall performance score: {performance['overall_score']:.1f}/100")
            
            for model_type, perf in performance['model_performance'].items():
                model_name = perf.get('model_type', 'N/A')
                grade = perf.get('performance_grade', 0)
                print(f"  {model_type}: {model_name} ({grade:.1f}/100)")
                
        except Exception as e:
            print(f"Performance evaluation failed: {e}")
        
        print("\n" + "=" * 40)
        print("Example completed successfully!")
        print(f"Models saved in: example_models/")
        print(f"Session ID: {session_id}")
        
    except Exception as e:
        logger.error(f"Example failed: {e}")
        print(f"\nExample failed with error: {e}")
        print("Please check your Snowflake connection and data availability.")


def quick_test():
    """Quick test of basic functionality without full training."""
    print("Quick Test - Basic Functionality")
    print("=" * 35)
    
    try:
        # Test individual models (without training or data connection)
        print("Testing model initialization...")
        
        lead_scorer = LeadScoringModel('random_forest')
        print("✓ Lead scoring model initialized")
        
        churn_predictor = ChurnPredictor('gradient_boosting')
        print("✓ Churn prediction model initialized")
        
        segmentation = CustomerSegmentation('kmeans')
        print("✓ Customer segmentation model initialized")
        
        analytics = PredictiveAnalytics()
        print("✓ Predictive analytics initialized")
        
        recommendation_engine = RecommendationEngine()
        print("✓ Recommendation engine initialized")
        
        trainer = ModelTrainer()
        print("✓ Model trainer initialized")
        
        # Test data processor initialization (without connection)
        print("Testing data processor initialization...")
        try:
            data_processor = HubSpotDataProcessor()
            print("✓ Data processor initialized")
        except Exception as e:
            print(f"⚠ Data processor initialization failed: {e}")
            print("  This is expected if Snowflake credentials are not configured.")
            print("  The AI models can still be used with pre-processed data.")
        
        print("\nAll core components initialized successfully!")
        print("\nTo run with actual data:")
        print("1. Configure Snowflake credentials in .env file")
        print("2. Run option 2 for full training and analysis example")
        print("\nRequired .env variables:")
        print("- SNOWFLAKE_ACCOUNT")
        print("- SNOWFLAKE_USER") 
        print("- SNOWFLAKE_PASSWORD")
        print("- SNOWFLAKE_WAREHOUSE")
        print("- SNOWFLAKE_DATABASE")
        print("- SNOWFLAKE_SCHEMA")
        
    except Exception as e:
        print(f"Quick test failed: {e}")


if __name__ == "__main__":
    print("HubSpot AI/ML Module")
    print("Choose an option:")
    print("1. Quick test (no training)")
    print("2. Full example (with training)")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        quick_test()
    elif choice == "2":
        main()
    else:
        print("Invalid choice. Running quick test...")
        quick_test()
