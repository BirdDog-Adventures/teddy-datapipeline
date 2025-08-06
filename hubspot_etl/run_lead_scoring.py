#!/usr/bin/env python3
"""
Lead Scoring Engine - Individual Test
Run this script to train and score all leads
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ai.lead_scoring import LeadScoringModel

def main():
    print("🎯 Lead Scoring Engine - Individual Test")
    print("=" * 50)
    
    # Initialize lead scoring model
    lead_scorer = LeadScoringModel()
    
    try:
        # Prepare training data
        print("\n📚 Preparing training data...")
        X_train, X_test, y_train, y_test = lead_scorer.prepare_training_data()
        print(f"✅ Training data prepared: {len(X_train)} training samples")
        
        # Train the model
        print("🎯 Training Lead Scoring Model...")
        results = lead_scorer.train(X_train, y_train)
        print(f"✅ Model trained successfully!")
        print(f"   Model Type: {results['model_type']}")
        print(f"   Training Samples: {results['training_samples']}")
        print(f"   Features: {results['features_count']}")
        print(f"   CV AUC: {results['cv_auc_mean']:.3f} ± {results['cv_auc_std']:.3f}")
        
        # Evaluate on test set
        print("📊 Evaluating model...")
        eval_results = lead_scorer.evaluate(X_test, y_test)
        print(f"   Test AUC: {eval_results['auc_score']:.3f}")
        print(f"   Precision: {eval_results['precision']:.3f}")
        print(f"   Recall: {eval_results['recall']:.3f}")
        print(f"   F1-Score: {eval_results['f1_score']:.3f}")
        
        # Score all leads (get fresh data)
        print("\n📊 Scoring all leads...")
        from ai.data_processor import HubSpotDataProcessor
        data_processor = HubSpotDataProcessor()
        all_leads = data_processor.extract_lead_data(days_back=365, include_deals=True, include_companies=True)
        all_leads = data_processor.engineer_features(all_leads)
        scored_leads = lead_scorer.score_leads(all_leads)
        print(f"✅ Successfully scored {len(scored_leads)} leads")
        
        # Show top 10 highest scoring leads
        print("\n🏆 TOP 10 HIGHEST SCORING LEADS:")
        print("-" * 100)
        print(f"{'Score':<6} | {'Email':<30} | {'Company':<20} | {'Stage':<15} | {'Probability':<12}")
        print("-" * 100)
        
        top_leads = scored_leads.nlargest(10, 'LEAD_SCORE')
        for idx, lead in top_leads.iterrows():
            email = str(lead.get('EMAIL', 'N/A'))[:29]
            company = str(lead.get('COMPANY', 'N/A'))[:19] 
            stage = str(lead.get('LIFECYCLESTAGE', 'N/A'))[:14]
            score = lead.get('LEAD_SCORE', 0)
            prob = lead.get('CONVERSION_PROBABILITY', 0)
            
            print(f"{score:5.0f} | {email:<30} | {company:<20} | {stage:<15} | {prob:11.1%}")
        
        # Show score distribution
        print(f"\n📈 LEAD SCORE DISTRIBUTION:")
        print("-" * 50)
        stats = scored_leads['LEAD_SCORE'].describe()
        print(f"Total Leads: {int(stats['count'])}")
        print(f"Average Score: {stats['mean']:.1f}")
        print(f"Median Score: {stats['50%']:.1f}")
        print(f"Highest Score: {stats['max']:.1f}")
        print(f"Lowest Score: {stats['min']:.1f}")
        
        # Show leads by score ranges
        print(f"\n🎯 LEADS BY SCORE RANGE:")
        print("-" * 30)
        high_value = len(scored_leads[scored_leads['LEAD_SCORE'] >= 80])
        medium_value = len(scored_leads[(scored_leads['LEAD_SCORE'] >= 50) & (scored_leads['LEAD_SCORE'] < 80)])
        low_value = len(scored_leads[scored_leads['LEAD_SCORE'] < 50])
        
        print(f"High Priority (80+): {high_value} leads")
        print(f"Medium Priority (50-79): {medium_value} leads") 
        print(f"Low Priority (<50): {low_value} leads")
        
        print(f"\n✅ Lead scoring completed successfully!")
        print(f"📁 Model saved for production use")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()