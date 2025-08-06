"""
Recommendation Engine Module

This module provides intelligent recommendations for next best actions
based on customer data and predictive models.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime, timedelta

from .data_processor import HubSpotDataProcessor
from .lead_scoring import LeadScoringModel
from .churn_prediction import ChurnPredictor
from .customer_segmentation import CustomerSegmentation

logger = logging.getLogger(__name__)


class RecommendationEngine:
    """
    Intelligent recommendation engine for customer actions.
    
    Features:
    - Next best action recommendations
    - Personalized engagement strategies
    - Priority-based action ranking
    - Multi-model integration
    - Campaign optimization suggestions
    """
    
    def __init__(self):
        """Initialize the recommendation engine."""
        self.data_processor = None  # Initialize lazily when needed
        self.lead_scorer = None
        self.churn_predictor = None
        self.segmentation = None
        self.recommendation_rules = {}
        self._initialize_rules()
    
    def _get_data_processor(self):
        """Get data processor, initializing if needed."""
        if self.data_processor is None:
            self.data_processor = HubSpotDataProcessor()
        return self.data_processor
    
    def _initialize_rules(self):
        """Initialize recommendation rules and strategies."""
        self.recommendation_rules = {
            'lead_nurturing': {
                'conditions': {
                    'lead_score': {'min': 0.3, 'max': 0.7},
                    'lifecycle_stage': ['subscriber', 'lead', 'marketingqualifiedlead'],
                    'days_since_created': {'min': 7, 'max': 90}
                },
                'actions': [
                    'Send educational content series',
                    'Invite to webinar or demo',
                    'Offer free consultation',
                    'Share case studies',
                    'Schedule discovery call'
                ],
                'priority': 'medium'
            },
            'sales_acceleration': {
                'conditions': {
                    'lead_score': {'min': 0.7},
                    'lifecycle_stage': ['marketingqualifiedlead', 'salesqualifiedlead'],
                    'engagement_score': {'min': 0.6}
                },
                'actions': [
                    'Schedule immediate sales call',
                    'Send personalized proposal',
                    'Arrange product demonstration',
                    'Connect with decision maker',
                    'Provide pricing information'
                ],
                'priority': 'high'
            },
            'churn_prevention': {
                'conditions': {
                    'churn_risk': {'min': 0.6},
                    'days_since_last_activity': {'min': 30}
                },
                'actions': [
                    'Immediate account manager outreach',
                    'Schedule retention call',
                    'Offer loyalty incentives',
                    'Provide additional training',
                    'Escalate to management'
                ],
                'priority': 'critical'
            },
            'customer_expansion': {
                'conditions': {
                    'lifecycle_stage': ['customer'],
                    'total_deals': {'min': 1},
                    'churn_risk': {'max': 0.3}
                },
                'actions': [
                    'Identify upsell opportunities',
                    'Introduce new products/features',
                    'Schedule expansion discussion',
                    'Offer volume discounts',
                    'Connect with other departments'
                ],
                'priority': 'medium'
            },
            're_engagement': {
                'conditions': {
                    'days_since_last_activity': {'min': 60},
                    'churn_risk': {'min': 0.4, 'max': 0.7}
                },
                'actions': [
                    'Send re-engagement email series',
                    'Offer special promotion',
                    'Update contact information',
                    'Share company updates',
                    'Request feedback survey'
                ],
                'priority': 'medium'
            },
            'data_enrichment': {
                'conditions': {
                    'completeness_score': {'max': 0.5},
                    'engagement_score': {'min': 0.3}
                },
                'actions': [
                    'Request missing information',
                    'Send profile completion form',
                    'Offer incentive for data update',
                    'Use progressive profiling',
                    'Enrich via third-party sources'
                ],
                'priority': 'low'
            }
        }
    
    def load_models(self, 
                   lead_scoring_model: Optional[LeadScoringModel] = None,
                   churn_predictor: Optional[ChurnPredictor] = None,
                   segmentation: Optional[CustomerSegmentation] = None):
        """
        Load trained models for recommendations.
        
        Args:
            lead_scoring_model: Trained lead scoring model
            churn_predictor: Trained churn prediction model
            segmentation: Trained segmentation model
        """
        self.lead_scorer = lead_scoring_model
        self.churn_predictor = churn_predictor
        self.segmentation = segmentation
        
        logger.info("Models loaded into recommendation engine")
    
    def prepare_recommendation_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare data with all necessary scores and predictions.
        
        Args:
            df: Input customer data
            
        Returns:
            DataFrame with enriched data for recommendations
        """
        logger.info("Preparing data for recommendations...")
        
        # Start with engineered features
        enriched_df = self.data_processor.engineer_features(df.copy())
        
        # Add lead scores if model available
        if self.lead_scorer and self.lead_scorer.is_trained:
            try:
                scored_df = self.lead_scorer.score_leads(enriched_df)
                enriched_df['LEAD_SCORE'] = scored_df['LEAD_SCORE']
                enriched_df['SCORE_CATEGORY'] = scored_df['SCORE_CATEGORY']
            except Exception as e:
                logger.warning(f"Could not generate lead scores: {e}")
                enriched_df['LEAD_SCORE'] = 0.5
                enriched_df['SCORE_CATEGORY'] = 'Medium'
        
        # Add churn risk if model available
        if self.churn_predictor and self.churn_predictor.is_trained:
            try:
                churn_df = self.churn_predictor.predict_churn_risk(enriched_df)
                enriched_df['CHURN_RISK_SCORE'] = churn_df['CHURN_RISK_SCORE']
                enriched_df['CHURN_RISK_CATEGORY'] = churn_df['CHURN_RISK_CATEGORY']
            except Exception as e:
                logger.warning(f"Could not generate churn predictions: {e}")
                enriched_df['CHURN_RISK_SCORE'] = 0.3
                enriched_df['CHURN_RISK_CATEGORY'] = 'Low'
        
        # Add segmentation if model available
        if self.segmentation and self.segmentation.model:
            try:
                segment_df = self.segmentation.predict_segment(enriched_df)
                enriched_df['PREDICTED_SEGMENT'] = segment_df['PREDICTED_SEGMENT']
            except Exception as e:
                logger.warning(f"Could not generate segments: {e}")
                enriched_df['PREDICTED_SEGMENT'] = 'Segment_0'
        
        logger.info(f"Prepared {len(enriched_df)} records for recommendations")
        
        return enriched_df
    
    def generate_recommendations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate personalized recommendations for each customer.
        
        Args:
            df: Customer data with scores and predictions
            
        Returns:
            DataFrame with recommendations
        """
        logger.info("Generating personalized recommendations...")
        
        # Prepare data if not already done
        if 'LEAD_SCORE' not in df.columns:
            df = self.prepare_recommendation_data(df)
        
        recommendations = []
        
        for idx, row in df.iterrows():
            customer_recommendations = self._generate_customer_recommendations(row)
            
            # Add customer identifier
            customer_recommendations['CUSTOMER_ID'] = row.get('ID', idx)
            customer_recommendations['EMAIL'] = row.get('EMAIL', '')
            customer_recommendations['COMPANY'] = row.get('COMPANY', '')
            
            recommendations.append(customer_recommendations)
        
        # Convert to DataFrame
        recommendations_df = pd.DataFrame(recommendations)
        
        logger.info(f"Generated recommendations for {len(recommendations_df)} customers")
        
        return recommendations_df
    
    def _generate_customer_recommendations(self, customer_row: pd.Series) -> Dict[str, Any]:
        """Generate recommendations for a single customer."""
        customer_data = {
            'lead_score': customer_row.get('LEAD_SCORE', 0.5),
            'churn_risk': customer_row.get('CHURN_RISK_SCORE', 0.3),
            'lifecycle_stage': customer_row.get('LIFECYCLESTAGE', '').lower(),
            'days_since_created': customer_row.get('DAYS_SINCE_CREATED', 0),
            'days_since_last_activity': customer_row.get('DAYS_SINCE_LAST_ACTIVITY', 0),
            'engagement_score': customer_row.get('ENGAGEMENT_SCORE', 0.5),
            'completeness_score': customer_row.get('COMPLETENESS_SCORE', 0.5),
            'total_deals': customer_row.get('TOTAL_DEALS', 0),
            'segment': customer_row.get('PREDICTED_SEGMENT', 'Unknown')
        }
        
        # Evaluate each rule category
        applicable_rules = []
        
        for rule_name, rule_config in self.recommendation_rules.items():
            if self._evaluate_rule_conditions(customer_data, rule_config['conditions']):
                applicable_rules.append({
                    'rule_name': rule_name,
                    'priority': rule_config['priority'],
                    'actions': rule_config['actions']
                })
        
        # Sort by priority
        priority_order = {'critical': 1, 'high': 2, 'medium': 3, 'low': 4}
        applicable_rules.sort(key=lambda x: priority_order.get(x['priority'], 5))
        
        # Select top recommendations
        if applicable_rules:
            primary_rule = applicable_rules[0]
            recommended_actions = primary_rule['actions'][:3]  # Top 3 actions
            priority = primary_rule['priority']
            strategy = primary_rule['rule_name']
        else:
            # Default recommendations
            recommended_actions = ['Monitor customer activity', 'Maintain regular communication']
            priority = 'low'
            strategy = 'maintenance'
        
        # Add context-specific recommendations
        contextual_actions = self._get_contextual_recommendations(customer_data)
        
        return {
            'PRIMARY_STRATEGY': strategy,
            'PRIORITY': priority,
            'RECOMMENDED_ACTIONS': recommended_actions,
            'CONTEXTUAL_ACTIONS': contextual_actions,
            'APPLICABLE_RULES': [rule['rule_name'] for rule in applicable_rules],
            'CUSTOMER_PROFILE': customer_data
        }
    
    def _evaluate_rule_conditions(self, customer_data: Dict, conditions: Dict) -> bool:
        """Evaluate if customer meets rule conditions."""
        for field, criteria in conditions.items():
            customer_value = customer_data.get(field)
            
            if customer_value is None:
                continue
            
            if isinstance(criteria, dict):
                # Numeric range conditions
                if 'min' in criteria and customer_value < criteria['min']:
                    return False
                if 'max' in criteria and customer_value > criteria['max']:
                    return False
            elif isinstance(criteria, list):
                # List membership conditions
                if customer_value not in criteria:
                    return False
            else:
                # Exact match conditions
                if customer_value != criteria:
                    return False
        
        return True
    
    def _get_contextual_recommendations(self, customer_data: Dict) -> List[str]:
        """Get additional contextual recommendations."""
        contextual_actions = []
        
        # High-value customer actions
        if customer_data.get('total_deals', 0) > 2:
            contextual_actions.append('Assign dedicated account manager')
        
        # New customer onboarding
        if customer_data.get('days_since_created', 0) < 30:
            contextual_actions.append('Include in new customer onboarding sequence')
        
        # Engagement-based actions
        if customer_data.get('engagement_score', 0) > 0.8:
            contextual_actions.append('Consider for customer advocacy program')
        elif customer_data.get('engagement_score', 0) < 0.3:
            contextual_actions.append('Focus on engagement improvement')
        
        # Data quality actions
        if customer_data.get('completeness_score', 0) < 0.4:
            contextual_actions.append('Prioritize data collection and enrichment')
        
        # Segment-specific actions
        segment = customer_data.get('segment', '')
        if 'enterprise' in segment.lower():
            contextual_actions.append('Offer enterprise-level support and features')
        elif 'small' in segment.lower():
            contextual_actions.append('Provide SMB-focused resources and pricing')
        
        return contextual_actions
    
    def prioritize_customers(self, recommendations_df: pd.DataFrame) -> pd.DataFrame:
        """
        Prioritize customers based on recommendations and business impact.
        
        Args:
            recommendations_df: DataFrame with recommendations
            
        Returns:
            DataFrame sorted by priority
        """
        logger.info("Prioritizing customers for action...")
        
        # Create priority score
        priority_weights = {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}
        recommendations_df['PRIORITY_SCORE'] = recommendations_df['PRIORITY'].map(priority_weights)
        
        # Add business impact factors
        if 'LEAD_SCORE' in recommendations_df.columns:
            recommendations_df['PRIORITY_SCORE'] += recommendations_df['LEAD_SCORE'] * 2
        
        if 'CHURN_RISK_SCORE' in recommendations_df.columns:
            recommendations_df['PRIORITY_SCORE'] += recommendations_df['CHURN_RISK_SCORE'] * 3
        
        # Sort by priority score
        prioritized_df = recommendations_df.sort_values('PRIORITY_SCORE', ascending=False)
        
        # Add rank
        prioritized_df['ACTION_RANK'] = range(1, len(prioritized_df) + 1)
        
        logger.info("Customer prioritization completed")
        
        return prioritized_df
    
    def get_campaign_recommendations(self, recommendations_df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate campaign-level recommendations based on customer insights.
        
        Args:
            recommendations_df: DataFrame with customer recommendations
            
        Returns:
            Dictionary with campaign recommendations
        """
        logger.info("Generating campaign recommendations...")
        
        campaign_recommendations = {
            'immediate_actions': [],
            'weekly_campaigns': [],
            'monthly_initiatives': [],
            'strategic_programs': []
        }
        
        # Immediate actions (critical priority)
        critical_customers = recommendations_df[recommendations_df['PRIORITY'] == 'critical']
        if len(critical_customers) > 0:
            campaign_recommendations['immediate_actions'] = [
                f"Urgent outreach to {len(critical_customers)} critical customers",
                "Activate retention team for high-risk accounts",
                "Schedule emergency account reviews"
            ]
        
        # Weekly campaigns
        high_priority = recommendations_df[recommendations_df['PRIORITY'] == 'high']
        if len(high_priority) > 0:
            campaign_recommendations['weekly_campaigns'] = [
                f"Sales acceleration campaign for {len(high_priority)} high-priority leads",
                "Personalized outreach sequence",
                "Product demonstration scheduling"
            ]
        
        # Monthly initiatives
        medium_priority = recommendations_df[recommendations_df['PRIORITY'] == 'medium']
        if len(medium_priority) > 0:
            campaign_recommendations['monthly_initiatives'] = [
                f"Nurturing campaign for {len(medium_priority)} medium-priority prospects",
                "Content marketing initiatives",
                "Customer expansion programs"
            ]
        
        # Strategic programs
        strategy_counts = recommendations_df['PRIMARY_STRATEGY'].value_counts()
        top_strategies = strategy_counts.head(3)
        
        for strategy, count in top_strategies.items():
            if count > 10:  # Only recommend if significant volume
                campaign_recommendations['strategic_programs'].append(
                    f"Develop {strategy} program for {count} customers"
                )
        
        return campaign_recommendations
    
    def generate_action_plan(self, recommendations_df: pd.DataFrame, days_ahead: int = 30) -> Dict[str, Any]:
        """
        Generate a detailed action plan for the next period.
        
        Args:
            recommendations_df: DataFrame with recommendations
            days_ahead: Number of days to plan ahead
            
        Returns:
            Dictionary with detailed action plan
        """
        logger.info(f"Generating {days_ahead}-day action plan...")
        
        # Prioritize customers
        prioritized_df = self.prioritize_customers(recommendations_df)
        
        # Calculate daily capacity (simplified)
        daily_capacity = max(10, len(prioritized_df) // days_ahead)
        
        action_plan = {
            'summary': {
                'total_customers': len(prioritized_df),
                'planning_period_days': days_ahead,
                'daily_capacity': daily_capacity,
                'total_actions': len(prioritized_df)
            },
            'daily_schedule': {},
            'resource_requirements': {},
            'success_metrics': {}
        }
        
        # Create daily schedule
        current_date = datetime.now()
        customer_idx = 0
        
        for day in range(days_ahead):
            schedule_date = current_date + timedelta(days=day)
            date_str = schedule_date.strftime('%Y-%m-%d')
            
            daily_customers = prioritized_df.iloc[customer_idx:customer_idx + daily_capacity]
            
            if len(daily_customers) > 0:
                action_plan['daily_schedule'][date_str] = {
                    'customers_to_contact': len(daily_customers),
                    'priority_breakdown': daily_customers['PRIORITY'].value_counts().to_dict(),
                    'top_actions': daily_customers['RECOMMENDED_ACTIONS'].explode().value_counts().head(5).to_dict()
                }
                
                customer_idx += daily_capacity
            
            if customer_idx >= len(prioritized_df):
                break
        
        # Resource requirements
        action_plan['resource_requirements'] = {
            'sales_team': len(prioritized_df[prioritized_df['PRIMARY_STRATEGY'].isin(['sales_acceleration', 'customer_expansion'])]),
            'marketing_team': len(prioritized_df[prioritized_df['PRIMARY_STRATEGY'].isin(['lead_nurturing', 're_engagement'])]),
            'customer_success': len(prioritized_df[prioritized_df['PRIMARY_STRATEGY'] == 'churn_prevention']),
            'data_team': len(prioritized_df[prioritized_df['PRIMARY_STRATEGY'] == 'data_enrichment'])
        }
        
        # Success metrics
        action_plan['success_metrics'] = {
            'target_conversions': len(prioritized_df[prioritized_df['PRIORITY'].isin(['high', 'critical'])]) * 0.2,
            'churn_prevention_target': len(prioritized_df[prioritized_df['PRIMARY_STRATEGY'] == 'churn_prevention']) * 0.7,
            'engagement_improvement_target': len(prioritized_df[prioritized_df['PRIMARY_STRATEGY'] == 're_engagement']) * 0.4
        }
        
        return action_plan
    
    def generate_recommendations_report(self, recommendations_df: pd.DataFrame) -> str:
        """
        Generate a comprehensive recommendations report.
        
        Args:
            recommendations_df: DataFrame with recommendations
            
        Returns:
            Formatted text report
        """
        prioritized_df = self.prioritize_customers(recommendations_df)
        campaign_recs = self.get_campaign_recommendations(recommendations_df)
        action_plan = self.generate_action_plan(recommendations_df)
        
        report = f"""
CUSTOMER ACTION RECOMMENDATIONS REPORT
=====================================
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

EXECUTIVE SUMMARY
-----------------
Total Customers Analyzed: {len(recommendations_df):,}
Customers Requiring Action: {len(recommendations_df[recommendations_df['PRIORITY'] != 'low']):,}
Critical Priority: {len(recommendations_df[recommendations_df['PRIORITY'] == 'critical']):,}
High Priority: {len(recommendations_df[recommendations_df['PRIORITY'] == 'high']):,}

PRIORITY BREAKDOWN
------------------
"""
        
        priority_counts = recommendations_df['PRIORITY'].value_counts()
        for priority, count in priority_counts.items():
            percentage = (count / len(recommendations_df)) * 100
            report += f"{priority.title()}: {count:,} ({percentage:.1f}%)\n"
        
        report += "\nTOP RECOMMENDED STRATEGIES\n"
        report += "--------------------------\n"
        strategy_counts = recommendations_df['PRIMARY_STRATEGY'].value_counts().head(5)
        for strategy, count in strategy_counts.items():
            report += f"• {strategy.replace('_', ' ').title()}: {count:,} customers\n"
        
        report += "\nIMMEDIATE ACTIONS REQUIRED\n"
        report += "--------------------------\n"
        for action in campaign_recs['immediate_actions']:
            report += f"• {action}\n"
        
        report += "\nTOP 10 PRIORITY CUSTOMERS\n"
        report += "-------------------------\n"
        top_customers = prioritized_df.head(10)
        for _, customer in top_customers.iterrows():
            report += f"• {customer.get('EMAIL', 'N/A')} - {customer['PRIMARY_STRATEGY']} ({customer['PRIORITY']})\n"
        
        report += f"\nRESOURCE ALLOCATION\n"
        report += "-------------------\n"
        for team, count in action_plan['resource_requirements'].items():
            report += f"{team.replace('_', ' ').title()}: {count} customers\n"
        
        return report
