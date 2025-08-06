"""
Churn Prediction Module

This module provides churn prediction capabilities to identify customers
at risk of churning or becoming inactive.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime, timedelta
import joblib

from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, roc_auc_score, precision_recall_curve
from sklearn.model_selection import cross_val_score
import matplotlib.pyplot as plt
import seaborn as sns

from .data_processor import HubSpotDataProcessor
from .birddog_qualification_engine import BirdDogQualificationEngine, enhance_dataframe_with_birddog_features

logger = logging.getLogger(__name__)


class ChurnPredictor:
    """
    Churn prediction model for identifying at-risk customers.
    
    Features:
    - Multiple ML algorithms for churn prediction
    - Risk scoring and categorization
    - Early warning system
    - Retention recommendations
    - Customer health scoring
    """
    
    def __init__(self, model_type: str = 'random_forest'):
        """
        Initialize the churn prediction module.
        
        Args:
            model_type: Type of model ('random_forest', 'gradient_boosting', 'logistic_regression')
        """
        self.model_type = model_type
        self.model = None
        self.data_processor = None  # Initialize lazily when needed
        self.feature_importance = {}
        self.model_metrics = {}
        self.is_trained = False
        self.churn_threshold_days = 90  # Days of inactivity to consider churn
        
        # Initialize model
        self._initialize_model()
    
    def _get_data_processor(self):
        """Get data processor, initializing if needed."""
        if self.data_processor is None:
            self.data_processor = HubSpotDataProcessor()
        return self.data_processor
    
    def _initialize_model(self):
        """Initialize the ML model based on model_type."""
        if self.model_type == 'random_forest':
            self.model = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=42,
                class_weight='balanced'
            )
        elif self.model_type == 'gradient_boosting':
            self.model = GradientBoostingClassifier(
                n_estimators=100,
                learning_rate=0.1,
                max_depth=6,
                min_samples_split=5,
                min_samples_leaf=2,
                random_state=42
            )
        elif self.model_type == 'logistic_regression':
            self.model = LogisticRegression(
                random_state=42,
                max_iter=1000,
                class_weight='balanced'
            )
        else:
            raise ValueError(f"Unsupported model type: {self.model_type}")
    
    def prepare_churn_data(self, days_back: int = 730) -> pd.DataFrame:
        """
        Prepare data for churn prediction.
        
        Args:
            days_back: Number of days to look back for data
            
        Returns:
            Prepared DataFrame with churn features
        """
        logger.info("Preparing data for churn prediction...")
        
        # Extract comprehensive customer data
        data_processor = self._get_data_processor()
        df = data_processor.extract_lead_data(
            days_back=days_back,
            include_deals=True,
            include_companies=True
        )
        
        if df.empty:
            logger.warning("No data available for churn prediction")
            return pd.DataFrame()
        
        # Engineer features
        df = data_processor.engineer_features(df)
        
        # Add BirdDog-specific qualification features
        logger.info("Adding BirdDog qualification features for churn prediction...")
        df = enhance_dataframe_with_birddog_features(df)
        
        # Create churn-specific features
        df = self._create_churn_features(df)
        
        # Define churn target
        df = self._define_churn_target(df)
        
        logger.info(f"Prepared {len(df)} records for churn prediction")
        
        return df
    
    def _create_churn_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create features specifically for churn prediction."""
        df = df.copy()
        
        # Activity recency features
        current_date = datetime.now()
        
        if 'LASTMODIFIEDDATE' in df.columns:
            df['DAYS_SINCE_LAST_ACTIVITY'] = (
                current_date - pd.to_datetime(df['LASTMODIFIEDDATE'])
            ).dt.days
        else:
            df['DAYS_SINCE_LAST_ACTIVITY'] = df.get('DAYS_SINCE_MODIFIED', 0)
        
        # Engagement decline indicators
        if 'DAYS_SINCE_CREATED' in df.columns and 'DAYS_SINCE_MODIFIED' in df.columns:
            # Activity frequency (lower = less active)
            df['ACTIVITY_FREQUENCY'] = df['DAYS_SINCE_CREATED'] / (df['DAYS_SINCE_MODIFIED'] + 1)
        else:
            df['ACTIVITY_FREQUENCY'] = 1
        
        # Deal activity trends
        if 'TOTAL_DEALS' in df.columns:
            # Recent deal activity (simplified - in real scenario, you'd look at time-based deal creation)
            df['HAS_RECENT_DEALS'] = (df['TOTAL_DEALS'] > 0).astype(int)
            df['DEAL_MOMENTUM'] = df['TOTAL_DEALS'] / (df['DAYS_SINCE_CREATED'] + 1) * 365  # Deals per year
        else:
            df['HAS_RECENT_DEALS'] = 0
            df['DEAL_MOMENTUM'] = 0
        
        # Lifecycle stage progression risk
        if 'LIFECYCLE_STAGE_ORDER' in df.columns:
            # Customers in early stages might be at higher churn risk
            df['EARLY_STAGE_RISK'] = (df['LIFECYCLE_STAGE_ORDER'] <= 2).astype(int)
        else:
            df['EARLY_STAGE_RISK'] = 0
        
        # Communication responsiveness
        if 'HAS_EMAIL' in df.columns and 'HAS_PHONE' in df.columns:
            df['COMMUNICATION_CHANNELS'] = df['HAS_EMAIL'].astype(int) + df['HAS_PHONE'].astype(int)
        else:
            df['COMMUNICATION_CHANNELS'] = 1
        
        # Company stability indicators
        if 'COMPANY_SIZE' in df.columns:
            # Smaller companies might have higher churn risk
            company_size_filled = df['COMPANY_SIZE'].fillna(0)
            unique_values = company_size_filled.nunique()
            
            if unique_values > 5:
                try:
                    df['COMPANY_STABILITY'] = pd.qcut(
                        company_size_filled,
                        q=5,
                        labels=[1, 2, 3, 4, 5],
                        duplicates='drop'
                    ).astype(float)
                except ValueError:
                    # Fallback to cut if qcut fails
                    df['COMPANY_STABILITY'] = pd.cut(
                        company_size_filled,
                        bins=5,
                        labels=[1, 2, 3, 4, 5],
                        duplicates='drop'
                    ).astype(float)
            else:
                # Simple categorization for limited unique values
                df['COMPANY_STABILITY'] = (company_size_filled > company_size_filled.median()).astype(int) * 2 + 3
        else:
            df['COMPANY_STABILITY'] = 3  # Neutral
        
        # Engagement quality score
        engagement_factors = []
        if 'COMPLETENESS_SCORE' in df.columns:
            engagement_factors.append('COMPLETENESS_SCORE')
        if 'IS_BUSINESS_EMAIL' in df.columns:
            engagement_factors.append('IS_BUSINESS_EMAIL')
        
        if engagement_factors:
            df['ENGAGEMENT_QUALITY'] = df[engagement_factors].mean(axis=1)
        else:
            df['ENGAGEMENT_QUALITY'] = 0.5
        
        # Time-based risk factors
        df['ACCOUNT_AGE_MONTHS'] = df['DAYS_SINCE_CREATED'] / 30
        
        # Risk score based on inactivity
        df['INACTIVITY_RISK'] = np.where(
            df['DAYS_SINCE_LAST_ACTIVITY'] > self.churn_threshold_days,
            1,
            df['DAYS_SINCE_LAST_ACTIVITY'] / self.churn_threshold_days
        )
        
        return df
    
    def _define_churn_target(self, df: pd.DataFrame) -> pd.DataFrame:
        """Define the churn target variable."""
        df = df.copy()
        
        # Define churn based on inactivity
        df['CHURNED'] = (df['DAYS_SINCE_LAST_ACTIVITY'] > self.churn_threshold_days).astype(int)
        
        # Additional churn indicators
        churn_indicators = []
        
        # No recent deals (if deal data available)
        if 'TOTAL_DEALS' in df.columns:
            df['NO_RECENT_DEALS'] = (df['TOTAL_DEALS'] == 0).astype(int)
            churn_indicators.append('NO_RECENT_DEALS')
        
        # Low engagement
        if 'ENGAGEMENT_QUALITY' in df.columns:
            df['LOW_ENGAGEMENT'] = (df['ENGAGEMENT_QUALITY'] < 0.3).astype(int)
            churn_indicators.append('LOW_ENGAGEMENT')
        
        # Combine indicators (if any additional indicators exist)
        if churn_indicators:
            df['CHURN_INDICATORS'] = df[churn_indicators].sum(axis=1)
            # Update churn definition to include multiple indicators
            df['CHURNED'] = np.where(
                (df['CHURNED'] == 1) | (df['CHURN_INDICATORS'] >= 2),
                1,
                0
            )
        
        return df
    
    def train_churn_model(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Train the churn prediction model.
        
        Args:
            df: DataFrame with churn features and target
            
        Returns:
            Training results and metrics
        """
        logger.info("Training churn prediction model...")
        
        if 'CHURNED' not in df.columns:
            raise ValueError("DataFrame must contain 'CHURNED' target column")
        
        # Select features for training (including BirdDog qualification features)
        feature_columns = [
            'DAYS_SINCE_LAST_ACTIVITY',
            'ACTIVITY_FREQUENCY',
            'HAS_RECENT_DEALS',
            'DEAL_MOMENTUM',
            'EARLY_STAGE_RISK',
            'COMMUNICATION_CHANNELS',
            'COMPANY_STABILITY',
            'ENGAGEMENT_QUALITY',
            'ACCOUNT_AGE_MONTHS',
            'INACTIVITY_RISK',
            'COMPLETENESS_SCORE',
            'DAYS_SINCE_CREATED',
            'TOTAL_DEALS',
            'TOTAL_DEAL_VALUE',
            # BirdDog-specific churn risk factors
            'birddog_score',
            'estimated_tax_savings',
            'service_revenue_potential',
            'confidence_score'
        ]
        
        # Filter to available features
        available_features = [col for col in feature_columns if col in df.columns]
        
        if len(available_features) < 3:
            raise ValueError("Insufficient features for churn prediction")
        
        # Prepare training data
        X = df[available_features].fillna(0)
        y = df['CHURNED']
        
        # Check class balance
        churn_rate = y.mean()
        logger.info(f"Churn rate: {churn_rate:.2%}")
        
        if churn_rate == 0 or churn_rate == 1:
            raise ValueError("No variation in churn target - cannot train model")
        
        # Train model
        self.model.fit(X, y)
        self.is_trained = True
        
        # Calculate feature importance
        if hasattr(self.model, 'feature_importances_'):
            self.feature_importance = dict(zip(available_features, self.model.feature_importances_))
            self.feature_importance = dict(
                sorted(self.feature_importance.items(), key=lambda x: x[1], reverse=True)
            )
        
        # Cross-validation
        cv_scores = cross_val_score(self.model, X, y, cv=5, scoring='roc_auc')
        
        # Training metrics
        y_pred_proba = self.model.predict_proba(X)[:, 1]
        train_auc = roc_auc_score(y, y_pred_proba)
        
        results = {
            'model_type': self.model_type,
            'features_used': available_features,
            'training_samples': len(X),
            'churn_rate': churn_rate,
            'cv_auc_mean': cv_scores.mean(),
            'cv_auc_std': cv_scores.std(),
            'train_auc': train_auc,
            'feature_importance': self.feature_importance
        }
        
        self.model_metrics = results
        logger.info(f"Churn model trained. CV AUC: {cv_scores.mean():.3f}")
        
        return results
    
    def predict_churn_risk(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Predict churn risk for customers.
        
        Args:
            df: DataFrame with customer data
            
        Returns:
            DataFrame with churn risk predictions
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before prediction")
        
        logger.info(f"Predicting churn risk for {len(df)} customers...")
        
        # Prepare features
        data_processor = self._get_data_processor()
        df_processed = data_processor.engineer_features(df.copy())
        
        # Add BirdDog qualification features
        df_processed = enhance_dataframe_with_birddog_features(df_processed)
        
        df_processed = self._create_churn_features(df_processed)
        
        # Get feature columns used in training
        feature_columns = list(self.feature_importance.keys())
        
        # Prepare feature matrix
        X = df_processed[feature_columns].fillna(0)
        
        # Predict churn probabilities
        churn_probabilities = self.model.predict_proba(X)[:, 1]
        
        # Create results DataFrame
        results = df.copy()
        results['CHURN_RISK_SCORE'] = churn_probabilities
        
        # Categorize risk levels
        results['CHURN_RISK_CATEGORY'] = pd.cut(
            churn_probabilities,
            bins=[0, 0.3, 0.6, 0.8, 1.0],
            labels=['Low', 'Medium', 'High', 'Critical'],
            duplicates='drop'
        )
        
        # Risk percentiles
        unique_probs = len(np.unique(churn_probabilities))
        if unique_probs >= 10:
            try:
                results['CHURN_RISK_PERCENTILE'] = pd.qcut(
                    churn_probabilities,
                    q=10,
                    labels=range(1, 11),
                    duplicates='drop'
                ).astype(int)
            except ValueError:
                # Fallback to simpler percentile calculation
                results['CHURN_RISK_PERCENTILE'] = pd.qcut(
                    churn_probabilities,
                    q=min(unique_probs, 5),
                    labels=range(1, min(unique_probs, 5) + 1),
                    duplicates='drop'
                ).astype(int)
        else:
            # Simple ranking for limited unique values
            results['CHURN_RISK_PERCENTILE'] = pd.qcut(
                churn_probabilities,
                q=min(unique_probs, 3),
                labels=range(1, min(unique_probs, 3) + 1),
                duplicates='drop'
            ).astype(int)
        
        # Add risk flags
        results['HIGH_CHURN_RISK'] = (churn_probabilities > 0.7).astype(int)
        results['IMMEDIATE_ATTENTION'] = (churn_probabilities > 0.8).astype(int)
        
        logger.info("Churn risk prediction completed")
        
        return results.sort_values('CHURN_RISK_SCORE', ascending=False)
    
    def identify_at_risk_customers(self, 
                                  df: pd.DataFrame,
                                  risk_threshold: float = 0.6,
                                  top_n: Optional[int] = None) -> pd.DataFrame:
        """
        Identify customers at high risk of churning.
        
        Args:
            df: DataFrame with churn predictions
            risk_threshold: Minimum risk score threshold
            top_n: Number of top at-risk customers to return
            
        Returns:
            DataFrame with at-risk customers
        """
        if 'CHURN_RISK_SCORE' not in df.columns:
            df = self.predict_churn_risk(df)
        
        # Filter by risk threshold
        at_risk = df[df['CHURN_RISK_SCORE'] >= risk_threshold].copy()
        
        # Sort by risk score
        at_risk = at_risk.sort_values('CHURN_RISK_SCORE', ascending=False)
        
        # Limit to top N if specified
        if top_n:
            at_risk = at_risk.head(top_n)
        
        logger.info(f"Identified {len(at_risk)} at-risk customers (threshold: {risk_threshold})")
        
        return at_risk
    
    def generate_retention_recommendations(self, df: pd.DataFrame) -> Dict[str, List[str]]:
        """
        Generate retention recommendations based on churn risk factors.
        
        Args:
            df: DataFrame with churn predictions and features
            
        Returns:
            Dictionary with recommendations by risk category
        """
        recommendations = {
            'Critical': [],
            'High': [],
            'Medium': [],
            'Low': []
        }
        
        # Analyze feature importance for recommendations
        if self.feature_importance:
            top_risk_factors = list(self.feature_importance.keys())[:5]
            
            # Critical risk recommendations
            recommendations['Critical'] = [
                "Immediate personal outreach by account manager",
                "Schedule urgent check-in call within 24 hours",
                "Offer special retention incentives or discounts",
                "Escalate to senior management for intervention",
                "Conduct exit interview to understand concerns"
            ]
            
            # High risk recommendations
            recommendations['High'] = [
                "Proactive outreach within 48 hours",
                "Personalized re-engagement campaign",
                "Offer additional training or support",
                "Review and optimize their current setup",
                "Provide success manager assignment"
            ]
            
            # Medium risk recommendations
            recommendations['Medium'] = [
                "Include in nurturing email campaigns",
                "Send relevant content and resources",
                "Monitor engagement closely",
                "Offer product updates or new features",
                "Schedule quarterly check-in calls"
            ]
            
            # Low risk recommendations
            recommendations['Low'] = [
                "Continue regular communication cadence",
                "Include in customer success programs",
                "Monitor for any changes in behavior",
                "Provide self-service resources",
                "Maintain standard support level"
            ]
            
            # Add specific recommendations based on top risk factors
            if 'DAYS_SINCE_LAST_ACTIVITY' in top_risk_factors:
                for category in ['Critical', 'High']:
                    recommendations[category].append("Focus on re-activating dormant accounts")
            
            if 'DEAL_MOMENTUM' in top_risk_factors:
                for category in ['Critical', 'High', 'Medium']:
                    recommendations[category].append("Identify new sales opportunities")
            
            if 'ENGAGEMENT_QUALITY' in top_risk_factors:
                for category in ['Critical', 'High']:
                    recommendations[category].append("Improve data quality and engagement tracking")
        
        return recommendations
    
    def create_churn_dashboard_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Create data for churn risk dashboard.
        
        Args:
            df: DataFrame with churn predictions
            
        Returns:
            Dictionary with dashboard data
        """
        if 'CHURN_RISK_SCORE' not in df.columns:
            df = self.predict_churn_risk(df)
        
        dashboard_data = {
            'summary': {
                'total_customers': len(df),
                'avg_risk_score': df['CHURN_RISK_SCORE'].mean(),
                'high_risk_count': len(df[df['CHURN_RISK_SCORE'] > 0.7]),
                'critical_risk_count': len(df[df['CHURN_RISK_SCORE'] > 0.8])
            },
            'risk_distribution': df['CHURN_RISK_CATEGORY'].value_counts().to_dict(),
            'top_at_risk': df.nlargest(10, 'CHURN_RISK_SCORE')[
                ['ID', 'EMAIL', 'COMPANY', 'CHURN_RISK_SCORE', 'CHURN_RISK_CATEGORY']
            ].to_dict('records'),
            'risk_factors': self.feature_importance,
            'trends': {}
        }
        
        # Add trend analysis if date columns available
        if 'CREATEDATE' in df.columns:
            df['CREATE_MONTH'] = pd.to_datetime(df['CREATEDATE']).dt.to_period('M')
            monthly_risk = df.groupby('CREATE_MONTH')['CHURN_RISK_SCORE'].mean()
            dashboard_data['trends']['monthly_avg_risk'] = monthly_risk.to_dict()
        
        return dashboard_data
    
    def generate_churn_report(self, df: pd.DataFrame) -> str:
        """
        Generate a comprehensive churn analysis report.
        
        Args:
            df: DataFrame with churn predictions
            
        Returns:
            Formatted text report
        """
        if 'CHURN_RISK_SCORE' not in df.columns:
            df = self.predict_churn_risk(df)
        
        dashboard_data = self.create_churn_dashboard_data(df)
        recommendations = self.generate_retention_recommendations(df)
        
        report = f"""
CHURN RISK ANALYSIS REPORT
=========================
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Model: {self.model_type}

EXECUTIVE SUMMARY
-----------------
Total Customers Analyzed: {dashboard_data['summary']['total_customers']:,}
Average Risk Score: {dashboard_data['summary']['avg_risk_score']:.3f}
High Risk Customers (>0.7): {dashboard_data['summary']['high_risk_count']:,}
Critical Risk Customers (>0.8): {dashboard_data['summary']['critical_risk_count']:,}

RISK DISTRIBUTION
-----------------
"""
        
        for category, count in dashboard_data['risk_distribution'].items():
            percentage = (count / dashboard_data['summary']['total_customers']) * 100
            report += f"{category}: {count:,} ({percentage:.1f}%)\n"
        
        report += "\nTOP AT-RISK CUSTOMERS\n"
        report += "---------------------\n"
        for customer in dashboard_data['top_at_risk'][:5]:
            report += f"• {customer.get('EMAIL', 'N/A')} ({customer.get('COMPANY', 'N/A')}) - Risk: {customer['CHURN_RISK_SCORE']:.3f}\n"
        
        report += "\nTOP RISK FACTORS\n"
        report += "----------------\n"
        for factor, importance in list(dashboard_data['risk_factors'].items())[:5]:
            report += f"• {factor}: {importance:.3f}\n"
        
        report += "\nRETENTION RECOMMENDATIONS\n"
        report += "-------------------------\n"
        for risk_level, actions in recommendations.items():
            if actions:
                report += f"\n{risk_level} Risk:\n"
                for action in actions[:3]:  # Top 3 recommendations
                    report += f"  • {action}\n"
        
        return report
    
    def save_model(self, filepath: str):
        """Save the trained churn model."""
        if not self.is_trained:
            raise ValueError("Cannot save untrained model")
        
        model_data = {
            'model': self.model,
            'model_type': self.model_type,
            'feature_importance': self.feature_importance,
            'model_metrics': self.model_metrics,
            'is_trained': self.is_trained,
            'churn_threshold_days': self.churn_threshold_days,
            'saved_at': datetime.now()
        }
        
        joblib.dump(model_data, filepath)
        logger.info(f"Churn model saved to {filepath}")
    
    def load_model(self, filepath: str):
        """Load a trained churn model."""
        model_data = joblib.load(filepath)
        
        self.model = model_data['model']
        self.model_type = model_data['model_type']
        self.feature_importance = model_data['feature_importance']
        self.model_metrics = model_data['model_metrics']
        self.is_trained = model_data['is_trained']
        self.churn_threshold_days = model_data.get('churn_threshold_days', 90)
        
        logger.info(f"Churn model loaded from {filepath}")
