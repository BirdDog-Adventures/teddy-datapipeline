"""
Lead Scoring Model

This module implements machine learning models for scoring and prioritizing leads
based on their likelihood to convert.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
import joblib
from datetime import datetime
import os

from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, roc_auc_score, precision_recall_curve
from sklearn.model_selection import cross_val_score, GridSearchCV
import matplotlib.pyplot as plt
import seaborn as sns

from .data_processor import HubSpotDataProcessor
from .birddog_qualification_engine import BirdDogQualificationEngine, enhance_dataframe_with_birddog_features

logger = logging.getLogger(__name__)


class LeadScoringModel:
    """
    Machine learning model for lead scoring and prioritization.
    
    Features:
    - Multiple ML algorithms (Random Forest, Gradient Boosting, Logistic Regression)
    - Automated feature selection
    - Model evaluation and comparison
    - Lead scoring and ranking
    - Feature importance analysis
    """
    
    def __init__(self, model_type: str = 'random_forest'):
        """
        Initialize the lead scoring model.
        
        Args:
            model_type: Type of model ('random_forest', 'gradient_boosting', 'logistic_regression')
        """
        self.model_type = model_type
        self.model = None
        self.data_processor = None  # Initialize lazily when needed
        self.feature_importance = {}
        self.model_metrics = {}
        self.is_trained = False
        
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
                n_jobs=-1
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
    
    def prepare_training_data(self, 
                            days_back: int = 365,
                            min_samples: int = 100) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
        """
        Prepare training data for the lead scoring model.
        
        Args:
            days_back: Number of days to look back for data
            min_samples: Minimum number of samples required
            
        Returns:
            Tuple of (X_train, X_test, y_train, y_test)
        """
        logger.info("Extracting and preparing training data...")
        
        # Extract lead data
        data_processor = self._get_data_processor()
        df = data_processor.extract_lead_data(
            days_back=days_back,
            include_deals=True,
            include_companies=True
        )
        
        if df.empty:
            raise ValueError("No data available for training")
        
        logger.info(f"Extracted {len(df)} records")
        
        # Engineer features
        df = data_processor.engineer_features(df)
        
        # Add BirdDog-specific qualification features
        logger.info("Adding BirdDog qualification and value features...")
        df = enhance_dataframe_with_birddog_features(df)
        
        # Create conversion target
        df = data_processor.create_conversion_target(df)
        
        # Check if we have enough samples
        if len(df) < min_samples:
            raise ValueError(f"Insufficient data: {len(df)} samples (minimum: {min_samples})")
        
        # Check conversion rate
        conversion_rate = df['CONVERTED'].mean()
        logger.info(f"Conversion rate: {conversion_rate:.2%}")
        
        if conversion_rate == 0 or conversion_rate == 1:
            raise ValueError("No variation in target variable - cannot train model")
        
        # Prepare data for training
        X_train, X_test, y_train, y_test = data_processor.prepare_for_training(
            df, target_column='CONVERTED'
        )
        
        logger.info(f"Training set: {len(X_train)} samples")
        logger.info(f"Test set: {len(X_test)} samples")
        
        return X_train, X_test, y_train, y_test
    
    def train(self, 
              X_train: pd.DataFrame, 
              y_train: pd.Series,
              optimize_hyperparameters: bool = False) -> Dict[str, Any]:
        """
        Train the lead scoring model.
        
        Args:
            X_train: Training features
            y_train: Training target
            optimize_hyperparameters: Whether to perform hyperparameter optimization
            
        Returns:
            Training results and metrics
        """
        logger.info(f"Training {self.model_type} model...")
        
        if optimize_hyperparameters:
            self._optimize_hyperparameters(X_train, y_train)
        
        # Train the model
        self.model.fit(X_train, y_train)
        self.is_trained = True
        
        # Calculate feature importance
        self._calculate_feature_importance(X_train)
        
        # Cross-validation scores
        cv_scores = cross_val_score(self.model, X_train, y_train, cv=5, scoring='roc_auc')
        
        training_results = {
            'model_type': self.model_type,
            'training_samples': len(X_train),
            'features_count': len(X_train.columns),
            'cv_auc_mean': cv_scores.mean(),
            'cv_auc_std': cv_scores.std(),
            'feature_importance': self.feature_importance
        }
        
        logger.info(f"Training completed. CV AUC: {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")
        
        return training_results
    
    def _optimize_hyperparameters(self, X_train: pd.DataFrame, y_train: pd.Series):
        """Optimize model hyperparameters using grid search."""
        logger.info("Optimizing hyperparameters...")
        
        if self.model_type == 'random_forest':
            param_grid = {
                'n_estimators': [50, 100, 200],
                'max_depth': [5, 10, 15],
                'min_samples_split': [2, 5, 10],
                'min_samples_leaf': [1, 2, 4]
            }
        elif self.model_type == 'gradient_boosting':
            param_grid = {
                'n_estimators': [50, 100, 200],
                'learning_rate': [0.05, 0.1, 0.2],
                'max_depth': [3, 6, 9],
                'min_samples_split': [2, 5, 10]
            }
        elif self.model_type == 'logistic_regression':
            param_grid = {
                'C': [0.1, 1, 10, 100],
                'penalty': ['l1', 'l2'],
                'solver': ['liblinear', 'saga']
            }
        
        grid_search = GridSearchCV(
            self.model, param_grid, cv=5, scoring='roc_auc', n_jobs=-1
        )
        grid_search.fit(X_train, y_train)
        
        self.model = grid_search.best_estimator_
        logger.info(f"Best parameters: {grid_search.best_params_}")
        logger.info(f"Best CV score: {grid_search.best_score_:.3f}")
    
    def _calculate_feature_importance(self, X_train: pd.DataFrame):
        """Calculate and store feature importance."""
        if hasattr(self.model, 'feature_importances_'):
            # Tree-based models
            importance_scores = self.model.feature_importances_
        elif hasattr(self.model, 'coef_'):
            # Linear models
            importance_scores = np.abs(self.model.coef_[0])
        else:
            logger.warning("Model does not support feature importance")
            return
        
        self.feature_importance = dict(zip(X_train.columns, importance_scores))
        
        # Sort by importance
        self.feature_importance = dict(
            sorted(self.feature_importance.items(), key=lambda x: x[1], reverse=True)
        )
    
    def evaluate(self, 
                X_test: pd.DataFrame, 
                y_test: pd.Series) -> Dict[str, Any]:
        """
        Evaluate the trained model.
        
        Args:
            X_test: Test features
            y_test: Test target
            
        Returns:
            Evaluation metrics
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before evaluation")
        
        logger.info("Evaluating model...")
        
        # Predictions
        y_pred = self.model.predict(X_test)
        y_pred_proba = self.model.predict_proba(X_test)[:, 1]
        
        # Calculate metrics
        auc_score = roc_auc_score(y_test, y_pred_proba)
        
        # Classification report
        class_report = classification_report(y_test, y_pred, output_dict=True)
        
        # Precision-Recall curve
        precision, recall, _ = precision_recall_curve(y_test, y_pred_proba)
        pr_auc = np.trapz(recall, precision)
        
        self.model_metrics = {
            'auc_score': auc_score,
            'precision': class_report['1']['precision'],
            'recall': class_report['1']['recall'],
            'f1_score': class_report['1']['f1-score'],
            'pr_auc': pr_auc,
            'accuracy': class_report['accuracy'],
            'test_samples': len(X_test)
        }
        
        logger.info(f"Model evaluation completed. AUC: {auc_score:.3f}")
        
        return self.model_metrics
    
    def score_leads(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Score leads using the trained model.
        
        Args:
            df: DataFrame with lead data
            
        Returns:
            DataFrame with lead scores and rankings
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before scoring leads")
        
        logger.info(f"Scoring {len(df)} leads...")
        
        # Get data processor
        data_processor = self._get_data_processor()
        
        # Prepare data (same preprocessing as training)
        df_processed = data_processor.engineer_features(df.copy())
        
        # Add BirdDog-specific qualification features (same as training)
        df_processed = enhance_dataframe_with_birddog_features(df_processed)
        
        # Remove target column if present
        if 'CONVERTED' in df_processed.columns:
            df_processed = df_processed.drop(columns=['CONVERTED'])
        
        # Apply same preprocessing as training data
        X = df_processed.copy()
        
        # Handle categorical variables
        X = data_processor._encode_categorical_features(X)
        
        # Handle missing values
        X = data_processor._handle_missing_values(X)
        
        # Scale features
        X = data_processor._scale_numerical_features(X)
        
        # Ensure we have the same features as training
        missing_features = set(data_processor.feature_columns) - set(X.columns)
        for feature in missing_features:
            X[feature] = 0
        
        # Reorder columns to match training data
        X = X[data_processor.feature_columns]
        
        # Get predictions
        lead_scores = self.model.predict_proba(X)[:, 1]
        
        # Create results DataFrame
        results = df.copy()
        results['LEAD_SCORE'] = lead_scores
        # Lead score percentiles - Simple robust approach
        try:
            # Use percentile-based ranking instead of qcut
            percentiles = np.percentile(lead_scores, [10, 20, 30, 40, 50, 60, 70, 80, 90])
            results['LEAD_SCORE_PERCENTILE'] = pd.cut(
                lead_scores, 
                bins=[-np.inf] + percentiles.tolist() + [np.inf],
                labels=range(1, 11),
                include_lowest=True
            ).astype(int)
        except (ValueError, TypeError):
            # Final fallback: simple ranking based on score ranges
            results['LEAD_SCORE_PERCENTILE'] = pd.cut(
                lead_scores,
                bins=[0, 20, 40, 60, 80, 100],
                labels=[1, 2, 3, 4, 5],
                include_lowest=True
            ).fillna(1).astype(int)
        
        # Rank leads (1 = highest score)
        results['LEAD_RANK'] = results['LEAD_SCORE'].rank(method='dense', ascending=False)
        
        # Add score categories
        results['SCORE_CATEGORY'] = pd.cut(
            lead_scores,
            bins=[0, 0.3, 0.6, 0.8, 1.0],
            labels=['Low', 'Medium', 'High', 'Very High'],
            duplicates='drop'
        )
        
        logger.info("Lead scoring completed")
        
        return results.sort_values('LEAD_SCORE', ascending=False)
    
    def get_top_leads(self, 
                     df: pd.DataFrame, 
                     top_n: int = 100,
                     min_score: float = 0.5) -> pd.DataFrame:
        """
        Get top scoring leads.
        
        Args:
            df: DataFrame with scored leads
            top_n: Number of top leads to return
            min_score: Minimum score threshold
            
        Returns:
            DataFrame with top leads
        """
        if 'LEAD_SCORE' not in df.columns:
            df = self.score_leads(df)
        
        # Filter by minimum score and get top N
        top_leads = df[
            df['LEAD_SCORE'] >= min_score
        ].head(top_n)
        
        logger.info(f"Retrieved {len(top_leads)} top leads (min score: {min_score})")
        
        return top_leads
    
    def analyze_feature_importance(self, top_n: int = 20) -> Dict[str, float]:
        """
        Get top feature importance scores.
        
        Args:
            top_n: Number of top features to return
            
        Returns:
            Dictionary of top features and their importance scores
        """
        if not self.feature_importance:
            raise ValueError("Feature importance not available. Train model first.")
        
        return dict(list(self.feature_importance.items())[:top_n])
    
    def save_model(self, filepath: str):
        """
        Save the trained model to disk.
        
        Args:
            filepath: Path to save the model
        """
        if not self.is_trained:
            raise ValueError("Cannot save untrained model")
        
        model_data = {
            'model': self.model,
            'model_type': self.model_type,
            'data_processor': self.data_processor,
            'feature_importance': self.feature_importance,
            'model_metrics': self.model_metrics,
            'is_trained': self.is_trained,
            'trained_at': datetime.now()
        }
        
        joblib.dump(model_data, filepath)
        logger.info(f"Model saved to {filepath}")
    
    def load_model(self, filepath: str):
        """
        Load a trained model from disk.
        
        Args:
            filepath: Path to the saved model
        """
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Model file not found: {filepath}")
        
        model_data = joblib.load(filepath)
        
        self.model = model_data['model']
        self.model_type = model_data['model_type']
        self.data_processor = model_data['data_processor']
        self.feature_importance = model_data['feature_importance']
        self.model_metrics = model_data['model_metrics']
        self.is_trained = model_data['is_trained']
        
        logger.info(f"Model loaded from {filepath}")
    
    def generate_insights(self, scored_leads: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate insights from scored leads.
        
        Args:
            scored_leads: DataFrame with scored leads
            
        Returns:
            Dictionary with insights and statistics
        """
        insights = {
            'total_leads': len(scored_leads),
            'average_score': scored_leads['LEAD_SCORE'].mean(),
            'high_quality_leads': len(scored_leads[scored_leads['LEAD_SCORE'] > 0.7]),
            'score_distribution': scored_leads['SCORE_CATEGORY'].value_counts().to_dict(),
            'top_sources': {},
            'top_industries': {},
            'conversion_indicators': {}
        }
        
        # Top lead sources
        if 'LEADSOURCE' in scored_leads.columns:
            insights['top_sources'] = (
                scored_leads.groupby('LEADSOURCE')['LEAD_SCORE']
                .mean()
                .sort_values(ascending=False)
                .head(5)
                .to_dict()
            )
        
        # Top industries
        if 'COMPANY_INDUSTRY' in scored_leads.columns:
            insights['top_industries'] = (
                scored_leads.groupby('COMPANY_INDUSTRY')['LEAD_SCORE']
                .mean()
                .sort_values(ascending=False)
                .head(5)
                .to_dict()
            )
        
        # Conversion indicators
        high_score_leads = scored_leads[scored_leads['LEAD_SCORE'] > 0.7]
        if not high_score_leads.empty:
            insights['conversion_indicators'] = {
                'avg_completeness_score': high_score_leads.get('COMPLETENESS_SCORE', pd.Series()).mean(),
                'business_email_rate': high_score_leads.get('IS_BUSINESS_EMAIL', pd.Series()).mean(),
                'has_phone_rate': high_score_leads.get('HAS_PHONE', pd.Series()).mean()
            }
        
        return insights
    
    def create_lead_report(self, scored_leads: pd.DataFrame) -> str:
        """
        Create a text report of lead scoring results.
        
        Args:
            scored_leads: DataFrame with scored leads
            
        Returns:
            Formatted text report
        """
        insights = self.generate_insights(scored_leads)
        
        report = f"""
LEAD SCORING REPORT
==================
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Model: {self.model_type}

SUMMARY STATISTICS
------------------
Total Leads Analyzed: {insights['total_leads']:,}
Average Lead Score: {insights['average_score']:.3f}
High Quality Leads (>0.7): {insights['high_quality_leads']:,}

SCORE DISTRIBUTION
------------------
"""
        
        for category, count in insights['score_distribution'].items():
            percentage = (count / insights['total_leads']) * 100
            report += f"{category}: {count:,} ({percentage:.1f}%)\n"
        
        if insights['top_sources']:
            report += "\nTOP LEAD SOURCES (by avg score)\n"
            report += "--------------------------------\n"
            for source, score in insights['top_sources'].items():
                report += f"{source}: {score:.3f}\n"
        
        if insights['top_industries']:
            report += "\nTOP INDUSTRIES (by avg score)\n"
            report += "-----------------------------\n"
            for industry, score in insights['top_industries'].items():
                report += f"{industry}: {score:.3f}\n"
        
        if self.feature_importance:
            report += "\nTOP FEATURES (importance)\n"
            report += "------------------------\n"
            top_features = self.analyze_feature_importance(10)
            for feature, importance in top_features.items():
                report += f"{feature}: {importance:.3f}\n"
        
        return report
