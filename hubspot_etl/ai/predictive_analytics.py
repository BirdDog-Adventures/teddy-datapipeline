"""
Predictive Analytics Module

This module provides predictive analytics capabilities for HubSpot data,
including deal closure prediction, revenue forecasting, and time-to-conversion analysis.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime, timedelta
import joblib

from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import cross_val_score, TimeSeriesSplit
import matplotlib.pyplot as plt
import seaborn as sns

from .data_processor import HubSpotDataProcessor

logger = logging.getLogger(__name__)


class PredictiveAnalytics:
    """
    Predictive analytics for HubSpot data.
    
    Features:
    - Deal closure probability prediction
    - Revenue forecasting
    - Time-to-conversion prediction
    - Customer lifetime value estimation
    - Seasonal trend analysis
    """
    
    def __init__(self):
        """Initialize the predictive analytics module."""
        self.data_processor = None  # Initialize lazily when needed
        self.models = {}
        self.model_metrics = {}
        self.forecasts = {}
    
    def _get_data_processor(self):
        """Get data processor, initializing if needed."""
        if self.data_processor is None:
            self.data_processor = HubSpotDataProcessor()
        return self.data_processor
        
    def prepare_deal_data(self, days_back: int = 730) -> pd.DataFrame:
        """
        Prepare deal data for predictive modeling.
        
        Args:
            days_back: Number of days to look back for data
            
        Returns:
            Prepared DataFrame with deal features
        """
        logger.info("Preparing deal data for predictive analytics...")
        
        data_processor = self._get_data_processor()
        with data_processor.snowflake_loader as loader:
            # Extract comprehensive deal data
            deals_query = f"""
            SELECT 
                ID,
                DEALNAME,
                DEALSTAGE,
                PIPELINE,
                AMOUNT,
                CLOSEDATE,
                CREATEDATE,
                LASTMODIFIEDDATE,
                HUBSPOT_OWNER_ID,
                DEALTYPE,
                HS_DEAL_STAGE_PROBABILITY,
                HS_DAYS_TO_CLOSE,
                HS_FORECAST_AMOUNT,
                HS_FORECAST_PROBABILITY,
                HS_IS_CLOSED_WON,
                HS_IS_CLOSED_LOST,
                ETL_LOADED_AT
            FROM HUBSPOT_DEALS 
            WHERE CREATEDATE >= DATEADD(day, -{days_back}, CURRENT_DATE())
            AND AMOUNT IS NOT NULL
            AND AMOUNT > 0
            """
            
            deals_df = pd.DataFrame(loader.execute_query(deals_query))
            
            if deals_df.empty:
                logger.warning("No deal data found")
                return pd.DataFrame()
            
            logger.info(f"Extracted {len(deals_df)} deals")
            
            # Engineer deal-specific features
            deals_df = self._engineer_deal_features(deals_df)
            
            return deals_df
    
    def _engineer_deal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Engineer features specific to deal prediction."""
        df = df.copy()
        
        # Convert date columns
        date_columns = ['CREATEDATE', 'CLOSEDATE', 'LASTMODIFIEDDATE']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        # Time-based features
        current_time = datetime.now()
        df['DAYS_SINCE_CREATED'] = (current_time - df['CREATEDATE']).dt.days
        df['DAYS_SINCE_MODIFIED'] = (current_time - df['LASTMODIFIEDDATE']).dt.days
        
        # Deal age and velocity
        if 'CLOSEDATE' in df.columns:
            df['DAYS_TO_CLOSE'] = (df['CLOSEDATE'] - df['CREATEDATE']).dt.days
            df['IS_CLOSED'] = df['CLOSEDATE'].notna()
        
        # Deal size categories
        df['DEAL_SIZE_CATEGORY'] = pd.cut(
            df['AMOUNT'],
            bins=[0, 1000, 5000, 25000, 100000, float('inf')],
            labels=['Micro', 'Small', 'Medium', 'Large', 'Enterprise']
        )
        
        # Stage progression features
        stage_order = {
            'appointmentscheduled': 1, 'qualifiedtobuy': 2, 'presentationscheduled': 3,
            'decisionmakerboughtin': 4, 'contractsent': 5, 'closedwon': 6, 'closedlost': 0
        }
        df['STAGE_ORDER'] = df['DEALSTAGE'].map(stage_order).fillna(0)
        
        # Probability and forecast features
        if 'HS_DEAL_STAGE_PROBABILITY' in df.columns:
            df['HIGH_PROBABILITY'] = df['HS_DEAL_STAGE_PROBABILITY'] > 0.7
        
        # Seasonal features
        df['CREATED_MONTH'] = df['CREATEDATE'].dt.month
        df['CREATED_QUARTER'] = df['CREATEDATE'].dt.quarter
        df['CREATED_WEEKDAY'] = df['CREATEDATE'].dt.dayofweek
        
        # Deal velocity (amount per day)
        df['DEAL_VELOCITY'] = df['AMOUNT'] / (df['DAYS_SINCE_CREATED'] + 1)
        
        return df
    
    def train_closure_prediction_model(self, 
                                     df: pd.DataFrame,
                                     model_type: str = 'random_forest') -> Dict[str, Any]:
        """
        Train a model to predict deal closure probability.
        
        Args:
            df: Deal data DataFrame
            model_type: Type of model to train
            
        Returns:
            Training results and metrics
        """
        logger.info("Training deal closure prediction model...")
        
        # Create target variable (closed won)
        if 'HS_IS_CLOSED_WON' in df.columns:
            df['WILL_CLOSE_WON'] = df['HS_IS_CLOSED_WON'].fillna(0).astype(int)
        else:
            # Fallback: use deal stage
            df['WILL_CLOSE_WON'] = (df['DEALSTAGE'] == 'closedwon').astype(int)
        
        # Prepare features
        feature_columns = [
            'AMOUNT', 'STAGE_ORDER', 'DAYS_SINCE_CREATED', 'DAYS_SINCE_MODIFIED',
            'CREATED_MONTH', 'CREATED_QUARTER', 'DEAL_VELOCITY'
        ]
        
        # Add probability if available
        if 'HS_DEAL_STAGE_PROBABILITY' in df.columns:
            feature_columns.append('HS_DEAL_STAGE_PROBABILITY')
        
        # Filter available columns
        available_features = [col for col in feature_columns if col in df.columns]
        
        X = df[available_features].fillna(0)
        y = df['WILL_CLOSE_WON']
        
        # Initialize model
        if model_type == 'random_forest':
            from sklearn.ensemble import RandomForestClassifier
            model = RandomForestClassifier(n_estimators=100, random_state=42)
        elif model_type == 'gradient_boosting':
            from sklearn.ensemble import GradientBoostingClassifier
            model = GradientBoostingClassifier(n_estimators=100, random_state=42)
        else:
            from sklearn.linear_model import LogisticRegression
            model = LogisticRegression(random_state=42)
        
        # Train model
        model.fit(X, y)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='roc_auc')
        
        # Store model
        self.models['closure_prediction'] = {
            'model': model,
            'features': available_features,
            'model_type': model_type
        }
        
        # Calculate metrics
        from sklearn.metrics import roc_auc_score
        y_pred_proba = model.predict_proba(X)[:, 1]
        train_auc = roc_auc_score(y, y_pred_proba)
        
        results = {
            'model_type': model_type,
            'features_used': available_features,
            'training_samples': len(X),
            'cv_auc_mean': cv_scores.mean(),
            'cv_auc_std': cv_scores.std(),
            'train_auc': train_auc,
            'conversion_rate': y.mean()
        }
        
        self.model_metrics['closure_prediction'] = results
        logger.info(f"Closure prediction model trained. CV AUC: {cv_scores.mean():.3f}")
        
        return results
    
    def train_revenue_forecasting_model(self, 
                                      df: pd.DataFrame,
                                      model_type: str = 'random_forest') -> Dict[str, Any]:
        """
        Train a model to forecast revenue from deals.
        
        Args:
            df: Deal data DataFrame
            model_type: Type of model to train
            
        Returns:
            Training results and metrics
        """
        logger.info("Training revenue forecasting model...")
        
        # Filter to closed won deals for revenue prediction
        closed_won_deals = df[df.get('HS_IS_CLOSED_WON', df['DEALSTAGE'] == 'closedwon')]
        
        if len(closed_won_deals) < 50:
            logger.warning("Insufficient closed won deals for revenue forecasting")
            return {}
        
        # Prepare features
        feature_columns = [
            'STAGE_ORDER', 'DAYS_SINCE_CREATED', 'CREATED_MONTH', 
            'CREATED_QUARTER', 'DEAL_VELOCITY'
        ]
        
        # Add probability if available
        if 'HS_DEAL_STAGE_PROBABILITY' in closed_won_deals.columns:
            feature_columns.append('HS_DEAL_STAGE_PROBABILITY')
        
        available_features = [col for col in feature_columns if col in closed_won_deals.columns]
        
        X = closed_won_deals[available_features].fillna(0)
        y = closed_won_deals['AMOUNT']
        
        # Initialize model
        if model_type == 'random_forest':
            model = RandomForestRegressor(n_estimators=100, random_state=42)
        elif model_type == 'gradient_boosting':
            model = GradientBoostingRegressor(n_estimators=100, random_state=42)
        else:
            model = LinearRegression()
        
        # Train model
        model.fit(X, y)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='r2')
        
        # Store model
        self.models['revenue_forecasting'] = {
            'model': model,
            'features': available_features,
            'model_type': model_type
        }
        
        # Calculate metrics
        y_pred = model.predict(X)
        mae = mean_absolute_error(y, y_pred)
        rmse = np.sqrt(mean_squared_error(y, y_pred))
        r2 = r2_score(y, y_pred)
        
        results = {
            'model_type': model_type,
            'features_used': available_features,
            'training_samples': len(X),
            'cv_r2_mean': cv_scores.mean(),
            'cv_r2_std': cv_scores.std(),
            'train_r2': r2,
            'mae': mae,
            'rmse': rmse,
            'avg_deal_value': y.mean()
        }
        
        self.model_metrics['revenue_forecasting'] = results
        logger.info(f"Revenue forecasting model trained. CV R²: {cv_scores.mean():.3f}")
        
        return results
    
    def train_time_to_close_model(self, 
                                 df: pd.DataFrame,
                                 model_type: str = 'random_forest') -> Dict[str, Any]:
        """
        Train a model to predict time to close for deals.
        
        Args:
            df: Deal data DataFrame
            model_type: Type of model to train
            
        Returns:
            Training results and metrics
        """
        logger.info("Training time-to-close prediction model...")
        
        # Filter to closed deals with valid close dates
        closed_deals = df[df['IS_CLOSED'] & df['DAYS_TO_CLOSE'].notna() & (df['DAYS_TO_CLOSE'] > 0)]
        
        if len(closed_deals) < 50:
            logger.warning("Insufficient closed deals for time-to-close prediction")
            return {}
        
        # Prepare features
        feature_columns = [
            'AMOUNT', 'STAGE_ORDER', 'CREATED_MONTH', 'CREATED_QUARTER', 'DEAL_VELOCITY'
        ]
        
        available_features = [col for col in feature_columns if col in closed_deals.columns]
        
        X = closed_deals[available_features].fillna(0)
        y = closed_deals['DAYS_TO_CLOSE']
        
        # Initialize model
        if model_type == 'random_forest':
            model = RandomForestRegressor(n_estimators=100, random_state=42)
        elif model_type == 'gradient_boosting':
            model = GradientBoostingRegressor(n_estimators=100, random_state=42)
        else:
            model = LinearRegression()
        
        # Train model
        model.fit(X, y)
        
        # Cross-validation
        cv_scores = cross_val_score(model, X, y, cv=5, scoring='r2')
        
        # Store model
        self.models['time_to_close'] = {
            'model': model,
            'features': available_features,
            'model_type': model_type
        }
        
        # Calculate metrics
        y_pred = model.predict(X)
        mae = mean_absolute_error(y, y_pred)
        rmse = np.sqrt(mean_squared_error(y, y_pred))
        r2 = r2_score(y, y_pred)
        
        results = {
            'model_type': model_type,
            'features_used': available_features,
            'training_samples': len(X),
            'cv_r2_mean': cv_scores.mean(),
            'cv_r2_std': cv_scores.std(),
            'train_r2': r2,
            'mae_days': mae,
            'rmse_days': rmse,
            'avg_days_to_close': y.mean()
        }
        
        self.model_metrics['time_to_close'] = results
        logger.info(f"Time-to-close model trained. CV R²: {cv_scores.mean():.3f}")
        
        return results
    
    def predict_deal_outcomes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Predict outcomes for active deals.
        
        Args:
            df: DataFrame with deal data
            
        Returns:
            DataFrame with predictions
        """
        logger.info(f"Predicting outcomes for {len(df)} deals...")
        
        results = df.copy()
        
        # Engineer features
        results = self._engineer_deal_features(results)
        
        # Closure probability prediction
        if 'closure_prediction' in self.models:
            model_info = self.models['closure_prediction']
            model = model_info['model']
            features = model_info['features']
            
            X = results[features].fillna(0)
            closure_proba = model.predict_proba(X)[:, 1]
            results['PREDICTED_CLOSURE_PROBABILITY'] = closure_proba
            
            # Closure prediction categories
            results['CLOSURE_LIKELIHOOD'] = pd.cut(
                closure_proba,
                bins=[0, 0.3, 0.6, 0.8, 1.0],
                labels=['Low', 'Medium', 'High', 'Very High']
            )
        
        # Revenue prediction
        if 'revenue_forecasting' in self.models:
            model_info = self.models['revenue_forecasting']
            model = model_info['model']
            features = model_info['features']
            
            X = results[features].fillna(0)
            predicted_revenue = model.predict(X)
            results['PREDICTED_REVENUE'] = predicted_revenue
        
        # Time to close prediction
        if 'time_to_close' in self.models:
            model_info = self.models['time_to_close']
            model = model_info['model']
            features = model_info['features']
            
            X = results[features].fillna(0)
            predicted_days = model.predict(X)
            results['PREDICTED_DAYS_TO_CLOSE'] = predicted_days
            
            # Estimated close date
            results['ESTIMATED_CLOSE_DATE'] = (
                results['CREATEDATE'] + pd.to_timedelta(predicted_days, unit='days')
            )
        
        logger.info("Deal outcome predictions completed")
        
        return results
    
    def generate_revenue_forecast(self, 
                                months_ahead: int = 6,
                                confidence_level: float = 0.95) -> Dict[str, Any]:
        """
        Generate revenue forecast for upcoming months.
        
        Args:
            months_ahead: Number of months to forecast
            confidence_level: Confidence level for intervals
            
        Returns:
            Dictionary with forecast data
        """
        logger.info(f"Generating {months_ahead}-month revenue forecast...")
        
        # Get current pipeline data
        pipeline_data = self.prepare_deal_data(days_back=365)
        
        if pipeline_data.empty:
            logger.warning("No pipeline data available for forecasting")
            return {}
        
        # Filter to open deals
        open_deals = pipeline_data[~pipeline_data.get('IS_CLOSED', False)]
        
        if open_deals.empty:
            logger.warning("No open deals found for forecasting")
            return {}
        
        # Predict outcomes for open deals
        predicted_deals = self.predict_deal_outcomes(open_deals)
        
        # Generate monthly forecasts
        forecast_data = []
        current_date = datetime.now()
        
        for month_offset in range(months_ahead):
            forecast_month = current_date + timedelta(days=30 * month_offset)
            month_start = forecast_month.replace(day=1)
            month_end = (month_start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            
            # Deals likely to close in this month
            if 'ESTIMATED_CLOSE_DATE' in predicted_deals.columns:
                month_deals = predicted_deals[
                    (predicted_deals['ESTIMATED_CLOSE_DATE'] >= month_start) &
                    (predicted_deals['ESTIMATED_CLOSE_DATE'] <= month_end)
                ]
            else:
                # Fallback: distribute deals evenly
                month_deals = predicted_deals.sample(
                    n=min(len(predicted_deals) // months_ahead, len(predicted_deals)),
                    random_state=42
                )
            
            # Calculate weighted revenue (probability * amount)
            if 'PREDICTED_CLOSURE_PROBABILITY' in month_deals.columns:
                weighted_revenue = (
                    month_deals['AMOUNT'] * month_deals['PREDICTED_CLOSURE_PROBABILITY']
                ).sum()
                
                # Confidence intervals (simplified)
                std_dev = (month_deals['AMOUNT'] * month_deals['PREDICTED_CLOSURE_PROBABILITY']).std()
                margin = 1.96 * std_dev if confidence_level == 0.95 else 1.645 * std_dev
                
                lower_bound = max(0, weighted_revenue - margin)
                upper_bound = weighted_revenue + margin
            else:
                # Fallback calculation
                weighted_revenue = month_deals['AMOUNT'].sum() * 0.3  # Assume 30% close rate
                lower_bound = weighted_revenue * 0.7
                upper_bound = weighted_revenue * 1.3
            
            forecast_data.append({
                'month': forecast_month.strftime('%Y-%m'),
                'forecasted_revenue': weighted_revenue,
                'lower_bound': lower_bound,
                'upper_bound': upper_bound,
                'deal_count': len(month_deals),
                'avg_deal_size': month_deals['AMOUNT'].mean() if not month_deals.empty else 0
            })
        
        # Calculate summary statistics
        total_forecast = sum(item['forecasted_revenue'] for item in forecast_data)
        avg_monthly = total_forecast / months_ahead if months_ahead > 0 else 0
        
        forecast_summary = {
            'forecast_period': f"{months_ahead} months",
            'total_forecasted_revenue': total_forecast,
            'average_monthly_revenue': avg_monthly,
            'confidence_level': confidence_level,
            'monthly_forecasts': forecast_data,
            'generated_at': datetime.now().isoformat()
        }
        
        self.forecasts['revenue'] = forecast_summary
        logger.info(f"Revenue forecast generated: ${total_forecast:,.2f} over {months_ahead} months")
        
        return forecast_summary
    
    def analyze_deal_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze patterns in deal data.
        
        Args:
            df: Deal data DataFrame
            
        Returns:
            Dictionary with pattern analysis
        """
        logger.info("Analyzing deal patterns...")
        
        patterns = {}
        
        # Seasonal patterns
        if 'CREATED_MONTH' in df.columns:
            monthly_stats = df.groupby('CREATED_MONTH').agg({
                'AMOUNT': ['count', 'mean', 'sum'],
                'DAYS_TO_CLOSE': 'mean'
            }).round(2)
            
            patterns['seasonal'] = {
                'best_month_volume': monthly_stats['AMOUNT']['count'].idxmax(),
                'best_month_value': monthly_stats['AMOUNT']['sum'].idxmax(),
                'fastest_month': monthly_stats['DAYS_TO_CLOSE'].idxmin(),
                'monthly_stats': monthly_stats.to_dict()
            }
        
        # Deal size patterns
        if 'DEAL_SIZE_CATEGORY' in df.columns:
            size_stats = df.groupby('DEAL_SIZE_CATEGORY').agg({
                'AMOUNT': ['count', 'mean'],
                'DAYS_TO_CLOSE': 'mean'
            }).round(2)
            
            patterns['deal_size'] = size_stats.to_dict()
        
        # Stage progression patterns
        if 'STAGE_ORDER' in df.columns:
            stage_stats = df.groupby('STAGE_ORDER').agg({
                'AMOUNT': ['count', 'mean'],
                'DAYS_TO_CLOSE': 'mean'
            }).round(2)
            
            patterns['stage_progression'] = stage_stats.to_dict()
        
        # Conversion rates by various dimensions
        if 'HS_IS_CLOSED_WON' in df.columns or 'DEALSTAGE' in df.columns:
            if 'HS_IS_CLOSED_WON' in df.columns:
                conversion_col = 'HS_IS_CLOSED_WON'
            else:
                df['IS_WON'] = (df['DEALSTAGE'] == 'closedwon').astype(int)
                conversion_col = 'IS_WON'
            
            # Overall conversion rate
            overall_conversion = df[conversion_col].mean()
            patterns['conversion_rates'] = {'overall': overall_conversion}
            
            # Conversion by deal size
            if 'DEAL_SIZE_CATEGORY' in df.columns:
                size_conversion = df.groupby('DEAL_SIZE_CATEGORY')[conversion_col].mean()
                patterns['conversion_rates']['by_size'] = size_conversion.to_dict()
            
            # Conversion by month
            if 'CREATED_MONTH' in df.columns:
                monthly_conversion = df.groupby('CREATED_MONTH')[conversion_col].mean()
                patterns['conversion_rates']['by_month'] = monthly_conversion.to_dict()
        
        logger.info("Deal pattern analysis completed")
        
        return patterns
    
    def save_models(self, filepath: str):
        """Save all trained models to disk."""
        if not self.models:
            raise ValueError("No models to save")
        
        model_data = {
            'models': self.models,
            'model_metrics': self.model_metrics,
            'forecasts': self.forecasts,
            'saved_at': datetime.now()
        }
        
        joblib.dump(model_data, filepath)
        logger.info(f"Models saved to {filepath}")
    
    def load_models(self, filepath: str):
        """Load trained models from disk."""
        model_data = joblib.load(filepath)
        
        self.models = model_data['models']
        self.model_metrics = model_data['model_metrics']
        self.forecasts = model_data.get('forecasts', {})
        
        logger.info(f"Models loaded from {filepath}")
    
    def generate_analytics_report(self) -> str:
        """Generate a comprehensive analytics report."""
        report = f"""
HUBSPOT PREDICTIVE ANALYTICS REPORT
===================================
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

TRAINED MODELS
--------------
"""
        
        for model_name, metrics in self.model_metrics.items():
            report += f"\n{model_name.upper()}:\n"
            report += f"  Model Type: {metrics.get('model_type', 'N/A')}\n"
            report += f"  Training Samples: {metrics.get('training_samples', 'N/A')}\n"
            
            if 'cv_auc_mean' in metrics:
                report += f"  CV AUC: {metrics['cv_auc_mean']:.3f} ± {metrics['cv_auc_std']:.3f}\n"
            elif 'cv_r2_mean' in metrics:
                report += f"  CV R²: {metrics['cv_r2_mean']:.3f} ± {metrics['cv_r2_std']:.3f}\n"
        
        # Revenue forecast
        if 'revenue' in self.forecasts:
            forecast = self.forecasts['revenue']
            report += f"\nREVENUE FORECAST\n"
            report += f"----------------\n"
            report += f"Period: {forecast['forecast_period']}\n"
            report += f"Total Forecasted: ${forecast['total_forecasted_revenue']:,.2f}\n"
            report += f"Average Monthly: ${forecast['average_monthly_revenue']:,.2f}\n"
            report += f"Confidence Level: {forecast['confidence_level']:.0%}\n"
        
        return report
