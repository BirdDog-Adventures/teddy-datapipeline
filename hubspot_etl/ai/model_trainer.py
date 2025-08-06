"""
Model Trainer Module

This module provides automated training and management of all AI/ML models
for HubSpot data analysis.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime
import os
import joblib

from .data_processor import HubSpotDataProcessor
from .lead_scoring import LeadScoringModel
from .predictive_analytics import PredictiveAnalytics
from .customer_segmentation import CustomerSegmentation
from .churn_prediction import ChurnPredictor
from .recommendation_engine import RecommendationEngine

logger = logging.getLogger(__name__)


class ModelTrainer:
    """
    Automated trainer for all HubSpot AI/ML models.
    
    Features:
    - Automated model training pipeline
    - Model performance evaluation
    - Model comparison and selection
    - Automated retraining schedules
    - Model versioning and management
    - Performance monitoring
    """
    
    def __init__(self, models_directory: str = "models"):
        """
        Initialize the model trainer.
        
        Args:
            models_directory: Directory to save trained models
        """
        self.models_directory = models_directory
        self.data_processor = None  # Initialize lazily when needed
        self.models = {}
        self.training_results = {}
        self.model_versions = {}
        
        # Create models directory if it doesn't exist
        os.makedirs(models_directory, exist_ok=True)
        
        # Initialize models
        self._initialize_models()
    
    def _get_data_processor(self):
        """Get data processor, initializing if needed."""
        if self.data_processor is None:
            self.data_processor = HubSpotDataProcessor()
        return self.data_processor
    
    def _initialize_models(self):
        """Initialize all model instances."""
        # Get data processor to pass to models
        data_processor = self._get_data_processor()
        
        self.models = {
            'lead_scoring': {
                'random_forest': LeadScoringModel('random_forest'),
                'gradient_boosting': LeadScoringModel('gradient_boosting'),
                'logistic_regression': LeadScoringModel('logistic_regression')
            },
            'churn_prediction': {
                'random_forest': ChurnPredictor('random_forest'),
                'gradient_boosting': ChurnPredictor('gradient_boosting'),
                'logistic_regression': ChurnPredictor('logistic_regression')
            },
            'customer_segmentation': {
                'kmeans': CustomerSegmentation('kmeans'),
                'dbscan': CustomerSegmentation('dbscan'),
                'hierarchical': CustomerSegmentation('hierarchical')
            },
            'predictive_analytics': PredictiveAnalytics(),
            'recommendation_engine': RecommendationEngine()
        }
        
        # Set data processor for models that need it
        for churn_model in self.models['churn_prediction'].values():
            churn_model.data_processor = data_processor
        
        for seg_model in self.models['customer_segmentation'].values():
            seg_model.data_processor = data_processor
            
        for lead_model in self.models['lead_scoring'].values():
            lead_model.data_processor = data_processor
            
        self.models['predictive_analytics'].data_processor = data_processor
    
    def train_all_models(self, 
                        days_back: int = 365,
                        test_size: float = 0.2,
                        optimize_hyperparameters: bool = False) -> Dict[str, Any]:
        """
        Train all models with the latest data.
        
        Args:
            days_back: Number of days to look back for training data
            test_size: Proportion of data for testing
            optimize_hyperparameters: Whether to optimize hyperparameters
            
        Returns:
            Dictionary with training results for all models
        """
        logger.info("Starting automated training of all models...")
        
        training_session = {
            'session_id': datetime.now().strftime('%Y%m%d_%H%M%S'),
            'start_time': datetime.now(),
            'parameters': {
                'days_back': days_back,
                'test_size': test_size,
                'optimize_hyperparameters': optimize_hyperparameters
            },
            'results': {}
        }
        
        try:
            # Train lead scoring models
            logger.info("Training lead scoring models...")
            lead_scoring_results = self._train_lead_scoring_models(
                days_back, test_size, optimize_hyperparameters
            )
            training_session['results']['lead_scoring'] = lead_scoring_results
            
            # Train churn prediction models
            logger.info("Training churn prediction models...")
            churn_results = self._train_churn_prediction_models(
                days_back, test_size, optimize_hyperparameters
            )
            training_session['results']['churn_prediction'] = churn_results
            
            # Train customer segmentation models
            logger.info("Training customer segmentation models...")
            segmentation_results = self._train_segmentation_models(days_back)
            training_session['results']['customer_segmentation'] = segmentation_results
            
            # Train predictive analytics models
            logger.info("Training predictive analytics models...")
            analytics_results = self._train_predictive_analytics_models(days_back)
            training_session['results']['predictive_analytics'] = analytics_results
            
            # Select best models
            best_models = self._select_best_models(training_session['results'])
            training_session['best_models'] = best_models
            
            # Save models
            self._save_best_models(best_models, training_session['session_id'])
            
            training_session['end_time'] = datetime.now()
            training_session['duration'] = (
                training_session['end_time'] - training_session['start_time']
            ).total_seconds()
            
            # Store training results
            self.training_results[training_session['session_id']] = training_session
            
            logger.info(f"Model training completed in {training_session['duration']:.2f} seconds")
            
            return training_session
            
        except Exception as e:
            logger.error(f"Model training failed: {e}")
            training_session['error'] = str(e)
            training_session['end_time'] = datetime.now()
            return training_session
    
    def _train_lead_scoring_models(self, 
                                  days_back: int,
                                  test_size: float,
                                  optimize_hyperparameters: bool) -> Dict[str, Any]:
        """Train all lead scoring model variants."""
        results = {}
        
        for model_name, model in self.models['lead_scoring'].items():
            try:
                logger.info(f"Training lead scoring model: {model_name}")
                
                # Prepare training data
                X_train, X_test, y_train, y_test = model.prepare_training_data(
                    days_back=days_back
                )
                
                # Train model
                training_result = model.train(
                    X_train, y_train, 
                    optimize_hyperparameters=optimize_hyperparameters
                )
                
                # Evaluate model
                evaluation_result = model.evaluate(X_test, y_test)
                
                results[model_name] = {
                    'training': training_result,
                    'evaluation': evaluation_result,
                    'status': 'success'
                }
                
            except Exception as e:
                logger.error(f"Failed to train lead scoring model {model_name}: {e}")
                results[model_name] = {
                    'status': 'failed',
                    'error': str(e)
                }
        
        return results
    
    def _train_churn_prediction_models(self,
                                     days_back: int,
                                     test_size: float,
                                     optimize_hyperparameters: bool) -> Dict[str, Any]:
        """Train all churn prediction model variants."""
        results = {}
        
        for model_name, model in self.models['churn_prediction'].items():
            try:
                logger.info(f"Training churn prediction model: {model_name}")
                
                # Prepare training data
                churn_data = model.prepare_churn_data(days_back=days_back)
                
                if churn_data.empty:
                    results[model_name] = {
                        'status': 'failed',
                        'error': 'No data available for training'
                    }
                    continue
                
                # Train model
                training_result = model.train_churn_model(churn_data)
                
                results[model_name] = {
                    'training': training_result,
                    'status': 'success'
                }
                
            except Exception as e:
                logger.error(f"Failed to train churn prediction model {model_name}: {e}")
                results[model_name] = {
                    'status': 'failed',
                    'error': str(e)
                }
        
        return results
    
    def _train_segmentation_models(self, days_back: int) -> Dict[str, Any]:
        """Train all customer segmentation model variants."""
        results = {}
        
        for model_name, model in self.models['customer_segmentation'].items():
            try:
                logger.info(f"Training customer segmentation model: {model_name}")
                
                # Prepare segmentation data
                segmentation_data = model.prepare_segmentation_data(days_back=days_back)
                
                if segmentation_data.empty:
                    results[model_name] = {
                        'status': 'failed',
                        'error': 'No data available for segmentation'
                    }
                    continue
                
                # Perform clustering
                clustered_data = model.perform_clustering(segmentation_data)
                
                # Profile segments
                segment_profiles = model.profile_segments(clustered_data)
                
                results[model_name] = {
                    'clusters_found': len(segment_profiles),
                    'total_customers': len(clustered_data),
                    'segment_profiles': segment_profiles,
                    'status': 'success'
                }
                
            except Exception as e:
                logger.error(f"Failed to train segmentation model {model_name}: {e}")
                results[model_name] = {
                    'status': 'failed',
                    'error': str(e)
                }
        
        return results
    
    def _train_predictive_analytics_models(self, days_back: int) -> Dict[str, Any]:
        """Train predictive analytics models."""
        results = {}
        
        try:
            logger.info("Training predictive analytics models...")
            
            analytics = self.models['predictive_analytics']
            
            # Prepare deal data
            deal_data = analytics.prepare_deal_data(days_back=days_back)
            
            if deal_data.empty:
                return {
                    'status': 'failed',
                    'error': 'No deal data available for training'
                }
            
            # Train closure prediction model
            try:
                closure_results = analytics.train_closure_prediction_model(deal_data)
                results['closure_prediction'] = closure_results
            except Exception as e:
                logger.warning(f"Closure prediction training failed: {e}")
                results['closure_prediction'] = {'status': 'failed', 'error': str(e)}
            
            # Train revenue forecasting model
            try:
                revenue_results = analytics.train_revenue_forecasting_model(deal_data)
                results['revenue_forecasting'] = revenue_results
            except Exception as e:
                logger.warning(f"Revenue forecasting training failed: {e}")
                results['revenue_forecasting'] = {'status': 'failed', 'error': str(e)}
            
            # Train time-to-close model
            try:
                time_results = analytics.train_time_to_close_model(deal_data)
                results['time_to_close'] = time_results
            except Exception as e:
                logger.warning(f"Time-to-close training failed: {e}")
                results['time_to_close'] = {'status': 'failed', 'error': str(e)}
            
            results['status'] = 'success'
            
        except Exception as e:
            logger.error(f"Failed to train predictive analytics models: {e}")
            results = {
                'status': 'failed',
                'error': str(e)
            }
        
        return results
    
    def _select_best_models(self, training_results: Dict[str, Any]) -> Dict[str, str]:
        """Select the best performing model for each category."""
        best_models = {}
        
        # Select best lead scoring model
        if 'lead_scoring' in training_results:
            best_lead_model = None
            best_auc = 0
            
            for model_name, result in training_results['lead_scoring'].items():
                if result.get('status') == 'success':
                    auc = result.get('evaluation', {}).get('auc_score', 0)
                    if auc > best_auc:
                        best_auc = auc
                        best_lead_model = model_name
            
            if best_lead_model:
                best_models['lead_scoring'] = best_lead_model
                logger.info(f"Best lead scoring model: {best_lead_model} (AUC: {best_auc:.3f})")
        
        # Select best churn prediction model
        if 'churn_prediction' in training_results:
            best_churn_model = None
            best_auc = 0
            
            for model_name, result in training_results['churn_prediction'].items():
                if result.get('status') == 'success':
                    auc = result.get('training', {}).get('cv_auc_mean', 0)
                    if auc > best_auc:
                        best_auc = auc
                        best_churn_model = model_name
            
            if best_churn_model:
                best_models['churn_prediction'] = best_churn_model
                logger.info(f"Best churn prediction model: {best_churn_model} (AUC: {best_auc:.3f})")
        
        # Select best segmentation model (based on number of clusters and silhouette score)
        if 'customer_segmentation' in training_results:
            best_seg_model = None
            best_score = 0
            
            for model_name, result in training_results['customer_segmentation'].items():
                if result.get('status') == 'success':
                    # Simple scoring: prefer models with reasonable number of clusters
                    clusters = result.get('clusters_found', 0)
                    if 3 <= clusters <= 8:  # Reasonable range
                        score = clusters  # Could be more sophisticated
                        if score > best_score:
                            best_score = score
                            best_seg_model = model_name
            
            if best_seg_model:
                best_models['customer_segmentation'] = best_seg_model
                logger.info(f"Best segmentation model: {best_seg_model}")
        
        # Predictive analytics (single model)
        if 'predictive_analytics' in training_results:
            if training_results['predictive_analytics'].get('status') == 'success':
                best_models['predictive_analytics'] = 'default'
        
        return best_models
    
    def _save_best_models(self, best_models: Dict[str, str], session_id: str):
        """Save the best performing models to disk."""
        logger.info("Saving best models...")
        
        for model_category, model_name in best_models.items():
            try:
                if model_category == 'lead_scoring':
                    model = self.models['lead_scoring'][model_name]
                    filepath = os.path.join(
                        self.models_directory, 
                        f"lead_scoring_{model_name}_{session_id}.joblib"
                    )
                    model.save_model(filepath)
                    
                elif model_category == 'churn_prediction':
                    model = self.models['churn_prediction'][model_name]
                    filepath = os.path.join(
                        self.models_directory,
                        f"churn_prediction_{model_name}_{session_id}.joblib"
                    )
                    model.save_model(filepath)
                    
                elif model_category == 'customer_segmentation':
                    model = self.models['customer_segmentation'][model_name]
                    filepath = os.path.join(
                        self.models_directory,
                        f"customer_segmentation_{model_name}_{session_id}.joblib"
                    )
                    model.save_segmentation(filepath)
                    
                elif model_category == 'predictive_analytics':
                    model = self.models['predictive_analytics']
                    filepath = os.path.join(
                        self.models_directory,
                        f"predictive_analytics_{session_id}.joblib"
                    )
                    model.save_models(filepath)
                
                logger.info(f"Saved {model_category} model: {model_name}")
                
            except Exception as e:
                logger.error(f"Failed to save {model_category} model: {e}")
    
    def load_best_models(self, session_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Load the best models from a training session.
        
        Args:
            session_id: Specific session ID to load (latest if None)
            
        Returns:
            Dictionary with loaded models
        """
        if session_id is None:
            # Find latest session
            if not self.training_results:
                raise ValueError("No training sessions found")
            session_id = max(self.training_results.keys())
        
        if session_id not in self.training_results:
            raise ValueError(f"Training session {session_id} not found")
        
        best_models = self.training_results[session_id].get('best_models', {})
        loaded_models = {}
        
        for model_category, model_name in best_models.items():
            try:
                if model_category == 'lead_scoring':
                    filepath = os.path.join(
                        self.models_directory,
                        f"lead_scoring_{model_name}_{session_id}.joblib"
                    )
                    model = LeadScoringModel(model_name)
                    model.data_processor = self._get_data_processor()
                    model.load_model(filepath)
                    loaded_models['lead_scoring'] = model
                    
                elif model_category == 'churn_prediction':
                    filepath = os.path.join(
                        self.models_directory,
                        f"churn_prediction_{model_name}_{session_id}.joblib"
                    )
                    model = ChurnPredictor(model_name)
                    model.data_processor = self._get_data_processor()
                    model.load_model(filepath)
                    loaded_models['churn_prediction'] = model
                    
                elif model_category == 'customer_segmentation':
                    filepath = os.path.join(
                        self.models_directory,
                        f"customer_segmentation_{model_name}_{session_id}.joblib"
                    )
                    model = CustomerSegmentation(model_name)
                    model.data_processor = self._get_data_processor()
                    model.load_segmentation(filepath)
                    loaded_models['customer_segmentation'] = model
                    
                elif model_category == 'predictive_analytics':
                    filepath = os.path.join(
                        self.models_directory,
                        f"predictive_analytics_{session_id}.joblib"
                    )
                    model = PredictiveAnalytics()
                    model.data_processor = self._get_data_processor()
                    model.load_models(filepath)
                    loaded_models['predictive_analytics'] = model
                
            except Exception as e:
                logger.error(f"Failed to load {model_category} model: {e}")
        
        logger.info(f"Loaded {len(loaded_models)} models from session {session_id}")
        
        return loaded_models
    
    def create_recommendation_engine(self, session_id: Optional[str] = None) -> RecommendationEngine:
        """
        Create a recommendation engine with loaded models.
        
        Args:
            session_id: Training session ID to load models from
            
        Returns:
            Configured recommendation engine
        """
        loaded_models = self.load_best_models(session_id)
        
        recommendation_engine = RecommendationEngine()
        recommendation_engine.load_models(
            lead_scoring_model=loaded_models.get('lead_scoring'),
            churn_predictor=loaded_models.get('churn_prediction'),
            segmentation=loaded_models.get('customer_segmentation')
        )
        
        return recommendation_engine
    
    def evaluate_model_performance(self, session_id: str) -> Dict[str, Any]:
        """
        Evaluate the performance of models from a training session.
        
        Args:
            session_id: Training session ID
            
        Returns:
            Performance evaluation results
        """
        if session_id not in self.training_results:
            raise ValueError(f"Training session {session_id} not found")
        
        session_data = self.training_results[session_id]
        evaluation = {
            'session_id': session_id,
            'training_date': session_data.get('start_time'),
            'model_performance': {},
            'overall_score': 0
        }
        
        results = session_data.get('results', {})
        
        # Evaluate lead scoring performance
        if 'lead_scoring' in results:
            best_model = session_data.get('best_models', {}).get('lead_scoring')
            if best_model and best_model in results['lead_scoring']:
                model_result = results['lead_scoring'][best_model]
                if model_result.get('status') == 'success':
                    auc = model_result.get('evaluation', {}).get('auc_score', 0)
                    evaluation['model_performance']['lead_scoring'] = {
                        'model_type': best_model,
                        'auc_score': auc,
                        'performance_grade': self._grade_performance(auc, 'auc')
                    }
        
        # Evaluate churn prediction performance
        if 'churn_prediction' in results:
            best_model = session_data.get('best_models', {}).get('churn_prediction')
            if best_model and best_model in results['churn_prediction']:
                model_result = results['churn_prediction'][best_model]
                if model_result.get('status') == 'success':
                    auc = model_result.get('training', {}).get('cv_auc_mean', 0)
                    evaluation['model_performance']['churn_prediction'] = {
                        'model_type': best_model,
                        'cv_auc_mean': auc,
                        'performance_grade': self._grade_performance(auc, 'auc')
                    }
        
        # Evaluate segmentation performance
        if 'customer_segmentation' in results:
            best_model = session_data.get('best_models', {}).get('customer_segmentation')
            if best_model and best_model in results['customer_segmentation']:
                model_result = results['customer_segmentation'][best_model]
                if model_result.get('status') == 'success':
                    clusters = model_result.get('clusters_found', 0)
                    evaluation['model_performance']['customer_segmentation'] = {
                        'model_type': best_model,
                        'clusters_found': clusters,
                        'performance_grade': self._grade_segmentation_performance(clusters)
                    }
        
        # Calculate overall score
        grades = [
            perf.get('performance_grade', 0) 
            for perf in evaluation['model_performance'].values()
        ]
        evaluation['overall_score'] = np.mean(grades) if grades else 0
        
        return evaluation
    
    def _grade_performance(self, score: float, metric_type: str) -> float:
        """Grade model performance on a 0-100 scale."""
        if metric_type == 'auc':
            if score >= 0.9:
                return 95
            elif score >= 0.8:
                return 85
            elif score >= 0.7:
                return 75
            elif score >= 0.6:
                return 65
            else:
                return 50
        elif metric_type == 'r2':
            if score >= 0.8:
                return 90
            elif score >= 0.6:
                return 80
            elif score >= 0.4:
                return 70
            elif score >= 0.2:
                return 60
            else:
                return 50
        
        return 50  # Default
    
    def _grade_segmentation_performance(self, clusters: int) -> float:
        """Grade segmentation performance based on number of clusters."""
        if 4 <= clusters <= 6:
            return 90  # Optimal range
        elif 3 <= clusters <= 8:
            return 80  # Good range
        elif 2 <= clusters <= 10:
            return 70  # Acceptable range
        else:
            return 50  # Poor
    
    def generate_training_report(self, session_id: str) -> str:
        """
        Generate a comprehensive training report.
        
        Args:
            session_id: Training session ID
            
        Returns:
            Formatted training report
        """
        if session_id not in self.training_results:
            raise ValueError(f"Training session {session_id} not found")
        
        session_data = self.training_results[session_id]
        evaluation = self.evaluate_model_performance(session_id)
        
        report = f"""
MODEL TRAINING REPORT
====================
Session ID: {session_id}
Training Date: {session_data.get('start_time', 'Unknown')}
Duration: {session_data.get('duration', 0):.2f} seconds
Overall Performance Score: {evaluation['overall_score']:.1f}/100

TRAINING PARAMETERS
-------------------
Days Back: {session_data.get('parameters', {}).get('days_back', 'N/A')}
Test Size: {session_data.get('parameters', {}).get('test_size', 'N/A')}
Hyperparameter Optimization: {session_data.get('parameters', {}).get('optimize_hyperparameters', 'N/A')}

MODEL PERFORMANCE
-----------------
"""
        
        for model_type, performance in evaluation['model_performance'].items():
            report += f"\n{model_type.upper().replace('_', ' ')}:\n"
            report += f"  Best Model: {performance.get('model_type', 'N/A')}\n"
            
            if 'auc_score' in performance:
                report += f"  AUC Score: {performance['auc_score']:.3f}\n"
            elif 'cv_auc_mean' in performance:
                report += f"  CV AUC: {performance['cv_auc_mean']:.3f}\n"
            elif 'clusters_found' in performance:
                report += f"  Clusters Found: {performance['clusters_found']}\n"
            
            report += f"  Performance Grade: {performance['performance_grade']:.1f}/100\n"
        
        # Add training details
        results = session_data.get('results', {})
        
        if 'lead_scoring' in results:
            report += "\nLEAD SCORING DETAILS:\n"
            for model_name, result in results['lead_scoring'].items():
                status = result.get('status', 'unknown')
                report += f"  {model_name}: {status}\n"
                if status == 'success':
                    training = result.get('training', {})
                    report += f"    Training Samples: {training.get('training_samples', 'N/A')}\n"
                    report += f"    Features: {training.get('features_count', 'N/A')}\n"
        
        if 'churn_prediction' in results:
            report += "\nCHURN PREDICTION DETAILS:\n"
            for model_name, result in results['churn_prediction'].items():
                status = result.get('status', 'unknown')
                report += f"  {model_name}: {status}\n"
                if status == 'success':
                    training = result.get('training', {})
                    report += f"    Training Samples: {training.get('training_samples', 'N/A')}\n"
                    report += f"    Churn Rate: {training.get('churn_rate', 0):.2%}\n"
        
        if 'customer_segmentation' in results:
            report += "\nCUSTOMER SEGMENTATION DETAILS:\n"
            for model_name, result in results['customer_segmentation'].items():
                status = result.get('status', 'unknown')
                report += f"  {model_name}: {status}\n"
                if status == 'success':
                    report += f"    Customers: {result.get('total_customers', 'N/A')}\n"
                    report += f"    Clusters: {result.get('clusters_found', 'N/A')}\n"
        
        return report
