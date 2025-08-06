"""
HubSpot AI/ML Module

This module provides AI and machine learning capabilities for HubSpot data analysis,
including lead scoring, predictive analytics, and customer insights.

Components:
- Lead Scoring: ML models to score and prioritize leads
- Predictive Analytics: Forecast conversion likelihood and deal closure
- Customer Segmentation: Automatic categorization of leads and customers
- Churn Prediction: Identify at-risk customers
- Recommendation Engine: Suggest next best actions
"""

from .lead_scoring import LeadScoringModel
from .predictive_analytics import PredictiveAnalytics
from .customer_segmentation import CustomerSegmentation
from .churn_prediction import ChurnPredictor
from .recommendation_engine import RecommendationEngine
from .data_processor import HubSpotDataProcessor
from .model_trainer import ModelTrainer

__version__ = "1.0.0"

__all__ = [
    "LeadScoringModel",
    "PredictiveAnalytics", 
    "CustomerSegmentation",
    "ChurnPredictor",
    "RecommendationEngine",
    "HubSpotDataProcessor",
    "ModelTrainer"
]
