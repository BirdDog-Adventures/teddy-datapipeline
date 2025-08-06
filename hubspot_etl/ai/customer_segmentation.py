"""
Customer Segmentation Module

This module provides customer segmentation capabilities using clustering algorithms
to automatically categorize leads and customers based on their characteristics and behavior.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime
import joblib

from sklearn.cluster import KMeans, DBSCAN, AgglomerativeClustering
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score, calinski_harabasz_score
import matplotlib.pyplot as plt
import seaborn as sns

from .data_processor import HubSpotDataProcessor
from .birddog_qualification_engine import BirdDogQualificationEngine, enhance_dataframe_with_birddog_features

logger = logging.getLogger(__name__)


class CustomerSegmentation:
    """
    Customer segmentation using machine learning clustering algorithms.
    
    Features:
    - Multiple clustering algorithms (K-Means, DBSCAN, Hierarchical)
    - Automatic optimal cluster number detection
    - Segment profiling and analysis
    - Customer journey mapping
    - Behavioral segmentation
    """
    
    def __init__(self, algorithm: str = 'kmeans'):
        """
        Initialize the customer segmentation module.
        
        Args:
            algorithm: Clustering algorithm ('kmeans', 'dbscan', 'hierarchical')
        """
        self.algorithm = algorithm
        self.model = None
        self.scaler = StandardScaler()
        self.pca = None
        self.data_processor = None  # Initialize lazily when needed
        self.segments = {}
        self.segment_profiles = {}
        self.feature_columns = []
    
    def _get_data_processor(self):
        """Get data processor, initializing if needed."""
        if self.data_processor is None:
            self.data_processor = HubSpotDataProcessor()
        return self.data_processor
        
    def prepare_segmentation_data(self, days_back: int = 365) -> pd.DataFrame:
        """
        Prepare data for customer segmentation.
        
        Args:
            days_back: Number of days to look back for data
            
        Returns:
            Prepared DataFrame for segmentation
        """
        logger.info("Preparing data for customer segmentation...")
        
        # Extract lead data with comprehensive features
        data_processor = self._get_data_processor()
        df = data_processor.extract_lead_data(
            days_back=days_back,
            include_deals=True,
            include_companies=True
        )
        
        if df.empty:
            logger.warning("No data available for segmentation")
            return pd.DataFrame()
        
        # Engineer features for segmentation
        df = data_processor.engineer_features(df)
        
        # Add BirdDog-specific qualification features
        logger.info("Adding BirdDog qualification features for segmentation...")
        df = enhance_dataframe_with_birddog_features(df)
        
        # Create additional segmentation-specific features
        df = self._create_segmentation_features(df)
        
        # Add BirdDog-specific segments
        df = self._create_birddog_segments(df)
        
        logger.info(f"Prepared {len(df)} records for segmentation")
        
        return df
    
    def _create_birddog_segments(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create BirdDog-specific customer segments based on qualification criteria."""
        df = df.copy()
        
        # Initialize segment assignment
        segments = []
        
        for idx, row in df.iterrows():
            try:
                # Use BirdDog qualification features if available
                eligible = row.get('eligible', False)
                qualification_tier = row.get('qualification_tier', 'Disqualified')
                value_tier = row.get('value_tier', 'Low-Value')
                estimated_tax_savings = row.get('estimated_tax_savings', 0)
                
                # Assign BirdDog segments based on qualification and value
                if not eligible or qualification_tier == 'Disqualified':
                    segment = 'Disqualified'
                elif value_tier == 'Enterprise' and qualification_tier in ['Premium', 'High']:
                    segment = 'Enterprise Landowners'
                elif qualification_tier == 'Premium' and estimated_tax_savings >= 20000:
                    segment = 'Premium Agricultural Clients'
                elif qualification_tier in ['High', 'Premium'] and value_tier in ['High-Value', 'Medium-Value']:
                    segment = 'Active Farmers'
                elif qualification_tier in ['Medium', 'High'] and estimated_tax_savings >= 5000:
                    segment = 'Agricultural Investors'
                elif qualification_tier == 'Medium':
                    segment = 'Potential Agricultural Prospects'
                elif qualification_tier == 'Low':
                    segment = 'Low-Priority Agricultural Leads'
                else:
                    segment = 'Unqualified'
                
                segments.append(segment)
                
            except Exception as e:
                logger.warning(f"Error assigning BirdDog segment for row {idx}: {str(e)}")
                segments.append('Unqualified')
        
        # Add segment assignments to DataFrame
        df['BIRDDOG_SEGMENT'] = segments
        
        # Create segment priority scores for ranking
        segment_priorities = {
            'Enterprise Landowners': 10,
            'Premium Agricultural Clients': 9,
            'Active Farmers': 8,
            'Agricultural Investors': 7,
            'Potential Agricultural Prospects': 5,
            'Low-Priority Agricultural Leads': 3,
            'Unqualified': 1,
            'Disqualified': 0
        }
        
        df['BIRDDOG_SEGMENT_PRIORITY'] = df['BIRDDOG_SEGMENT'].map(segment_priorities).fillna(0)
        
        logger.info(f"Created BirdDog segments. Distribution:")
        segment_counts = df['BIRDDOG_SEGMENT'].value_counts()
        for segment, count in segment_counts.items():
            logger.info(f"  {segment}: {count} customers")
        
        return df
    
    def _create_segmentation_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create features specifically for segmentation analysis."""
        df = df.copy()
        
        # Engagement score
        engagement_factors = []
        if 'HAS_EMAIL' in df.columns:
            engagement_factors.append('HAS_EMAIL')
        if 'HAS_PHONE' in df.columns:
            engagement_factors.append('HAS_PHONE')
        if 'HAS_FULL_NAME' in df.columns:
            engagement_factors.append('HAS_FULL_NAME')
        if 'IS_BUSINESS_EMAIL' in df.columns:
            engagement_factors.append('IS_BUSINESS_EMAIL')
        
        if engagement_factors:
            df['ENGAGEMENT_SCORE'] = df[engagement_factors].sum(axis=1) / len(engagement_factors)
        else:
            df['ENGAGEMENT_SCORE'] = 0
        
        # Company maturity score
        maturity_factors = []
        if 'COMPANY_SIZE' in df.columns:
            company_size_filled = df['COMPANY_SIZE'].fillna(0)
            unique_values = company_size_filled.nunique()
            
            if unique_values > 5:
                try:
                    df['COMPANY_SIZE_SCORE'] = pd.qcut(
                        company_size_filled, 
                        q=5, 
                        labels=[1, 2, 3, 4, 5],
                        duplicates='drop'
                    ).astype(float)
                except ValueError:
                    # Fallback to simple binning
                    df['COMPANY_SIZE_SCORE'] = pd.cut(
                        company_size_filled,
                        bins=5,
                        labels=[1, 2, 3, 4, 5],
                        duplicates='drop'
                    ).astype(float)
            else:
                # Simple scoring for limited unique values
                df['COMPANY_SIZE_SCORE'] = (company_size_filled > company_size_filled.median()).astype(int) * 2 + 3
            
            maturity_factors.append('COMPANY_SIZE_SCORE')
        
        if 'COMPANY_REVENUE' in df.columns:
            company_revenue_filled = df['COMPANY_REVENUE'].fillna(0)
            unique_values = company_revenue_filled.nunique()
            
            if unique_values > 5:
                try:
                    df['COMPANY_REVENUE_SCORE'] = pd.qcut(
                        company_revenue_filled, 
                        q=5, 
                        labels=[1, 2, 3, 4, 5],
                        duplicates='drop'
                    ).astype(float)
                except ValueError:
                    # Fallback to simple binning
                    df['COMPANY_REVENUE_SCORE'] = pd.cut(
                        company_revenue_filled,
                        bins=5,
                        labels=[1, 2, 3, 4, 5],
                        duplicates='drop'
                    ).astype(float)
            else:
                # Simple scoring for limited unique values
                df['COMPANY_REVENUE_SCORE'] = (company_revenue_filled > company_revenue_filled.median()).astype(int) * 2 + 3
            
            maturity_factors.append('COMPANY_REVENUE_SCORE')
        
        if maturity_factors:
            df['COMPANY_MATURITY_SCORE'] = df[maturity_factors].mean(axis=1)
        else:
            df['COMPANY_MATURITY_SCORE'] = 0
        
        # Deal activity score
        if 'TOTAL_DEALS' in df.columns:
            total_deals_filled = df['TOTAL_DEALS'].fillna(0)
            unique_values = total_deals_filled.nunique()
            
            if unique_values > 5:
                try:
                    df['DEAL_ACTIVITY_SCORE'] = pd.qcut(
                        total_deals_filled, 
                        q=5, 
                        labels=[0, 1, 2, 3, 4],
                        duplicates='drop'
                    ).astype(float)
                except ValueError:
                    # Fallback to simple binning
                    df['DEAL_ACTIVITY_SCORE'] = pd.cut(
                        total_deals_filled,
                        bins=5,
                        labels=[0, 1, 2, 3, 4],
                        duplicates='drop'
                    ).astype(float)
            else:
                # Simple scoring for limited unique values
                df['DEAL_ACTIVITY_SCORE'] = (total_deals_filled > 0).astype(int) * 2
        else:
            df['DEAL_ACTIVITY_SCORE'] = 0
        
        # Recency score (how recently created)
        if 'DAYS_SINCE_CREATED' in df.columns:
            # Invert so recent = high score
            max_days = df['DAYS_SINCE_CREATED'].max()
            df['RECENCY_SCORE'] = 1 - (df['DAYS_SINCE_CREATED'] / max_days)
        else:
            df['RECENCY_SCORE'] = 0
        
        # Lifecycle stage score
        if 'LIFECYCLE_STAGE_ORDER' in df.columns:
            max_stage = df['LIFECYCLE_STAGE_ORDER'].max()
            df['LIFECYCLE_SCORE'] = df['LIFECYCLE_STAGE_ORDER'] / max_stage if max_stage > 0 else 0
        else:
            df['LIFECYCLE_SCORE'] = 0
        
        return df
    
    def select_features_for_clustering(self, df: pd.DataFrame) -> List[str]:
        """
        Select the best features for clustering.
        
        Args:
            df: Input DataFrame
            
        Returns:
            List of selected feature column names
        """
        # Define potential clustering features
        potential_features = [
            'ENGAGEMENT_SCORE',
            'COMPANY_MATURITY_SCORE', 
            'DEAL_ACTIVITY_SCORE',
            'RECENCY_SCORE',
            'LIFECYCLE_SCORE',
            'COMPLETENESS_SCORE',
            'DAYS_SINCE_CREATED',
            'TOTAL_DEALS',
            'TOTAL_DEAL_VALUE',
            'AVG_DEAL_VALUE',
            'COMPANY_SIZE',
            'COMPANY_REVENUE'
        ]
        
        # Filter to available features with sufficient variance
        selected_features = []
        for feature in potential_features:
            if feature in df.columns:
                # Check if feature has sufficient variance
                if df[feature].std() > 0.01:  # Minimum variance threshold
                    selected_features.append(feature)
        
        # Ensure we have at least some features
        if len(selected_features) < 2:
            # Fallback to basic features
            basic_features = ['DAYS_SINCE_CREATED', 'COMPLETENESS_SCORE']
            selected_features = [f for f in basic_features if f in df.columns]
        
        logger.info(f"Selected {len(selected_features)} features for clustering: {selected_features}")
        
        return selected_features
    
    def find_optimal_clusters(self, X: np.ndarray, max_clusters: int = 10) -> int:
        """
        Find the optimal number of clusters using elbow method and silhouette analysis.
        
        Args:
            X: Feature matrix
            max_clusters: Maximum number of clusters to test
            
        Returns:
            Optimal number of clusters
        """
        if self.algorithm != 'kmeans':
            return 5  # Default for non-kmeans algorithms
        
        logger.info("Finding optimal number of clusters...")
        
        inertias = []
        silhouette_scores = []
        K_range = range(2, min(max_clusters + 1, len(X)))
        
        for k in K_range:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            kmeans.fit(X)
            
            inertias.append(kmeans.inertia_)
            
            # Calculate silhouette score
            if len(np.unique(kmeans.labels_)) > 1:
                sil_score = silhouette_score(X, kmeans.labels_)
                silhouette_scores.append(sil_score)
            else:
                silhouette_scores.append(0)
        
        # Find elbow point (simplified)
        if len(inertias) >= 3:
            # Calculate rate of change
            deltas = np.diff(inertias)
            delta_deltas = np.diff(deltas)
            
            # Find the point where the rate of change starts to level off
            if len(delta_deltas) > 0:
                elbow_idx = np.argmax(delta_deltas) + 2  # +2 because of double diff
                elbow_k = K_range[min(elbow_idx, len(K_range) - 1)]
            else:
                elbow_k = K_range[len(K_range) // 2]
        else:
            elbow_k = K_range[0] if K_range else 3
        
        # Find best silhouette score
        if silhouette_scores:
            best_sil_idx = np.argmax(silhouette_scores)
            best_sil_k = K_range[best_sil_idx]
        else:
            best_sil_k = elbow_k
        
        # Choose the optimal k (prefer silhouette if reasonable)
        optimal_k = best_sil_k if silhouette_scores[best_sil_idx] > 0.3 else elbow_k
        
        logger.info(f"Optimal number of clusters: {optimal_k}")
        logger.info(f"Elbow method suggests: {elbow_k}")
        logger.info(f"Best silhouette score: {max(silhouette_scores):.3f} at k={best_sil_k}")
        
        return optimal_k
    
    def perform_clustering(self, 
                          df: pd.DataFrame,
                          n_clusters: Optional[int] = None,
                          use_pca: bool = True) -> pd.DataFrame:
        """
        Perform customer segmentation clustering.
        
        Args:
            df: Input DataFrame
            n_clusters: Number of clusters (auto-detect if None)
            use_pca: Whether to use PCA for dimensionality reduction
            
        Returns:
            DataFrame with cluster assignments
        """
        logger.info(f"Performing {self.algorithm} clustering...")
        
        # Select features
        self.feature_columns = self.select_features_for_clustering(df)
        
        if len(self.feature_columns) < 2:
            raise ValueError("Insufficient features for clustering")
        
        # Prepare feature matrix
        X = df[self.feature_columns].fillna(0)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Apply PCA if requested and beneficial
        if use_pca and len(self.feature_columns) > 3:
            # Determine number of components (explain 95% variance)
            pca_temp = PCA()
            pca_temp.fit(X_scaled)
            cumsum = np.cumsum(pca_temp.explained_variance_ratio_)
            n_components = np.argmax(cumsum >= 0.95) + 1
            n_components = max(2, min(n_components, len(self.feature_columns) - 1))
            
            self.pca = PCA(n_components=n_components)
            X_final = self.pca.fit_transform(X_scaled)
            logger.info(f"Applied PCA: {len(self.feature_columns)} -> {n_components} components")
        else:
            X_final = X_scaled
        
        # Initialize clustering model
        if self.algorithm == 'kmeans':
            if n_clusters is None:
                n_clusters = self.find_optimal_clusters(X_final)
            
            self.model = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            
        elif self.algorithm == 'dbscan':
            # Auto-tune DBSCAN parameters
            from sklearn.neighbors import NearestNeighbors
            
            # Find optimal eps using k-distance graph
            k = min(4, len(X_final) - 1)
            nbrs = NearestNeighbors(n_neighbors=k).fit(X_final)
            distances, indices = nbrs.kneighbors(X_final)
            distances = np.sort(distances[:, k-1], axis=0)
            
            # Use knee point as eps (simplified)
            eps = np.percentile(distances, 90)
            min_samples = max(2, len(X_final) // 50)
            
            # Ensure eps is not zero or negative
            if eps <= 0.0:
                eps = 0.5  # Default fallback value
            
            self.model = DBSCAN(eps=eps, min_samples=min_samples)
            
        elif self.algorithm == 'hierarchical':
            if n_clusters is None:
                n_clusters = self.find_optimal_clusters(X_final)
            
            self.model = AgglomerativeClustering(n_clusters=n_clusters)
        
        # Fit the model
        cluster_labels = self.model.fit_predict(X_final)
        
        # Handle noise points in DBSCAN (label -1)
        if self.algorithm == 'dbscan':
            n_clusters = len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0)
            logger.info(f"DBSCAN found {n_clusters} clusters")
            
            # Reassign noise points to nearest cluster
            if -1 in cluster_labels:
                from sklearn.neighbors import NearestNeighbors
                
                noise_mask = cluster_labels == -1
                if np.sum(~noise_mask) > 0:  # If there are non-noise points
                    nbrs = NearestNeighbors(n_neighbors=1).fit(X_final[~noise_mask])
                    _, indices = nbrs.kneighbors(X_final[noise_mask])
                    cluster_labels[noise_mask] = cluster_labels[~noise_mask][indices.flatten()]
        
        # Add cluster labels to dataframe
        result_df = df.copy()
        result_df['CLUSTER'] = cluster_labels
        result_df['CLUSTER_NAME'] = result_df['CLUSTER'].apply(lambda x: f"Segment_{x}")
        
        # Calculate cluster quality metrics
        if len(set(cluster_labels)) > 1:
            sil_score = silhouette_score(X_final, cluster_labels)
            ch_score = calinski_harabasz_score(X_final, cluster_labels)
            
            logger.info(f"Clustering completed with {len(set(cluster_labels))} clusters")
            logger.info(f"Silhouette Score: {sil_score:.3f}")
            logger.info(f"Calinski-Harabasz Score: {ch_score:.3f}")
        
        return result_df
    
    def profile_segments(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Create detailed profiles for each customer segment.
        
        Args:
            df: DataFrame with cluster assignments
            
        Returns:
            Dictionary with segment profiles
        """
        logger.info("Creating segment profiles...")
        
        if 'CLUSTER' not in df.columns:
            raise ValueError("DataFrame must contain cluster assignments")
        
        profiles = {}
        
        for cluster_id in df['CLUSTER'].unique():
            cluster_data = df[df['CLUSTER'] == cluster_id]
            cluster_name = f"Segment_{cluster_id}"
            
            # Basic statistics
            profile = {
                'cluster_id': cluster_id,
                'cluster_name': cluster_name,
                'size': len(cluster_data),
                'percentage': len(cluster_data) / len(df) * 100,
                'characteristics': {},
                'behavioral_patterns': {},
                'business_metrics': {}
            }
            
            # Feature characteristics
            for feature in self.feature_columns:
                if feature in cluster_data.columns:
                    profile['characteristics'][feature] = {
                        'mean': cluster_data[feature].mean(),
                        'median': cluster_data[feature].median(),
                        'std': cluster_data[feature].std()
                    }
            
            # Behavioral patterns
            if 'LIFECYCLESTAGE' in cluster_data.columns:
                lifecycle_dist = cluster_data['LIFECYCLESTAGE'].value_counts(normalize=True)
                profile['behavioral_patterns']['lifecycle_stages'] = lifecycle_dist.to_dict()
            
            if 'LEADSOURCE' in cluster_data.columns:
                source_dist = cluster_data['LEADSOURCE'].value_counts(normalize=True).head(5)
                profile['behavioral_patterns']['top_lead_sources'] = source_dist.to_dict()
            
            if 'COMPANY_INDUSTRY' in cluster_data.columns:
                industry_dist = cluster_data['COMPANY_INDUSTRY'].value_counts(normalize=True).head(5)
                profile['behavioral_patterns']['top_industries'] = industry_dist.to_dict()
            
            # Business metrics
            if 'TOTAL_DEALS' in cluster_data.columns:
                profile['business_metrics']['avg_deals'] = cluster_data['TOTAL_DEALS'].mean()
                profile['business_metrics']['deal_creators_pct'] = (cluster_data['TOTAL_DEALS'] > 0).mean() * 100
            
            if 'TOTAL_DEAL_VALUE' in cluster_data.columns:
                profile['business_metrics']['avg_deal_value'] = cluster_data['TOTAL_DEAL_VALUE'].mean()
            
            if 'COMPANY_SIZE' in cluster_data.columns:
                profile['business_metrics']['avg_company_size'] = cluster_data['COMPANY_SIZE'].mean()
            
            # Conversion metrics
            if 'CONVERTED' in cluster_data.columns:
                profile['business_metrics']['conversion_rate'] = cluster_data['CONVERTED'].mean() * 100
            
            profiles[cluster_name] = profile
        
        self.segment_profiles = profiles
        logger.info(f"Created profiles for {len(profiles)} segments")
        
        return profiles
    
    def assign_segment_names(self, profiles: Dict[str, Any]) -> Dict[str, str]:
        """
        Assign meaningful names to segments based on their characteristics.
        
        Args:
            profiles: Segment profiles dictionary
            
        Returns:
            Dictionary mapping cluster IDs to meaningful names
        """
        segment_names = {}
        
        for cluster_name, profile in profiles.items():
            cluster_id = profile['cluster_id']
            characteristics = profile['characteristics']
            business_metrics = profile['business_metrics']
            
            # Determine segment name based on characteristics
            engagement = characteristics.get('ENGAGEMENT_SCORE', {}).get('mean', 0)
            company_maturity = characteristics.get('COMPANY_MATURITY_SCORE', {}).get('mean', 0)
            deal_activity = characteristics.get('DEAL_ACTIVITY_SCORE', {}).get('mean', 0)
            
            # High-level categorization
            if engagement > 0.7 and company_maturity > 0.7:
                if deal_activity > 0.5:
                    name = "Enterprise Champions"
                else:
                    name = "Enterprise Prospects"
            elif engagement > 0.7 and deal_activity > 0.5:
                name = "Active Customers"
            elif engagement > 0.5:
                if company_maturity > 0.5:
                    name = "Qualified Prospects"
                else:
                    name = "Engaged Leads"
            elif company_maturity > 0.7:
                name = "Enterprise Dormant"
            elif deal_activity > 0.3:
                name = "Occasional Buyers"
            else:
                name = "Cold Leads"
            
            segment_names[cluster_id] = name
        
        return segment_names
    
    def recommend_actions(self, profiles: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Recommend actions for each customer segment.
        
        Args:
            profiles: Segment profiles dictionary
            
        Returns:
            Dictionary with recommended actions for each segment
        """
        recommendations = {}
        
        for cluster_name, profile in profiles.items():
            cluster_id = profile['cluster_id']
            characteristics = profile['characteristics']
            business_metrics = profile['business_metrics']
            
            actions = []
            
            # Engagement-based recommendations
            engagement = characteristics.get('ENGAGEMENT_SCORE', {}).get('mean', 0)
            if engagement < 0.3:
                actions.append("Implement lead nurturing campaigns")
                actions.append("Improve data collection processes")
            elif engagement < 0.6:
                actions.append("Increase touchpoint frequency")
                actions.append("Personalize communication")
            
            # Deal activity recommendations
            deal_activity = characteristics.get('DEAL_ACTIVITY_SCORE', {}).get('mean', 0)
            conversion_rate = business_metrics.get('conversion_rate', 0)
            
            if deal_activity < 0.2:
                actions.append("Focus on lead qualification")
                actions.append("Implement lead scoring")
            elif conversion_rate < 10:
                actions.append("Optimize sales process")
                actions.append("Provide sales training")
            
            # Company maturity recommendations
            company_maturity = characteristics.get('COMPANY_MATURITY_SCORE', {}).get('mean', 0)
            if company_maturity > 0.7:
                actions.append("Assign dedicated account managers")
                actions.append("Offer enterprise-level solutions")
            elif company_maturity < 0.3:
                actions.append("Focus on SMB-friendly offerings")
                actions.append("Simplify onboarding process")
            
            # Recency recommendations
            recency = characteristics.get('RECENCY_SCORE', {}).get('mean', 0)
            if recency < 0.3:
                actions.append("Re-engagement campaign needed")
                actions.append("Update contact information")
            
            recommendations[cluster_id] = actions
        
        return recommendations
    
    def generate_segmentation_report(self, df: pd.DataFrame) -> str:
        """
        Generate a comprehensive segmentation report.
        
        Args:
            df: DataFrame with cluster assignments
            
        Returns:
            Formatted text report
        """
        if not self.segment_profiles:
            self.profile_segments(df)
        
        segment_names = self.assign_segment_names(self.segment_profiles)
        recommendations = self.recommend_actions(self.segment_profiles)
        
        report = f"""
CUSTOMER SEGMENTATION REPORT
============================
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Algorithm: {self.algorithm}
Total Customers: {len(df):,}

SEGMENT OVERVIEW
----------------
"""
        
        for cluster_name, profile in self.segment_profiles.items():
            cluster_id = profile['cluster_id']
            meaningful_name = segment_names.get(cluster_id, cluster_name)
            
            report += f"\n{meaningful_name} (Cluster {cluster_id})\n"
            report += f"{'=' * len(meaningful_name)}\n"
            report += f"Size: {profile['size']:,} customers ({profile['percentage']:.1f}%)\n"
            
            # Key characteristics
            report += "\nKey Characteristics:\n"
            for feature, stats in profile['characteristics'].items():
                if 'SCORE' in feature:
                    report += f"  {feature}: {stats['mean']:.2f} (avg)\n"
            
            # Business metrics
            if profile['business_metrics']:
                report += "\nBusiness Metrics:\n"
                for metric, value in profile['business_metrics'].items():
                    if isinstance(value, float):
                        report += f"  {metric}: {value:.2f}\n"
                    else:
                        report += f"  {metric}: {value}\n"
            
            # Recommendations
            if cluster_id in recommendations:
                report += "\nRecommended Actions:\n"
                for action in recommendations[cluster_id]:
                    report += f"  • {action}\n"
            
            report += "\n"
        
        return report
    
    def save_segmentation(self, filepath: str):
        """Save the segmentation model and results."""
        segmentation_data = {
            'model': self.model,
            'scaler': self.scaler,
            'pca': self.pca,
            'algorithm': self.algorithm,
            'feature_columns': self.feature_columns,
            'segment_profiles': self.segment_profiles,
            'saved_at': datetime.now()
        }
        
        joblib.dump(segmentation_data, filepath)
        logger.info(f"Segmentation saved to {filepath}")
    
    def load_segmentation(self, filepath: str):
        """Load a saved segmentation model."""
        segmentation_data = joblib.load(filepath)
        
        self.model = segmentation_data['model']
        self.scaler = segmentation_data['scaler']
        self.pca = segmentation_data['pca']
        self.algorithm = segmentation_data['algorithm']
        self.feature_columns = segmentation_data['feature_columns']
        self.segment_profiles = segmentation_data['segment_profiles']
        
        logger.info(f"Segmentation loaded from {filepath}")
    
    def predict_segment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Predict segments for new data.
        
        Args:
            df: New customer data
            
        Returns:
            DataFrame with predicted segments
        """
        if self.model is None:
            raise ValueError("Model must be trained before prediction")
        
        # Prepare features
        data_processor = self._get_data_processor()
        df_processed = data_processor.engineer_features(df.copy())
        df_processed = self._create_segmentation_features(df_processed)
        
        # Extract features
        X = df_processed[self.feature_columns].fillna(0)
        X_scaled = self.scaler.transform(X)
        
        if self.pca is not None:
            X_final = self.pca.transform(X_scaled)
        else:
            X_final = X_scaled
        
        # Predict clusters
        if hasattr(self.model, 'predict'):
            cluster_labels = self.model.predict(X_final)
        else:
            # For DBSCAN, use fit_predict on new data (not ideal, but fallback)
            cluster_labels = self.model.fit_predict(X_final)
        
        # Add predictions to dataframe
        result_df = df.copy()
        result_df['PREDICTED_CLUSTER'] = cluster_labels
        result_df['PREDICTED_SEGMENT'] = result_df['PREDICTED_CLUSTER'].apply(
            lambda x: f"Segment_{x}"
        )
        
        return result_df
