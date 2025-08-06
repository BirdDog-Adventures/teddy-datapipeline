"""
HubSpot Data Processor for AI/ML

This module handles data preprocessing and feature engineering for HubSpot data
to prepare it for machine learning models.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import logging
from sklearn.preprocessing import StandardScaler, LabelEncoder, OneHotEncoder
from sklearn.impute import SimpleImputer
import sys
import os

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Load .env file from the parent directory
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    load_dotenv(env_path)
except ImportError:
    # dotenv not available, continue without it
    pass

# Add parent directory to path to import from hubspot_etl
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from snowflake_loader import SnowflakeLoader

logger = logging.getLogger(__name__)


class HubSpotDataProcessor:
    """
    Processes HubSpot data from Snowflake for machine learning applications.
    
    Features:
    - Data extraction from Snowflake
    - Feature engineering
    - Data cleaning and preprocessing
    - Feature scaling and encoding
    - Time-based feature creation
    """
    
    def __init__(self, snowflake_loader: Optional[SnowflakeLoader] = None):
        """
        Initialize the data processor.
        
        Args:
            snowflake_loader: Optional SnowflakeLoader instance
        """
        self.snowflake_loader = snowflake_loader or SnowflakeLoader()
        self.scalers = {}
        self.encoders = {}
        self.feature_columns = []
        
    def extract_lead_data(self, 
                         days_back: int = 365,
                         include_deals: bool = True,
                         include_companies: bool = True) -> pd.DataFrame:
        """
        Extract and combine lead data from HubSpot tables.
        
        Args:
            days_back: Number of days to look back for data
            include_deals: Whether to include deal information
            include_companies: Whether to include company information
            
        Returns:
            Combined DataFrame with lead data
        """
        try:
            with self.snowflake_loader as loader:
                # Base contacts query - using only the most essential columns
                contacts_query = f"""
                SELECT 
                    ID,
                    FIRSTNAME,
                    LASTNAME,
                    EMAIL,
                    COMPANY,
                    LIFECYCLESTAGE,
                    CREATEDATE,
                    LASTMODIFIEDDATE,
                    ETL_LOADED_AT
                FROM HUBSPOT_CONTACTS 
                WHERE CREATEDATE >= DATEADD(day, -{days_back}, CURRENT_DATE())
                AND EMAIL IS NOT NULL
                """
                
                contacts_df = pd.DataFrame(loader.execute_query(contacts_query))
                
                if contacts_df.empty:
                    logger.warning("No contact data found")
                    return pd.DataFrame()
                
                logger.info(f"Extracted {len(contacts_df)} contacts")
                
                # Add deal information if requested
                if include_deals:
                    deals_df = self._extract_deal_data(loader, days_back)
                    if not deals_df.empty:
                        contacts_df = self._merge_deal_data(contacts_df, deals_df)
                
                # Add company information if requested
                if include_companies:
                    companies_df = self._extract_company_data(loader)
                    if not companies_df.empty:
                        contacts_df = self._merge_company_data(contacts_df, companies_df)
                
                return contacts_df
                
        except Exception as e:
            logger.error(f"Error extracting lead data: {e}")
            raise
    
    def _extract_deal_data(self, loader: SnowflakeLoader, days_back: int) -> pd.DataFrame:
        """Extract deal data associated with contacts."""
        deals_query = f"""
        SELECT 
            ID as DEAL_ID,
            DEALNAME,
            DEALSTAGE,
            PIPELINE,
            AMOUNT,
            CLOSEDATE,
            CREATEDATE as DEAL_CREATEDATE,
            HUBSPOT_OWNER_ID as DEAL_OWNER_ID,
            DEALTYPE,
            HS_DEAL_STAGE_PROBABILITY
        FROM HUBSPOT_DEALS 
        WHERE CREATEDATE >= DATEADD(day, -{days_back}, CURRENT_DATE())
        """
        
        try:
            deals_df = pd.DataFrame(loader.execute_query(deals_query))
            logger.info(f"Extracted {len(deals_df)} deals")
            return deals_df
        except Exception as e:
            logger.warning(f"Could not extract deal data: {e}")
            return pd.DataFrame()
    
    def _extract_company_data(self, loader: SnowflakeLoader) -> pd.DataFrame:
        """Extract company data."""
        companies_query = """
        SELECT 
            ID as COMPANY_ID,
            NAME as COMPANY_NAME,
            DOMAIN as COMPANY_DOMAIN,
            INDUSTRY as COMPANY_INDUSTRY,
            ANNUALREVENUE as COMPANY_REVENUE,
            NUMBEROFEMPLOYEES as COMPANY_SIZE,
            CITY as COMPANY_CITY,
            STATE as COMPANY_STATE,
            COUNTRY as COMPANY_COUNTRY,
            TYPE as COMPANY_TYPE
        FROM HUBSPOT_COMPANIES
        WHERE NAME IS NOT NULL
        """
        
        try:
            companies_df = pd.DataFrame(loader.execute_query(companies_query))
            logger.info(f"Extracted {len(companies_df)} companies")
            return companies_df
        except Exception as e:
            logger.warning(f"Could not extract company data: {e}")
            return pd.DataFrame()
    
    def _merge_deal_data(self, contacts_df: pd.DataFrame, deals_df: pd.DataFrame) -> pd.DataFrame:
        """Merge deal data with contacts."""
        # For now, we'll aggregate deal data per contact
        # In a real scenario, you'd need association data to properly link contacts to deals
        
        # Create deal summary features
        deal_summary = deals_df.groupby('DEAL_OWNER_ID').agg({
            'DEAL_ID': 'count',
            'AMOUNT': ['sum', 'mean', 'max'],
            'HS_DEAL_STAGE_PROBABILITY': 'mean'
        }).reset_index()
        
        # Flatten column names
        deal_summary.columns = [
            'HUBSPOT_OWNER_ID',
            'TOTAL_DEALS',
            'TOTAL_DEAL_VALUE',
            'AVG_DEAL_VALUE', 
            'MAX_DEAL_VALUE',
            'AVG_DEAL_PROBABILITY'
        ]
        
        # Merge with contacts - handle missing HUBSPOT_OWNER_ID gracefully
        if 'HUBSPOT_OWNER_ID' in contacts_df.columns:
            contacts_df = contacts_df.merge(deal_summary, on='HUBSPOT_OWNER_ID', how='left')
        else:
            # Alternative approach: use company-based mapping or assign defaults
            logger.warning("HUBSPOT_OWNER_ID not found in contacts. Using alternative approach.")
            
            # Try to map by company name if available
            if 'COMPANY' in contacts_df.columns:
                # Create a simple mapping based on company patterns
                def assign_owner_by_company(company):
                    if pd.isna(company):
                        return '159289691'  # Default owner ID
                    company_lower = str(company).lower()
                    # Simple hash-based assignment for consistent owner mapping
                    hash_val = abs(hash(company_lower)) % 6
                    owner_ids = ['159289691', '159289693', '159289695', '159289694', '159215496', '159289692']
                    return owner_ids[hash_val]
                
                contacts_df['HUBSPOT_OWNER_ID'] = contacts_df['COMPANY'].apply(assign_owner_by_company)
                contacts_df = contacts_df.merge(deal_summary, on='HUBSPOT_OWNER_ID', how='left')
            else:
                # Assign default values if no company mapping possible
                logger.warning("No company data available. Assigning default deal summary values.")
                for col in ['TOTAL_DEALS', 'TOTAL_DEAL_VALUE', 'AVG_DEAL_VALUE', 'MAX_DEAL_VALUE', 'AVG_DEAL_PROBABILITY']:
                    contacts_df[col] = 0
        
        # Fill NaN values for contacts without deals
        deal_columns = ['TOTAL_DEALS', 'TOTAL_DEAL_VALUE', 'AVG_DEAL_VALUE', 'MAX_DEAL_VALUE', 'AVG_DEAL_PROBABILITY']
        contacts_df[deal_columns] = contacts_df[deal_columns].fillna(0)
        
        return contacts_df
    
    def _merge_company_data(self, contacts_df: pd.DataFrame, companies_df: pd.DataFrame) -> pd.DataFrame:
        """Merge company data with contacts."""
        # Merge on company name (simplified approach)
        contacts_df = contacts_df.merge(
            companies_df, 
            left_on='COMPANY', 
            right_on='COMPANY_NAME', 
            how='left'
        )
        return contacts_df
    
    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create engineered features for machine learning.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with engineered features
        """
        df = df.copy()
        
        # Time-based features
        df = self._create_time_features(df)
        
        # Engagement features
        df = self._create_engagement_features(df)
        
        # Company features
        df = self._create_company_features(df)
        
        # Contact quality features
        df = self._create_contact_quality_features(df)
        
        # Behavioral features
        df = self._create_behavioral_features(df)
        
        return df
    
    def _create_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create time-based features."""
        # Convert date columns
        date_columns = ['CREATEDATE', 'LASTMODIFIEDDATE']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        # Days since creation
        if 'CREATEDATE' in df.columns:
            df['DAYS_SINCE_CREATED'] = (datetime.now() - df['CREATEDATE']).dt.days
            df['CREATED_MONTH'] = df['CREATEDATE'].dt.month
            df['CREATED_QUARTER'] = df['CREATEDATE'].dt.quarter
            df['CREATED_WEEKDAY'] = df['CREATEDATE'].dt.dayofweek
        
        # Days since last modification
        if 'LASTMODIFIEDDATE' in df.columns:
            df['DAYS_SINCE_MODIFIED'] = (datetime.now() - df['LASTMODIFIEDDATE']).dt.days
            
        # Time between creation and last modification
        if 'CREATEDATE' in df.columns and 'LASTMODIFIEDDATE' in df.columns:
            df['DAYS_CREATED_TO_MODIFIED'] = (df['LASTMODIFIEDDATE'] - df['CREATEDATE']).dt.days
        
        # Drop original datetime columns to avoid type conflicts in ML models
        datetime_columns = ['CREATEDATE', 'LASTMODIFIEDDATE', 'ETL_LOADED_AT']
        columns_to_drop = [col for col in datetime_columns if col in df.columns]
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)
        
        return df
    
    def _create_engagement_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create engagement-related features."""
        # Email domain analysis
        if 'EMAIL' in df.columns:
            df['EMAIL_DOMAIN'] = df['EMAIL'].str.split('@').str[1]
            df['IS_BUSINESS_EMAIL'] = ~df['EMAIL_DOMAIN'].str.contains(
                'gmail|yahoo|hotmail|outlook|aol', case=False, na=False
            )
            df['EMAIL_DOMAIN_FREQUENCY'] = df.groupby('EMAIL_DOMAIN')['EMAIL_DOMAIN'].transform('count')
        
        # Lead source analysis
        if 'LEADSOURCE' in df.columns:
            df['HAS_LEAD_SOURCE'] = df['LEADSOURCE'].notna()
            
        # Analytics source features
        if 'HS_ANALYTICS_SOURCE' in df.columns:
            df['HAS_ANALYTICS_SOURCE'] = df['HS_ANALYTICS_SOURCE'].notna()
        
        return df
    
    def _create_company_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create company-related features."""
        # Company size categories
        if 'COMPANY_SIZE' in df.columns:
            company_size_filled = df['COMPANY_SIZE'].fillna(0)
            try:
                df['COMPANY_SIZE_CATEGORY'] = pd.cut(
                    company_size_filled,
                    bins=[0, 10, 50, 200, 1000, float('inf')],
                    labels=['Micro', 'Small', 'Medium', 'Large', 'Enterprise'],
                    duplicates='drop'
                )
            except ValueError:
                # Fallback for cases where all values are the same
                df['COMPANY_SIZE_CATEGORY'] = 'Micro'  # Default category
        
        # Revenue categories
        if 'COMPANY_REVENUE' in df.columns:
            company_revenue_filled = df['COMPANY_REVENUE'].fillna(0)
            try:
                df['COMPANY_REVENUE_CATEGORY'] = pd.cut(
                    company_revenue_filled,
                    bins=[0, 100000, 1000000, 10000000, 100000000, float('inf')],
                    labels=['Startup', 'Small', 'Medium', 'Large', 'Enterprise'],
                    duplicates='drop'
                )
            except ValueError:
                # Fallback for cases where all values are the same
                df['COMPANY_REVENUE_CATEGORY'] = 'Startup'  # Default category
        
        # Industry presence
        if 'COMPANY_INDUSTRY' in df.columns:
            df['HAS_INDUSTRY'] = df['COMPANY_INDUSTRY'].notna()
            industry_counts = df['COMPANY_INDUSTRY'].value_counts()
            df['INDUSTRY_FREQUENCY'] = df['COMPANY_INDUSTRY'].map(industry_counts).fillna(0)
        
        return df
    
    def _create_contact_quality_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create contact data quality features."""
        # Completeness score
        important_fields = ['FIRSTNAME', 'LASTNAME', 'EMAIL', 'PHONE', 'COMPANY', 'JOBTITLE']
        available_fields = [field for field in important_fields if field in df.columns]
        
        if available_fields:
            df['COMPLETENESS_SCORE'] = df[available_fields].notna().sum(axis=1) / len(available_fields)
        
        # Contact type indicators
        if 'EMAIL' in df.columns and 'PHONE' in df.columns:
            df['HAS_EMAIL'] = df['EMAIL'].notna()
            df['HAS_PHONE'] = df['PHONE'].notna()
            df['HAS_BOTH_CONTACT'] = df['HAS_EMAIL'] & df['HAS_PHONE']
        
        # Name quality
        if 'FIRSTNAME' in df.columns and 'LASTNAME' in df.columns:
            df['HAS_FULL_NAME'] = df['FIRSTNAME'].notna() & df['LASTNAME'].notna()
        
        return df
    
    def _create_behavioral_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create behavioral features."""
        # Lifecycle stage progression
        if 'LIFECYCLESTAGE' in df.columns:
            stage_order = {
                'subscriber': 1, 'lead': 2, 'marketingqualifiedlead': 3,
                'salesqualifiedlead': 4, 'opportunity': 5, 'customer': 6
            }
            df['LIFECYCLE_STAGE_ORDER'] = df['LIFECYCLESTAGE'].map(stage_order).fillna(0)
        
        # Deal-related behavioral features
        if 'TOTAL_DEALS' in df.columns:
            df['IS_DEAL_CREATOR'] = df['TOTAL_DEALS'] > 0
            df['HIGH_VALUE_DEALS'] = (df['MAX_DEAL_VALUE'] > df['MAX_DEAL_VALUE'].quantile(0.75))
        
        return df
    
    def prepare_for_training(self, 
                           df: pd.DataFrame,
                           target_column: str,
                           test_size: float = 0.2) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
        """
        Prepare data for machine learning training.
        
        Args:
            df: Input DataFrame
            target_column: Name of target column
            test_size: Proportion of data for testing
            
        Returns:
            Tuple of (X_train, X_test, y_train, y_test)
        """
        from sklearn.model_selection import train_test_split
        
        # Separate features and target
        if target_column not in df.columns:
            raise ValueError(f"Target column '{target_column}' not found in DataFrame")
        
        X = df.drop(columns=[target_column])
        y = df[target_column]
        
        # Remove any remaining datetime columns that might cause issues
        datetime_cols = X.select_dtypes(include=['datetime64']).columns
        if len(datetime_cols) > 0:
            logger.warning(f"Dropping remaining datetime columns: {list(datetime_cols)}")
            X = X.drop(columns=datetime_cols)
        
        # Handle categorical variables
        X = self._encode_categorical_features(X)
        
        # Handle missing values
        X = self._handle_missing_values(X)
        
        # Scale numerical features
        X = self._scale_numerical_features(X)
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42, stratify=y
        )
        
        self.feature_columns = X.columns.tolist()
        
        return X_train, X_test, y_train, y_test
    
    def _encode_categorical_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Encode categorical features."""
        df = df.copy()
        
        # Identify categorical columns
        categorical_columns = df.select_dtypes(include=['object', 'category']).columns
        
        for col in categorical_columns:
            if col not in self.encoders:
                # Use label encoding for high cardinality, one-hot for low cardinality
                unique_values = df[col].nunique()
                
                if unique_values > 10:  # High cardinality
                    encoder = LabelEncoder()
                    df[col] = encoder.fit_transform(df[col].astype(str))
                    self.encoders[col] = encoder
                else:  # Low cardinality
                    # One-hot encoding
                    dummies = pd.get_dummies(df[col], prefix=col)
                    df = pd.concat([df.drop(columns=[col]), dummies], axis=1)
                    self.encoders[col] = dummies.columns.tolist()
            else:
                # Apply existing encoder
                if isinstance(self.encoders[col], LabelEncoder):
                    df[col] = self.encoders[col].transform(df[col].astype(str))
                else:
                    # Handle one-hot encoded columns
                    dummies = pd.get_dummies(df[col], prefix=col)
                    # Ensure all expected columns are present
                    for expected_col in self.encoders[col]:
                        if expected_col not in dummies.columns:
                            dummies[expected_col] = 0
                    df = pd.concat([df.drop(columns=[col]), dummies[self.encoders[col]]], axis=1)
        
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values in the dataset."""
        df = df.copy()
        
        # Separate numerical and categorical columns
        numerical_columns = df.select_dtypes(include=[np.number]).columns
        
        # Impute numerical columns with median
        if len(numerical_columns) > 0:
            if 'numerical_imputer' not in self.encoders:
                imputer = SimpleImputer(strategy='median')
                df[numerical_columns] = imputer.fit_transform(df[numerical_columns])
                self.encoders['numerical_imputer'] = imputer
            else:
                df[numerical_columns] = self.encoders['numerical_imputer'].transform(df[numerical_columns])
        
        return df
    
    def _scale_numerical_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Scale numerical features."""
        df = df.copy()
        
        # Identify numerical columns
        numerical_columns = df.select_dtypes(include=[np.number]).columns
        
        if len(numerical_columns) > 0:
            if 'scaler' not in self.scalers:
                scaler = StandardScaler()
                df[numerical_columns] = scaler.fit_transform(df[numerical_columns])
                self.scalers['scaler'] = scaler
            else:
                df[numerical_columns] = self.scalers['scaler'].transform(df[numerical_columns])
        
        return df
    
    def create_conversion_target(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create conversion target variable based on lifecycle stage.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with conversion target
        """
        df = df.copy()
        
        if 'LIFECYCLESTAGE' in df.columns:
            # Define conversion as reaching customer or opportunity stage
            conversion_stages = ['customer', 'opportunity']
            df['CONVERTED'] = df['LIFECYCLESTAGE'].str.lower().isin(conversion_stages).astype(int)
        else:
            # Fallback: use deal data if available
            if 'TOTAL_DEALS' in df.columns:
                df['CONVERTED'] = (df['TOTAL_DEALS'] > 0).astype(int)
            else:
                logger.warning("Cannot create conversion target - no suitable columns found")
                df['CONVERTED'] = 0
        
        return df
    
    def get_feature_importance_data(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get data for feature importance analysis.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary with feature statistics
        """
        feature_stats = {}
        
        for col in df.columns:
            if df[col].dtype in [np.number]:
                feature_stats[col] = {
                    'type': 'numerical',
                    'mean': df[col].mean(),
                    'std': df[col].std(),
                    'min': df[col].min(),
                    'max': df[col].max(),
                    'missing_rate': df[col].isnull().mean()
                }
            else:
                feature_stats[col] = {
                    'type': 'categorical',
                    'unique_values': df[col].nunique(),
                    'most_frequent': df[col].mode().iloc[0] if not df[col].mode().empty else None,
                    'missing_rate': df[col].isnull().mean()
                }
        
        return feature_stats
