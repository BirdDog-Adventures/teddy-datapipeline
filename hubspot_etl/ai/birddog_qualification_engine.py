"""
BirdDog Qualification Engine

This module implements business-specific qualification logic for BirdDog's
agricultural tax consulting services, including IRS Section 180 eligibility
and customer value estimation.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
import logging
import re
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class BirdDogQualificationEngine:
    """
    Business rule engine for BirdDog's agricultural tax consulting qualification.
    
    Implements IRS Section 180 eligibility requirements and customer value estimation
    based on residual fertility tax benefits analysis.
    """
    
    def __init__(self):
        self.agricultural_keywords = [
            'farm', 'farming', 'ranch', 'ranching', 'agriculture', 'agricultural',
            'livestock', 'cattle', 'dairy', 'crop', 'crops', 'grain', 'wheat',
            'corn', 'soybean', 'cotton', 'hay', 'pasture', 'feedlot', 'agri',
            'rural', 'country', 'acreage', 'acres', 'land', 'soil', 'harvest',
            'plantation', 'orchard', 'vineyard', 'greenhouse', 'nursery'
        ]
        
        self.landowner_indicators = [
            'landowner', 'land owner', 'property owner', 'farm owner', 'ranch owner',
            'estate', 'holdings', 'properties', 'real estate', 'development',
            'investment', 'trust', 'family farm', 'generations'
        ]
        
        # Agricultural regions (simplified - can be enhanced with actual geographic data)
        self.agricultural_states = {
            'IA': 10, 'IL': 10, 'NE': 10, 'IN': 9, 'OH': 9, 'MN': 9,
            'KS': 8, 'MO': 8, 'WI': 8, 'MI': 8, 'TX': 7, 'CA': 7,
            'PA': 7, 'NY': 6, 'ND': 9, 'SD': 9, 'OK': 6, 'AR': 6,
            'KY': 6, 'TN': 6, 'NC': 6, 'GA': 6, 'FL': 5, 'AL': 6,
            'MS': 6, 'LA': 6, 'SC': 6, 'VA': 6, 'WV': 5, 'MD': 5
        }
        
    def check_irs_section_180_eligibility(self, lead_data: pd.Series) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if lead meets IRS Section 180 requirements for residual fertility deductions.
        
        Args:
            lead_data: Series containing lead information
            
        Returns:
            Tuple of (eligible, qualification_details)
        """
        rules = {
            'land_ownership': self.verify_ownership_status(lead_data),
            'business_use': self.verify_business_operations(lead_data),
            'acquisition_timing': self.verify_acquisition_timing(lead_data),
            'geographic_eligibility': self.verify_agricultural_region(lead_data)
        }
        
        # All rules must pass for eligibility
        eligible = all([rules[key]['passes'] for key in rules])
        
        qualification_details = {
            'eligible': eligible,
            'rules': rules,
            'confidence_score': self.calculate_eligibility_confidence(rules),
            'qualification_tier': self.determine_qualification_tier(rules)
        }
        
        return eligible, qualification_details
    
    def verify_ownership_status(self, lead_data: pd.Series) -> Dict[str, Any]:
        """Verify likelihood of land ownership based on available data."""
        ownership_score = 0
        indicators = []
        
        # Check company name for ownership indicators
        company = str(lead_data.get('COMPANY', '')).lower()
        for indicator in self.landowner_indicators:
            if indicator in company:
                ownership_score += 30
                indicators.append(f"Company name contains '{indicator}'")
        
        # Check email domain patterns
        email = str(lead_data.get('EMAIL', '')).lower()
        if any(pattern in email for pattern in ['@farm', '@ranch', '@agri', '@land']):
            ownership_score += 25
            indicators.append("Email domain suggests agricultural/land business")
        
        # Check for business email (not consumer)
        if not any(domain in email for domain in ['gmail.', 'yahoo.', 'hotmail.', 'outlook.']):
            ownership_score += 15
            indicators.append("Business email address")
        
        # Check job title if available
        job_title = str(lead_data.get('JOBTITLE', '')).lower()
        if any(title in job_title for title in ['owner', 'president', 'ceo', 'founder']):
            ownership_score += 20
            indicators.append("Executive/ownership role")
        
        return {
            'passes': ownership_score >= 30,
            'score': min(ownership_score, 100),
            'indicators': indicators,
            'confidence': 'high' if ownership_score >= 60 else 'medium' if ownership_score >= 30 else 'low'
        }
    
    def verify_business_operations(self, lead_data: pd.Series) -> Dict[str, Any]:
        """Verify active agricultural business operations."""
        business_score = 0
        indicators = []
        
        # Check company name for agricultural keywords
        company = str(lead_data.get('COMPANY', '')).lower()
        agricultural_matches = sum(1 for keyword in self.agricultural_keywords if keyword in company)
        if agricultural_matches > 0:
            business_score += min(agricultural_matches * 15, 50)
            indicators.append(f"Company name contains {agricultural_matches} agricultural terms")
        
        # Check industry classification if available
        industry = str(lead_data.get('COMPANY_INDUSTRY', '')).lower()
        if any(term in industry for term in ['agriculture', 'farming', 'food', 'livestock']):
            business_score += 40
            indicators.append("Industry classified as agricultural")
        
        # Check company size (larger operations more likely to qualify)
        company_size = lead_data.get('COMPANY_SIZE', 0)
        if isinstance(company_size, (int, float)) and company_size > 10:
            business_score += 15
            indicators.append("Substantial company size")
        
        return {
            'passes': business_score >= 25,
            'score': min(business_score, 100),
            'indicators': indicators,
            'confidence': 'high' if business_score >= 60 else 'medium' if business_score >= 25 else 'low'
        }
    
    def verify_acquisition_timing(self, lead_data: pd.Series) -> Dict[str, Any]:
        """Verify acquisition timing for tax benefit eligibility."""
        timing_score = 0
        indicators = []
        current_year = datetime.now().year
        
        # Check creation date as proxy for acquisition timing
        create_date = lead_data.get('CREATEDATE')
        if pd.notna(create_date):
            if isinstance(create_date, str):
                try:
                    create_date = pd.to_datetime(create_date)
                except:
                    create_date = None
            
            if create_date:
                years_since_creation = (datetime.now() - create_date).days / 365.25
                
                if years_since_creation <= 1:
                    timing_score += 50
                    indicators.append("Very recent contact creation (likely recent acquisition)")
                elif years_since_creation <= 2:
                    timing_score += 35
                    indicators.append("Recent contact creation")
                elif years_since_creation <= 3:
                    timing_score += 20
                    indicators.append("Moderately recent contact")
                else:
                    timing_score += 5
                    indicators.append("Older contact")
        
        # Default moderate score if no timing data available
        if not indicators:
            timing_score = 30
            indicators.append("No timing data available - assuming moderate eligibility")
        
        return {
            'passes': timing_score >= 20,
            'score': timing_score,
            'indicators': indicators,
            'confidence': 'high' if timing_score >= 40 else 'medium' if timing_score >= 20 else 'low'
        }
    
    def verify_agricultural_region(self, lead_data: pd.Series) -> Dict[str, Any]:
        """Verify geographic location in agricultural regions."""
        geo_score = 0
        indicators = []
        
        # Check state
        state = str(lead_data.get('COMPANY_STATE', '')).upper()
        if state in self.agricultural_states:
            state_score = self.agricultural_states[state]
            geo_score += state_score * 10
            indicators.append(f"Located in agricultural state {state} (score: {state_score}/10)")
        
        # Check city for rural indicators
        city = str(lead_data.get('COMPANY_CITY', '')).lower()
        rural_indicators = ['farm', 'rural', 'county', 'valley', 'plains', 'prairie']
        if any(indicator in city for indicator in rural_indicators):
            geo_score += 20
            indicators.append("City name suggests rural/agricultural area")
        
        # Default moderate score for unknown locations
        if geo_score == 0:
            geo_score = 40
            indicators.append("Location unknown - assuming moderate agricultural potential")
        
        return {
            'passes': geo_score >= 30,
            'score': min(geo_score, 100),
            'indicators': indicators,
            'confidence': 'high' if geo_score >= 70 else 'medium' if geo_score >= 30 else 'low'
        }
    
    def calculate_eligibility_confidence(self, rules: Dict[str, Dict]) -> float:
        """Calculate overall confidence in eligibility assessment."""
        confidence_weights = {
            'land_ownership': 0.4,
            'business_use': 0.3,
            'acquisition_timing': 0.2,
            'geographic_eligibility': 0.1
        }
        
        total_confidence = 0
        for rule_name, weight in confidence_weights.items():
            rule_score = rules[rule_name]['score']
            total_confidence += (rule_score / 100) * weight
        
        return total_confidence
    
    def determine_qualification_tier(self, rules: Dict[str, Dict]) -> str:
        """Determine qualification tier based on rule scores."""
        total_score = sum(rules[rule]['score'] for rule in rules) / len(rules)
        
        if total_score >= 80:
            return 'Premium'
        elif total_score >= 60:
            return 'High'
        elif total_score >= 40:
            return 'Medium'
        elif total_score >= 20:
            return 'Low'
        else:
            return 'Disqualified'
    
    def calculate_customer_value(self, lead_data: pd.Series, qualification_details: Dict) -> Dict[str, Any]:
        """
        Calculate potential customer value based on BirdDog's tax benefit model.
        
        Based on business documents:
        - $750-$1,500 per acre in deductions
        - 35% blended tax rate
        - Service tied to property value and acreage
        """
        # Estimate acreage based on available data
        estimated_acres = self.estimate_acreage(lead_data)
        
        # Calculate deduction per acre (average $1,125)
        base_deduction_per_acre = 1125
        
        # Adjust based on qualification tier
        tier_multipliers = {
            'Premium': 1.3,
            'High': 1.1,
            'Medium': 1.0,
            'Low': 0.8,
            'Disqualified': 0
        }
        
        tier = qualification_details.get('qualification_tier', 'Medium')
        deduction_per_acre = base_deduction_per_acre * tier_multipliers.get(tier, 1.0)
        
        # Calculate tax savings (35% blended rate from documents)
        tax_rate = 0.35
        total_deduction = estimated_acres * deduction_per_acre
        tax_savings = total_deduction * tax_rate
        
        # Estimate service revenue (assume 10-15% of tax savings)
        service_revenue = tax_savings * 0.125  # 12.5% average
        
        return {
            'estimated_acres': estimated_acres,
            'deduction_per_acre': deduction_per_acre,
            'total_deduction': total_deduction,
            'estimated_tax_savings': tax_savings,
            'service_revenue_potential': service_revenue,
            'value_tier': self.classify_value_tier(tax_savings),
            'confidence_score': qualification_details.get('confidence_score', 0.5)
        }
    
    def estimate_acreage(self, lead_data: pd.Series) -> float:
        """Estimate property acreage based on available indicators."""
        base_acres = 100  # Conservative baseline
        
        # Adjust based on company size
        company_size = lead_data.get('COMPANY_SIZE', 0)
        if isinstance(company_size, (int, float)):
            if company_size > 100:
                base_acres = 500
            elif company_size > 50:
                base_acres = 300
            elif company_size > 20:
                base_acres = 200
        
        # Adjust based on company revenue
        revenue = lead_data.get('COMPANY_REVENUE', 0)
        if isinstance(revenue, (int, float)):
            if revenue > 10000000:  # $10M+
                base_acres = 1000
            elif revenue > 5000000:  # $5M+
                base_acres = 600
            elif revenue > 1000000:  # $1M+
                base_acres = 300
        
        # Add randomization for realistic estimates
        variance = np.random.uniform(0.7, 1.5)
        return max(50, base_acres * variance)  # Minimum 50 acres
    
    def classify_value_tier(self, tax_savings: float) -> str:
        """Classify customer value tier based on estimated tax savings."""
        if tax_savings >= 100000:  # $100K+
            return 'Enterprise'
        elif tax_savings >= 50000:  # $50K+
            return 'High-Value'
        elif tax_savings >= 20000:  # $20K+
            return 'Medium-Value'
        elif tax_savings >= 5000:   # $5K+
            return 'Standard'
        else:
            return 'Low-Value'
    
    def calculate_birddog_lead_score(self, lead_data: pd.Series) -> Dict[str, Any]:
        """
        Calculate comprehensive BirdDog-specific lead score.
        
        Returns:
            Dictionary with score, qualification details, and value estimation
        """
        # Check eligibility
        eligible, qualification_details = self.check_irs_section_180_eligibility(lead_data)
        
        # Calculate value
        value_details = self.calculate_customer_value(lead_data, qualification_details)
        
        # Calculate final score (0-100)
        base_score = 0
        
        if not eligible:
            final_score = 0
        else:
            # Qualification score (60% weight)
            qual_score = sum(qualification_details['rules'][rule]['score'] 
                           for rule in qualification_details['rules']) / len(qualification_details['rules'])
            
            # Value score (40% weight)
            value_score = min(100, (value_details['estimated_tax_savings'] / 50000) * 100)
            
            final_score = (qual_score * 0.6) + (value_score * 0.4)
        
        return {
            'birddog_score': final_score,
            'eligible': eligible,
            'qualification_tier': qualification_details['qualification_tier'],
            'value_tier': value_details['value_tier'],
            'estimated_tax_savings': value_details['estimated_tax_savings'],
            'service_revenue_potential': value_details['service_revenue_potential'],
            'confidence_score': qualification_details['confidence_score'],
            'qualification_details': qualification_details,
            'value_details': value_details
        }


def enhance_dataframe_with_birddog_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enhance DataFrame with BirdDog-specific qualification and scoring features.
    
    Args:
        df: Input DataFrame with lead data
        
    Returns:
        Enhanced DataFrame with BirdDog business features
    """
    engine = BirdDogQualificationEngine()
    
    # Initialize new columns
    birddog_features = []
    
    logger.info(f"Enhancing {len(df)} leads with BirdDog qualification features...")
    
    for idx, row in df.iterrows():
        try:
            birddog_analysis = engine.calculate_birddog_lead_score(row)
            birddog_features.append(birddog_analysis)
        except Exception as e:
            logger.warning(f"Error processing lead {idx}: {str(e)}")
            # Default values for failed processing
            birddog_features.append({
                'birddog_score': 0,
                'eligible': False,
                'qualification_tier': 'Disqualified',
                'value_tier': 'Low-Value',
                'estimated_tax_savings': 0,
                'service_revenue_potential': 0,
                'confidence_score': 0
            })
    
    # Add features to DataFrame - only include scalar values
    scalar_features = []
    for feature_dict in birddog_features:
        scalar_dict = {
            'birddog_score': feature_dict.get('birddog_score', 0),
            'eligible': feature_dict.get('eligible', False),
            'qualification_tier': feature_dict.get('qualification_tier', 'Disqualified'),
            'value_tier': feature_dict.get('value_tier', 'Low-Value'),
            'estimated_tax_savings': feature_dict.get('estimated_tax_savings', 0),
            'service_revenue_potential': feature_dict.get('service_revenue_potential', 0),
            'confidence_score': feature_dict.get('confidence_score', 0)
        }
        scalar_features.append(scalar_dict)
    
    birddog_df = pd.DataFrame(scalar_features)
    
    # Merge with original DataFrame
    enhanced_df = pd.concat([df.reset_index(drop=True), birddog_df], axis=1)
    
    logger.info(f"BirdDog enhancement complete. Added {len(birddog_df.columns)} new features.")
    
    return enhanced_df