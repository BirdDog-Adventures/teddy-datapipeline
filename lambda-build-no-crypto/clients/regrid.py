"""
Regrid API Client for Parcel Data

This client handles interactions with the Regrid API for parcel data retrieval.
"""

import requests
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class RegridClient:
    """Client for interacting with Regrid API"""
    
    def __init__(self, api_key: str):
        """
        Initialize Regrid client
        
        Args:
            api_key: Regrid API key (JWT token)
        """
        self.api_key = api_key
        self.base_url = "https://app.regrid.com/api/v1"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def get_parcel_by_coordinates(self, lat: float, lon: float) -> Optional[Dict[str, Any]]:
        """
        Get parcel data by coordinates
        
        Args:
            lat: Latitude
            lon: Longitude
            
        Returns:
            Parcel data dictionary or None if not found
        """
        try:
            # Use the search endpoint for coordinate queries (this works!)
            url = f"{self.base_url}/search"
            params = {
                "lat": lat,
                "lon": lon,
                "limit": 1
            }
            
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get("results") and len(data["results"]) > 0:
                return data["results"][0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching parcel by coordinates: {e}")
            return None
    
    def get_parcel_by_address(self, address: str) -> Optional[Dict[str, Any]]:
        """
        Get parcel data by address
        
        Args:
            address: Street address
            
        Returns:
            Parcel data dictionary or None if not found
        """
        try:
            url = f"{self.base_url}/parcels"
            params = {
                "query": address,
                "limit": 1
            }
            
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get("results") and len(data["results"]) > 0:
                return data["results"][0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching parcel by address: {e}")
            return None
    
    def get_parcel_by_id(self, parcel_id: str) -> Optional[Dict[str, Any]]:
        """
        Get parcel data by parcel ID
        
        Args:
            parcel_id: Regrid parcel ID
            
        Returns:
            Parcel data dictionary or None if not found
        """
        try:
            url = f"{self.base_url}/parcels/{parcel_id}"
            
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Error fetching parcel by ID: {e}")
            return None
