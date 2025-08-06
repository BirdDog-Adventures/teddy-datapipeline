"""
Regrid API Client

This module provides a client for interacting with the Regrid API to fetch
parcel boundaries, ownership data, and other property information.
"""

import logging
import time
import requests

from typing import Dict, Any, Optional, List
from urllib.parse import urljoin


logger = logging.getLogger(__name__)


class RegridClient:
    """
    Client for the Regrid API to fetch parcel data and property information.
    """

    def __init__(self, api_key: str, base_url: str = "https://app.regrid.com/api/v2/"):
        """
        Initialize the Regrid API client.
        
        Args:
            api_key: Regrid API key.
            base_url: Base URL for the Regrid API.
        """
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        self.params = {
            "token": api_key,
        }
        logger.info(f"Initialized Regrid client with base URL: {base_url}")

    def _make_request(
        self, endpoint: str, method: str = "GET", params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None, retries: int = 3, retry_delay: int = 2
    ) -> Dict[str, Any]:
        """
        Make a request to the Regrid API with retry logic.
        
        Args:
            endpoint: API endpoint.
            method: HTTP method (GET, POST, etc.).
            params: Query parameters.
            data: Request body data.
            retries: Number of retry attempts.
            retry_delay: Delay between retries in seconds.
            
        Returns:
            API response as a dictionary.
            
        Raises:
            ValueError: If the API request fails after all retries.
        """
        logger.info(f"make request with base_url: {self.base_url}")
        url = urljoin(self.base_url, endpoint)
        params = params or {}
        params.update(self.params)
        logger.info(f"Make request by url {url}")
        attempt = 0
        
        while attempt < retries:
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    headers=self.session.headers,
                    json=data,
                    timeout=30,  # 30 second timeout
                )
                
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code
                
                # Handle rate limiting (429)
                if status_code == 429:
                    retry_after = int(e.response.headers.get("Retry-After", retry_delay))
                    logger.warning(f"Rate limited by Regrid API. Retrying after {retry_after} seconds.")
                    time.sleep(retry_after)
                    attempt += 1
                    continue
                    
                # Handle 5xx errors
                elif 500 <= status_code < 600:
                    logger.warning(f"Regrid API server error: {status_code}. Retrying...")
                    time.sleep(retry_delay)
                    attempt += 1
                    continue
                    
                # Other HTTP errors
                else:
                    logger.error(f"Regrid API HTTP error: {status_code}")
                    raise ValueError(f"Regrid API error: {e.response.text}")
                    
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                logger.warning(f"Regrid API connection error: {str(e)}. Retrying...")
                time.sleep(retry_delay)
                attempt += 1
                continue
                
            except Exception as e:
                logger.error(f"Unexpected error with Regrid API: {str(e)}")
                raise ValueError(f"Regrid API unexpected error: {str(e)}")
        
        # If we've exhausted all retries
        logger.error(f"Failed to connect to Regrid API after {retries} attempts")
        raise ValueError(f"Failed to connect to Regrid API after {retries} attempts")

    def search_by_address(self, address: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Search for parcels by address.
        
        Args:
            address: Property address to search for.
            limit: Maximum number of results to return.
            
        Returns:
            List of matching parcels.
        """
        logger.info(f"Searching Regrid for address: {address}")
        
        endpoint = "parcels/address"
        params = {
            "query": address,
            "limit": limit,
        }
        
        try:
            response = self._make_request(endpoint, params=params)
            logger.info(f"Found parcels for address: {address}")
            return response
        except Exception as e:
            logger.error(f"Error searching Regrid by address: {str(e)}")
            raise ValueError(f"Failed to search by address: {str(e)}")

    def get_parcel_by_id(self, parcel_id: str) -> Dict[str, Any]:
        """
        Get parcel details by Regrid parcel ID.
        
        Args:
            parcel_id: Regrid parcel ID.
            
        Returns:
            Parcel details.
        """
        logger.info(f"Fetching Regrid parcel details for ID: {parcel_id}")
        
        endpoint = "apn"

        params = {
            "parcelnumb": parcel_id,
        }
        
        try:
            response = self._make_request(endpoint, params=params)
            logger.info(f"Successfully retrieved details for parcel ID: {parcel_id}")
            return response
        except Exception as e:
            logger.error(f"Error fetching Regrid parcel by ID: {str(e)}")
            raise ValueError(f"Failed to fetch parcel by ID: {str(e)}")

    def search_by_coordinates(
        self, latitude: float, longitude: float
    ) -> Dict[str, Any]:
        """
        Find the parcel at the specified coordinates.
        
        Args:
            latitude: Latitude coordinate.
            longitude: Longitude coordinate.
            
        Returns:
            Parcel at the specified coordinates.
        """
        logger.info(f"Searching Regrid for coordinates: ({latitude}, {longitude})")
        
        endpoint = "parcels/point"
        params = {
            "lat": latitude,
            "lon": longitude,
        }
        
        try:
            response = self._make_request(endpoint, params=params)
            logger.info(f"Found parcel at coordinates: ({latitude}, {longitude})")
            return response
        except Exception as e:
            logger.error(f"Error searching Regrid by coordinates: {str(e)}")
            raise ValueError(f"Failed to search by coordinates: {str(e)}")
