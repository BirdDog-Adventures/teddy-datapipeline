"""
HubSpot API Client

Provides authentication and data extraction capabilities for HubSpot CRM API.
Supports both OAuth and API Key authentication methods.
"""

import os
import time
import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json

logger = logging.getLogger(__name__)


class HubSpotAuthError(Exception):
    """Raised when authentication fails"""
    pass


class HubSpotAPIError(Exception):
    """Raised when API requests fail"""
    pass


class HubSpotClient:
    """
    HubSpot API Client with comprehensive authentication and data extraction capabilities.
    
    Supports:
    - API Key authentication
    - OAuth 2.0 authentication
    - Rate limiting and retry logic
    - Pagination handling
    - Comprehensive error handling
    """
    
    BASE_URL = "https://api.hubapi.com"
    
    # API endpoints for different object types
    ENDPOINTS = {
        'contacts': '/crm/v3/objects/contacts',
        'companies': '/crm/v3/objects/companies',
        'deals': '/crm/v3/objects/deals',
        'tickets': '/crm/v3/objects/tickets',
        'products': '/crm/v3/objects/products',
        'line_items': '/crm/v3/objects/line_items',
        'quotes': '/crm/v3/objects/quotes',
        'calls': '/crm/v3/objects/calls',
        'emails': '/crm/v3/objects/emails',
        'meetings': '/crm/v3/objects/meetings',
        'notes': '/crm/v3/objects/notes',
        'tasks': '/crm/v3/objects/tasks'
    }
    
    # Default properties to fetch for each object type
    DEFAULT_PROPERTIES = {
        'contacts': [
            'firstname', 'lastname', 'email', 'phone', 'company', 'website',
            'jobtitle', 'lifecyclestage', 'lead_status', 'hs_lead_status',
            'createdate', 'lastmodifieddate', 'hs_analytics_source',
            'hs_analytics_source_data_1', 'hs_analytics_source_data_2',
            'hubspot_owner_id', 'hubspot_owner_assigneddate', 'hubspot_team_id',
            'hs_all_owner_ids'
        ],
        'companies': [
            'name', 'domain', 'industry', 'phone', 'city', 'state', 'country',
            'numberofemployees', 'annualrevenue', 'type', 'website',
            'createdate', 'hs_lastmodifieddate', 'lifecyclestage'
        ],
        'deals': [
            'dealname', 'amount', 'dealstage', 'pipeline', 'closedate',
            'createdate', 'hs_lastmodifieddate', 'hubspot_owner_id',
            'dealtype', 'hs_analytics_source', 'hs_deal_stage_probability'
        ],
        'tickets': [
            'subject', 'content', 'hs_pipeline', 'hs_pipeline_stage',
            'hs_ticket_priority', 'hs_ticket_category', 'hs_ticket_id',
            'createdate', 'hs_lastmodifieddate', 'hubspot_owner_id'
        ]
    }
    
    def __init__(self, 
                 api_key: Optional[str] = None,
                 access_token: Optional[str] = None,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 refresh_token: Optional[str] = None,
                 rate_limit_delay: float = 0.1):
        """
        Initialize HubSpot client with authentication credentials.
        
        Args:
            api_key: HubSpot API key (for API key authentication)
            access_token: OAuth access token
            client_id: OAuth client ID
            client_secret: OAuth client secret
            refresh_token: OAuth refresh token
            rate_limit_delay: Delay between requests to respect rate limits
        """
        self.api_key = api_key or os.getenv('HUBSPOT_API_KEY')
        self.access_token = access_token or os.getenv('HUBSPOT_ACCESS_TOKEN')
        self.client_id = client_id or os.getenv('HUBSPOT_CLIENT_ID')
        self.client_secret = client_secret or os.getenv('HUBSPOT_CLIENT_SECRET')
        self.refresh_token = refresh_token or os.getenv('HUBSPOT_REFRESH_TOKEN')
        self.rate_limit_delay = rate_limit_delay
        
        # Token expiration tracking
        self.token_expires_at = None
        
        # Setup session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Validate authentication
        self._validate_auth()
        
    def _validate_auth(self):
        """Validate that we have proper authentication credentials"""
        if not self.api_key and not self.access_token:
            raise HubSpotAuthError(
                "Either api_key or access_token must be provided"
            )
            
        if self.access_token and not self.refresh_token:
            logger.warning(
                "Access token provided without refresh token. "
                "Token refresh will not be possible."
            )
    
    def _get_headers(self) -> Dict[str, str]:
        """Get appropriate headers for API requests"""
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'BirdDog-HubSpot-ETL/1.0'
        }
        
        if self.access_token:
            headers['Authorization'] = f'Bearer {self.access_token}'
        elif self.api_key:
            # API key will be added as query parameter
            pass
        
        return headers
    
    def _refresh_access_token(self):
        """Refresh OAuth access token using refresh token"""
        if not self.refresh_token or not self.client_id or not self.client_secret:
            raise HubSpotAuthError(
                "Refresh token, client_id, and client_secret required for token refresh"
            )
        
        url = f"{self.BASE_URL}/oauth/v1/token"
        data = {
            'grant_type': 'refresh_token',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': self.refresh_token
        }
        
        response = requests.post(url, data=data)
        
        if response.status_code != 200:
            raise HubSpotAuthError(f"Token refresh failed: {response.text}")
        
        token_data = response.json()
        self.access_token = token_data['access_token']
        self.refresh_token = token_data.get('refresh_token', self.refresh_token)
        
        # Set expiration time (subtract 5 minutes for safety)
        expires_in = token_data.get('expires_in', 3600)
        self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 300)
        
        logger.info("Access token refreshed successfully")
    
    def _check_token_expiry(self):
        """Check if access token needs to be refreshed"""
        if (self.token_expires_at and 
            datetime.now() >= self.token_expires_at and 
            self.refresh_token):
            self._refresh_access_token()
    
    def _make_request(self, 
                     method: str, 
                     endpoint: str, 
                     params: Optional[Dict] = None,
                     data: Optional[Dict] = None) -> requests.Response:
        """
        Make authenticated request to HubSpot API with error handling and rate limiting.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: Query parameters
            data: Request body data
            
        Returns:
            Response object
        """
        # Check token expiry for OAuth
        if self.access_token:
            self._check_token_expiry()
        
        url = f"{self.BASE_URL}{endpoint}"
        headers = self._get_headers()
        
        # Add API key to params if using API key auth
        if self.api_key and not self.access_token:
            params = params or {}
            params['hapikey'] = self.api_key
        
        # Rate limiting
        time.sleep(self.rate_limit_delay)
        
        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=data,
                timeout=30
            )
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 1))
                logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                time.sleep(retry_after)
                return self._make_request(method, endpoint, params, data)
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            raise HubSpotAPIError(f"API request failed: {str(e)}")
    
    def get_objects(self, 
                   object_type: str,
                   properties: Optional[List[str]] = None,
                   limit: int = 100,
                   after: Optional[str] = None,
                   associations: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Get objects from HubSpot CRM.
        
        Args:
            object_type: Type of object (contacts, companies, deals, etc.)
            properties: List of properties to retrieve
            limit: Number of objects per page (max 100)
            after: Pagination cursor
            associations: List of associated objects to include
            
        Returns:
            API response containing objects and pagination info
        """
        if object_type not in self.ENDPOINTS:
            raise ValueError(f"Unsupported object type: {object_type}")
        
        endpoint = self.ENDPOINTS[object_type]
        
        # Use default properties if none specified
        if properties is None:
            properties = self.DEFAULT_PROPERTIES.get(object_type, [])
        
        params = {
            'limit': min(limit, 100),  # HubSpot max is 100
            'properties': ','.join(properties) if properties else ''
        }
        
        if after:
            params['after'] = after
            
        if associations:
            params['associations'] = ','.join(associations)
        
        response = self._make_request('GET', endpoint, params=params)
        return response.json()
    
    def get_all_objects(self, 
                       object_type: str,
                       properties: Optional[List[str]] = None,
                       associations: Optional[List[str]] = None,
                       max_results: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get all objects of a specific type, handling pagination automatically.
        
        Args:
            object_type: Type of object to retrieve
            properties: List of properties to retrieve
            associations: List of associated objects to include
            max_results: Maximum number of results to return
            
        Returns:
            List of all objects
        """
        all_objects = []
        after = None
        retrieved_count = 0
        
        while True:
            # Calculate limit for this request
            remaining = max_results - retrieved_count if max_results else None
            limit = min(100, remaining) if remaining else 100
            
            if limit <= 0:
                break
            
            response = self.get_objects(
                object_type=object_type,
                properties=properties,
                limit=limit,
                after=after,
                associations=associations
            )
            
            objects = response.get('results', [])
            all_objects.extend(objects)
            retrieved_count += len(objects)
            
            # Check pagination
            paging = response.get('paging', {})
            after = paging.get('next', {}).get('after')
            
            if not after or (max_results and retrieved_count >= max_results):
                break
            
            logger.info(f"Retrieved {retrieved_count} {object_type} so far...")
        
        logger.info(f"Retrieved total of {len(all_objects)} {object_type}")
        return all_objects
    
    def get_recently_modified_objects(self,
                                    object_type: str,
                                    since: datetime,
                                    properties: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get objects modified since a specific date.
        
        Args:
            object_type: Type of object to retrieve
            since: DateTime to filter from
            properties: List of properties to retrieve
            
        Returns:
            List of recently modified objects
        """
        # Convert datetime to timestamp (milliseconds)
        since_timestamp = int(since.timestamp() * 1000)
        
        # Use search API for date filtering
        search_endpoint = f"/crm/v3/objects/{object_type}/search"
        
        search_data = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "hs_lastmodifieddate",
                    "operator": "GTE",
                    "value": since_timestamp
                }]
            }],
            "properties": properties or self.DEFAULT_PROPERTIES.get(object_type, []),
            "limit": 100
        }
        
        all_objects = []
        after = None
        
        while True:
            if after:
                search_data["after"] = after
            
            response = self._make_request('POST', search_endpoint, data=search_data)
            result = response.json()
            
            objects = result.get('results', [])
            all_objects.extend(objects)
            
            # Check pagination
            paging = result.get('paging', {})
            after = paging.get('next', {}).get('after')
            
            if not after:
                break
        
        logger.info(f"Retrieved {len(all_objects)} {object_type} modified since {since}")
        return all_objects
    
    def get_object_by_id(self, 
                        object_type: str, 
                        object_id: str,
                        properties: Optional[List[str]] = None,
                        associations: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Get a specific object by ID.
        
        Args:
            object_type: Type of object
            object_id: Object ID
            properties: List of properties to retrieve
            associations: List of associated objects to include
            
        Returns:
            Object data
        """
        if object_type not in self.ENDPOINTS:
            raise ValueError(f"Unsupported object type: {object_type}")
        
        endpoint = f"{self.ENDPOINTS[object_type]}/{object_id}"
        
        params = {}
        if properties:
            params['properties'] = ','.join(properties)
        if associations:
            params['associations'] = ','.join(associations)
        
        response = self._make_request('GET', endpoint, params=params)
        return response.json()
    
    def test_connection(self) -> bool:
        """
        Test the connection to HubSpot API.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            # Try to get account info
            response = self._make_request('GET','/crm/v3/objects/contacts')
            #response = self._make_request('GET', '/account-info/v3/api-usage/daily')
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
    
    def get_api_usage(self) -> Dict[str, Any]:
        """
        Get current API usage statistics.
        
        Returns:
            API usage data
        """
        response = self._make_request('GET', '/account-info/v3/api-usage/daily')
        return response.json()
    
    def get_properties(self, object_type: str) -> List[Dict[str, Any]]:
        """
        Get all available properties for an object type.
        
        Args:
            object_type: Type of object
            
        Returns:
            List of property definitions
        """
        endpoint = f"/crm/v3/properties/{object_type}"
        response = self._make_request('GET', endpoint)
        return response.json().get('results', [])
