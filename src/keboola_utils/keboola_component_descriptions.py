"""
Component descriptions for Keboola components.
Fetches component information dynamically from the Keboola API.

Usage:
    from keboola_component_descriptions import ComponentDescriptionManager
    
    # Get a component description
    manager = ComponentDescriptionManager()
    description = manager.get_description('component.id')
"""

import os
import logging
import sys
import requests
from typing import Dict, List
from dotenv import load_dotenv

# Add the parent directory to sys.path to import the KeboolaClient
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set up logging
LOG = logging.getLogger(__name__)


class ComponentDescriptionManager:
    """
    Manages component descriptions from Keboola API
    """

    def __init__(self):
        """Initialize the component description manager and load environment variables"""
        self._cache = None
        load_dotenv(verbose=True)


    def _get_components_from_api(self, token: str, base_url: str) -> List[Dict]:
        """
        Fetch components directly from the Keboola API endpoint.
        
        Args:
            token: Keboola Storage API token
            base_url: Keboola base URL
            
        Returns:
            List of component dictionaries
        """
        url = f"{base_url}/v2/storage"
        headers = {"X-StorageApi-Token": token}
        
        LOG.info(f"Fetching components from API endpoint: {url}")
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            components = data.get('components', [])
            
            LOG.info(f"Retrieved {len(components)} components from API")
            return components
        except Exception as e:
            LOG.error(f"Error fetching components from API: {e}")
            return []


    def _fetch_component_list(self) -> Dict[str, str]:
        """
        Fetch component definitions from the Keboola API.
        
        Returns:
            A dictionary mapping component IDs to their descriptions
        """
        # Get Keboola API token and base URL
        token = os.getenv('KEBOOLA_API_TOKEN')
        base_url = os.getenv('KEBOOLA_PROJECT_URL', '').split('/admin')[0]
        
        if not token or not base_url:
            LOG.warning("Missing Keboola API credentials, returning empty component descriptions")
            return {}
        
        try:
            LOG.info("Fetching component list from API")
            
            # Call the API directly
            components = self._get_components_from_api(token, base_url)
            
            # Create a mapping of component ID to description
            component_map = {}
            for component in components:
                component_id = component.get('id')
                description = component.get('description', '')
                name = component.get('name', '')
                
                # Use description if available, otherwise use name
                component_map[component_id] = description if description else name
            
            LOG.info(f"Successfully fetched {len(component_map)} component descriptions")
            return component_map
        
        except Exception as e:
            LOG.error(f"Error fetching component list: {e}")
            return {}


    def get_description(self, component_id: str) -> str:
        """
        Get a human-readable description for a component based on its ID.
        Fetches the component list from the API on first call.
        
        Args:
            component_id: The Keboola component ID
            
        Returns:
            A human-readable description of the component or default message if not found
        """
        # Initialize cache if it's the first call
        if self._cache is None:
            self._cache = self._fetch_component_list()
        
        # Return description from cache or default message
        return self._cache.get(component_id, f"Component {component_id} (no description available)")
