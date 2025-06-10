"""
Component descriptions for Keboola components.
Fetches component information dynamically from the Keboola API.

Usage:
    from component_descriptions import ComponentDescriptionManager
    
    # Get a component description
    manager = ComponentDescriptionManager()
    description = manager.get_description('component.id')
"""

import os
import logging
import requests

# Set up logging and initialize Keboola API configuration from environment
LOG = logging.getLogger(__name__)
KEBOOLA_API_TOKEN = os.getenv('KEBOOLA_API_TOKEN')
KEBOOLA_BASE_URL = os.getenv('KEBOOLA_PROJECT_URL', '').split('/admin')[0]


class ComponentDescriptionManager:
    """
    Manages component descriptions from Keboola API
    """

    def __init__(self):
        """Initialize the component description manager"""
        self._cache = None
        
        self._token = KEBOOLA_API_TOKEN
        self._base_url = KEBOOLA_BASE_URL
        self._headers = {"X-StorageApi-Token": self._token} if self._token else {}


    def _get_components_from_api(self, base_url: str) -> list[dict]:
        """
        Fetch components directly from the Keboola API endpoint.
        
        Args:
            base_url: Keboola base URL
            
        Returns:
            List of component dictionaries
        """
        url = f"{base_url}/v2/storage"
        
        LOG.info(f"Fetching components from API endpoint: {url}")
        
        try:
            response = requests.get(url, headers=self._headers)
            response.raise_for_status()
            
            data = response.json()
            components = data.get('components', [])
            
            LOG.info(f"Retrieved {len(components)} components from API")
            return components
        except Exception as e:
            LOG.error(f"Error fetching components from API: {e}")
            return []


    def _fetch_component_list(self) -> dict[str, str]:
        """
        Fetch component definitions from the Keboola API.
        
        Returns:
            A dictionary mapping component IDs to their descriptions
        """
        if not self._token or not self._base_url:
            LOG.warning("Missing Keboola API credentials, returning empty component descriptions")
            return {}
        
        try:
            LOG.info("Fetching component list from API")
            components = self._get_components_from_api(self._base_url)
            
            # Create a mapping of component ID to description
            component_map = {}
            for component in components:
                component_id = component.get('id')
                if not component_id:
                    LOG.warning("Found component without ID in API response, skipping")
                    continue
                
                # Try to get the best available description in order of preference:
                # 1. long_description (most detailed)
                # 2. description (shorter summary)
                # 3. documentation (if available)
                # 4. name (fallback)
                # 5. component ID (last resort)
                description = (
                    component.get('longDescription') or 
                    component.get('description') or 
                    component.get('documentation') or 
                    component.get('name') or 
                    f"Component {component_id}"
                )
                
                # Store additional metadata that might be useful
                component_info = {
                    'description': description,
                    'name': component.get('name', ''),
                    'long_description': component.get('longDescription', ''),
                    'documentation_url': component.get('documentationUrl', '')
                }
                
                # Log warning if component has no meaningful description
                if not any([
                    component.get('description'), 
                    component.get('longDescription'),
                    component.get('documentation')
                ]):
                    if component.get('name'):
                        LOG.warning(
                            f"Component {component_id} ({component.get('name')}) has no description, "
                            "long description, or documentation available"
                        )
                    else:
                        LOG.warning(
                            f"Component {component_id} has no name, description, "
                            "long description, or documentation available"
                        )
                
                component_map[component_id] = component_info
            
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
        
        component_info = self._cache.get(component_id)
        if not component_info:
            LOG.warning(f"No information found for component {component_id}")
            return f"Component {component_id} (no description available)"
            
        return component_info['description']

    def get_full_component_info(self, component_id: str) -> dict:
        """
        Get all available information for a component based on its ID.
        Fetches the component list from the API on first call.
        
        Args:
            component_id: The Keboola component ID
            
        Returns:
            A dictionary containing all available component information
        """
        # Initialize cache if it's the first call
        if self._cache is None:
            self._cache = self._fetch_component_list()
            
        component_info = self._cache.get(component_id)
        if not component_info:
            LOG.warning(f"No information found for component {component_id}")
            return {
                'description': f"Component {component_id} (no description available)",
                'name': '',
                'long_description': '',
                'documentation_url': ''
            }
            
        return component_info
