"""
Component descriptions for Keboola components.
Fetches component information dynamically from the Keboola API.

Usage:
    from component_descriptions import ComponentDescriptionManager
    
    # Get a component description
    manager = ComponentDescriptionManager(api_token, base_url)
    description = manager.get_description('component.id')
"""

import logging
import requests

# Set up logging
LOG = logging.getLogger(__name__)


class ComponentDescriptionManager:
    """
    Manages component descriptions from Keboola API
    """

    def __init__(self, api_token: str, base_url: str):
        """
        Initialize the component description manager.
        
        Args:
            api_token: Keboola Storage API token
            base_url: Keboola base URL (without /admin suffix)
        """
        self._cache = None
        self._token = api_token
        self._base_url = base_url
        self._headers = {"X-StorageApi-Token": self._token} if self._token else {}


    def _get_components_from_api(self) -> list[dict]:
        """
        Fetch components directly from the Keboola API endpoint.
        
        Returns:
            List of component dictionaries
        """
        url = f"{self._base_url}/v2/storage"
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
            components = self._get_components_from_api()
            
            component_map = {}
            for component in components:
                component_id = component.get('id')
                if not component_id:
                    continue
                
                long_desc = component.get('longDescription', '').strip()
                desc = component.get('description', '').strip()
                doc = component.get('documentation', '').strip()
                name = component.get('name', '').strip()
                
                description = ''
                if long_desc and long_desc != component_id:
                    description = long_desc
                elif desc and desc != component_id:
                    description = desc
                elif doc and doc != component_id:
                    description = doc
                elif name and name != component_id and len(name) > len(component_id):
                    description = name
                
                # Store component information
                component_map[component_id] = {
                    'description': description,
                    'name': name,
                    'long_description': long_desc,
                    'documentation_url': component.get('documentationUrl', '')
                }
                
                if not description:
                    LOG.debug(f"Component {component_id} has no meaningful description available")
            
            LOG.info(f"Successfully fetched {len(component_map)} component descriptions")
            return component_map
        
        except Exception as e:
            LOG.error(f"Error fetching component list: {e}")
            return {}


    def get_description(self, component_id: str) -> str | None:
        """
        Get a human-readable description for a component based on its ID.
        Fetches the component list from the API on first call.
        
        Args:
            component_id: The Keboola component ID
            
        Returns:
            A human-readable description of the component or None if not found
        """
        if self._cache is None:
            self._cache = self._fetch_component_list()
        
        component_info = self._cache.get(component_id)
        if not component_info:
            return None
            
        description = component_info['description']
        return description if description else None

    def get_full_component_info(self, component_id: str) -> dict:
        """
        Get all available information for a component based on its ID.
        Fetches the component list from the API on first call.
        
        Args:
            component_id: The Keboola component ID
            
        Returns:
            A dictionary containing all available component information
        """
        if self._cache is None:
            self._cache = self._fetch_component_list()
            
        component_info = self._cache.get(component_id, {})
        
        if not component_info:
            return {
                'description': '',
                'name': '',
                'long_description': '',
                'documentation_url': ''
            }
        
        return component_info
