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
from dotenv import load_dotenv
from keboola.waii_integration.keboola_utils.models import Component

# Set up logging
LOG = logging.getLogger(__name__)


class ComponentDescriptionManager:
    """
    Manages component descriptions from Keboola API
    """

    def __init__(self):
        """Initialize the component description manager and load environment variables"""
        self._component_cache: dict[str, Component] | None = None
        load_dotenv(verbose=True)


    def _get_components_from_api(self, token: str, base_url: str) -> list[Component]:
        """
        Fetch components directly from the Keboola API endpoint.
        
        Args:
            token: Keboola Storage API token
            base_url: Keboola base URL
            
        Returns:
            List of Component objects
        """
        url = f"{base_url}/v2/storage"
        headers = {"X-StorageApi-Token": token}
        
        LOG.info(f"Fetching components from API endpoint: {url}")

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            components_data = data.get('components', [])
            
            # Convert to pydantic models
            components = [Component(**comp) for comp in components_data]
            
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
            
            # Store full component objects in component cache
            self._component_cache = {comp.id: comp for comp in components}
            
            # Create a mapping of component ID to description
            component_map = {}
            for component in components:
                component_map[component.id] = component.get_display_description()
            
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
        # Initialize cache if needed
        if self._component_cache is None:
            self._fetch_component_list()
        
        # Get component and return description or default message
        component = self._component_cache.get(component_id)
        return component.get_display_description() if component else f"Component {component_id} (no description available)"


    def get_component(self, component_id: str) -> Component | None:
        """
        Get a component object by its ID.
        Fetches the component list from the API on first call.
        
        Args:
            component_id: The Keboola component ID
            
        Returns:
            Component object or None if not found
        """
        # Initialize cache if it's the first call
        if self._component_cache is None:
            self._fetch_component_list()
            
        # Return component from cache or None
        return self._component_cache.get(component_id)
