"""
Module for collecting metadata from Keboola projects.
"""

import logging
from typing import Dict

from keboola_component_descriptions import ComponentDescriptionManager
from keboola_client import KeboolaClient

LOG = logging.getLogger(__name__)


class KeboolaMetadataKeys:
    """Active table metadata keys"""
    NAME = 'KBC.name'
    DESCRIPTION = 'KBC.description'
    LAST_IMPORT_DATE = 'lastImportDate'
    LAST_CHANGE_DATE = 'lastChangeDate'

    class CreatedBy:
        """Table creation metadata keys"""
        COMPONENT_ID = 'KBC.createdBy.component.id' 


class KeboolaMetadataCollector:
    """Collects metadata from Keboola projects"""
    def __init__(self, api_token: str, project_url: str):
        # Extract just the base domain from the project URL
        # From: https://connection.keboola.com/admin/projects/9382/dashboard
        # To: https://connection.keboola.com
        self.base_url = project_url.split('/admin')[0]
        self.client = KeboolaClient(api_token, self.base_url)
        self.component_manager = ComponentDescriptionManager()


    def _get_metadata_value(self, metadata_list: list, key: str) -> str:
        """Extract value from metadata list by key"""
        for meta in metadata_list:
            if meta.get('key') == key:
                return meta.get('value', 'NO_DATA_AVAILABLE')
        return 'NO_DATA_AVAILABLE'


    def get_tables_metadata_sample(self, limit=None) -> Dict[str, dict]:
        """
        Fetch basic metadata for tables in the project.
        
        Args:
            limit: Maximum number of tables to fetch (all tables if None)
        """
        # Pass the limit to the client to restrict API calls
        metadata = self.client.extract_metadata(limit=limit)
        result = {}

        # Process each bucket's tables
        for bucket_id, tables in metadata.get('tables', {}).items():
            for table in tables:
                table_id = table.get('id', 'unknown')
                # Only process tables that have details
                if table_id not in metadata.get('table_details', {}):
                    continue
                
                table_detail = metadata.get('table_details', {}).get(table_id, {})
                table_metadata = table_detail.get('metadata', [])
                
                # Get the component ID
                component_id = self._get_metadata_value(table_metadata, KeboolaMetadataKeys.CreatedBy.COMPONENT_ID)
                
                # Get the component description using ComponentDescriptionManager
                component_description = self.component_manager.get_description(component_id)

                result[table_id] = {
                    # Basic table attributes
                    'name': self._get_metadata_value(table_metadata, KeboolaMetadataKeys.NAME),
                    'description': self._get_metadata_value(table_metadata, KeboolaMetadataKeys.DESCRIPTION),
                    'display_name': table_detail.get('displayName', table_id),
                    
                    # Import and change dates
                    'last_import_date': table_detail.get(KeboolaMetadataKeys.LAST_IMPORT_DATE),
                    'last_change_date': table_detail.get(KeboolaMetadataKeys.LAST_CHANGE_DATE),

                    # Component information
                    'created_by_component': {
                        'id': component_id,
                        'description': component_description
                    },

                    # Add column information
                    'columns': table_detail.get('columns', []),
                    
                    # Add bucket information
                    'bucket': {
                        'id': bucket_id,
                        'stage': table_detail.get('bucket', {}).get('stage')
                    },
                    
                    # Add rows count information
                    'rows_count': table_detail.get('rowsCount', 0)
                }
            
        return result


    def print_metadata_results(self, metadata: Dict[str, dict], limit: int = None, sample: int = 5) -> None:
        """
        Print formatted metadata results.
        
        Args:
            metadata: Dictionary of table metadata
            limit: The limit that was applied to the metadata collection (if any)
        """
        print("\n===== METADATA COLLECTION RESULTS =====")
        # Update the output message to be clear
        output_msg = f"Tables fetched: {len(metadata)}"
        if limit is not None:
            output_msg += f" (limited to {limit})"
        print(output_msg)
        
        # Count tables with descriptions
        tables_with_desc = sum(1 for table in metadata.values() if table['description'] != 'NO_DATA_AVAILABLE')
        if metadata:  # Prevent division by zero
            desc_percentage = tables_with_desc / len(metadata) * 100
            print(f"Tables with descriptions: {tables_with_desc} ({desc_percentage:.1f}%)")
        
        # Show sample of metadata (first 5 tables)
        print("\n----- SAMPLE OF TABLE METADATA -----")
        for i, (table_id, table_metadata) in enumerate(list(metadata.items())[:sample]):
            print(f"\nTable {i+1}: {table_id}")
            for key, value in table_metadata.items():
                print(f"  {key}: {value}")
        
        # Show tables with good descriptions
        print("\n----- TABLES WITH NON-EMPTY DESCRIPTIONS -----")
        good_desc_tables = [(tid, meta) for tid, meta in metadata.items() 
                            if meta['description'] != 'NO_DATA_AVAILABLE'][:sample]
        
        for i, (table_id, table_metadata) in enumerate(good_desc_tables):
            print(f"\nTable {i+1}: {table_id}")
            print(f"  display_name: {table_metadata['display_name']}")
            print(f"  description: {table_metadata['description']}")
        
        # Check for missing metadata
        print("\n----- METADATA VALIDATION -----")
        for field in ['name', 'description', 'display_name', 'created_by_component']:
            missing = sum(1 for meta in metadata.values() if field not in meta or meta[field] == 'NO_DATA_AVAILABLE')
            if metadata:  # Prevent division by zero
                missing_percentage = missing / len(metadata) * 100
                print(f"Tables missing '{field}': {missing} ({missing_percentage:.1f}%)")
