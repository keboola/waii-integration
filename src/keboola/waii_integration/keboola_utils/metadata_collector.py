"""
Collector for Keboola metadata.
Handles fetching and processing metadata from Keboola projects.
"""

import logging
import os
import datetime
import json
from typing import Optional
from keboola.waii_integration.keboola_utils.client import KeboolaClient
from keboola.waii_integration.keboola_utils.component_descriptions import ComponentDescriptionManager
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime

LOG = logging.getLogger(__name__)


class TableMetadataKeys:
    """Active table metadata keys"""
    NAME = 'KBC.name'
    DESCRIPTION = 'KBC.description'
    LAST_IMPORT_DATE = 'lastImportDate'
    LAST_CHANGE_DATE = 'lastChangeDate'

    class CreatedBy:
        """Table creation metadata keys"""
        COMPONENT_ID = 'KBC.createdBy.component.id'


class Bucket(BaseModel):
    """Represents a Keboola bucket."""
    id: str
    stage: Optional[str] = None


class Table(BaseModel):
    """Represents a Keboola table with its metadata."""
    id: str
    name: str
    description: str
    display_name: str
    last_import_date: Optional[datetime] = Field(None, alias="lastImportDate")
    last_change_date: Optional[datetime] = Field(None, alias="lastChangeDate")
    created_by_component: dict
    columns: list[str] = Field(default_factory=list)
    bucket: Bucket
    rows_count: int = 0


class Metadata(BaseModel):
    """Represents the metadata extracted from Keboola."""
    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat() if v else None
        }
    )
    
    tables: dict[str, Table] = Field(default_factory=dict)


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

    def _save_metadata_to_file(self, metadata: Metadata) -> Optional[str]:
        """
        Save collected metadata to a file.
        
        Args:
            metadata: Metadata model containing table metadata
            
        Returns:
            str: Path to the saved file, or None if save failed
        """
        try:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            metadata_dir = os.path.join(project_root, 'data', 'metadata')
            os.makedirs(metadata_dir, exist_ok=True)
            
            # Generate a timestamp for the filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(metadata_dir, f"keboola_metadata_{timestamp}.json")
            
            # Save the metadata to a JSON file
            with open(filename, 'w') as f:
                json.dump({
                    'timestamp': timestamp,
                    'project': os.getenv('KEBOOLA_PROJECT_NAME', 'unknown'),
                    'table_count': len(metadata.tables),
                    'metadata': metadata.model_dump()
                }, f, indent=2)
            
            LOG.info(f"Saved metadata for {len(metadata.tables)} tables to {filename}")
            return filename
            
        except Exception as e:
            LOG.error(f"Error saving metadata to file: {e}")
            return None

    def get_tables_metadata_sample(self, limit=None) -> Metadata:
        """
        Fetch basic metadata for tables in the project.
        
        Args:
            limit: Maximum number of tables to fetch (all tables if None)
            
        Returns:
            Metadata: Pydantic model containing metadata for tables
        """
        raw_metadata = self.client.extract_metadata(limit=limit)
        metadata = Metadata()

        # Process each bucket's tables
        for bucket_id, tables in raw_metadata.get('tables', {}).items():
            for table in tables:
                table_id = table.get('id', 'unknown')
                if table_id not in raw_metadata.get('table_details', {}):
                    continue
                
                table_detail = raw_metadata.get('table_details', {}).get(table_id, {})
                table_metadata = table_detail.get('metadata', [])     
                component_id = self._get_metadata_value(table_metadata, TableMetadataKeys.CreatedBy.COMPONENT_ID)
                component_description = self.component_manager.get_description(component_id)

                table_model = Table(
                    id=table_id,
                    name=self._get_metadata_value(table_metadata, TableMetadataKeys.NAME),
                    description=self._get_metadata_value(table_metadata, TableMetadataKeys.DESCRIPTION),
                    display_name=table_detail.get('displayName', table_id),
                    last_import_date=table_detail.get(TableMetadataKeys.LAST_IMPORT_DATE),
                    last_change_date=table_detail.get(TableMetadataKeys.LAST_CHANGE_DATE),
                    created_by_component={
                        'id': component_id,
                        'description': component_description
                    },
                    columns=table_detail.get('columns', []),
                    bucket=Bucket(
                        id=bucket_id,
                        stage=table_detail.get('bucket', {}).get('stage')
                    ),
                    rows_count=table_detail.get('rowsCount', 0)
                )
                
                metadata.tables[table_id] = table_model
        
        saved_file = self._save_metadata_to_file(metadata)
        if saved_file:
            LOG.info(f"Metadata saved to {saved_file}")
            
        return metadata
