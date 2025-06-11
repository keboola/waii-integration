"""
Collector for Keboola metadata.
Handles fetching and processing metadata from Keboola projects.
"""

import logging
import os
from pathlib import Path
import datetime
import json
from typing import Optional, Any
from keboola.waii_integration.keboola_utils.client import KeboolaClient
from keboola.waii_integration.keboola_utils.component_descriptions import ComponentDescriptionManager
from pydantic import BaseModel, Field, ConfigDict, model_validator
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


class MetadataItem(BaseModel):
    """Model for metadata key-value pairs"""
    key: str
    value: str


class ComponentInfo(BaseModel):
    """
    Model for component information.
    Matches the structure returned by ComponentDescriptionManager.get_full_component_info()
    """
    id: str
    description: str
    name: str
    long_description: str = Field(alias="longDescription")
    documentation_url: str = Field(alias="documentationUrl")

    model_config = ConfigDict(
        populate_by_name=True,
        extra='ignore',  # Ignore extra fields from API
        arbitrary_types_allowed=True
    )

    def __getitem__(self, key: str) -> Any:
        """Support dictionary-style access"""
        try:
            return getattr(self, key)
        except AttributeError:
            field_definitions = self.model_fields
            for field_name, field in field_definitions.items():
                if field.alias == key:
                    return getattr(self, field_name)
            raise KeyError(key)


class Bucket(BaseModel):
    """Represents a Keboola bucket."""
    id: str
    stage: Optional[str] = None


class Table(BaseModel):
    """Represents a Keboola table with its metadata."""
    id: str = Field()
    name: str = Field(default="")
    description: str = Field(default="")
    display_name: str = Field(alias="displayName")
    last_import_date: Optional[datetime] = Field(None, alias="lastImportDate")
    last_change_date: Optional[datetime] = Field(None, alias="lastChangeDate")
    created_by_component: ComponentInfo = Field()
    columns: list[str] = Field(default_factory=list)
    bucket: Bucket = Field()
    rows_count: int = Field(0, alias="rowsCount")
    metadata: list[MetadataItem] = Field()

    model_config = ConfigDict(
        populate_by_name=True,
        extra='ignore'
    )

    @model_validator(mode='after')
    def extract_metadata_fields(self) -> 'Table':
        """Extract name and description from metadata if not already set"""
        if not self.metadata:
            return self
            
        for item in self.metadata:
            if item.key == TableMetadataKeys.NAME and not self.name:
                self.name = item.value
            elif item.key == TableMetadataKeys.DESCRIPTION and not self.description:
                self.description = item.value
        return self


class Metadata(BaseModel):
    """Represents the metadata extracted from Keboola."""
    tables: dict[str, Table] = Field(default_factory=dict)

    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat() if v else None
        }
    )


class KeboolaMetadataCollector:
    """Collects metadata from Keboola projects"""
    def __init__(self, api_token: str, project_url: str):
        self.base_url = project_url.split('/admin')[0]
        self.client = KeboolaClient(api_token, self.base_url)
        self.component_manager = ComponentDescriptionManager()

    def _process_table_data(self, table_id: str, table_data: dict, bucket_id: str) -> Optional[Table]:
        """
        Process raw table data into a Table model.
        
        Args:
            table_id: The ID of the table
            table_data: Raw table data from API
            bucket_id: The ID of the bucket containing the table
            
        Returns:
            Table model if successful, None if required data is missing
        """
        try:
            metadata = [
                MetadataItem(key=m['key'], value=m['value'])
                for m in table_data.get('metadata', [])
            ]

            component_id = next(
                (m.value for m in metadata if m.key == TableMetadataKeys.CreatedBy.COMPONENT_ID),
                None
            )
            if not component_id:
                LOG.warning(f"No component ID found for table {table_id}")
                return None

            # Get component info and add ID
            component_data = self.component_manager.get_full_component_info(component_id)
            component_data['id'] = component_id

            # Create table model with all data at once
            return Table(
                id=table_id,
                display_name=table_data.get('displayName', table_id),
                last_import_date=table_data.get('lastImportDate'),
                last_change_date=table_data.get('lastChangeDate'),
                created_by_component=component_data,
                columns=table_data.get('columns', []),
                bucket=Bucket(
                    id=bucket_id,
                    stage=table_data.get('bucket', {}).get('stage')
                ),
                rows_count=table_data.get('rowsCount', 0),
                metadata=metadata
            )
        except Exception as e:
            LOG.error(f"Error processing table data for {table_id}: {e}")
            return None

    def _save_metadata_to_file(self, metadata: Metadata) -> Optional[str]:
        """
        Save collected metadata to a file.
        
        Args:
            metadata: Metadata model containing table metadata
            
        Returns:
            str: Path to the saved file, or None if save failed
        """
        try:
            project_root = Path(__file__).parents[4]
            metadata_dir = project_root / 'data' / 'metadata'
            metadata_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = metadata_dir / f"keboola_metadata_{timestamp}.json"
            
            # Reorganize tables by bucket_id
            tables_by_bucket = {}
            for table_id, table in metadata.tables.items():
                bucket_id = table.bucket.id
                if bucket_id not in tables_by_bucket:
                    tables_by_bucket[bucket_id] = []
                tables_by_bucket[bucket_id] = tables_by_bucket.get(bucket_id, []) + [table]
            
            # Convert metadata to dict with proper datetime handling
            metadata_dict = {
                'tables': {
                    bucket_id: [
                        {
                            'id': table.id,
                            'name': table.name,
                            'description': table.description,
                            'display_name': table.display_name,
                            'last_import_date': table.last_import_date.isoformat() if table.last_import_date else None,
                            'last_change_date': table.last_change_date.isoformat() if table.last_change_date else None,
                            'created_by_component': table.created_by_component.model_dump(),
                            'columns': table.columns,
                            'bucket': table.bucket.model_dump(),
                            'rows_count': table.rows_count,
                            'metadata': [m.model_dump() for m in table.metadata]
                        }
                        for table in tables
                    ]
                    for bucket_id, tables in tables_by_bucket.items()
                }
            }
            
            # Prepare the final JSON structure
            output_data = {
                'timestamp': timestamp,
                'project': os.getenv('KEBOOLA_PROJECT_NAME', 'unknown'),
                'table_count': sum(len(tables) for tables in tables_by_bucket.values()),
                'metadata': metadata_dict
            }
            
            with open(filename, 'w') as f:
                json.dump(output_data, f, indent=2)
            
            LOG.info(f"Saved metadata for {len(metadata.tables)} tables to {filename}")
            return str(filename)
            
        except Exception as e:
            LOG.error(f"Error saving metadata to file: {e}")
            return None

    def get_tables_metadata_sample(self, limit: Optional[int] = None) -> Metadata:
        """
        Fetch basic metadata for tables in the project.
        
        Args:
            limit: Maximum number of tables to fetch (all tables if None)
            
        Returns:
            Metadata: Pydantic model containing metadata for tables
        """
        raw_metadata: dict[str, Any] = self.client.extract_metadata(limit=limit)
        metadata = Metadata()

        # Process each bucket's tables
        for bucket_id, tables in raw_metadata.get('tables', {}).items():
            for table in tables:
                table_id = table.get('id', 'unknown')
                if table_id not in raw_metadata.get('table_details', {}):
                    continue
                
                table_data = raw_metadata.get('table_details', {}).get(table_id, {})
                if table_model := self._process_table_data(table_id, table_data, bucket_id):
                    metadata.tables[table_id] = table_model
        
        if saved_file := self._save_metadata_to_file(metadata):
            LOG.info(f"Metadata saved to {saved_file}")
            
        return metadata
