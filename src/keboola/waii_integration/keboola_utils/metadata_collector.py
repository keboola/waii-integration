"""
Collector for Keboola metadata.
Handles fetching and processing metadata from Keboola projects.
"""

import logging
import os
from pathlib import Path
import datetime
import json

from keboola.waii_integration.keboola_utils.client import KeboolaClient
from keboola.waii_integration.keboola_utils.component_descriptions import ComponentDescriptionManager
from keboola.waii_integration.models import (
    TableMetadataKey, ComponentInfo, Bucket, Table, Metadata
)
from datetime import datetime

LOG = logging.getLogger(__name__)


class KeboolaMetadataCollector:
    """Collects metadata from Keboola projects"""
    def __init__(self, api_token: str, project_url: str):
        self.base_url = project_url.split('/admin')[0]
        self.client = KeboolaClient(api_token, self.base_url)
        self.component_manager = ComponentDescriptionManager()

    def _process_table_data(self, table_id: str, table_data: dict, bucket_id: str) -> Table | None:
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
            metadata = table_data.get('metadata', [])

            component_id = next(
                (m['value'] for m in metadata if m['key'] == TableMetadataKey.CREATED_BY_COMPONENT_ID),
                None
            )
            if not component_id:
                LOG.warning(f"No component ID found for table {table_id}")
                return None

            # Get component info and add ID
            component_data = self.component_manager.get_full_component_info(component_id)
            component_info = ComponentInfo(id=component_id, **component_data)

            # Create table model with all data at once
            return Table(
                id=table_id,
                displayName=table_data.get('displayName', table_id),
                lastImportDate=table_data.get('lastImportDate'),
                lastChangeDate=table_data.get('lastChangeDate'),
                created_by_component=component_info,
                columns=table_data.get('columns', []),
                bucket=Bucket(
                    id=bucket_id,
                    stage=table_data.get('bucket', {}).get('stage')
                ),
                rowsCount=table_data.get('rowsCount', 0),
                metadata=metadata
            )
        except Exception as e:
            LOG.error(f"Error processing table data for {table_id}: {e}")
            return None

    def _save_metadata_to_file(self, metadata: Metadata) -> str | None:
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
                            'display_name': table.displayName,
                            'last_import_date': table.last_import_date.isoformat() if table.last_import_date else None,
                            'last_change_date': table.last_change_date.isoformat() if table.last_change_date else None,
                            'created_by_component': table.created_by_component.model_dump() if table.created_by_component else None,
                            'columns': table.columns,
                            'bucket': table.bucket.model_dump() if hasattr(table.bucket, 'model_dump') else table.bucket,
                            'rows_count': table.rowsCount,
                            'metadata': table.metadata
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


    def get_tables_metadata_sample(self, limit: int | None = None) -> Metadata:
        """
        Fetch basic metadata for tables in the project.
        
        Args:
            limit: Maximum number of tables to fetch (all tables if None)
            
        Returns:
            Metadata: Pydantic model containing metadata for tables
        """
        raw_metadata = self.client.extract_metadata_from_project(limit=limit)
        metadata = Metadata(tables={})

        # Process each bucket's tables
        for bucket_id, tables in raw_metadata.tables.items():
            for table in tables:
                table_id = table.id
                if table_id not in raw_metadata.table_details:
                    continue
                
                table_detail = raw_metadata.table_details[table_id]
                # Convert Pydantic model to dict for processing
                table_data = table_detail.model_dump()
                if table_model := self._process_table_data(table_id, table_data, bucket_id):
                    metadata.tables[table_id] = table_model
        
        if saved_file := self._save_metadata_to_file(metadata):
            LOG.info(f"Metadata saved to {saved_file}")
            
        return metadata
