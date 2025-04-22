"""
Pydantic models for Keboola resources.
These models represent the structure of various Keboola resources 
and can be used for parsing and validating data from the Keboola API.
"""

import logging
import json
import os
from typing import ClassVar, Union, Tuple, Any
from datetime import datetime, timezone
from pydantic import BaseModel, Field, field_validator, ConfigDict

LOG = logging.getLogger(__name__)


def parse_datetime(dt_value: Union[str, datetime, None]) -> datetime | None:
    """Parse datetime from string with timezone handling."""
    if dt_value is None:
        return None
    if isinstance(dt_value, datetime):
        return dt_value
    try:
        # Parse string datetime with timezone info
        return datetime.fromisoformat(dt_value)
    except (ValueError, TypeError):
        return None


def format_datetime_with_timezone(dt: datetime | None) -> str | None:
    """Format datetime to ISO 8601 with timezone offset."""
    if dt is None:
        return None
    # Ensure datetime has timezone info
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


class KeboolaMetadataKeys:
    """Centralized storage of all Keboola API metadata keys"""
    
    # Metadata field structure keys - used when accessing metadata lists
    class MetadataItem:
        """Keys for metadata list item structure"""
        KEY = 'key'
        VALUE = 'value'
        ID = 'id'
        PROVIDER = 'provider'
        TIMESTAMP = 'timestamp'
        DEFAULT_NO_DATA = 'NO_DATA_AVAILABLE'
    
    class Table:
        """Table keys for direct properties and structure"""
        # Direct table properties
        ID = 'id'
        URI = 'uri'
        NAME = 'name'
        DISPLAY_NAME = 'displayName'
        PRIMARY_KEY = 'primaryKey'
        ROWS_COUNT = 'rowsCount'
        COLUMNS = 'columns'
        COLUMN_METADATA = 'columnMetadata'
        ATTRIBUTES = 'attributes'
        TABLE_DETAILS = 'table_details'
        LAST_IMPORT_DATE = 'lastImportDate'
        LAST_CHANGE_DATE = 'lastChangeDate'
        
        # Metadata list property
        METADATA_LIST = 'metadata'
        
        # Metadata keys for table metadata (KBC.* keys)
        class Metadata:
            """Keys used in table metadata list"""
            NAME = 'KBC.name'
            DESCRIPTION = 'KBC.description'
            
            class CreatedBy:
                """Table creation metadata keys"""
                COMPONENT_ID = 'KBC.createdBy.component.id'
                CONFIGURATION_ID = 'KBC.createdBy.configuration.id'
            
            class LastUpdatedBy:
                """Table update metadata keys"""
                COMPONENT_ID = 'KBC.lastUpdatedBy.component.id'
                CONFIGURATION_ID = 'KBC.lastUpdatedBy.configuration.id'
        
    class Bucket:
        """Bucket keys"""
        ID = 'id'
        URI = 'uri'
        NAME = 'name'
        DISPLAY_NAME = 'displayName'
        STAGE = 'stage'
        DESCRIPTION = 'description'
        CREATED = 'created'
        LAST_CHANGE_DATE = 'lastChangeDate'
        DATA_SIZE_BYTES = 'dataSizeBytes'
        ROWS_COUNT = 'rowsCount'
        BACKEND = 'backend'
        METADATA = 'metadata'
        BUCKETS = 'buckets'
        TABLES = 'tables'


class Table(BaseModel):
    """Represents a Keboola table with its metadata."""
    # Static metadata keys for the class
    metadata_keys: ClassVar[KeboolaMetadataKeys] = KeboolaMetadataKeys
    
    id: str
    uri: str | None = None
    name: str
    display_name: str | None = Field(None, alias="displayName")
    description: str | None = None
    transactional: bool | None = False
    primary_key: list[str] = Field(default_factory=list, alias="primaryKey")
    index_type: Any | None = Field(None, alias="indexType")
    index_key: list[str] = Field(default_factory=list, alias="indexKey")
    distribution_type: Any | None = Field(None, alias="distributionType")
    distribution_key: list[str] = Field(default_factory=list, alias="distributionKey")
    synthetic_primary_key_enabled: bool | None = Field(None, alias="syntheticPrimaryKeyEnabled")
    bucket_id: str | None = None  # Not in example, derived from id
    created: datetime | None = None
    last_change_date: datetime | None = Field(None, alias="lastChangeDate")
    last_import_date: datetime | None = Field(None, alias="lastImportDate")
    rows_count: int | None = Field(None, alias="rowsCount")
    data_size_bytes: int | None = Field(None, alias="dataSizeBytes")
    is_alias: bool = Field(False, alias="isAlias")
    is_aliasable: bool | None = Field(None, alias="isAliasable")
    is_typed: bool | None = Field(False, alias="isTyped")
    table_type: str | None = Field(None, alias="tableType")
    path: str | None = None
    created_by_component_id: str | None = None
    created_by_component_description: str | None = None

    # Added from TableDetail
    columns: list[str] = Field(default_factory=list) 
    column_metadata: dict[str, list[dict[str, Any]]] = Field(default_factory=dict, alias="columnMetadata")
    attributes: list[dict[str, Any]] = Field(default_factory=list)
    metadata: list[dict[str, Any]] = Field(default_factory=list)
    bucket: "Bucket" | None = None

    # Parse datetime fields
    @field_validator("created", "last_change_date", "last_import_date", mode="before")
    @classmethod
    def parse_datetime_fields(cls, value):
        return parse_datetime(value)
    
    @staticmethod
    def get_metadata_value(metadata_list: list, key: str) -> str:
        """Extract value from metadata list by key"""
        for meta in metadata_list:
            if meta.get(KeboolaMetadataKeys.MetadataItem.KEY) == key:
                return meta.get(KeboolaMetadataKeys.MetadataItem.VALUE, KeboolaMetadataKeys.MetadataItem.DEFAULT_NO_DATA)
        LOG.debug("Metadata key '%s' not found in metadata list", key)
        return KeboolaMetadataKeys.MetadataItem.DEFAULT_NO_DATA

    def update_from_detail(self, detail: dict[str, Any]) -> None:
        """
        Update table with detailed information from the API.
        
        This method handles mapping between API field names and model field names
        in a centralized, elegant way.
        
        Args:
            detail: Table detail dictionary from the API
        """
        # Define mapping between API field names and model attributes
        # Use KeboolaMetadataKeys constants for consistent key references
        field_mapping = {
            KeboolaMetadataKeys.Table.COLUMNS: "columns",
            KeboolaMetadataKeys.Table.COLUMN_METADATA: "column_metadata",
            KeboolaMetadataKeys.Table.ATTRIBUTES: "attributes",
            KeboolaMetadataKeys.Table.METADATA_LIST: "metadata"  # Keep using parent class METADATA as it's shared across entities
        }

        # Update fields that exist in the detail response
        for api_field, model_field in field_mapping.items():
            if api_field in detail:
                setattr(self, model_field, detail[api_field])


class Bucket(BaseModel):
    """Represents a Keboola bucket."""
    id: str
    uri: str | None = None
    name: str
    display_name: str | None = Field(None, alias="displayName")
    id_branch: int | None = Field(None, alias="idBranch")
    stage: str
    description: str | None = ""
    created: datetime | None = None
    last_change_date: datetime | None = Field(None, alias="lastChangeDate")
    updated: datetime | None = None
    is_read_only: bool | None = Field(False, alias="isReadOnly")
    data_size_bytes: int | None = Field(None, alias="dataSizeBytes")
    rows_count: int | None = Field(None, alias="rowsCount")
    is_maintenance: bool | None = Field(False, alias="isMaintenance")
    backend: str | None = None
    sharing: Any | None = None
    has_external_schema: bool | None = Field(False, alias="hasExternalSchema")
    database_name: str | None = Field("", alias="databaseName")
    path: str | None = None
    is_snowflake_shared_database: bool | None = Field(False, alias="isSnowflakeSharedDatabase")
    color: Any | None = None
    owner: Any | None = None
    attributes: list[dict[str, Any]] = Field(default_factory=list)
    metadata: list[dict[str, Any]] | None = Field(default_factory=list)

    # Parse datetime fields
    @field_validator("created", "last_change_date", "updated", mode="before")
    @classmethod
    def parse_datetime_fields(cls, value):
        return parse_datetime(value)


class Component(BaseModel):
    """Model representing a Keboola component."""
    id: str
    name: str
    description: str | None = None

    def get_display_description(self) -> str:
        """Returns the description if available, otherwise the name."""
        return self.description if self.description else self.name


class Metadata(BaseModel):
    """Represents the metadata extracted from Keboola."""
    # Configure model serialization
    model_config = ConfigDict(
        json_encoders={
            datetime: format_datetime_with_timezone
        }
    )
    
    buckets: list[Bucket] = Field(default_factory=list)
    tables: dict[str, list[Table]] = Field(default_factory=dict)
    
    @classmethod
    def get_from_keboola(cls, client: Any, limit: int | None = None, output_dir: str | None = None) -> Tuple['Metadata', str | None]:
        """
        Fetch metadata from Keboola project and return as pydantic models.
        
        Args:
            client: Initialized KeboolaClient
            limit: Maximum number of tables to fetch (all tables if None)
            output_dir: If provided, saves the metadata to this directory
            
        Returns:
            Tuple containing:
                - Metadata model with tables as Pydantic models
                - Path to the saved file (if output_dir was provided, otherwise None)
        """
        LOG.info("Fetching Keboola metadata with limit: %s", limit)
        
        # Fetch raw metadata from API
        raw_metadata = client.extract_metadata(limit=limit)
        
        # Create the metadata model
        metadata_model = cls()
        output_file = None
        
        # Process each bucket's tables
        LOG.info("Processing tables from %d buckets", len(raw_metadata.get(KeboolaMetadataKeys.Bucket.TABLES, {})))
        for bucket_id, tables in raw_metadata.get(KeboolaMetadataKeys.Bucket.TABLES, {}).items():
            LOG.debug("Processing bucket: %s with %d tables", bucket_id, len(tables))
            
            # Create a list for tables in this bucket
            table_list = []
            
            for table in tables:
                table_id = table.get(KeboolaMetadataKeys.Table.ID, 'unknown')
                # Only process tables that have details
                if table_id not in raw_metadata.get(KeboolaMetadataKeys.Table.TABLE_DETAILS, {}):
                    LOG.warning("Table %s has no details available, skipping", table_id)
                    continue
                
                table_detail = raw_metadata.get(KeboolaMetadataKeys.Table.TABLE_DETAILS, {}).get(table_id, {})
                table_metadata = table_detail.get(KeboolaMetadataKeys.Table.METADATA_LIST, [])
                
                # Get the component ID
                component_id = Table.get_metadata_value(table_metadata, KeboolaMetadataKeys.Table.Metadata.CreatedBy.COMPONENT_ID)
                
                # Create Table model with essential fields
                table_model = Table(
                    id=table_id,
                    uri=table.get(KeboolaMetadataKeys.Table.URI),
                    name=Table.get_metadata_value(table_metadata, KeboolaMetadataKeys.Table.Metadata.NAME),
                    display_name=table_detail.get(KeboolaMetadataKeys.Table.DISPLAY_NAME, table_id),
                    description=Table.get_metadata_value(table_metadata, KeboolaMetadataKeys.Table.Metadata.DESCRIPTION),
                    primary_key=table.get(KeboolaMetadataKeys.Table.PRIMARY_KEY, []),
                    bucket_id=bucket_id,
                    last_change_date=table_detail.get(KeboolaMetadataKeys.Table.LAST_CHANGE_DATE),
                    last_import_date=table_detail.get(KeboolaMetadataKeys.Table.LAST_IMPORT_DATE),
                    rows_count=table_detail.get(KeboolaMetadataKeys.Table.ROWS_COUNT, 0),
                    columns=table_detail.get(KeboolaMetadataKeys.Table.COLUMNS, []),
                    metadata=table_metadata,
                    created_by_component_id=component_id
                )
                
                # Add any additional fields from the detail
                if table_detail:
                    table_model.update_from_detail(table_detail)
                
                # Add to bucket's table list
                table_list.append(table_model)
            
            # Store tables for this bucket
            if table_list:
                metadata_model.tables[bucket_id] = table_list
        
        # Optionally save the metadata to file
        if output_dir:
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{output_dir}/keboola_metadata_{timestamp}.json"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                # Convert to dict and then to JSON for better control
                json_data = metadata_model.model_dump(exclude_none=True)
                json.dump(json_data, f, indent=2, ensure_ascii=False)
            
            LOG.info("Metadata saved to %s", output_file)
        
        table_count = sum(len(tables) for tables in metadata_model.tables.values())
        LOG.info("Collected metadata for %d tables as Pydantic models", table_count)

        return metadata_model, output_file
