"""
Unified Pydantic models for Keboola Waii Integration.
Contains all data models used across the application.
"""

import logging
from datetime import datetime
from enum import Enum, unique
from pydantic import BaseModel, Field, ConfigDict, model_validator

LOG = logging.getLogger(__name__)


@unique
class TableMetadataKey(str, Enum):
    """Metadata keys for tables."""
    NAME = 'KBC.name'
    DESCRIPTION = 'KBC.description'
    CREATED_BY_COMPONENT_ID = 'KBC.createdBy.component.id'


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


class Bucket(BaseModel):
    """Pydantic model representing a Keboola bucket."""
    model_config = ConfigDict(extra='allow', arbitrary_types_allowed=True)
    
    id: str = Field(description='Unique identifier of the bucket')
    name: str | None = Field(default=None, description='Human-readable name of the bucket')
    stage: str | None = Field(default=None, description='Stage of the bucket (in, out, sys)')
    description: str | None = Field(default=None, description='Optional description of the bucket')


class Table(BaseModel):
    """Pydantic model representing a Keboola table with full metadata support."""
    model_config = ConfigDict(
        extra='allow', 
        arbitrary_types_allowed=True,
        populate_by_name=True
    )
    
    # Basic table information
    id: str = Field(description='Unique identifier of the table')
    name: str = Field(default="", description='Human-readable name of the table')
    description: str = Field(default="", description='Table description')
    uri: str | None = Field(default=None, description='URI of the table')
    displayName: str | None = Field(default=None, alias="displayName", description='Display name of the table')
    
    # Date information
    lastChangeDate: str | None = Field(default=None, description='Last modification date')
    lastImportDate: str | None = Field(default=None, description='Last import date')
    last_import_date: datetime | None = Field(None, alias="lastImportDate", description='Last import date as datetime')
    last_change_date: datetime | None = Field(None, alias="lastChangeDate", description='Last change date as datetime')
    
    # Size and structure information
    rowsCount: int | None = Field(default=None, alias="rowsCount", description='Number of rows in the table')
    dataSizeBytes: int | None = Field(default=None, description='Size of the table data in bytes')
    columns: list[str] = Field(default_factory=list, description='List of column names')
    
    # Detailed bucket information
    bucket: dict | Bucket | None = Field(default=None, description='Bucket information')
    
    # Component and metadata information (optional for enriched tables)
    created_by_component: ComponentInfo | None = Field(default=None, description='Component that created this table')
    metadata: list[dict[str, str]] = Field(default_factory=list, description='Table metadata')

    @model_validator(mode='after')
    def extract_metadata_fields(self) -> 'Table':
        """Extract name and description from metadata if not already set."""
        if not self.metadata:
            return self
            
        for item in self.metadata:
            if item['key'] == TableMetadataKey.NAME and not self.name:
                self.name = item['value']
            elif item['key'] == TableMetadataKey.DESCRIPTION and not self.description:
                self.description = item['value']
        return self


class Metadata(BaseModel):
    """
    Pydantic model representing metadata extracted from a Keboola project.
    
    This unified model supports both basic table lists (from client) and 
    enriched table mappings (from metadata collector).
    """
    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat() if v else None
        }
    )
    
    # For client usage - project-level metadata
    buckets: list[Bucket] | None = Field(
        default=None,
        description='List of all buckets in the project'
    )
    tables: dict[str, list[Table]] | dict[str, Table] | None = Field(
        default=None,
        description='Mapping of bucket IDs to tables OR table IDs to table data'
    )
    table_details: dict[str, Table] | None = Field(
        default=None,
        description='Mapping of table IDs to detailed table metadata'
    )