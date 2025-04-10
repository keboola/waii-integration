"""
Pydantic models for Keboola resources.

These models represent the structure of various Keboola resources 
and can be used for parsing and validating data from the Keboola API.
"""

from typing import Optional, Any
from pydantic import BaseModel, Field


class Bucket(BaseModel):
    """Represents a Keboola bucket."""
    id: str
    uri: Optional[str] = None
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    id_branch: Optional[int] = Field(None, alias="idBranch")
    stage: str
    description: Optional[str] = ""
    tables: Optional[str] = None
    created: Optional[str] = None
    last_change_date: Optional[str] = Field(None, alias="lastChangeDate")
    updated: Optional[str] = None
    is_read_only: Optional[bool] = Field(False, alias="isReadOnly")
    data_size_bytes: Optional[int] = Field(None, alias="dataSizeBytes")
    rows_count: Optional[int] = Field(None, alias="rowsCount")
    is_maintenance: Optional[bool] = Field(False, alias="isMaintenance")
    backend: Optional[str] = None
    sharing: Optional[Any] = None
    has_external_schema: Optional[bool] = Field(False, alias="hasExternalSchema")
    database_name: Optional[str] = Field("", alias="databaseName")
    path: Optional[str] = None
    is_snowflake_shared_database: Optional[bool] = Field(False, alias="isSnowflakeSharedDatabase")
    color: Optional[Any] = None
    owner: Optional[Any] = None
    attributes: list[dict[str, Any]] = Field(default_factory=list)
    metadata: Optional[list[dict[str, Any]]] = Field(default_factory=list)


class Table(BaseModel):
    """Represents a Keboola table metadata."""
    id: str
    uri: Optional[str] = None
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    transactional: Optional[bool] = False
    primary_key: list[str] = Field(default_factory=list, alias="primaryKey")
    index_type: Optional[Any] = Field(None, alias="indexType")
    index_key: list[str] = Field(default_factory=list, alias="indexKey")
    distribution_type: Optional[Any] = Field(None, alias="distributionType")
    distribution_key: list[str] = Field(default_factory=list, alias="distributionKey")
    synthetic_primary_key_enabled: Optional[bool] = Field(None, alias="syntheticPrimaryKeyEnabled")
    bucket_id: Optional[str] = None  # Not in example, derived from id
    created: Optional[str] = None
    last_change_date: Optional[str] = Field(None, alias="lastChangeDate")
    last_import_date: Optional[str] = Field(None, alias="lastImportDate")
    rows_count: Optional[int] = Field(None, alias="rowsCount")
    data_size_bytes: Optional[int] = Field(None, alias="dataSizeBytes")
    is_alias: bool = Field(False, alias="isAlias")
    is_aliasable: Optional[bool] = Field(None, alias="isAliasable")
    is_typed: Optional[bool] = Field(False, alias="isTyped")
    table_type: Optional[str] = Field(None, alias="tableType")
    path: Optional[str] = None
    attributes: list[dict[str, Any]] = Field(default_factory=list)
    metadata: list[dict[str, Any]] = Field(default_factory=list)
    columns: list[str] = Field(default_factory=list)

    def __init__(self, **data):
        # Extract bucket_id from id if not provided
        if 'bucket_id' not in data and 'bucketId' not in data and 'id' in data:
            table_id = data.get('id')
            if table_id and '.' in table_id:
                parts = table_id.split('.')
                if len(parts) >= 2:
                    data['bucket_id'] = parts[0] + '.' + parts[1]
        super().__init__(**data)


class TableDetail(Table):
    """Represents detailed metadata for a Keboola table."""
    column_metadata: dict[str, list[dict[str, Any]]] = Field(default_factory=dict, alias="columnMetadata")
    bucket: Optional[dict[str, Any]] = None


class Component(BaseModel):
    """Model representing a Keboola component."""
    id: str
    name: str
    description: Optional[str] = None

    def get_display_description(self) -> str:
        """Returns the description if available, otherwise the name."""
        return self.description if self.description else self.name


class Metadata(BaseModel):
    """Represents the metadata extracted from Keboola."""
    buckets: list[Bucket] = Field(default_factory=list)
    tables: dict[str, list[Table]] = Field(default_factory=dict)
    table_details: dict[str, TableDetail] = Field(default_factory=dict)
