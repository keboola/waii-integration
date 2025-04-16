"""
Pydantic models for Keboola resources.

These models represent the structure of various Keboola resources 
and can be used for parsing and validating data from the Keboola API.
"""

from typing import Optional, Any, Union, Dict
from datetime import datetime, timezone
from pydantic import BaseModel, Field, field_validator


def parse_datetime(dt_value: Union[str, datetime, None]) -> Optional[datetime]:
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


def format_datetime_with_timezone(dt: Optional[datetime]) -> Optional[str]:
    """Format datetime to ISO 8601 with timezone offset."""
    if dt is None:
        return None
    # Ensure datetime has timezone info
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


class Table(BaseModel):
    """Represents a Keboola table with its metadata."""
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
    created: Optional[datetime] = None
    last_change_date: Optional[datetime] = Field(None, alias="lastChangeDate")
    last_import_date: Optional[datetime] = Field(None, alias="lastImportDate")
    rows_count: Optional[int] = Field(None, alias="rowsCount")
    data_size_bytes: Optional[int] = Field(None, alias="dataSizeBytes")
    is_alias: bool = Field(False, alias="isAlias")
    is_aliasable: Optional[bool] = Field(None, alias="isAliasable")
    is_typed: Optional[bool] = Field(False, alias="isTyped")
    table_type: Optional[str] = Field(None, alias="tableType")
    path: Optional[str] = None

    # Added from TableDetail
    columns: list[str] = Field(default_factory=list) 
    column_metadata: dict[str, list[dict[str, Any]]] = Field(default_factory=dict, alias="columnMetadata")
    attributes: list[dict[str, Any]] = Field(default_factory=list)
    metadata: list[dict[str, Any]] = Field(default_factory=list)
    bucket: Optional["Bucket"] = None

    # Parse datetime fields
    @field_validator("created", "last_change_date", "last_import_date", mode="before")
    @classmethod
    def parse_datetime_fields(cls, value):
        return parse_datetime(value)
    
    def update_from_detail(self, detail: Dict[str, Any]) -> None:
        """
        Update table with detailed information from the API.
        
        This method handles mapping between API field names and model field names
        in a centralized, elegant way.
        
        Args:
            detail: Table detail dictionary from the API
        """
        # Define mapping between API field names and model attributes
        field_mapping = {
            "columns": "columns",
            "columnMetadata": "column_metadata",
            "attributes": "attributes",
            "metadata": "metadata"
        }
        
        # Update fields that exist in the detail response
        for api_field, model_field in field_mapping.items():
            if api_field in detail:
                setattr(self, model_field, detail[api_field])


class Bucket(BaseModel):
    """Represents a Keboola bucket."""
    id: str
    uri: Optional[str] = None
    name: str
    display_name: Optional[str] = Field(None, alias="displayName")
    id_branch: Optional[int] = Field(None, alias="idBranch")
    stage: str
    description: Optional[str] = ""
    created: Optional[datetime] = None
    last_change_date: Optional[datetime] = Field(None, alias="lastChangeDate")
    updated: Optional[datetime] = None
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

    # Parse datetime fields
    @field_validator("created", "last_change_date", "updated", mode="before")
    @classmethod
    def parse_datetime_fields(cls, value):
        return parse_datetime(value)


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

    model_config = {
        "json_encoders": {
            datetime: format_datetime_with_timezone
        }
    }
