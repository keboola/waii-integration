"""
WAII semantic context manager for handling interactions with WAII semantic context.
"""

import logging
import json
import datetime
from typing import Dict
from pathlib import Path
from keboola.waii_integration.keboola_utils.models import Table

# Import WAII SDK
from waii_sdk_py import WAII
from waii_sdk_py.semantic_context import (
    ModifySemanticContextRequest,
    ModifySemanticContextResponse,
    SemanticContext,
    SemanticStatement
)

LOG = logging.getLogger(__name__)


class WaiiSemanticContextManager:
    """Manages interactions with WAII semantic context"""
    
    # Directory names
    SEMANTIC_STATEMENTS_DIR = 'semantic_statements'
    STATEMENT_IDS_DIR = 'statement_ids'
    
    # File patterns
    SEMANTIC_STATEMENTS_FILE_PATTERN = 'semantic_statements_{}.json'
    STATEMENT_IDS_FILE_PATTERN = 'semantic_statements_ids_{}.json'
    
    # Labels
    KB_PROJECT_LABEL = 'kb_project'
    
    def __init__(
        self, 
        api_url: str,
        api_key: str,
        connection_name: str,
        project_name: str = 'unknown',
        statement_ids_path: str = STATEMENT_IDS_DIR,
        db_database: str = None,
        db_username: str = None
    ):
        """Initialize the WAII semantic context manager
        
        Args:
            api_url: WAII API URL
            api_key: WAII API key
            connection_name: WAII connection name
            project_name: Human-readable project name for saved files (default: 'unknown')
            statement_ids_path: Path where statement IDs will be saved, relative to data/ directory (default: 'statement_ids')
            db_database: Optional database name for fallback connection search
            db_username: Optional username for fallback connection search
        """
        self._api_url = api_url
        self._api_key = api_key
        self._connection_name = connection_name
        self._project_name = project_name
        self._db_database = db_database
        self._db_username = db_username
        
        self._validate_required_parameters()
        self._initialize_connection()
        
        self.statement_ids = []  # Store statement IDs for later removal
        self.statement_ids_path = statement_ids_path

    def _get_data_directory(self, subdir: str) -> str:
        """Get the full path to a data subdirectory.
        
        Args:
            subdir: Subdirectory name under data/
            
        Returns:
            str: Full path to the data subdirectory
        """
        project_root = Path(__file__).parents[4]
        data_dir = project_root / 'data' / subdir
        data_dir.mkdir(parents=True, exist_ok=True)
        return str(data_dir)

    def _validate_required_parameters(self) -> None:
        """Validate that required parameters are provided.

        Raises:
            ValueError: If one or more required parameters are missing.
        """
        required_params = {
            'api_url': self._api_url,
            'api_key': self._api_key,
            'connection_name': self._connection_name
        }

        missing_params = [name for name, value in required_params.items() if not value]
        if missing_params:
            raise ValueError(f"Missing required WAII parameters: {', '.join(missing_params)}")
        
        LOG.info("WAII parameters validated successfully")

    def _initialize_connection(self) -> None:
        """Initialize Waii SDK with API URL and key, and activate the connection."""
        LOG.info('Initializing Waii with API URL: %s', self._api_url)
        WAII.initialize(url=self._api_url, api_key=self._api_key)

        LOG.info(f'Using connection: {self._connection_name}')
        
        try:
            # Try to use the connection from parameters
            LOG.info(f'Activating Waii connection: {self._connection_name}')
            WAII.Database.activate_connection(self._connection_name)
            LOG.info("Connection activated successfully")
        
        except Exception as e:
            LOG.warning(f"Failed to activate connection from parameters: {e}")
            
            LOG.info("Looking for an alternative connection...")
            connections = WAII.Database.get_connections()
            connection_ids = []
            for conn in connections:
                try:
                    if hasattr(conn, 'id'):
                        connection_ids.append(conn.id)
                    else:
                        connection_ids.append(str(conn))
                except Exception:
                    pass

            LOG.info(f"Available connections: {connection_ids}")

            if self._db_database and self._db_username:
                for conn_id in connection_ids:
                    if self._db_database in conn_id and self._db_username in conn_id:
                        alternative_connection = conn_id
                        LOG.info(f"Found matching connection: {alternative_connection}")
                        
                        try:
                            WAII.Database.activate_connection(alternative_connection)
                            LOG.info("Alternative connection activated successfully")
                            return
                        except Exception as alt_error:
                            LOG.warning(f"Failed to activate alternative connection: {alt_error}")
            
            raise ValueError("Could not find or activate a connection with required workspace and database identifiers")

    def create_semantic_context_statements(self, tables: Dict[str, Table], max_columns: int = 10) -> list[SemanticStatement]:
        """
        Create semantic context statements from Keboola metadata.
        
        The function creates two types of statements:
        1. A global statement about the entire dataset
        2. For each table, a comprehensive statement that includes:
           - Basic description with row count
           - Component information (if available)
           - Data freshness information (if available)
        
        Args:
            tables: Dictionary of table metadata as Pydantic Table models
            max_columns: Maximum number of columns to include in each table statement (default: 10)
        
        Returns:
            List of SemanticStatement objects
        """
        statements = []
        
        # Add statements for each table
        for table_id, table in tables.items():
            statement_parts = []
            display_name = table.displayName or table.name or table_id
            description = table.description

            # Start with table description and row count
            if not description or not description.strip():
                statement_parts.append(f"Table '{display_name}' contains {table.rowsCount or 0} rows.")
            else:
                statement_parts.append(f"Table '{display_name}' has this description: {description}. It contains {table.rowsCount or 0} rows.")
            
            # Component information if available (only if component exists)
            if table.created_by_component:
                comp_id = table.created_by_component.id
                comp_description = table.created_by_component.description
                if comp_id and comp_id.strip():
                    if comp_description and comp_description.strip():
                        statement_parts.append(f"It was created by {comp_id} ({comp_description}).")
                    else:
                        statement_parts.append(f"It was created by {comp_id}.")
            
            # Data freshness information if available
            freshness_info = []
            if table.last_import_date:
                freshness_info.append(f"last imported on {table.last_import_date}")
            if table.last_change_date:
                freshness_info.append(f"last changed on {table.last_change_date}")
            if freshness_info:
                statement_parts.append(f"Data is {', '.join(freshness_info)}.")
            
            # Combine all parts into one statement
            table_statement = SemanticStatement(
                statement=" ".join(statement_parts),
                always_include=False,
                critical=False,
                labels=[self.KB_PROJECT_LABEL],
                lookup_summaries=[
                    display_name, 
                    table_id, 
                    "source", 
                    "component", 
                    "created by",
                    "fresh",
                    "updated",
                    "recent",
                    "size",
                    "row count",
                    "volume"
                ]
            )
            statements.append(table_statement)

        return statements

    def add_to_semantic_context(self, statements: list[SemanticStatement]) -> None:
        """
        Add statements to WAII semantic context.
        
        Args:
            statements: List of SemanticStatement objects
        """
        LOG.info(f"Adding {len(statements)} semantic context statements to WAII")
        
        try:
            saved_file = self._save_semantic_statements_to_file(statements)
            if saved_file:
                LOG.info(f"Semantic statements saved to {saved_file}")

            resp: ModifySemanticContextResponse = SemanticContext.modify_semantic_context(
                ModifySemanticContextRequest(updated=statements)
            )
            statement_ids = [stmt.id for stmt in resp.updated]
            
            LOG.info(f"Successfully added {len(resp.updated)} semantic context statements to WAII")
            if len(resp.updated) != len(statements):
                LOG.warning(f"Not all statements were added: {len(resp.updated)}/{len(statements)}")
            
            self._save_statement_ids_to_file(statement_ids)
            
        except Exception as e:
            LOG.error(f"Error adding semantic context: {str(e)}")
            if hasattr(e, 'response') and e.response:
                try:
                    error_detail = e.response.json()
                    LOG.error(f"Error details: {error_detail}")
                except:
                    LOG.error(f"Response status: {e.response.status_code}, content: {e.response.text}")
            raise

    def _save_json_to_file(self, data: dict, directory: str, filename_pattern: str) -> str | None:
        """Save data to a JSON file in the specified directory.
        
        Args:
            data: Dictionary to save as JSON
            directory: Directory name under data/
            filename_pattern: Pattern for the filename with {} for timestamp
            
        Returns:
            str | None: Path to the saved file, or None if saving failed
        """
        try:
            target_dir = self._get_data_directory(directory)
            
            # Generate filename with timestamp
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = str(Path(target_dir) / filename_pattern.format(timestamp))
            
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            
            LOG.info(f"Saved data to {filename}")
            return filename
            
        except Exception as e:
            LOG.error(f"Error saving data to file: {e}")
            return None

    def _save_semantic_statements_to_file(self, statements: list[SemanticStatement]) -> str | None:
        """
        Save semantic statements to a file before adding them to WAII.
        
        Args:
            statements: List of SemanticStatement objects to save
            
        Returns:
            str | None: Path to the saved file, or None if saving failed
        """
        statements_data = []
        for stmt in statements:
            statements_data.append({
                'statement': stmt.statement,
                'always_include': stmt.always_include,
                'critical': stmt.critical,
                'labels': stmt.labels
            })
        
        data = {
            'timestamp': datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
            'project': self._project_name,
            'statement_count': len(statements),
            'statements': statements_data
        }
        
        return self._save_json_to_file(
            data=data,
            directory=self.SEMANTIC_STATEMENTS_DIR,
            filename_pattern=self.SEMANTIC_STATEMENTS_FILE_PATTERN
        )

    def _save_statement_ids_to_file(self, statement_ids: list[str]) -> None:
        """
        Save statement IDs to a file for later reference.
        
        Args:
            statement_ids: List of statement IDs to save
        """
        data = {
            'timestamp': datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
            'project': self._project_name,
            'statement_count': len(statement_ids),
            'statement_ids': statement_ids
        }
        
        self._save_json_to_file(
            data=data,
            directory=self.statement_ids_path,
            filename_pattern=self.STATEMENT_IDS_FILE_PATTERN
        )
