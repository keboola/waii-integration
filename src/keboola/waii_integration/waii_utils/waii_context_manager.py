"""
WAII semantic context manager for handling interactions with WAII semantic context.
"""

import logging
import os
import json
import datetime
from typing import Dict
from pathlib import Path
from keboola.waii_integration.keboola_utils.metadata_collector import Table

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
    
    def __init__(self, statement_ids_path: str = STATEMENT_IDS_DIR):
        """Initialize the WAII semantic context manager
        
        Args:
            statement_ids_path: Path where statement IDs will be saved, relative to data/ directory (default: 'statement_ids')
        """
        self._setup_environment()
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

    def _setup_environment(self) -> None:
        """Set up WAII environment variables and validate required ones are present.

        Raises:
            ValueError: If one or more required environment variables are missing.
        """
        required_vars = [
            'WAII_API_URL',
            'WAII_API_KEY',
            'WAII_CONNECTION'
        ]

        missing_vars = [var for var in required_vars if os.getenv(var) is None]
        if missing_vars:
            raise ValueError(f"Missing WAII environment variables: {', '.join(missing_vars)}")
        
        LOG.info("Environment variables validated successfully")

    def _initialize_connection(self) -> None:
        """Initialize Waii SDK with API URL and key, and activate the connection."""
        api_url = os.getenv('WAII_API_URL')
        LOG.info('Initializing Waii with API URL: %s', api_url)
        WAII.initialize(url=api_url, api_key=os.getenv('WAII_API_KEY'))

        waii_connection = os.getenv('WAII_CONNECTION')
        LOG.info(f'Using connection from environment: {waii_connection}')
        
        try:
            # Try to use the connection from environment variable
            LOG.info(f'Activating Waii connection: {waii_connection}')
            WAII.Database.activate_connection(waii_connection)
            LOG.info("Connection activated successfully")
        
        except Exception as e:
            LOG.warning(f"Failed to activate connection from environment variable: {e}")
            
            # Get all available connections and find one containing our workspace and database
            LOG.info("Looking for an alternative connection...")
            connections = WAII.Database.get_connections()
            
            # Extract connection IDs in a safe way
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

            # Look for a connection with our workspace and database
            waii_db_database = os.getenv('WAII_DB_DATABASE')
            waii_db_username = os.getenv('WAII_DB_USERNAME')
            
            for conn_id in connection_ids:
                if waii_db_database in conn_id and waii_db_username in conn_id:
                    waii_connection = conn_id
                    LOG.info(f"Found matching connection: {waii_connection}")
                    
                    try:
                        WAII.Database.activate_connection(waii_connection)
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
           - Basic description
           - Component information (if available)
           - Data freshness information (if available)
           - Row count information
        
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
            display_name = table.display_name
            description = table.description

            if description == 'NO_DATA_AVAILABLE':
                statement_parts.append(f"Table '{display_name}' contains {table.rows_count} rows.")
            else:
                statement_parts.append(f"Table '{display_name}' has this description: {description}.")
            
            # Component information if available
            comp_id = table.created_by_component['id']
            if comp_id != 'NO_DATA_AVAILABLE':
                statement_parts.append(f"It was created by {comp_id} ({table.created_by_component['description']}).")
            
            # Data freshness information if available
            freshness_info = []
            if table.last_import_date:
                freshness_info.append(f"last imported on {table.last_import_date}")
            if table.last_change_date:
                freshness_info.append(f"last changed on {table.last_change_date}")
            if freshness_info:
                statement_parts.append(f"Data is {', '.join(freshness_info)}.")
            
            # Row count information
            statement_parts.append(f"The table contains {table.rows_count} rows of data.")
            
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
            
            # Update WAII semantic context
            resp: ModifySemanticContextResponse = SemanticContext.modify_semantic_context(
                ModifySemanticContextRequest(updated=statements)
            )
            statement_ids = [stmt.id for stmt in resp.updated]
            
            LOG.info(f"Successfully added {len(resp.updated)} semantic context statements to WAII")
            if len(resp.updated) != len(statements):
                LOG.warning(f"Not all statements were added: {len(resp.updated)}/{len(statements)}")
            
            # Save statement IDs to a file for later reference
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
            filename = os.path.join(target_dir, filename_pattern.format(timestamp))
            
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
        # Convert statements to a serializable format
        statements_data = []
        for stmt in statements:
            statements_data.append({
                'statement': stmt.statement,
                'always_include': stmt.always_include,
                'critical': stmt.critical,
                'labels': stmt.labels
            })
        
        # Prepare the data to save
        data = {
            'timestamp': datetime.datetime.now().strftime("%Y%m%d_%H%M%S"),
            'project': os.getenv('KEBOOLA_PROJECT_NAME', 'unknown'),
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
            'project': os.getenv('KEBOOLA_PROJECT_NAME', 'unknown'),
            'statement_count': len(statement_ids),
            'statement_ids': statement_ids
        }
        
        self._save_json_to_file(
            data=data,
            directory=self.statement_ids_path,
            filename_pattern=self.STATEMENT_IDS_FILE_PATTERN
        )
