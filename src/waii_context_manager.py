"""
Module for managing WAII semantic context and statements.
"""

import logging
import os
import datetime
import json
from typing import Dict, List

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
    
    def __init__(self):
        """Initialize the WAII semantic context manager"""
        self._setup_environment()
        self._initialize_connection()
        self.statement_ids = []  # Store statement IDs for later removal
    
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
                        # If it's not an object with an id attribute, try to convert to string
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
            
            # If we couldn't find or activate a specific connection, raise an error
            raise ValueError("Could not find or activate a connection with required workspace and database identifiers")

    def create_semantic_context_statements(self, metadata: Dict[str, dict], max_columns: int = 10) -> List[SemanticStatement]:
        """
        Create semantic context statements from Keboola metadata.
        
        Args:
            metadata: Dictionary of table metadata
            max_columns: Maximum number of columns to include in each table statement (default: 10)
        
        Returns:
            List of SemanticStatement objects
        """
        statements = []
        
        # Add a global statement about the entire dataset
        global_statement = SemanticStatement(
            statement=f"This is metadata imported from Keboola Connection. It contains information about {len(metadata)} tables.",
            always_include=True,
            critical=False,
            labels=["kb_project"]
        )
        statements.append(global_statement)
        
        # Add statements for each table
        for table_id, table_meta in metadata.items():
            display_name = table_meta['display_name']
            description = table_meta['description']
            
            # Skip tables with no useful descriptions
            if description == 'NO_DATA_AVAILABLE':
                description = f"A table with {table_meta['rows_count']} rows"
            
            # Create a statement for the table
            table_statement = (
                f"Table '{display_name}' ({table_id}) contains {table_meta['rows_count']} rows. "
                f"Description: {description}. "
            )
            
            # Add column information if available
            if table_meta['columns']:
                # Extract and format column names properly based on the actual structure
                columns = table_meta['columns']
                
                # Assuming columns are either strings or objects with a 'name' attribute
                column_names = []
                for col in columns[:max_columns]:
                    if isinstance(col, str):
                        column_names.append(col)
                    elif isinstance(col, dict) and 'name' in col:
                        column_names.append(col['name'])
                    else:
                        # If we can't determine the column name, use a generic placeholder
                        column_names.append("unnamed column")
                
                column_info = ", ".join(column_names)
                if len(columns) > max_columns:
                    column_info += f", and {len(columns) - max_columns} more columns"
                table_statement += f"Columns: {column_info}."
            
            # Add component information
            comp_id = table_meta['created_by_component']['id']
            if comp_id != 'NO_DATA_AVAILABLE':
                table_statement += f" Created by {comp_id} ({table_meta['created_by_component']['description']})."
            
            # Add last import and change dates if available
            if table_meta.get('last_import_date'):
                table_statement += f" Last imported on {table_meta['last_import_date']}."
                
            if table_meta.get('last_change_date'):
                table_statement += f" Last changed on {table_meta['last_change_date']}."
            
            # Add bucket and stage information
            table_statement += f" Located in {table_meta['bucket']['stage']} stage, bucket {table_meta['bucket']['id']}."
            
            # Create a semantic statement for this table
            statement = SemanticStatement(
                statement=table_statement,
                always_include=False,  # Only include when relevant
                critical=False,
                labels=["kb_project"]
            )
            statements.append(statement)
        
        return statements

    def add_to_semantic_context(self, statements: List[SemanticStatement]) -> None:
        """
        Add statements to WAII semantic context.
        
        Args:
            statements: List of SemanticStatement objects
        """
        # Log the number of statements being added
        LOG.info(f"Adding {len(statements)} semantic context statements to WAII")
        
        try:
            # Update WAII semantic context
            resp: ModifySemanticContextResponse = SemanticContext.modify_semantic_context(
                ModifySemanticContextRequest(updated=statements)
            )
            
            # Store the statement IDs
            statement_ids = [stmt.id for stmt in resp.updated]
            
            # Log the results
            LOG.info(f"Successfully added {len(resp.updated)} semantic context statements to WAII")
            
            if len(resp.updated) != len(statements):
                LOG.warning(f"Not all statements were added: {len(resp.updated)}/{len(statements)}")
            
            # Save statement IDs to a file for later reference
            self._save_statement_ids_to_file(statement_ids)
            
        except Exception as e:
            # Print detailed error information
            LOG.error(f"Error adding semantic context: {str(e)}")
            # If there's an HTTP response in the error, log it
            if hasattr(e, 'response') and e.response:
                try:
                    error_detail = e.response.json()
                    LOG.error(f"Error details: {error_detail}")
                except:
                    LOG.error(f"Response status: {e.response.status_code}, content: {e.response.text}")
            raise
    
    def _save_statement_ids_to_file(self, statement_ids: List[str]) -> None:
        """
        Save statement IDs to a file for later reference.
        
        Args:
            statement_ids: List of statement IDs to save
        """
        try:
            # Get the directory where this script is located
            script_dir = os.path.dirname(os.path.abspath(__file__))
            
            # Create a directory for storing statement IDs if it doesn't exist
            statements_dir = os.path.join(script_dir, "..", "scripts", "statement_ids")
            os.makedirs(statements_dir, exist_ok=True)
            
            # Generate a timestamp for the filename
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.join(statements_dir, f"kb_statements_{timestamp}.json")
            
            # Save the statement IDs to a JSON file
            with open(filename, 'w') as f:
                json.dump({
                    'timestamp': timestamp,
                    'project': os.getenv('KEBOOLA_PROJECT_NAME', 'unknown'),
                    'statement_count': len(statement_ids),
                    'statement_ids': statement_ids
                }, f, indent=2)
            
            LOG.info(f"Saved {len(statement_ids)} statement IDs to {filename}")
        
        except Exception as e:
            LOG.error(f"Error saving statement IDs to file: {e}")
