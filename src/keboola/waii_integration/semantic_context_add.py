"""
Test WAII semantic context with Keboola metadata.
Tests different combinations of metadata types and their impact on WAII responses.
Adds Keboola metadata to WAII semantic context.
"""

import logging
import sys
import os
import argparse
from pathlib import Path
from keboola.waii_integration.waii_utils.waii_context_manager import WaiiSemanticContextManager
from keboola.waii_integration.keboola_utils.metadata_collector import KeboolaMetadataCollector

LOG = logging.getLogger(__name__)


def main():
    """CLI interface for testing Keboola metadata collection and adding to WAII semantic context"""
    parser = argparse.ArgumentParser(description='Pull metadata from Keboola project and add to WAII semantic context')
    parser.add_argument('--limit', type=int, default=None, help='Limit the number of tables to process')
    parser.add_argument('--out-dir', type=str, default='statement_ids', 
                        help='Output directory name inside data/ folder where statement IDs will be saved (default: statement_ids)')
    args = parser.parse_args()
    
    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s',
        level=logging.INFO
    )

    # Create data directory - use pathlib to get project root
    project_root = Path(__file__).parents[3]
    data_dir = project_root / 'data'
    out_dir = data_dir / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    LOG.info(f"Ensuring output directory exists: {out_dir}")
    
    # Get settings from environment variables
    required_vars = [
        'KEBOOLA_API_TOKEN',
        'KEBOOLA_PROJECT_URL',
        'WAII_API_URL',
        'WAII_API_KEY',
        'WAII_CONNECTION'
    ]
    missing_vars = [var for var in required_vars if os.getenv(var) is None]
    if missing_vars:
        LOG.error(f"Missing environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    # If limit not provided via command line, use environment variable or default
    limit = args.limit
    project_name = os.getenv('KEBOOLA_PROJECT_NAME', 'default')

    # Get API credentials from environment
    api_token = os.getenv('KEBOOLA_API_TOKEN')
    project_url = os.getenv('KEBOOLA_PROJECT_URL')
    
    # Get WAII credentials from environment
    waii_api_url = os.getenv('WAII_API_URL')
    waii_api_key = os.getenv('WAII_API_KEY')
    waii_connection = os.getenv('WAII_CONNECTION')
    waii_db_database = os.getenv('WAII_DB_DATABASE')
    waii_db_username = os.getenv('WAII_DB_USERNAME') 

    LOG.info(f"Using project: {project_name}")

    try:
        # Step 1: Collect metadata from Keboola
        LOG.info("Collecting metadata from Keboola")
        collector = KeboolaMetadataCollector(api_token, project_url, project_name)
        metadata = collector.get_tables_metadata_sample(limit=limit)

        # Add to WAII semantic context
        table_count = len(metadata.tables)
        LOG.info(f"Adding metadata for {table_count} tables to WAII")
        try:
            waii_manager = WaiiSemanticContextManager(
                api_url=waii_api_url,
                api_key=waii_api_key,
                connection_name=waii_connection,
                project_name=project_name,
                statement_ids_path=args.out_dir,
                db_database=waii_db_database,
                db_username=waii_db_username
            )
            statements = waii_manager.create_semantic_context_statements(metadata.tables)
            waii_manager.add_to_semantic_context(statements)

            LOG.info("Successfully added Keboola metadata to WAII semantic context")

        except Exception as waii_error:
            LOG.exception(f"Error adding metadata to WAII: {waii_error}")
            sys.exit(1)

    except Exception as e:
        LOG.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
