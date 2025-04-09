"""
Test WAII semantic context with Keboola metadata.
Tests different combinations of metadata types and their impact on WAII responses.
Adds Keboola metadata to WAII semantic context.
"""

import logging
import sys
import os
import argparse
from dotenv import load_dotenv

# Add parent directory to path to allow importing the src modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the modules
from src.keboola_metadata_collector import KeboolaMetadataCollector
from src.waii_context_manager import WaiiSemanticContextManager

LOG = logging.getLogger(__name__)

def main():
    """CLI interface for testing Keboola metadata collection and adding to WAII semantic context"""
    parser = argparse.ArgumentParser(description='Pull metadata from Keboola project and add to WAII semantic context')
    parser.add_argument('--limit', type=int, default=None, help='Limit the number of tables to process')
    parser.add_argument('--dry-run', action='store_true', help='Only print metadata results without adding to WAII')
    args = parser.parse_args()
    
    # Load environment variables
    load_dotenv(verbose=True)
    
    # Set up logging
    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s',
        level=logging.INFO
    )

    # Get settings from environment variables
    required_vars = ['KEBOOLA_API_TOKEN', 'KEBOOLA_PROJECT_URL']
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

    LOG.info(f"Using project: {project_name}")

    try:
        # Step 1: Collect metadata from Keboola
        LOG.info("Collecting metadata from Keboola")
        collector = KeboolaMetadataCollector(api_token, project_url)
        metadata = collector.get_tables_metadata_sample(limit=limit)

        # Dry-run mode: Print metadata results
        if args.dry_run:
            LOG.info("Dry run mode: Printing metadata results")
            collector.print_metadata_results(metadata, limit)
            LOG.info("Dry run mode: Not adding metadata to WAII semantic context")

        # Not dry-run: Add to WAII semantic context and do not print results
        else:
            LOG.info(f"Production mode: Adding metadata for {len(metadata)} tables to WAII")
            try:
                # Initialize WAII manager and add statements
                waii_manager = WaiiSemanticContextManager()
                
                # Create and add semantic context statements
                statements = waii_manager.create_semantic_context_statements(metadata)
                waii_manager.add_to_semantic_context(statements)
  
                LOG.info("Successfully added Keboola metadata to WAII semantic context")

            except Exception as waii_error:
                LOG.error(f"Error adding metadata to WAII: {waii_error}")
                sys.exit(1)

    except Exception as e:
        LOG.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
