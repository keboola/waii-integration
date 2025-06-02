"""Client for interacting with the Keboola Storage API."""

import logging
from typing import Dict, Optional
from kbcstorage.client import Client

LOG = logging.getLogger(__name__)

class KeboolaClient:
    """Client for interacting with the Keboola Storage API."""

    def __init__(self, token: str, api_url: str):
        """Initialize the client with token and API URL."""
        self.client = Client(api_url, token)
        logging.info("Initialized KeboolaClient with API URL: %s", api_url)

    def extract_metadata(self, limit: Optional[int] = None) -> Dict:
        """
        Extract metadata from Keboola project.
        
        Args:
            limit: Maximum total number of tables to fetch across all buckets (all tables if None)
            
        Returns:
            Dictionary containing buckets, tables and their metadata
        """
        logging.info("Starting metadata extraction (limit=%s)", limit)

        try:
            # Get all buckets
            buckets = self.client.buckets.list()
            
            # Initialize result structure
            result = {
                'buckets': buckets,
                'tables': {},
                'table_details': {}
            }
            
            # Track total tables processed
            total_tables_processed = 0
            
            # Process each bucket
            for bucket in buckets:
                bucket_id = bucket['id']
                
                # Get tables in bucket
                tables = self.client.buckets.list_tables(bucket_id)
                
                # Store tables in the result
                result['tables'][bucket_id] = tables
                
                # Get detailed info for each table
                for table in tables:
                    # Check if we've reached the total limit
                    if limit is not None and total_tables_processed >= limit:
                        logging.info(f"Reached total table limit ({limit}). Stopping metadata extraction.")
                        return result

                    table_id = table['id']
                    try:
                        table_detail = self.client.tables.detail(table_id)
                        result['table_details'][table_id] = table_detail
                        logging.info(f"Fetched details for table {table_id}")
                        total_tables_processed += 1
                    except Exception as e:
                        logging.error(f"Error fetching details for table {table_id}: {e}")
                        continue

            return result

        except Exception as e:
            logging.error(f"Error during metadata extraction: {e}")
            raise
