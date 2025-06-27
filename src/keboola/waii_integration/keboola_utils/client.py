"""Client for interacting with the Keboola Storage API."""

import logging
from kbcstorage.client import Client
from keboola.waii_integration.models import Bucket, Table, Metadata

LOG = logging.getLogger(__name__)


class KeboolaClient:
    """Client for interacting with the Keboola Storage API."""

    def __init__(self, token: str, api_url: str):
        """Initialize the client with token and API URL."""
        self.client = Client(api_url, token)
        logging.info("Initialized KeboolaClient with API URL: %s", api_url)

    def extract_metadata_from_project(self, limit: int | None = None) -> Metadata:
        """
        Extract metadata from Keboola project.
        
        Args:
            limit: Maximum total number of tables to fetch across all buckets (all tables if None)
            
                    Returns:
            Metadata: Pydantic model containing buckets, tables and their metadata
        """
        logging.info("Starting metadata extraction (limit=%s)", limit)

        # Fetch and convert buckets to Pydantic models
        raw_buckets = self.client.buckets.list()
        buckets = [Bucket(**bucket) for bucket in raw_buckets]
        
        tables = dict()
        table_details = dict()
        total_tables_processed = 0
        
        # Process each bucket to extract tables and their details
        for bucket in raw_buckets:
            bucket_id = bucket['id']
            raw_bucket_tables = self.client.buckets.list_tables(bucket_id)
            
            # Convert raw tables to Pydantic models
            bucket_tables = [Table(**table) for table in raw_bucket_tables]
            tables[bucket_id] = bucket_tables
            
            # Process each table in the current bucket
            for table in raw_bucket_tables:
                # Check limit before processing each table
                if limit is not None and total_tables_processed >= limit:
                    logging.info(f"Reached total table limit ({limit}). Returning metadata.")
                    return Metadata(
                        buckets=buckets,
                        tables=tables,
                        table_details=table_details
                    )
        
                table_id = table['id']
                # Fetch detailed information for the current table
                raw_table_detail = self.client.tables.detail(table_id)
                # Convert to Pydantic model
                table_detail = Table(**raw_table_detail)
                table_details[table_id] = table_detail
                logging.info(f"Fetched details for table {table_id}")
                total_tables_processed += 1

        # Create and return the Pydantic model with all data
        return Metadata(
            buckets=buckets,
            tables=tables,
            table_details=table_details
        )
