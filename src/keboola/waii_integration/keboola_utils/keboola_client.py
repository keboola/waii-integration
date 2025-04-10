"""
Client for interacting with the Keboola Storage API.
"""

import logging
from typing import Optional
from kbcstorage.client import Client
from keboola.waii_integration.keboola_utils.keboola_models import KeboolaMetadata, KeboolaBucket, KeboolaTable, KeboolaTableDetail

LOG = logging.getLogger(__name__)


class KeboolaClient:
    """Client for interacting with the Keboola Storage API."""

    def __init__(self, token: str, api_url: str):
        """Initialize the client with token and API URL."""
        self.client = Client(api_url, token)
        LOG.info("Initialized KeboolaClient with API URL: %s", api_url)

    def extract_metadata(self, limit: Optional[int] = None) -> KeboolaMetadata:
        """
        Extract metadata from Keboola project.
        
        Args:
            limit: Maximum total number of tables to fetch across all buckets (all tables if None)
            
        Returns:
            KeboolaMetadata containing buckets, tables and their metadata
        """
        LOG.info("Starting metadata extraction (limit=%s)", limit)

        try:
            # Create empty result structure
            result = KeboolaMetadata()

            # Get and convert buckets
            result.buckets = [KeboolaBucket(**b) for b in self.client.buckets.list()]

            # Track total tables processed
            total_tables_processed = 0

            # Process each bucket
            for bucket in result.buckets:
                # Get and convert tables for this bucket
                tables = [KeboolaTable(**t) for t in self.client.buckets.list_tables(bucket.id)]
                result.tables[bucket.id] = tables

                # Get detailed info for each table
                for table in tables:
                    # Stop if we've reached the limit
                    if limit is not None and total_tables_processed >= limit:
                        LOG.info(f"Reached table limit ({limit}). Stopping extraction.")
                        return result

                    try:
                        # Get and convert table details
                        detail = self.client.tables.detail(table.id)
                        result.table_details[table.id] = KeboolaTableDetail(**detail)
                        LOG.info(f"Fetched details for table {table.id}")
                        total_tables_processed += 1
                    except Exception as e:
                        LOG.error(f"Error fetching details for table {table.id}: {e}")
                        continue

            return result

        except Exception as e:
            LOG.error(f"Error during metadata extraction: {e}")
            raise e
