import asyncio
import logging
from typing import Callable, List

from google.auth import default
from google.cloud.bigtable.data import (
    BigtableDataClientAsync,
    RowRange,
    ReadRowsQuery,
    Row as AsyncRow,
)
from google.cloud.bigtable.data import row_filters as async_row_filters
from retry import retry


# Logging configuration
logging.basicConfig(level=logging.INFO)


# Latency logging decorators
def log_latency_async(service_name: str):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            t1 = asyncio.get_event_loop().time()
            result = await func(*args, **kwargs)
            t2 = asyncio.get_event_loop().time()
            logging.info(f"Service '{service_name}' executed in {t2 - t1:.4f}s")
            return result

        return wrapper

    return decorator


# BigTable Async Client
class BigTableClientAsync:
    def __init__(self, project_id: str, instance_id: str, table_id: str):
        """Initialize the BigTableClientAsync with project, instance, and table details."""
        self.project_id = str(project_id)
        self.instance_id = instance_id
        self.table_id = table_id
        self.credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self.client = BigtableDataClientAsync(project=self.project_id, credentials=self.credentials)
        self._table = None
        logging.info(f"Project ID: {self.project_id}, Instance ID: {self.instance_id}, Table ID: {self.table_id}")

    @property
    def table(self):
        """Lazily initialize and return the Bigtable table instance."""
        if not self._table:
            try:
                logging.info("Initializing Bigtable table...")
                self._table = self.client.get_table(self.instance_id, self.table_id)
                logging.info("Bigtable table initialized successfully.")
            except Exception as e:
                logging.error(f"Failed to initialize Bigtable table: {e}")
                raise
        return self._table

    @log_latency_async("big_table")
    async def read_row(self, key: str, row_filter=None):
        """Read a single row by its key with an optional filter."""
        table = self.table  # Ensure the table is initialized
        if row_filter is None:
            return await table.read_row(key)
        return await table.read_row(key, row_filter=row_filter)

    @log_latency_async("big_table")
    async def read_rows(
        self,
        row_keys: List[str] | None = None,
        row_ranges: List[RowRange] | None = None,
        filter_: async_row_filters.RowFilter | None = None,
    ):
        """Read multiple rows based on keys, ranges, or filters."""
        query = ReadRowsQuery(row_keys=row_keys, row_ranges=row_ranges, row_filter=filter_)
        return await self.table.read_rows(query=query)

    def retrieve_row(self, row: AsyncRow) -> dict[str, str]:
        """Convert an async row object to a dictionary."""
        row_dict = {}
        for cell in sorted(row.cells, key=lambda c: c.qualifier):
            row_dict[cell.qualifier.decode("utf-8")] = cell.value.decode("utf-8")
        return row_dict

    async def return_rows_with_prefix(
        self,
        prefix: str,
        filter_: async_row_filters.RowFilter | None = None,
        filter_callback: Callable | None = None,
    ):
        """Fetch rows with a specific prefix and optional callback filter."""
        res = []
        end_key = prefix[:-1] + chr(ord(prefix[-1]) + 1)
        row_range = RowRange(prefix, end_key)
        rows = await self.read_rows(row_ranges=[row_range], filter_=filter_)
        for row in rows:
            row_info = self.retrieve_row(row)
            if filter_callback is None or filter_callback(row_info):
                res.append(row_info)
        return res

    async def close(self):
        """Close the Bigtable client."""
        await self.client.close()


# Main function for usage example
async def main():
    project_id = 'qualified-root-393107'
    instance_id= 'my-instance-1'
    table_id= 'my-table-1'

    # Initialize the BigTable async client
    bigtable_client = BigTableClientAsync(project_id, instance_id, table_id)

    try:
        # Example: Read a single row
        row_key = "20241225_061415528944"
        logging.info(f"Fetching row for key: {row_key}")
        row = await bigtable_client.read_row(row_key)
        if row:
            row_data = bigtable_client.retrieve_row(row)
            logging.info(f"Row data: {row_data}")
        else:
            logging.info("Row not found.")

        # Example: Read multiple rows with a prefix
        prefix = "20241225"
        rows_with_prefix = await bigtable_client.return_rows_with_prefix(prefix)
        logging.info(f"Rows with prefix '{prefix}': {rows_with_prefix}")

    except Exception as e:
        logging.error(f"Error occurred: {e}")
    finally:
        await bigtable_client.close()


# Entry point
if __name__ == "__main__":
    asyncio.run(main())