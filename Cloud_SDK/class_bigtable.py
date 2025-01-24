from typing import Callable, List
from google.cloud import bigtable
from google.cloud.bigtable import row_filters
from google.cloud.bigtable.row_set import RowSet


# Imports specific to async API (optional)
from google.cloud.bigtable.data import (
    BigtableDataClientAsync,
    ReadRowsQuery,
    RowRange,
    Row as AsyncRow,
)
from google.cloud.bigtable.data import row_filters as async_row_filters

import google.auth
from retry import retry  # Retry for fault tolerance
from src.handlers import log_latency


class BigTable:
    @retry(tries=3, delay=0.5, backoff=2, max_delay=2)  # Retry decorator
    def __init__(self, project_id=None, instance_id=None, table_id=None):
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id

        # Authenticate and initialize Bigtable client
        self.credentials, _ = google.auth.default()
        self.client = bigtable.Client(
            project=self.project_id, credentials=self.credentials, admin=True
        )
        self.instance = self.client.instance(self.instance_id)
        self.table = self.instance.table(self.table_id)

    @log_latency("big_table")  # Log latency for monitoring
    def read_row(self, key, row_filter=None):
        """Reads a single row from Bigtable."""
        if row_filter is None:
            row = self.table.read_row(key)
        else:
            row = self.table.read_row(key, row_filter=row_filter)
        return row

    @log_latency("big_table")
    def read_rows(self, row_set, filter_=None):
        """Reads multiple rows from Bigtable."""
        if filter_ is None:
            rows = self.table.read_rows(row_set=row_set)
        else:
            rows = self.table.read_rows(row_set=row_set, filter_=filter_)
        return rows

    def retrieve_row(self, row):
        """Converts a Bigtable row into a dictionary."""
        row_dict = {}
        for cf, cols in sorted(row.cells.items()):  # Loop through column families
            for col, cells in sorted(cols.items()):  # Loop through columns
                for cell in cells:
                    # Decode column name and cell value
                    row_dict[col.decode("utf-8")] = cell.value.decode("utf-8")
        return row_dict

    def filter_row(self, related_documents_id):
        """Fetches rows based on IDs and applies a column filter."""
        source_text_info = []
        row_filter = row_filters.CellsColumnLimitFilter(1)  # Limit to 1 cell per column
        for row_id in related_documents_id:
            key = row_id.encode()
            row = self.read_row(key, row_filter)
            if row:
                source_text_info.append(self.retrieve_row(row))
        return source_text_info


if __name__ == "__main__":
    # Initialize the BigTable client
    bigtable_client = BigTable(
        project_id = 'shan-practice-dev',
        instance_id="my-instance-1",
        table_id="my-big-table-1"
    )
    print("Connection Setup Successfull")
    # Read a single row
    row_key = "20241225_061415528944"
    row = bigtable_client.read_row(row_key)

    if row:
        print("Single Row Data:", bigtable_client.retrieve_row(row))
    else:
        print("Row not found.")
    
    # Read multiple rows
    row_set = RowSet()
    row_set.add_row_key("20241225_061415528944".encode())
    row_set.add_row_key("20241225_061415528944".encode())

    rows = bigtable_client.read_rows(row_set)
    for row in rows:
        print("Row Data:", bigtable_client.retrieve_row(row))