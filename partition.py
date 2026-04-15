"""
Partition specification and utilities for ionbus_parquet_cache.

Partitions define the granularity at which data is stored and updated.
Uses ionbus_utils.date_utils for Hive-style date partitioning.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from ionbus_utils.date_utils import (
    DATE_PARTITION_GRANULARITIES,
    date_partition_range,
    date_partition_value,
)


@dataclass
class PartitionSpec:
    """
    Specification for a single partition or chunk to be processed.

    A PartitionSpec represents either:
    - A complete partition (when date range covers full partition period)
    - A chunk within a partition (when date range is smaller than partition
        period)

    Multiple PartitionSpecs with the same `partition_values` but different
    date ranges are chunks that will be consolidated into a single partition
    file.

    Attributes:
        partition_values: Dict mapping partition column names to their values
            for this partition. Example: {"FutureRoot": "ES", "year": "Y2024"}
        start_date: First date (inclusive) of data in this partition/chunk.
        end_date: Last date (inclusive) of data in this partition/chunk.
        chunk_info: Optional dict for DataSource-specific chunking metadata
            when a single logical partition needs to be split into multiple
            reads (e.g., by ticker subset, exchange, or other dimension not in
            the DPD's partition_columns). Not used for file naming or grouping
            by the DPD - purely for the DataSource's benefit.
        temp_file_path: Temporary file path assigned by DPD during processing.
            Not set by DataSource - DPD assigns this before calling get_data().
    """

    partition_values: dict[str, Any]
    start_date: dt.date
    end_date: dt.date
    chunk_info: dict[str, Any] = field(default_factory=dict, compare=False)
    temp_file_path: str | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        """Validate the partition spec after initialization."""
        if self.start_date > self.end_date:
            raise ValueError(
                f"start_date ({self.start_date}) must be <= end_date "
                f"({self.end_date})"
            )

    @property
    def partition_key(self) -> tuple[tuple[str, Any], ...]:
        """
        Return a hashable key for grouping specs by partition.

        Specs with the same partition_key belong to the same final
        partition file (they are chunks to be consolidated).
        """
        return tuple(sorted(self.partition_values.items()))

    def __hash__(self) -> int:
        """Hash based on partition values, date range, and chunk info."""
        # Convert chunk_info values to hashable types (lists -> tuples)
        chunk_items = []
        for k, v in sorted(self.chunk_info.items()):
            if isinstance(v, list):
                v = tuple(v)
            chunk_items.append((k, v))
        chunk_key = tuple(chunk_items)
        return hash(
            (self.partition_key, self.start_date, self.end_date, chunk_key)
        )


def date_partition_column_name(
    date_partition: str, date_col: str = "Date"
) -> str:
    """
    Get the partition column name for a date partition granularity.

    Args:
        date_partition: One of "day", "week", "month", "quarter", "year".
        date_col: The name of the date column (used for "day" partitions).

    Returns:
        The partition column name (e.g., "Date", "week", "month", etc.)
        For "day" partitions, returns the actual date_col name.

    Raises:
        ValueError: If date_partition is not a valid granularity.
    """
    if date_partition not in DATE_PARTITION_GRANULARITIES:
        raise ValueError(
            f"Invalid date_partition '{date_partition}'. "
            f"Must be one of: {DATE_PARTITION_GRANULARITIES}"
        )

    # "day" partition uses the actual date column, not a separate partition
    # column
    if date_partition == "day":
        return date_col

    return date_partition


def date_partitions(
    start_date: dt.date,
    end_date: dt.date,
    date_partition: str,
    max_business_days: int | None = None,
) -> list[tuple[dt.date, dt.date, str]]:
    """
    Generate date partition ranges for a date range.

    Returns a list of (start_date, end_date, partition_value) tuples
    that cover the input range. Each tuple represents either a full
    partition period or a chunk if max_business_days is specified.

    Args:
        start_date: First date of the range.
        end_date: Last date of the range.
        date_partition: Partition granularity.
        max_business_days: Optional maximum business days per chunk.
            If specified, partitions may be split into smaller chunks.

    Returns:
        List of (start, end, partition_value) tuples covering the range.

    Example:
        >>> date_partitions(dt.date(2024, 1, 15), dt.date(2024, 3, 15), "month")
        [
            (dt.date(2024, 1, 15), dt.date(2024, 1, 31), "M2024-01"),
            (dt.date(2024, 2, 1), dt.date(2024, 2, 29), "M2024-02"),
            (dt.date(2024, 3, 1), dt.date(2024, 3, 15), "M2024-03"),
        ]
    """
    if start_date > end_date:
        return []

    result = []
    current = start_date

    while current <= end_date:
        # Get the partition value for current date
        part_val = date_partition_value(current, date_partition)

        # Get the full range for this partition
        part_start, part_end = date_partition_range(part_val, date_partition)

        # Clamp to our actual range
        chunk_start = max(current, part_start)
        chunk_end = min(end_date, part_end)

        # If max_business_days is set, we may need to split further
        if max_business_days is not None:
            # Use pandas business day offset for accurate chunking
            temp_start = chunk_start
            while temp_start <= chunk_end:
                # Calculate end date: start + (n-1) business days
                # BDay(0) is same day if business day, so use n-1
                offset = pd.offsets.BDay(max_business_days - 1)
                temp_end_ts = pd.Timestamp(temp_start) + offset
                temp_end = min(temp_end_ts.date(), chunk_end)
                result.append((temp_start, temp_end, part_val))
                # Move to next business day after temp_end
                next_ts = pd.Timestamp(temp_end) + pd.offsets.BDay(1)
                temp_start = next_ts.date()
        else:
            result.append((chunk_start, chunk_end, part_val))

        # Move to next partition
        current = part_end + dt.timedelta(days=1)

    return result
