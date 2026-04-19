"""
ionbus_parquet_cache: Parquet dataset management with snapshot versioning.

This package provides:
- DatedParquetDataset: Date-partitioned datasets with snapshot metadata
- NonDatedParquetDataset: Non-dated datasets imported as complete snapshots
- CacheRegistry: Multi-cache search across prioritized locations
- DataSource: Abstract interface for supplying data to datasets
- DataCleaner: Abstract interface for custom data transformations
"""

from __future__ import annotations

from ionbus_parquet_cache.exceptions import (
    ParquetCacheError,
    SchemaMismatchError,
    SnapshotError,
    SnapshotPublishError,
    SnapshotNotFoundError,
    DataSourceError,
    SyncError,
    ValidationError,
    ConfigurationError,
    UpdateLockedError,
)
from ionbus_parquet_cache.snapshot import (
    generate_snapshot_suffix,
    parse_snapshot_suffix,
    extract_suffix_from_filename,
    get_current_suffix,
)
from ionbus_parquet_cache.partition import (
    PartitionSpec,
    DATE_PARTITION_GRANULARITIES,
    date_partition_column_name,
    date_partition_value,
    date_partition_range,
    date_partitions,
)
from ionbus_parquet_cache.parquet_dataset_base import ParquetDataset
from ionbus_parquet_cache.filter_utils import (
    or_filter,
    and_filter,
    pandas_filter_list_to_expression,
)
from ionbus_parquet_cache.dated_dataset import (
    DatedParquetDataset,
    SnapshotMetadata,
)
from ionbus_parquet_cache.non_dated_dataset import NonDatedParquetDataset
from ionbus_parquet_cache.cache_registry import CacheRegistry, DatasetType
from ionbus_parquet_cache.data_source import DataFrameType, DataSource
from ionbus_parquet_cache.data_cleaner import DataCleaner, NoOpCleaner
from ionbus_parquet_cache.yaml_config import (
    DatasetConfig,
    load_yaml_file,
    load_all_configs,
    get_dataset_config,
    apply_yaml_transforms,
)
from ionbus_parquet_cache.builtin_sources import (
    HiveParquetSource,
    DPDSource,
)
from ionbus_parquet_cache.bucketing import (
    instrument_bucket,
    bucket_instruments,
)

__all__ = [
    # Exceptions
    "ParquetCacheError",
    "SchemaMismatchError",
    "SnapshotError",
    "SnapshotPublishError",
    "SnapshotNotFoundError",
    "DataSourceError",
    "SyncError",
    "ValidationError",
    "ConfigurationError",
    "UpdateLockedError",
    # Snapshot utilities
    "generate_snapshot_suffix",
    "parse_snapshot_suffix",
    "extract_suffix_from_filename",
    "get_current_suffix",
    # Partition utilities
    "PartitionSpec",
    "DATE_PARTITION_GRANULARITIES",
    "date_partition_column_name",
    "date_partition_value",
    "date_partition_range",
    "date_partitions",
    # Base class
    "ParquetDataset",
    # Filter utilities
    "or_filter",
    "and_filter",
    "pandas_filter_list_to_expression",
    # Dataset classes
    "DatedParquetDataset",
    "NonDatedParquetDataset",
    "SnapshotMetadata",
    # Registry
    "CacheRegistry",
    "DatasetType",
    # Data interfaces
    "DataFrameType",
    "DataSource",
    "DataCleaner",
    "NoOpCleaner",
    # YAML configuration
    "DatasetConfig",
    "load_yaml_file",
    "load_all_configs",
    "get_dataset_config",
    "apply_yaml_transforms",
    # Built-in data sources
    "HiveParquetSource",
    "DPDSource",
    # Bucketing utilities
    "instrument_bucket",
    "bucket_instruments",
]

__version__ = "0.1.0"
