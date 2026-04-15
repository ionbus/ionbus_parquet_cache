"""
Exception taxonomy for ionbus_parquet_cache.

All exceptions inherit from ParquetCacheError for easy catching of
any package-related errors.
"""

from __future__ import annotations


class ParquetCacheError(Exception):
    """Base exception for all ionbus_parquet_cache errors."""

    pass


class SchemaMismatchError(ParquetCacheError):
    """
    Raised when schema validation fails.

    This occurs when:
    - A column type changes incompatibly between updates
    - Required columns are missing from new data
    """

    def __init__(
        self,
        message: str,
        column: str | None = None,
        expected_type: str | None = None,
        actual_type: str | None = None,
    ):
        super().__init__(message)
        self.column = column
        self.expected_type = expected_type
        self.actual_type = actual_type


class SnapshotError(ParquetCacheError):
    """Base class for snapshot-related errors."""

    pass


class SnapshotPublishError(SnapshotError):
    """
    Raised when publishing a new snapshot fails.

    This may occur due to:
    - Filesystem errors during atomic rename
    - Duplicate snapshot suffix (same-second update)
    - Metadata serialization failure
    """

    pass


class SnapshotNotFoundError(SnapshotError):
    """
    Raised when no valid snapshot exists.

    This occurs when:
    - A dataset has no metadata files
    - All snapshot files are corrupted or missing
    """

    def __init__(self, message: str, dataset_name: str | None = None):
        super().__init__(message)
        self.dataset_name = dataset_name


class DataSourceError(ParquetCacheError):
    """
    Raised when a DataSource fails to provide data.

    This may occur due to:
    - API/database connection failures
    - Invalid partition specifications
    - Data fetch timeouts
    """

    def __init__(
        self,
        message: str,
        source_class: str | None = None,
        partition_info: dict | None = None,
    ):
        super().__init__(message)
        self.source_class = source_class
        self.partition_info = partition_info


class SyncError(ParquetCacheError):
    """
    Raised when cache synchronization fails.

    This may occur due to:
    - Network errors during transfer
    - Permission issues on source/target
    - Partial transfer failures
    """

    def __init__(
        self,
        message: str,
        source_path: str | None = None,
        target_path: str | None = None,
    ):
        super().__init__(message)
        self.source_path = source_path
        self.target_path = target_path


class ValidationError(ParquetCacheError):
    """
    Raised when data validation fails.

    This occurs when:
    - File integrity checks fail (checksum mismatch)
    - Required data is missing or malformed
    - Partition structure is invalid
    """

    pass


class ConfigurationError(ParquetCacheError):
    """
    Raised when configuration is invalid.

    This occurs when:
    - YAML configuration is malformed
    - Required fields are missing
    - DataSource class cannot be loaded
    """

    def __init__(self, message: str, config_file: str | None = None):
        super().__init__(message)
        self.config_file = config_file
