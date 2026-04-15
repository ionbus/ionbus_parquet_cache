"""Tests for exceptions.py - exception taxonomy."""

from __future__ import annotations

import pytest

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
)


class TestExceptionHierarchy:
    """Tests for exception inheritance hierarchy."""

    def test_all_inherit_from_base(self) -> None:
        """All exceptions should inherit from ParquetCacheError."""
        assert issubclass(SchemaMismatchError, ParquetCacheError)
        assert issubclass(SnapshotError, ParquetCacheError)
        assert issubclass(SnapshotPublishError, ParquetCacheError)
        assert issubclass(SnapshotNotFoundError, ParquetCacheError)
        assert issubclass(DataSourceError, ParquetCacheError)
        assert issubclass(SyncError, ParquetCacheError)
        assert issubclass(ValidationError, ParquetCacheError)
        assert issubclass(ConfigurationError, ParquetCacheError)

    def test_snapshot_errors_inherit_from_snapshot_error(self) -> None:
        """Snapshot-related errors should inherit from SnapshotError."""
        assert issubclass(SnapshotPublishError, SnapshotError)
        assert issubclass(SnapshotNotFoundError, SnapshotError)

    def test_can_catch_with_base(self) -> None:
        """Should be able to catch all exceptions with ParquetCacheError."""
        with pytest.raises(ParquetCacheError):
            raise SchemaMismatchError("test")

        with pytest.raises(ParquetCacheError):
            raise SnapshotNotFoundError("test")

        with pytest.raises(ParquetCacheError):
            raise DataSourceError("test")


class TestSchemaMismatchError:
    """Tests for SchemaMismatchError."""

    def test_basic_message(self) -> None:
        """Basic error with just message."""
        err = SchemaMismatchError("Column type changed")
        assert str(err) == "Column type changed"
        assert err.column is None
        assert err.expected_type is None
        assert err.actual_type is None

    def test_with_details(self) -> None:
        """Error with column and type details."""
        err = SchemaMismatchError(
            "Column 'price' type changed from float to string",
            column="price",
            expected_type="float64",
            actual_type="string",
        )
        assert err.column == "price"
        assert err.expected_type == "float64"
        assert err.actual_type == "string"


class TestSnapshotNotFoundError:
    """Tests for SnapshotNotFoundError."""

    def test_basic_message(self) -> None:
        """Basic error with just message."""
        err = SnapshotNotFoundError("No snapshot found")
        assert str(err) == "No snapshot found"
        assert err.dataset_name is None

    def test_with_dataset_name(self) -> None:
        """Error with dataset name."""
        err = SnapshotNotFoundError(
            "No snapshot found for 'md.futures'", dataset_name="md.futures"
        )
        assert err.dataset_name == "md.futures"


class TestDataSourceError:
    """Tests for DataSourceError."""

    def test_basic_message(self) -> None:
        """Basic error with just message."""
        err = DataSourceError("API connection failed")
        assert str(err) == "API connection failed"
        assert err.source_class is None
        assert err.partition_info is None

    def test_with_details(self) -> None:
        """Error with source class and partition info."""
        err = DataSourceError(
            "Failed to fetch data",
            source_class="MyFuturesSource",
            partition_info={"FutureRoot": "ES", "year": "Y2024"},
        )
        assert err.source_class == "MyFuturesSource"
        assert err.partition_info == {"FutureRoot": "ES", "year": "Y2024"}


class TestSyncError:
    """Tests for SyncError."""

    def test_basic_message(self) -> None:
        """Basic error with just message."""
        err = SyncError("Network timeout")
        assert str(err) == "Network timeout"
        assert err.source_path is None
        assert err.target_path is None

    def test_with_paths(self) -> None:
        """Error with source and target paths."""
        err = SyncError(
            "Permission denied",
            source_path="/source/cache",
            target_path="/target/cache",
        )
        assert err.source_path == "/source/cache"
        assert err.target_path == "/target/cache"


class TestConfigurationError:
    """Tests for ConfigurationError."""

    def test_basic_message(self) -> None:
        """Basic error with just message."""
        err = ConfigurationError("Invalid YAML")
        assert str(err) == "Invalid YAML"
        assert err.config_file is None

    def test_with_config_file(self) -> None:
        """Error with config file path."""
        err = ConfigurationError(
            "Missing required field 'source_class_name'",
            config_file="/cache/yaml/futures.yaml",
        )
        assert err.config_file == "/cache/yaml/futures.yaml"
