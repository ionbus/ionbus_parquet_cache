"""Tests for GCS code paths using mocks."""

from __future__ import annotations

import gzip
import io
import pickle
from unittest import mock

import pyarrow as pa
import pytest

from ionbus_parquet_cache.cache_registry import CacheRegistry
from ionbus_parquet_cache.dated_dataset import (
    SNAPSHOT_METADATA_PICKLE_PROTOCOL,
    SnapshotMetadata,
)
from ionbus_parquet_cache.non_dated_dataset import NonDatedParquetDataset


@pytest.fixture(autouse=True)
def reset_registry() -> None:
    """Reset the singleton registry before each test."""
    CacheRegistry.reset()
    yield
    CacheRegistry.reset()


class TestNonDatedDatasetGCSSnapshotPathResolution:
    """Tests for NPD GCS snapshot path resolution."""

    def test_get_current_snapshot_path_file_based_snapshot(self) -> None:
        """
        Test that _get_current_snapshot_path() correctly handles GCS file-based
        snapshots.
        """
        with mock.patch(
            "ionbus_parquet_cache.gcs_utils.gcs_exists"
        ) as mock_gcs_exists, mock.patch(
            "ionbus_parquet_cache.gcs_utils.gcs_join"
        ) as mock_gcs_join:
            # Setup mocks
            mock_gcs_join.side_effect = lambda base, *parts: (
                f"{base}/{'/'.join(parts)}"
            )

            def gcs_exists_side_effect(path: str) -> bool:
                # Directory doesn't exist, file exists
                return "_1H4DW01.parquet" in path

            mock_gcs_exists.side_effect = gcs_exists_side_effect

            # Create NPD with GCS path
            npd = NonDatedParquetDataset(
                cache_dir="gs://bucket/cache",
                name="test_dataset",
                is_gcs=True,
            )
            npd.current_suffix = "1H4DW01"

            # Get the snapshot path
            result = npd._get_current_snapshot_path()

            # Verify it returns correct GCS URL
            assert result == "gs://bucket/cache/non-dated/test_dataset/test_dataset_1H4DW01.parquet"
            assert npd._is_directory_snapshot is False

            # Verify mocks were called correctly
            assert mock_gcs_join.called
            assert mock_gcs_exists.called

    def test_get_current_snapshot_path_directory_snapshot(self) -> None:
        """
        Test that _get_current_snapshot_path() correctly handles GCS
        directory-based snapshots.
        """
        with mock.patch(
            "ionbus_parquet_cache.gcs_utils.gcs_exists"
        ) as mock_gcs_exists, mock.patch(
            "ionbus_parquet_cache.gcs_utils.gcs_join"
        ) as mock_gcs_join:
            # Setup mocks
            mock_gcs_join.side_effect = lambda base, *parts: (
                f"{base}/{'/'.join(parts)}"
            )

            def gcs_exists_side_effect(path: str) -> bool:
                # Directory exists
                return "_1H4DW01" in path and ".parquet" not in path

            mock_gcs_exists.side_effect = gcs_exists_side_effect

            # Create NPD with GCS path
            npd = NonDatedParquetDataset(
                cache_dir="gs://bucket/cache",
                name="calendar_data",
                is_gcs=True,
            )
            npd.current_suffix = "1H4DW01"

            # Get the snapshot path
            result = npd._get_current_snapshot_path()

            # Verify it returns correct GCS URL for directory
            assert result == "gs://bucket/cache/non-dated/calendar_data/calendar_data_1H4DW01"
            assert npd._is_directory_snapshot is True

    def test_get_current_snapshot_path_not_found(self) -> None:
        """
        Test that _get_current_snapshot_path() returns None when no snapshot
        exists in GCS.
        """
        with mock.patch(
            "ionbus_parquet_cache.gcs_utils.gcs_exists"
        ) as mock_gcs_exists, mock.patch(
            "ionbus_parquet_cache.gcs_utils.gcs_join"
        ) as mock_gcs_join:
            # Setup mocks - neither file nor directory exists
            mock_gcs_exists.return_value = False
            mock_gcs_join.side_effect = lambda base, *parts: (
                f"{base}/{'/'.join(parts)}"
            )

            # Create NPD with GCS path
            npd = NonDatedParquetDataset(
                cache_dir="gs://bucket/cache",
                name="missing_dataset",
                is_gcs=True,
            )
            npd.current_suffix = "1H4DW01"

            # Get the snapshot path
            result = npd._get_current_snapshot_path()

            # Verify it returns None
            assert result is None

    def test_get_current_snapshot_path_discovers_suffix(self) -> None:
        """
        Test that _get_current_snapshot_path() discovers the current suffix
        when not set.
        """
        with mock.patch(
            "ionbus_parquet_cache.gcs_utils.gcs_ls"
        ) as mock_gcs_ls, mock.patch(
            "ionbus_parquet_cache.gcs_utils.gcs_exists"
        ) as mock_gcs_exists, mock.patch(
            "ionbus_parquet_cache.gcs_utils.gcs_join"
        ) as mock_gcs_join:
            # Setup mocks
            mock_gcs_ls.return_value = [
                "gs://bucket/cache/non-dated/test_dataset/test_dataset_1H4DW00.parquet",
                "gs://bucket/cache/non-dated/test_dataset/test_dataset_1H4DW02.parquet",
            ]
            mock_gcs_join.side_effect = lambda base, *parts: (
                f"{base}/{'/'.join(parts)}"
            )
            mock_gcs_exists.return_value = True

            # Create NPD with GCS path, no suffix set
            npd = NonDatedParquetDataset(
                cache_dir="gs://bucket/cache",
                name="test_dataset",
                is_gcs=True,
            )

            # Get the snapshot path
            result = npd._get_current_snapshot_path()

            # Verify it discovered and used the highest suffix
            assert npd.current_suffix == "1H4DW02"
            assert result is not None
            assert "1H4DW02" in result


class TestCacheRegistryGCSMetadataLoading:
    """Tests for CacheRegistry GCS metadata loading."""

    def test_get_dpd_loads_from_gcs(self) -> None:
        """
        Test that _get_dpd() correctly loads DPD metadata from GCS.
        """
        # Create a mock SnapshotMetadata
        mock_metadata = SnapshotMetadata(
            name="test.dataset",
            suffix="1H4DW01",
            schema=pa.schema([("date", pa.date32()), ("value", pa.int64())]),
            files=[],
            yaml_config={
                "date_col": "date",
                "date_partition": "day",
                "partition_columns": [],
                "sort_columns": None,
                "description": "Test dataset",
                "instrument_column": None,
                "num_instrument_buckets": None,
                "row_group_size": None,
            },
        )

        # Pickle and gzip the metadata as it would be stored
        pickled = pickle.dumps(
            mock_metadata,
            protocol=SNAPSHOT_METADATA_PICKLE_PROTOCOL,
        )
        gzip_buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=gzip_buffer, mode="wb") as gz:
            gz.write(pickled)
        gzip_buffer.seek(0)

        with mock.patch.object(
            CacheRegistry, "discover_dpds"
        ) as mock_discover, mock.patch(
            "ionbus_parquet_cache.gcs_utils.gcs_ls"
        ) as mock_gcs_ls, mock.patch(
            "ionbus_parquet_cache.gcs_utils.gcs_open"
        ) as mock_gcs_open:
            # Setup mocks
            mock_discover.return_value = {
                "test.dataset": "gs://bucket/cache/test.dataset"
            }
            mock_gcs_ls.return_value = [
                "gs://bucket/cache/test.dataset/_meta_data/test.dataset_1H4DW01.pkl.gz"
            ]
            mock_gcs_open.return_value.__enter__.return_value = gzip_buffer

            # Create registry and get DPD
            registry = CacheRegistry.instance(gcs="gs://bucket/cache")
            dpd = registry._get_dpd("test.dataset", "gcs")

            # Verify DPD was created and loaded correctly
            assert dpd is not None
            assert dpd.name == "test.dataset"
            assert dpd.current_suffix == "1H4DW01"
            assert dpd._metadata == mock_metadata
            assert dpd.date_col == "date"
            assert dpd.date_partition == "day"

    def test_get_dpd_ignores_trimmed_files(self) -> None:
        """
        Test that _get_dpd() ignores _trimmed metadata files when loading from GCS.
        """
        # Create mock metadata for the real file
        mock_metadata = SnapshotMetadata(
            name="test.dataset",
            suffix="1H4DW01",
            schema=pa.schema([("date", pa.date32()), ("value", pa.int64())]),
            files=[],
            yaml_config={
                "date_col": "date",
                "date_partition": "day",
                "partition_columns": [],
                "sort_columns": None,
                "description": "Test dataset",
                "instrument_column": None,
                "num_instrument_buckets": None,
                "row_group_size": None,
            },
        )

        pickled = pickle.dumps(
            mock_metadata,
            protocol=SNAPSHOT_METADATA_PICKLE_PROTOCOL,
        )
        gzip_buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=gzip_buffer, mode="wb") as gz:
            gz.write(pickled)
        gzip_buffer.seek(0)

        with mock.patch.object(
            CacheRegistry, "discover_dpds"
        ) as mock_discover, mock.patch(
            "ionbus_parquet_cache.gcs_utils.gcs_ls"
        ) as mock_gcs_ls, mock.patch(
            "ionbus_parquet_cache.gcs_utils.gcs_open"
        ) as mock_gcs_open:
            # Setup mocks - list includes a _trimmed file but it should be ignored
            mock_discover.return_value = {
                "test.dataset": "gs://bucket/cache/test.dataset"
            }
            mock_gcs_ls.return_value = [
                "gs://bucket/cache/test.dataset/_meta_data/test.dataset_1H4DW01.pkl.gz",
                "gs://bucket/cache/test.dataset/_meta_data/test.dataset_zzzzzz_trimmed.pkl.gz",
            ]
            mock_gcs_open.return_value.__enter__.return_value = gzip_buffer

            # Create registry and get DPD
            registry = CacheRegistry.instance(gcs="gs://bucket/cache")
            dpd = registry._get_dpd("test.dataset", "gcs")

            # Verify correct (non-trimmed) metadata was loaded
            assert dpd is not None
            assert dpd.current_suffix == "1H4DW01"
            # Verify gcs_open was called with the non-trimmed file
            gcs_open_calls = [
                call[0][0]
                for call in mock_gcs_open.call_args_list
            ]
            assert any("1H4DW01" in call for call in gcs_open_calls)

    def test_get_dpd_not_found_in_gcs(self) -> None:
        """
        Test that _get_dpd() returns None when dataset is not found in GCS.
        """
        with mock.patch.object(
            CacheRegistry, "discover_dpds"
        ) as mock_discover:
            # Setup mocks - dataset not discovered
            mock_discover.return_value = {}

            # Create registry and try to get non-existent DPD
            registry = CacheRegistry.instance(gcs="gs://bucket/cache")
            dpd = registry._get_dpd("missing.dataset", "gcs")

            # Verify None is returned
            assert dpd is None

    def test_get_dpd_no_metadata_in_gcs(self) -> None:
        """
        Test that _get_dpd() returns None when no metadata is found in GCS.
        """
        with mock.patch.object(
            CacheRegistry, "discover_dpds"
        ) as mock_discover, mock.patch(
            "ionbus_parquet_cache.gcs_utils.gcs_ls"
        ) as mock_gcs_ls:
            # Setup mocks - dataset exists but no metadata
            mock_discover.return_value = {
                "test.dataset": "gs://bucket/cache/test.dataset"
            }
            mock_gcs_ls.return_value = []  # No metadata files

            # Create registry and try to get DPD without metadata
            registry = CacheRegistry.instance(gcs="gs://bucket/cache")
            dpd = registry._get_dpd("test.dataset", "gcs")

            # Verify None is returned
            assert dpd is None

    def test_get_dpd_gcs_config_preserved(self) -> None:
        """
        Test that _get_dpd() correctly loads all configuration options from
        GCS metadata.
        """
        # Create mock metadata with full configuration
        mock_metadata = SnapshotMetadata(
            name="futures.prices",
            suffix="1H4DW05",
            schema=pa.schema([
                ("Date", pa.timestamp("ns")),
                ("FutureRoot", pa.string()),
                ("price", pa.float64()),
                ("year", pa.string()),
            ]),
            files=[],
            yaml_config={
                "date_col": "Date",
                "date_partition": "year",
                "partition_columns": ["year"],
                "sort_columns": ["FutureRoot"],
                "description": "Futures prices",
                "instrument_column": "FutureRoot",
                "num_instrument_buckets": 5,
                "row_group_size": 10000,
            },
        )

        pickled = pickle.dumps(
            mock_metadata,
            protocol=SNAPSHOT_METADATA_PICKLE_PROTOCOL,
        )
        gzip_buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=gzip_buffer, mode="wb") as gz:
            gz.write(pickled)
        gzip_buffer.seek(0)

        with mock.patch.object(
            CacheRegistry, "discover_dpds"
        ) as mock_discover, mock.patch(
            "ionbus_parquet_cache.gcs_utils.gcs_ls"
        ) as mock_gcs_ls, mock.patch(
            "ionbus_parquet_cache.gcs_utils.gcs_open"
        ) as mock_gcs_open:
            # Setup mocks
            mock_discover.return_value = {
                "futures.prices": "gs://bucket/cache/futures.prices"
            }
            mock_gcs_ls.return_value = [
                "gs://bucket/cache/futures.prices/_meta_data/futures.prices_1H4DW05.pkl.gz"
            ]
            mock_gcs_open.return_value.__enter__.return_value = gzip_buffer

            # Create registry and get DPD
            registry = CacheRegistry.instance(gcs="gs://bucket/cache")
            dpd = registry._get_dpd("futures.prices", "gcs")

            # Verify all configuration options were correctly loaded
            assert dpd is not None
            assert dpd.name == "futures.prices"
            assert dpd.date_col == "Date"
            assert dpd.date_partition == "year"
            # year is in partition_columns (date partition auto-added)
            assert "year" in dpd.partition_columns
            assert dpd.sort_columns == ["FutureRoot"]
            assert dpd.description == "Futures prices"
            assert dpd.instrument_column == "FutureRoot"
            assert dpd.num_instrument_buckets == 5
            assert dpd.row_group_size == 10000
