"""Tests for built-in data sources."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ionbus_parquet_cache.builtin_sources import DPDSource, HiveParquetSource
from ionbus_parquet_cache.dated_dataset import (
    DatedParquetDataset,
    FileMetadata,
)
from ionbus_parquet_cache.exceptions import (
    ConfigurationError,
    DataSourceError,
)
from ionbus_parquet_cache.partition import PartitionSpec
from ionbus_parquet_cache.yaml_config import get_dataset_config


@pytest.fixture
def temp_cache(tmp_path: Path) -> Path:
    """Create a temporary cache directory."""
    cache = tmp_path / "cache"
    cache.mkdir()
    return cache


@pytest.fixture
def simple_dpd(temp_cache: Path) -> DatedParquetDataset:
    """Create a simple DPD for testing."""
    return DatedParquetDataset(
        cache_dir=temp_cache,
        name="test_dataset",
        date_col="Date",
        date_partition="month",
    )


@pytest.fixture
def parquet_source_dir(tmp_path: Path) -> Path:
    """Create a directory with test Parquet files."""
    source_dir = tmp_path / "source_data"
    source_dir.mkdir()

    # Create test data for January 2024
    df_jan = pd.DataFrame(
        {
            "Date": pd.date_range("2024-01-01", "2024-01-31"),
            "value": range(31),
            "symbol": ["TEST"] * 31,
        }
    )
    pq.write_table(
        pa.Table.from_pandas(df_jan),
        source_dir / "2024-01" / "data.parquet",
    )
    (source_dir / "2024-01").mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(df_jan),
        source_dir / "2024-01" / "data.parquet",
    )

    # Create test data for February 2024
    df_feb = pd.DataFrame(
        {
            "Date": pd.date_range("2024-02-01", "2024-02-29"),
            "value": range(29),
            "symbol": ["TEST"] * 29,
        }
    )
    (source_dir / "2024-02").mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(df_feb),
        source_dir / "2024-02" / "data.parquet",
    )

    return source_dir


@pytest.fixture
def single_parquet_file(tmp_path: Path) -> Path:
    """Create a single test Parquet file."""
    df = pd.DataFrame(
        {
            "Date": pd.date_range("2024-01-01", "2024-03-31"),
            "value": range(91),
            "symbol": ["TEST"] * 91,
        }
    )
    file_path = tmp_path / "test_data.parquet"
    pq.write_table(pa.Table.from_pandas(df), file_path)
    return file_path


class TestHiveParquetSourceInit:
    """Tests for HiveParquetSource initialization."""

    def test_requires_path(self, simple_dpd: DatedParquetDataset) -> None:
        """Should raise if path not provided."""
        with pytest.raises(ConfigurationError, match="requires 'path'"):
            HiveParquetSource(simple_dpd)

    def test_init_with_path(
        self, simple_dpd: DatedParquetDataset, tmp_path: Path
    ) -> None:
        """Should initialize with path."""
        source_dir = tmp_path / "data"
        source_dir.mkdir()

        source = HiveParquetSource(simple_dpd, path=str(source_dir))

        assert source.source_path == source_dir

    def test_default_glob_pattern(
        self, simple_dpd: DatedParquetDataset, tmp_path: Path
    ) -> None:
        """Should use default glob pattern."""
        source_dir = tmp_path / "data"
        source_dir.mkdir()

        source = HiveParquetSource(simple_dpd, path=str(source_dir))

        assert source.glob_pattern == "**/*.parquet"


class TestHiveParquetSourceAvailableDates:
    """Tests for HiveParquetSource.available_dates()."""

    def test_single_file(
        self, simple_dpd: DatedParquetDataset, single_parquet_file: Path
    ) -> None:
        """Should determine date range from single file."""
        source = HiveParquetSource(simple_dpd, path=str(single_parquet_file))

        start, end = source.available_dates()

        assert start == dt.date(2024, 1, 1)
        assert end == dt.date(2024, 3, 31)

    def test_nonexistent_path_raises(
        self, simple_dpd: DatedParquetDataset, tmp_path: Path
    ) -> None:
        """Should raise for nonexistent path."""
        source = HiveParquetSource(
            simple_dpd,
            path=str(tmp_path / "nonexistent"),
        )

        with pytest.raises(DataSourceError, match="does not exist"):
            source.available_dates()


class TestHiveParquetSourceGetData:
    """Tests for HiveParquetSource.get_data()."""

    def test_get_data_from_single_file(
        self, simple_dpd: DatedParquetDataset, single_parquet_file: Path
    ) -> None:
        """Should read data from single file with date filter."""
        source = HiveParquetSource(simple_dpd, path=str(single_parquet_file))
        source._do_prepare(dt.date(2024, 1, 1), dt.date(2024, 1, 31))

        spec = PartitionSpec(
            partition_values={"month": "M2024-01"},
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        table = source.get_data(spec)

        assert isinstance(table, pa.Table)
        assert table.num_rows == 31

    def test_get_data_filters_by_date(
        self, simple_dpd: DatedParquetDataset, single_parquet_file: Path
    ) -> None:
        """Should filter data by date range."""
        source = HiveParquetSource(simple_dpd, path=str(single_parquet_file))
        source._do_prepare(dt.date(2024, 2, 1), dt.date(2024, 2, 29))

        spec = PartitionSpec(
            partition_values={"month": "M2024-02"},
            start_date=dt.date(2024, 2, 1),
            end_date=dt.date(2024, 2, 29),
        )

        table = source.get_data(spec)

        assert table.num_rows == 29


class TestDPDSourceInit:
    """Tests for DPDSource initialization."""

    def test_requires_dpd_name(self, simple_dpd: DatedParquetDataset) -> None:
        """Should raise if dpd_name not provided."""
        with pytest.raises(ConfigurationError, match="requires 'dpd_name'"):
            DPDSource(simple_dpd)

    def test_init_with_dpd_name(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Should initialize with dpd_name."""
        source = DPDSource(simple_dpd, dpd_name="source_dataset")

        assert source.dpd_name == "source_dataset"

    def test_uses_same_cache_by_default(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Should use same cache if dpd_cache_path not provided."""
        source = DPDSource(simple_dpd, dpd_name="source_dataset")

        assert source.dpd_cache_path == simple_dpd.cache_dir


class TestDPDSourceIntegration:
    """Integration tests for DPDSource."""

    def test_read_from_source_dpd(self, temp_cache: Path) -> None:
        """Should read data from source DPD."""
        # Create source DPD with data
        source_dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="source_dataset",
            date_col="Date",
            date_partition="month",
        )

        # Create and write test data
        df = pd.DataFrame(
            {
                "Date": pd.date_range("2024-01-01", "2024-01-31"),
                "value": range(31),
            }
        )
        table = pa.Table.from_pandas(df)

        # Publish snapshot
        data_dir = temp_cache / "source_dataset"
        data_dir.mkdir(parents=True)
        (data_dir / "month=M2024-01").mkdir()
        pq.write_table(
            table,
            data_dir / "month=M2024-01" / "data_abc123.parquet",
        )

        file_metadata = FileMetadata(
            path="month=M2024-01/data_abc123.parquet",
            partition_values={"month": "M2024-01"},
            checksum="dummy_checksum",
            size_bytes=(data_dir / "month=M2024-01" / "data_abc123.parquet")
            .stat()
            .st_size,
        )
        source_dpd._publish_snapshot(
            files=[file_metadata],
            schema=table.schema,
            cache_start_date=dt.date(2024, 1, 1),
            cache_end_date=dt.date(2024, 1, 31),
            partition_values={"month": ["M2024-01"]},
        )

        # Create target DPD and DPDSource
        target_dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="target_dataset",
            date_col="Date",
            date_partition="month",
        )

        source = DPDSource(target_dpd, dpd_name="source_dataset")

        # Test available_dates
        start, end = source.available_dates()
        assert start == dt.date(2024, 1, 1)
        assert end == dt.date(2024, 1, 31)

        # Test get_data
        spec = PartitionSpec(
            partition_values={"month": "M2024-01"},
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        result = source.get_data(spec)
        assert isinstance(result, pa.Table)
        assert result.num_rows == 31


class TestBuiltinSourcesWithYamlConfig:
    """Tests for built-in sources loaded via YAML config."""

    def test_hive_source_via_config(
        self, temp_cache: Path, single_parquet_file: Path
    ) -> None:
        """Should load HiveParquetSource via YAML config."""
        # Create YAML config
        (temp_cache / "yaml").mkdir()
        # Use forward slashes to avoid YAML escape issues on Windows
        path_str = str(single_parquet_file).replace("\\", "/")
        yaml_content = f"""
datasets:
    test_dataset:
        date_col: Date
        date_partition: month
        source_location: ""
        source_class_name: HiveParquetSource
        source_init_args:
            path: "{path_str}"
"""
        (temp_cache / "yaml" / "test.yaml").write_text(yaml_content)

        config = get_dataset_config(temp_cache, "test_dataset")
        dpd = config.to_dpd()
        source = config.create_source(dpd)

        assert isinstance(source, HiveParquetSource)

        start, end = source.available_dates()
        assert start == dt.date(2024, 1, 1)
        assert end == dt.date(2024, 3, 31)


class TestDPDSourceCacheRegistry:
    """Tests for DPDSource with CacheRegistry lookup."""

    @pytest.fixture(autouse=True)
    def reset_registry(self) -> None:
        """Reset CacheRegistry before and after each test."""
        from ionbus_parquet_cache.cache_registry import CacheRegistry

        CacheRegistry.reset()
        yield
        CacheRegistry.reset()

    def test_dpd_cache_name_uses_registry(self, tmp_path: Path) -> None:
        """DPDSource with dpd_cache_name should use CacheRegistry to look up source DPD."""
        from ionbus_parquet_cache.cache_registry import CacheRegistry
        import pyarrow.parquet as pq

        # Set up source cache with a DPD
        source_cache = tmp_path / "source_cache"
        source_cache.mkdir()

        source_dpd = DatedParquetDataset(
            cache_dir=source_cache,
            name="source_dataset",
            date_col="Date",
            date_partition="month",
        )

        # Create and write test data
        df = pd.DataFrame(
            {
                "Date": pd.date_range("2024-01-01", "2024-01-31"),
                "value": range(31),
            }
        )
        table = pa.Table.from_pandas(df)

        # Publish snapshot
        data_dir = source_cache / "source_dataset"
        data_dir.mkdir(parents=True)
        (data_dir / "month=M2024-01").mkdir()
        pq.write_table(
            table,
            data_dir / "month=M2024-01" / "data_abc123.parquet",
        )

        file_metadata = FileMetadata(
            path="month=M2024-01/data_abc123.parquet",
            partition_values={"month": "M2024-01"},
            checksum="dummy_checksum",
            size_bytes=(data_dir / "month=M2024-01" / "data_abc123.parquet")
            .stat()
            .st_size,
        )
        source_dpd._publish_snapshot(
            files=[file_metadata],
            schema=table.schema,
            cache_start_date=dt.date(2024, 1, 1),
            cache_end_date=dt.date(2024, 1, 31),
            partition_values={"month": ["M2024-01"]},
        )

        # Register source cache in CacheRegistry
        CacheRegistry.instance(source_data=source_cache)

        # Create target DPD in different cache
        target_cache = tmp_path / "target_cache"
        target_cache.mkdir()
        target_dpd = DatedParquetDataset(
            cache_dir=target_cache,
            name="target_dataset",
            date_col="Date",
            date_partition="month",
        )

        # Create DPDSource with dpd_cache_name
        source = DPDSource(
            target_dpd,
            dpd_name="source_dataset",
            dpd_cache_name="source_data",
        )

        # Verify that it uses CacheRegistry path (not target's cache_dir)
        assert source.dpd_cache_name == "source_data"
        # The actual dpd_cache_path should still be target's cache_dir (default)
        # because dpd_cache_path was not provided
        assert source.dpd_cache_path == target_cache

        # But _get_source_dpd should use the registry
        result_dpd = source._get_source_dpd()
        assert result_dpd is not None
        assert result_dpd.name == "source_dataset"
        assert result_dpd.cache_dir == source_cache

        # Test available_dates works
        start, end = source.available_dates()
        assert start == dt.date(2024, 1, 1)
        assert end == dt.date(2024, 1, 31)

    def test_dpd_cache_name_takes_precedence_over_dpd_cache_path(
        self, tmp_path: Path
    ) -> None:
        """When both dpd_cache_name and dpd_cache_path are provided, dpd_cache_name takes precedence."""
        from ionbus_parquet_cache.cache_registry import CacheRegistry
        import pyarrow.parquet as pq

        # Set up source cache for registry (will be used)
        registry_cache = tmp_path / "registry_cache"
        registry_cache.mkdir()

        source_dpd = DatedParquetDataset(
            cache_dir=registry_cache,
            name="source_dataset",
            date_col="Date",
            date_partition="month",
        )

        # Create and write test data
        df = pd.DataFrame(
            {
                "Date": pd.date_range("2024-02-01", "2024-02-29"),
                "value": range(29),
            }
        )
        table = pa.Table.from_pandas(df)

        # Publish snapshot
        data_dir = registry_cache / "source_dataset"
        data_dir.mkdir(parents=True)
        (data_dir / "month=M2024-02").mkdir()
        pq.write_table(
            table,
            data_dir / "month=M2024-02" / "data_abc123.parquet",
        )

        file_metadata = FileMetadata(
            path="month=M2024-02/data_abc123.parquet",
            partition_values={"month": "M2024-02"},
            checksum="dummy_checksum",
            size_bytes=(data_dir / "month=M2024-02" / "data_abc123.parquet")
            .stat()
            .st_size,
        )
        source_dpd._publish_snapshot(
            files=[file_metadata],
            schema=table.schema,
            cache_start_date=dt.date(2024, 2, 1),
            cache_end_date=dt.date(2024, 2, 29),
            partition_values={"month": ["M2024-02"]},
        )

        # Set up a different path (will NOT be used due to registry lookup)
        path_cache = tmp_path / "path_cache"
        path_cache.mkdir()

        # Register the registry_cache in CacheRegistry
        CacheRegistry.instance(my_cache=registry_cache)

        # Create target DPD
        target_cache = tmp_path / "target_cache"
        target_cache.mkdir()
        target_dpd = DatedParquetDataset(
            cache_dir=target_cache,
            name="target_dataset",
            date_col="Date",
            date_partition="month",
        )

        # Create DPDSource with BOTH dpd_cache_name AND dpd_cache_path
        source = DPDSource(
            target_dpd,
            dpd_name="source_dataset",
            dpd_cache_name="my_cache",
            dpd_cache_path=str(path_cache),
        )

        # dpd_cache_path should be set to path_cache (the explicit value)
        assert source.dpd_cache_path == path_cache

        # But _get_source_dpd should use registry lookup (dpd_cache_name takes precedence)
        result_dpd = source._get_source_dpd()
        assert result_dpd is not None
        assert (
            result_dpd.cache_dir == registry_cache
        )  # From registry, not path_cache

        # Verify we got the February data (from registry_cache)
        start, end = source.available_dates()
        assert start == dt.date(2024, 2, 1)
        assert end == dt.date(2024, 2, 29)

    def test_invalid_dpd_cache_name_raises_config_error(
        self, tmp_path: Path
    ) -> None:
        """DPDSource with invalid dpd_cache_name should raise ConfigurationError."""
        from ionbus_parquet_cache.cache_registry import CacheRegistry

        # Set up registry with a different cache name
        some_cache = tmp_path / "some_cache"
        some_cache.mkdir()
        CacheRegistry.instance(some_cache=some_cache)

        # Create target DPD
        target_cache = tmp_path / "target_cache"
        target_cache.mkdir()
        target_dpd = DatedParquetDataset(
            cache_dir=target_cache,
            name="target_dataset",
            date_col="Date",
            date_partition="month",
        )

        # Create DPDSource with non-existent cache name
        source = DPDSource(
            target_dpd,
            dpd_name="source_dataset",
            dpd_cache_name="nonexistent_cache",
        )

        # Should raise ConfigurationError when trying to get the source DPD
        with pytest.raises(ConfigurationError, match="not found in cache"):
            source._get_source_dpd()

    def test_dpd_not_found_in_valid_cache_raises_config_error(
        self, tmp_path: Path
    ) -> None:
        """DPDSource with valid cache but non-existent DPD should raise ConfigurationError."""
        from ionbus_parquet_cache.cache_registry import CacheRegistry

        # Set up registry with a valid but empty cache
        empty_cache = tmp_path / "empty_cache"
        empty_cache.mkdir()
        CacheRegistry.instance(empty_cache=empty_cache)

        # Create target DPD
        target_cache = tmp_path / "target_cache"
        target_cache.mkdir()
        target_dpd = DatedParquetDataset(
            cache_dir=target_cache,
            name="target_dataset",
            date_col="Date",
            date_partition="month",
        )

        # Create DPDSource with valid cache name but non-existent DPD
        source = DPDSource(
            target_dpd,
            dpd_name="nonexistent_dataset",
            dpd_cache_name="empty_cache",
        )

        # Should raise ConfigurationError when trying to get the source DPD
        with pytest.raises(
            ConfigurationError, match="not found in cache 'empty_cache'"
        ):
            source._get_source_dpd()


class TestBuiltinSourcesChunkValues:
    """Tests for chunk_values support in built-in sources."""

    def test_hive_source_accepts_chunk_values(
        self, simple_dpd: DatedParquetDataset, single_parquet_file: Path
    ) -> None:
        """HiveParquetSource should accept chunk_values parameter."""
        source = HiveParquetSource(
            simple_dpd,
            path=str(single_parquet_file),
            chunk_values={"batch": [0, 1, 2]},
        )

        assert source._chunk_values == {"batch": [0, 1, 2]}

    def test_hive_source_chunk_values_expands_partitions(
        self, simple_dpd: DatedParquetDataset, single_parquet_file: Path
    ) -> None:
        """HiveParquetSource with chunk_values should expand partitions."""
        source = HiveParquetSource(
            simple_dpd,
            path=str(single_parquet_file),
            chunk_values={"batch": [0, 1]},
        )
        source.prepare(dt.date(2024, 1, 1), dt.date(2024, 1, 31))

        specs = source.get_partitions()

        # 1 month * 2 batches = 2 specs
        assert len(specs) == 2
        batches = [s.chunk_info["batch"] for s in specs]
        assert sorted(batches) == [0, 1]

    def test_dpd_source_accepts_chunk_values(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """DPDSource should accept chunk_values parameter."""
        source = DPDSource(
            simple_dpd,
            dpd_name="source_dataset",
            chunk_values={"exchange": ["NYSE", "NASDAQ"]},
        )

        assert source._chunk_values == {"exchange": ["NYSE", "NASDAQ"]}

    def test_chunk_values_via_yaml_config(
        self, temp_cache: Path, single_parquet_file: Path
    ) -> None:
        """chunk_values should be configurable via YAML."""
        (temp_cache / "yaml").mkdir(exist_ok=True)
        path_str = str(single_parquet_file).replace("\\", "/")
        yaml_content = f"""
datasets:
    test_dataset:
        date_col: Date
        date_partition: month
        source_location: ""
        source_class_name: HiveParquetSource
        source_init_args:
            path: "{path_str}"
            chunk_values:
                batch: [0, 1, 2]
"""
        (temp_cache / "yaml" / "test.yaml").write_text(yaml_content)

        config = get_dataset_config(temp_cache, "test_dataset")
        dpd = config.to_dpd()
        source = config.create_source(dpd)

        assert isinstance(source, HiveParquetSource)
        assert source._chunk_values == {"batch": [0, 1, 2]}
