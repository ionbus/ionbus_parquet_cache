"""Tests for CacheRegistry."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

from ionbus_parquet_cache.cache_registry import CacheRegistry, DatasetType
from ionbus_parquet_cache.dated_dataset import (
    DatedParquetDataset,
    FileMetadata,
)
from ionbus_parquet_cache.non_dated_dataset import NonDatedParquetDataset
from ionbus_parquet_cache.exceptions import SnapshotNotFoundError


@pytest.fixture(autouse=True)
def reset_registry() -> None:
    """Reset the singleton registry before each test."""
    CacheRegistry.reset()
    yield
    CacheRegistry.reset()


@pytest.fixture
def temp_cache(tmp_path: Path) -> Path:
    """Create a temporary cache directory."""
    return tmp_path / "cache"


@pytest.fixture
def cache_with_npd(tmp_path: Path) -> Path:
    """Create a cache with an NPD."""
    cache = tmp_path / "cache_npd"

    # Create NPD
    npd = NonDatedParquetDataset(cache_dir=cache, name="ref.instruments")
    df = pd.DataFrame(
        {"symbol": ["AAPL", "GOOGL"], "name": ["Apple", "Alphabet"]}
    )
    source = tmp_path / "source.parquet"
    df.to_parquet(source, index=False)
    npd.import_snapshot(source)

    return cache


@pytest.fixture
def cache_with_dpd(tmp_path: Path) -> Path:
    """Create a cache with a DPD."""
    cache = tmp_path / "cache_dpd"

    dpd = DatedParquetDataset(
        cache_dir=cache,
        name="md.prices",
        date_col="Date",
        date_partition="year",
        partition_columns=["year"],
    )

    # Create test data
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2024-01-15", "2024-02-20"]),
            "price": [100.0, 105.0],
        }
    )

    # Create data directory structure
    year_dir = dpd.dataset_dir / "year=Y2024"
    year_dir.mkdir(parents=True)
    file_path = year_dir / "md.prices_test.parquet"
    df.to_parquet(file_path, index=False)

    # Create schema and publish
    schema = pa.schema(
        [
            ("Date", pa.timestamp("ns")),
            ("price", pa.float64()),
            ("year", pa.string()),
        ]
    )
    file_metadata = FileMetadata(
        path="year=Y2024/md.prices_test.parquet",
        partition_values={"year": "Y2024"},
        checksum="dummy_checksum",
        size_bytes=file_path.stat().st_size,
    )
    dpd._publish_snapshot(
        files=[file_metadata],
        schema=schema,
        cache_start_date=dt.date(2024, 1, 15),
        cache_end_date=dt.date(2024, 2, 20),
        partition_values={"year": ["Y2024"]},
    )

    return cache


class TestCacheRegistrySingleton:
    """Tests for singleton behavior."""

    def test_instance_returns_same_object(self, temp_cache: Path) -> None:
        """Multiple calls to instance() should return same object."""
        r1 = CacheRegistry.instance(cache1=temp_cache)
        r2 = CacheRegistry.instance()

        assert r1 is r2

    def test_instance_adds_caches(self, tmp_path: Path) -> None:
        """instance() should add caches to existing registry."""
        cache1 = tmp_path / "cache1"
        cache2 = tmp_path / "cache2"

        r1 = CacheRegistry.instance(local=cache1)
        r2 = CacheRegistry.instance(team=cache2)

        assert r1 is r2
        assert r1.get_cache_path("local") == cache1
        assert r1.get_cache_path("team") == cache2

    def test_reset_clears_singleton(self, temp_cache: Path) -> None:
        """reset() should clear the singleton."""
        r1 = CacheRegistry.instance(cache=temp_cache)
        CacheRegistry.reset()
        r2 = CacheRegistry.instance()

        assert r1 is not r2


class TestCacheRegistryAddCache:
    """Tests for adding caches."""

    def test_add_cache(self, temp_cache: Path) -> None:
        """Test adding a cache."""
        registry = CacheRegistry.instance()
        registry.add_cache("local", temp_cache)

        assert registry.get_cache_path("local") == temp_cache

    def test_add_cache_preserves_order(self, tmp_path: Path) -> None:
        """Caches should be searched in registration order."""
        registry = CacheRegistry.instance()

        cache1 = tmp_path / "cache1"
        cache2 = tmp_path / "cache2"
        cache3 = tmp_path / "cache3"

        registry.add_cache("first", cache1)
        registry.add_cache("second", cache2)
        registry.add_cache("third", cache3)

        assert registry._cache_order == ["first", "second", "third"]


class TestCacheRegistryDiscovery:
    """Tests for dataset discovery."""

    def test_discover_dpds(self, cache_with_dpd: Path) -> None:
        """Should discover DPDs by _meta_data directory."""
        registry = CacheRegistry.instance(test=cache_with_dpd)

        dpds = registry.discover_dpds(cache_with_dpd)
        assert "md.prices" in dpds

    def test_discover_npds(self, cache_with_npd: Path) -> None:
        """Should discover NPDs in non-dated directory."""
        registry = CacheRegistry.instance(test=cache_with_npd)

        npds = registry.discover_npds(cache_with_npd)
        assert "ref.instruments" in npds


class TestCacheRegistryGet:
    """Tests for getting datasets."""

    def test_get_npd(self, cache_with_npd: Path) -> None:
        """Should get NPD by name."""
        registry = CacheRegistry.instance(test=cache_with_npd)

        dataset = registry.get_dataset("ref.instruments")
        assert dataset is not None
        assert isinstance(dataset, NonDatedParquetDataset)

    def test_get_dpd(self, cache_with_dpd: Path) -> None:
        """Should get DPD by name."""
        registry = CacheRegistry.instance(test=cache_with_dpd)

        dataset = registry.get_dataset("md.prices")
        assert dataset is not None
        assert isinstance(dataset, DatedParquetDataset)

    def test_get_nonexistent_returns_none(self, temp_cache: Path) -> None:
        """Getting nonexistent dataset should return None."""
        registry = CacheRegistry.instance(test=temp_cache)

        dataset = registry.get_dataset("nonexistent")
        assert dataset is None

    def test_get_from_specific_cache(
        self, cache_with_npd: Path, cache_with_dpd: Path
    ) -> None:
        """Should get from specific cache when specified."""
        registry = CacheRegistry.instance(
            npd_cache=cache_with_npd,
            dpd_cache=cache_with_dpd,
        )

        # Get NPD from specific cache
        npd = registry.get_dataset("ref.instruments", cache_name="npd_cache")
        assert npd is not None

        # Should not find NPD in DPD cache
        npd2 = registry.get_dataset("ref.instruments", cache_name="dpd_cache")
        assert npd2 is None

    def test_get_with_dataset_type_dated(
        self, cache_with_dpd: Path, cache_with_npd: Path
    ) -> None:
        """Should only search for DPD when dataset_type=DATED."""
        registry = CacheRegistry.instance(
            dpd_cache=cache_with_dpd,
            npd_cache=cache_with_npd,
        )

        # Should find DPD
        dpd = registry.get_dataset(
            "md.prices", dataset_type=DatasetType.DATED
        )
        assert dpd is not None
        assert isinstance(dpd, DatedParquetDataset)

        # Should not find NPD when searching for DATED
        result = registry.get_dataset(
            "ref.instruments", dataset_type=DatasetType.DATED
        )
        assert result is None

    def test_get_with_dataset_type_non_dated(
        self, cache_with_dpd: Path, cache_with_npd: Path
    ) -> None:
        """Should only search for NPD when dataset_type=NON_DATED."""
        registry = CacheRegistry.instance(
            dpd_cache=cache_with_dpd,
            npd_cache=cache_with_npd,
        )

        # Should find NPD
        npd = registry.get_dataset(
            "ref.instruments", dataset_type=DatasetType.NON_DATED
        )
        assert npd is not None
        assert isinstance(npd, NonDatedParquetDataset)

        # Should not find DPD when searching for NON_DATED
        result = registry.get_dataset(
            "md.prices", dataset_type=DatasetType.NON_DATED
        )
        assert result is None

    def test_history_and_provenance_wrappers(
        self,
        cache_with_dpd: Path,
    ) -> None:
        """Registry should delegate DPD history and provenance APIs."""
        registry = CacheRegistry.instance(test=cache_with_dpd)

        assert registry.read_provenance("md.prices") == {}
        history = registry.cache_history("md.prices")

        assert len(history) == 1
        assert history[0].status == "missing_lineage"


class TestCacheRegistryReadData:
    """Tests for reading data."""

    def test_read_npd_data(self, cache_with_npd: Path) -> None:
        """Should read NPD data."""
        registry = CacheRegistry.instance(test=cache_with_npd)

        df = registry.read_data("ref.instruments")
        assert len(df) == 2
        assert "symbol" in df.columns

    def test_read_nonexistent_raises(self, temp_cache: Path) -> None:
        """Reading nonexistent dataset should raise."""
        registry = CacheRegistry.instance(test=temp_cache)

        with pytest.raises(SnapshotNotFoundError, match="not found"):
            registry.read_data("nonexistent")


class TestCacheRegistryDataSummary:
    """Tests for data_summary()."""

    def test_data_summary_empty(self, temp_cache: Path) -> None:
        """Empty cache should return empty DataFrame."""
        registry = CacheRegistry.instance(test=temp_cache)
        temp_cache.mkdir(parents=True, exist_ok=True)

        summary = registry.data_summary()
        assert len(summary) == 0

    def test_data_summary_with_datasets(
        self, cache_with_npd: Path, cache_with_dpd: Path
    ) -> None:
        """Summary should include all datasets."""
        registry = CacheRegistry.instance(
            npd_cache=cache_with_npd,
            dpd_cache=cache_with_dpd,
        )

        summary = registry.data_summary()

        assert len(summary) == 2
        assert set(summary["type"]) == {"DPD", "NPD"}
        assert "ref.instruments" in summary["name"].values
        assert "md.prices" in summary["name"].values


class TestCacheRegistrySearchOrder:
    """Tests for cache search order."""

    def test_first_cache_wins(self, tmp_path: Path) -> None:
        """First cache with dataset should be used."""
        cache1 = tmp_path / "cache1"
        cache2 = tmp_path / "cache2"

        # Create same NPD name in both caches with different data
        for i, cache in enumerate([cache1, cache2], 1):
            npd = NonDatedParquetDataset(cache_dir=cache, name="shared_name")
            df = pd.DataFrame({"cache_id": [i]})
            source = tmp_path / f"source{i}.parquet"
            df.to_parquet(source, index=False)
            npd.import_snapshot(source)

        registry = CacheRegistry.instance(
            first=cache1,
            second=cache2,
        )

        # Should get from first cache
        df = registry.read_data("shared_name")
        assert df.iloc[0]["cache_id"] == 1

    def test_fallback_to_second_cache(self, tmp_path: Path) -> None:
        """Should fall back to second cache if first doesn't have dataset."""
        cache1 = tmp_path / "cache1"
        cache2 = tmp_path / "cache2"
        cache1.mkdir(parents=True)

        # Only create NPD in second cache
        npd = NonDatedParquetDataset(cache_dir=cache2, name="only_in_second")
        df = pd.DataFrame({"value": [42]})
        source = tmp_path / "source.parquet"
        df.to_parquet(source, index=False)
        npd.import_snapshot(source)

        registry = CacheRegistry.instance(
            first=cache1,
            second=cache2,
        )

        # Should get from second cache
        df = registry.read_data("only_in_second")
        assert df.iloc[0]["value"] == 42


class TestCacheRegistryTrimmedFiles:
    """Tests for _trimmed file filtering in CacheRegistry."""

    def test_get_dpd_ignores_trimmed_metadata(self, tmp_path: Path) -> None:
        """Should ignore _trimmed.pkl.gz files when finding current snapshot."""
        cache = tmp_path / "cache"

        # Create DPD with data
        dpd = DatedParquetDataset(
            cache_dir=cache,
            name="md.test",
            date_col="Date",
            date_partition="year",
            partition_columns=["year"],
        )

        # Create test data
        df = pd.DataFrame(
            {
                "Date": pd.to_datetime(["2024-01-15", "2024-02-20"]),
                "price": [100.0, 105.0],
            }
        )

        # Create data directory structure
        year_dir = dpd.dataset_dir / "year=Y2024"
        year_dir.mkdir(parents=True)
        file_path = year_dir / "md.test_1Abc23.parquet"
        df.to_parquet(file_path, index=False)

        # Create schema and publish
        schema = pa.schema(
            [
                ("Date", pa.timestamp("ns")),
                ("price", pa.float64()),
                ("year", pa.string()),
            ]
        )
        file_metadata = FileMetadata(
            path="year=Y2024/md.test_1Abc23.parquet",
            partition_values={"year": "Y2024"},
            checksum="dummy_checksum",
            size_bytes=file_path.stat().st_size,
        )
        suffix = dpd._publish_snapshot(
            files=[file_metadata],
            schema=schema,
            cache_start_date=dt.date(2024, 1, 15),
            cache_end_date=dt.date(2024, 2, 20),
            partition_values={"year": ["Y2024"]},
        )

        # Create a _trimmed metadata file with a HIGHER suffix (would be selected
        # if not filtered)
        trimmed_meta = dpd.meta_dir / "md.test_zzzzzz_trimmed.pkl.gz"
        trimmed_meta.write_bytes(b"fake trimmed metadata")

        # Verify trimmed file exists and has higher suffix than real metadata
        assert trimmed_meta.exists()
        assert "zzzzzz" > suffix  # trimmed suffix is lexicographically higher

        # Get DPD through registry - should use the real metadata, not trimmed
        registry = CacheRegistry.instance(test=cache)
        dataset = registry.get_dataset("md.test")

        assert dataset is not None
        assert isinstance(dataset, DatedParquetDataset)

        # Discover the current suffix - it should be the original one, not the
        # trimmed one (even though trimmed has higher suffix lexicographically)
        discovered_suffix = dataset._discover_current_suffix()
        assert discovered_suffix == suffix
        assert "_trimmed" not in discovered_suffix
        # Verify the trimmed suffix would be higher (proving the filter works)
        assert "zzzzzz" > discovered_suffix


class TestCacheRegistryLoadedState:
    """Tests for get_dataset() setting loaded state."""

    def test_get_dpd_sets_current_suffix(self, cache_with_dpd: Path) -> None:
        """get_dataset() should set current_suffix on returned DPD."""
        registry = CacheRegistry.instance(test=cache_with_dpd)

        dpd = registry.get_dataset("md.prices")
        assert dpd is not None
        assert isinstance(dpd, DatedParquetDataset)
        assert dpd.current_suffix is not None

    def test_get_dpd_sets_metadata(self, cache_with_dpd: Path) -> None:
        """get_dataset() should set _metadata on returned DPD."""
        registry = CacheRegistry.instance(test=cache_with_dpd)

        dpd = registry.get_dataset("md.prices")
        assert dpd is not None
        assert isinstance(dpd, DatedParquetDataset)
        assert dpd._metadata is not None

    def test_get_dpd_is_update_available_works(
        self, cache_with_dpd: Path
    ) -> None:
        """is_update_available() should work on DPD without additional I/O."""
        registry = CacheRegistry.instance(test=cache_with_dpd)

        dpd = registry.get_dataset("md.prices")
        assert dpd is not None

        # This should work without raising - current_suffix and _metadata are set
        result = dpd.is_update_available()
        assert result is False  # No newer snapshot exists

    def test_get_npd_sets_current_suffix(self, cache_with_npd: Path) -> None:
        """get_dataset() should set current_suffix on returned NPD."""
        registry = CacheRegistry.instance(test=cache_with_npd)

        npd = registry.get_dataset("ref.instruments")
        assert npd is not None
        assert isinstance(npd, NonDatedParquetDataset)
        assert npd.current_suffix is not None


class TestCacheRegistryDiscoverAll:
    """Tests for discover_all_dpds() and discover_all_npds()."""

    def test_discover_all_dpds_single_cache(
        self, cache_with_dpd: Path
    ) -> None:
        """discover_all_dpds() should find DPDs in a single cache."""
        registry = CacheRegistry.instance(test=cache_with_dpd)

        result = registry.discover_all_dpds()
        assert "md.prices" in result
        cache_name, path = result["md.prices"]
        assert cache_name == "test"
        assert path == cache_with_dpd / "md.prices"

    def test_discover_all_dpds_multiple_caches(self, tmp_path: Path) -> None:
        """discover_all_dpds() should find DPDs across multiple caches."""
        cache1 = tmp_path / "cache1"
        cache2 = tmp_path / "cache2"

        # Create DPD in cache1
        dpd1 = DatedParquetDataset(
            cache_dir=cache1,
            name="dataset1",
            date_col="Date",
            date_partition="year",
            partition_columns=["year"],
        )
        year_dir = dpd1.dataset_dir / "year=Y2024"
        year_dir.mkdir(parents=True)
        df = pd.DataFrame(
            {"Date": pd.to_datetime(["2024-01-15"]), "price": [100.0]}
        )
        file_path = year_dir / "dataset1_test.parquet"
        df.to_parquet(file_path, index=False)
        schema = pa.schema(
            [
                ("Date", pa.timestamp("ns")),
                ("price", pa.float64()),
                ("year", pa.string()),
            ]
        )
        file_metadata = FileMetadata(
            path="year=Y2024/dataset1_test.parquet",
            partition_values={"year": "Y2024"},
            checksum="dummy",
            size_bytes=file_path.stat().st_size,
        )
        dpd1._publish_snapshot(
            files=[file_metadata],
            schema=schema,
            cache_start_date=dt.date(2024, 1, 15),
            cache_end_date=dt.date(2024, 1, 15),
            partition_values={"year": ["Y2024"]},
        )

        # Create DPD in cache2
        dpd2 = DatedParquetDataset(
            cache_dir=cache2,
            name="dataset2",
            date_col="Date",
            date_partition="year",
            partition_columns=["year"],
        )
        year_dir2 = dpd2.dataset_dir / "year=Y2024"
        year_dir2.mkdir(parents=True)
        file_path2 = year_dir2 / "dataset2_test.parquet"
        df.to_parquet(file_path2, index=False)
        file_metadata2 = FileMetadata(
            path="year=Y2024/dataset2_test.parquet",
            partition_values={"year": "Y2024"},
            checksum="dummy",
            size_bytes=file_path2.stat().st_size,
        )
        dpd2._publish_snapshot(
            files=[file_metadata2],
            schema=schema,
            cache_start_date=dt.date(2024, 1, 15),
            cache_end_date=dt.date(2024, 1, 15),
            partition_values={"year": ["Y2024"]},
        )

        registry = CacheRegistry.instance(first=cache1, second=cache2)

        result = registry.discover_all_dpds()
        assert "dataset1" in result
        assert "dataset2" in result
        assert result["dataset1"][0] == "first"
        assert result["dataset2"][0] == "second"

    def test_discover_all_dpds_first_cache_precedence(
        self, tmp_path: Path
    ) -> None:
        """discover_all_dpds() should use first cache for duplicate names."""
        cache1 = tmp_path / "cache1"
        cache2 = tmp_path / "cache2"

        # Create same DPD name in both caches
        for cache in [cache1, cache2]:
            dpd = DatedParquetDataset(
                cache_dir=cache,
                name="shared_dpd",
                date_col="Date",
                date_partition="year",
                partition_columns=["year"],
            )
            year_dir = dpd.dataset_dir / "year=Y2024"
            year_dir.mkdir(parents=True)
            df = pd.DataFrame(
                {"Date": pd.to_datetime(["2024-01-15"]), "price": [100.0]}
            )
            file_path = year_dir / "shared_dpd_test.parquet"
            df.to_parquet(file_path, index=False)
            schema = pa.schema(
                [
                    ("Date", pa.timestamp("ns")),
                    ("price", pa.float64()),
                    ("year", pa.string()),
                ]
            )
            file_metadata = FileMetadata(
                path="year=Y2024/shared_dpd_test.parquet",
                partition_values={"year": "Y2024"},
                checksum="dummy",
                size_bytes=file_path.stat().st_size,
            )
            dpd._publish_snapshot(
                files=[file_metadata],
                schema=schema,
                cache_start_date=dt.date(2024, 1, 15),
                cache_end_date=dt.date(2024, 1, 15),
                partition_values={"year": ["Y2024"]},
            )

        registry = CacheRegistry.instance(first=cache1, second=cache2)

        result = registry.discover_all_dpds()
        assert "shared_dpd" in result
        # First cache should take precedence
        cache_name, path = result["shared_dpd"]
        assert cache_name == "first"
        assert path == cache1 / "shared_dpd"

    def test_discover_all_npds_single_cache(
        self, cache_with_npd: Path
    ) -> None:
        """discover_all_npds() should find NPDs in a single cache."""
        registry = CacheRegistry.instance(test=cache_with_npd)

        result = registry.discover_all_npds()
        assert "ref.instruments" in result
        cache_name, path = result["ref.instruments"]
        assert cache_name == "test"

    def test_discover_all_npds_multiple_caches(self, tmp_path: Path) -> None:
        """discover_all_npds() should find NPDs across multiple caches."""
        cache1 = tmp_path / "cache1"
        cache2 = tmp_path / "cache2"

        # Create NPD in cache1
        npd1 = NonDatedParquetDataset(cache_dir=cache1, name="ref1")
        df1 = pd.DataFrame({"symbol": ["AAPL"]})
        source1 = tmp_path / "source1.parquet"
        df1.to_parquet(source1, index=False)
        npd1.import_snapshot(source1)

        # Create NPD in cache2
        npd2 = NonDatedParquetDataset(cache_dir=cache2, name="ref2")
        df2 = pd.DataFrame({"symbol": ["GOOGL"]})
        source2 = tmp_path / "source2.parquet"
        df2.to_parquet(source2, index=False)
        npd2.import_snapshot(source2)

        registry = CacheRegistry.instance(first=cache1, second=cache2)

        result = registry.discover_all_npds()
        assert "ref1" in result
        assert "ref2" in result
        assert result["ref1"][0] == "first"
        assert result["ref2"][0] == "second"

    def test_discover_all_npds_first_cache_precedence(
        self, tmp_path: Path
    ) -> None:
        """discover_all_npds() should use first cache for duplicate names."""
        cache1 = tmp_path / "cache1"
        cache2 = tmp_path / "cache2"

        # Create same NPD name in both caches
        for i, cache in enumerate([cache1, cache2], 1):
            npd = NonDatedParquetDataset(cache_dir=cache, name="shared_npd")
            df = pd.DataFrame({"cache_id": [i]})
            source = tmp_path / f"source{i}.parquet"
            df.to_parquet(source, index=False)
            npd.import_snapshot(source)

        registry = CacheRegistry.instance(first=cache1, second=cache2)

        result = registry.discover_all_npds()
        assert "shared_npd" in result
        # First cache should take precedence
        cache_name, path = result["shared_npd"]
        assert cache_name == "first"


class TestRefreshAll:
    """Tests for refresh_all() method."""

    def test_refresh_all_no_cached_datasets(self, tmp_path: Path) -> None:
        """refresh_all() returns False when no datasets are cached."""
        cache = tmp_path / "cache"
        cache.mkdir()
        registry = CacheRegistry.instance(cache=cache)

        # No datasets accessed yet
        result = registry.refresh_all()
        assert result is False

    def test_refresh_all_nothing_new(self, tmp_path: Path) -> None:
        """refresh_all() returns False when all datasets are current."""
        cache = tmp_path / "cache"

        # Create NPD
        npd = NonDatedParquetDataset(cache_dir=cache, name="ref.instruments")
        df = pd.DataFrame({"symbol": ["AAPL"]})
        source = tmp_path / "source.parquet"
        df.to_parquet(source, index=False)
        npd.import_snapshot(source)

        # Access via registry
        registry = CacheRegistry.instance(cache=cache)
        registry.get_dataset("ref.instruments")

        # Nothing new
        result = registry.refresh_all()
        assert result is False

    def test_refresh_all_finds_new_snapshot(self, tmp_path: Path) -> None:
        """refresh_all() returns True when new snapshot is found."""
        import time

        cache = tmp_path / "cache"

        # Create NPD with first snapshot
        npd = NonDatedParquetDataset(cache_dir=cache, name="ref.instruments")
        df = pd.DataFrame({"symbol": ["AAPL"]})
        source = tmp_path / "source.parquet"
        df.to_parquet(source, index=False)
        npd.import_snapshot(source)

        # Access via registry (caches the dataset)
        registry = CacheRegistry.instance(cache=cache)
        ds = registry.get_dataset("ref.instruments")
        old_suffix = ds.current_suffix

        # Wait and create new snapshot
        time.sleep(1.1)
        df2 = pd.DataFrame({"symbol": ["AAPL", "GOOGL"]})
        source2 = tmp_path / "source2.parquet"
        df2.to_parquet(source2, index=False)
        npd.import_snapshot(source2)

        # refresh_all should find the new snapshot
        result = registry.refresh_all()
        assert result is True
        assert ds.current_suffix != old_suffix

    def test_refresh_all_multiple_datasets(self, tmp_path: Path) -> None:
        """refresh_all() refreshes multiple datasets."""
        import time

        cache = tmp_path / "cache"

        # Create two NPDs
        npd1 = NonDatedParquetDataset(cache_dir=cache, name="ref.one")
        npd2 = NonDatedParquetDataset(cache_dir=cache, name="ref.two")

        df = pd.DataFrame({"val": [1]})
        source = tmp_path / "source.parquet"
        df.to_parquet(source, index=False)
        npd1.import_snapshot(source)
        npd2.import_snapshot(source)

        # Access via registry
        registry = CacheRegistry.instance(cache=cache)
        ds1 = registry.get_dataset("ref.one")
        ds2 = registry.get_dataset("ref.two")
        old_suffix1 = ds1.current_suffix
        old_suffix2 = ds2.current_suffix

        # Wait and update only one
        time.sleep(1.1)
        df2 = pd.DataFrame({"val": [2]})
        source2 = tmp_path / "source2.parquet"
        df2.to_parquet(source2, index=False)
        npd1.import_snapshot(source2)

        # refresh_all should return True (one was refreshed)
        result = registry.refresh_all()
        assert result is True
        assert ds1.current_suffix != old_suffix1  # refreshed
        assert ds2.current_suffix == old_suffix2  # unchanged

    def test_refresh_all_with_dpd(self, tmp_path: Path) -> None:
        """refresh_all() works with DatedParquetDataset."""
        import time

        cache = tmp_path / "cache"

        # Create DPD with first snapshot
        dpd = DatedParquetDataset(
            cache_dir=cache,
            name="md.test_dpd",
            date_col="Date",
            date_partition="year",
            partition_columns=["year"],
        )

        # Create initial data
        df1 = pd.DataFrame(
            {
                "Date": pd.to_datetime(["2024-01-15"]),
                "price": [100.0],
            }
        )
        year_dir = dpd.dataset_dir / "year=Y2024"
        year_dir.mkdir(parents=True)
        file_path = year_dir / "data1.parquet"
        df1.to_parquet(file_path, index=False)

        schema = pa.schema(
            [
                ("Date", pa.timestamp("ns")),
                ("price", pa.float64()),
                ("year", pa.string()),
            ]
        )
        file_metadata = FileMetadata(
            path="year=Y2024/data1.parquet",
            partition_values={"year": "Y2024"},
            checksum="checksum1",
            size_bytes=file_path.stat().st_size,
        )
        dpd._publish_snapshot(
            files=[file_metadata],
            schema=schema,
            cache_start_date=dt.date(2024, 1, 15),
            cache_end_date=dt.date(2024, 1, 15),
            partition_values={"year": ["Y2024"]},
        )

        # Access via registry (caches the dataset)
        registry = CacheRegistry.instance(cache=cache)
        ds = registry.get_dataset("md.test_dpd")
        old_suffix = ds.current_suffix

        # Wait and create new snapshot with more data
        time.sleep(1.1)
        df2 = pd.DataFrame(
            {
                "Date": pd.to_datetime(["2024-01-15", "2024-02-20"]),
                "price": [100.0, 105.0],
            }
        )
        file_path2 = year_dir / "data2.parquet"
        df2.to_parquet(file_path2, index=False)

        file_metadata2 = FileMetadata(
            path="year=Y2024/data2.parquet",
            partition_values={"year": "Y2024"},
            checksum="checksum2",
            size_bytes=file_path2.stat().st_size,
        )
        dpd._publish_snapshot(
            files=[file_metadata2],
            schema=schema,
            cache_start_date=dt.date(2024, 1, 15),
            cache_end_date=dt.date(2024, 2, 20),
            partition_values={"year": ["Y2024"]},
        )

        # refresh_all should find the new snapshot
        result = registry.refresh_all()
        assert result is True
        assert ds.current_suffix != old_suffix

    def test_refresh_all_mixed_dpd_and_npd(self, tmp_path: Path) -> None:
        """refresh_all() refreshes both DPD and NPD datasets."""
        import time

        cache = tmp_path / "cache"

        # Create NPD
        npd = NonDatedParquetDataset(cache_dir=cache, name="ref.instruments")
        df_npd = pd.DataFrame({"symbol": ["AAPL"]})
        source_npd = tmp_path / "source_npd.parquet"
        df_npd.to_parquet(source_npd, index=False)
        npd.import_snapshot(source_npd)

        # Create DPD
        dpd = DatedParquetDataset(
            cache_dir=cache,
            name="md.prices",
            date_col="Date",
            date_partition="year",
            partition_columns=["year"],
        )
        df_dpd = pd.DataFrame(
            {
                "Date": pd.to_datetime(["2024-01-15"]),
                "price": [100.0],
            }
        )
        year_dir = dpd.dataset_dir / "year=Y2024"
        year_dir.mkdir(parents=True)
        file_path = year_dir / "data.parquet"
        df_dpd.to_parquet(file_path, index=False)

        schema = pa.schema(
            [
                ("Date", pa.timestamp("ns")),
                ("price", pa.float64()),
                ("year", pa.string()),
            ]
        )
        file_metadata = FileMetadata(
            path="year=Y2024/data.parquet",
            partition_values={"year": "Y2024"},
            checksum="checksum",
            size_bytes=file_path.stat().st_size,
        )
        dpd._publish_snapshot(
            files=[file_metadata],
            schema=schema,
            cache_start_date=dt.date(2024, 1, 15),
            cache_end_date=dt.date(2024, 1, 15),
            partition_values={"year": ["Y2024"]},
        )

        # Access both via registry
        registry = CacheRegistry.instance(cache=cache)
        ds_npd = registry.get_dataset("ref.instruments")
        ds_dpd = registry.get_dataset("md.prices")
        old_suffix_npd = ds_npd.current_suffix
        old_suffix_dpd = ds_dpd.current_suffix

        # Wait and update DPD only
        time.sleep(1.1)
        df_dpd2 = pd.DataFrame(
            {
                "Date": pd.to_datetime(["2024-01-15", "2024-02-20"]),
                "price": [100.0, 110.0],
            }
        )
        file_path2 = year_dir / "data2.parquet"
        df_dpd2.to_parquet(file_path2, index=False)

        file_metadata2 = FileMetadata(
            path="year=Y2024/data2.parquet",
            partition_values={"year": "Y2024"},
            checksum="checksum2",
            size_bytes=file_path2.stat().st_size,
        )
        dpd._publish_snapshot(
            files=[file_metadata2],
            schema=schema,
            cache_start_date=dt.date(2024, 1, 15),
            cache_end_date=dt.date(2024, 2, 20),
            partition_values={"year": ["Y2024"]},
        )

        # refresh_all should find the new DPD snapshot
        result = registry.refresh_all()
        assert result is True
        assert ds_dpd.current_suffix != old_suffix_dpd  # DPD refreshed
        assert ds_npd.current_suffix == old_suffix_npd  # NPD unchanged
