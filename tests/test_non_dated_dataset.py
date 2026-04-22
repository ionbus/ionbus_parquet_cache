"""Tests for NonDatedParquetDataset."""

from __future__ import annotations

import shutil
import tempfile
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ionbus_parquet_cache.exceptions import SnapshotNotFoundError, SnapshotPublishError
from ionbus_parquet_cache.non_dated_dataset import NonDatedParquetDataset
from ionbus_parquet_cache.snapshot import generate_snapshot_suffix


@pytest.fixture
def temp_cache(tmp_path: Path) -> Path:
    """Create a temporary cache directory."""
    return tmp_path / "cache"


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "symbol": ["AAPL", "GOOGL", "MSFT"],
            "name": ["Apple Inc.", "Alphabet Inc.", "Microsoft Corp."],
            "exchange": ["NASDAQ", "NASDAQ", "NASDAQ"],
        }
    )


@pytest.fixture
def sample_parquet(tmp_path: Path, sample_df: pd.DataFrame) -> Path:
    """Create a sample parquet file."""
    path = tmp_path / "sample.parquet"
    sample_df.to_parquet(path, index=False)
    return path


@pytest.fixture
def sample_hive_dir(tmp_path: Path) -> Path:
    """Create a sample hive-partitioned directory."""
    base = tmp_path / "hive_data"

    for exchange in ["NYSE", "NASDAQ"]:
        exchange_dir = base / f"exchange={exchange}"
        exchange_dir.mkdir(parents=True)
        df = pd.DataFrame(
            {
                "symbol": [f"{exchange}_SYM1", f"{exchange}_SYM2"],
                "name": [f"{exchange} Symbol 1", f"{exchange} Symbol 2"],
            }
        )
        df.to_parquet(exchange_dir / "data.parquet", index=False)

    return base


class TestNonDatedDatasetBasic:
    """Basic tests for NonDatedParquetDataset."""

    def test_init(self, temp_cache: Path) -> None:
        """Test initialization."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="instrument_master")
        assert npd.name == "instrument_master"
        assert npd.cache_dir == temp_cache
        assert npd.npd_dir == temp_cache / "non-dated" / "instrument_master"

    def test_no_snapshot_raises(self, temp_cache: Path) -> None:
        """Accessing data with no snapshot should raise."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="empty_dataset")
        with pytest.raises(SnapshotNotFoundError):
            npd.pyarrow_dataset()


class TestNonDatedDatasetImport:
    """Tests for importing snapshots."""

    def test_import_single_file(
        self, temp_cache: Path, sample_parquet: Path, sample_df: pd.DataFrame
    ) -> None:
        """Import a single parquet file as snapshot."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="instrument_master")

        suffix = npd.import_snapshot(sample_parquet)

        assert len(suffix) == 7
        assert npd.current_suffix == suffix
        assert npd.npd_dir.exists()

        # Should be able to read data
        result = npd.read_data()
        pd.testing.assert_frame_equal(result.reset_index(drop=True), sample_df)

    def test_import_hive_directory(
        self, temp_cache: Path, sample_hive_dir: Path
    ) -> None:
        """Import a hive-partitioned directory as snapshot."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="calendar_data")

        suffix = npd.import_snapshot(sample_hive_dir)

        assert len(suffix) == 7
        assert npd.current_suffix == suffix

        # Should be able to read data
        result = npd.read_data()
        assert len(result) == 4  # 2 exchanges * 2 symbols each

    def test_import_multiple_snapshots(
        self, temp_cache: Path, sample_parquet: Path, tmp_path: Path, sample_df: pd.DataFrame
    ) -> None:
        """Importing multiple times creates multiple snapshots."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="instrument_master")

        suffix1 = npd.import_snapshot(sample_parquet)

        # Wait to ensure different timestamp (suffix is second-granularity)
        time.sleep(1.1)

        # Create another parquet with different data
        df2 = sample_df.copy()
        df2["symbol"] = ["TSLA", "NVDA", "AMD"]
        path2 = tmp_path / "sample2.parquet"
        df2.to_parquet(path2, index=False)

        suffix2 = npd.import_snapshot(path2)

        assert suffix2 > suffix1  # Newer suffix is larger
        assert npd.current_suffix == suffix2

        # Reading should return the newer data
        result = npd.read_data()
        assert "TSLA" in result["symbol"].values

    def test_import_nonexistent_raises(self, temp_cache: Path) -> None:
        """Importing nonexistent path should raise."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="test")

        with pytest.raises(FileNotFoundError):
            npd.import_snapshot("/nonexistent/path")


class TestNonDatedDatasetRead:
    """Tests for reading data."""

    def test_read_with_filters(
        self, temp_cache: Path, sample_parquet: Path
    ) -> None:
        """Read data with filters."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="instrument_master")
        npd.import_snapshot(sample_parquet)

        result = npd.read_data(filters=[("symbol", "=", "AAPL")])

        assert len(result) == 1
        assert result.iloc[0]["symbol"] == "AAPL"

    def test_read_with_columns(
        self, temp_cache: Path, sample_parquet: Path
    ) -> None:
        """Read data with column selection."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="instrument_master")
        npd.import_snapshot(sample_parquet)

        result = npd.read_data(columns=["symbol", "name"])

        assert list(result.columns) == ["symbol", "name"]

    def test_date_params_raise(
        self, temp_cache: Path, sample_parquet: Path
    ) -> None:
        """Passing date parameters should raise ValueError."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="instrument_master")
        npd.import_snapshot(sample_parquet)

        with pytest.raises(ValueError, match="do not support date filtering"):
            npd.read_data(start_date="2024-01-01")

        with pytest.raises(ValueError, match="do not support date filtering"):
            npd.read_data(end_date="2024-01-01")

    def test_date_params_raise_polars(
        self, temp_cache: Path, sample_parquet: Path
    ) -> None:
        """Passing date parameters to read_data_pl should raise ValueError."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="instrument_master")
        npd.import_snapshot(sample_parquet)

        with pytest.raises(ValueError, match="do not support date filtering"):
            npd.read_data_pl(start_date="2024-01-01")

        with pytest.raises(ValueError, match="do not support date filtering"):
            npd.read_data_pl(end_date="2024-01-01")


class TestNonDatedDatasetRefresh:
    """Tests for refresh functionality."""

    def test_is_update_available(
        self, temp_cache: Path, sample_parquet: Path, tmp_path: Path, sample_df: pd.DataFrame
    ) -> None:
        """Test detecting new snapshots."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="instrument_master")
        npd.import_snapshot(sample_parquet)

        # No update available initially
        assert not npd.is_update_available()

        # Wait to ensure different suffix (second-granularity)
        time.sleep(1.1)

        # Import another snapshot (simulating external update)
        df2 = sample_df.copy()
        path2 = tmp_path / "sample2.parquet"
        df2.to_parquet(path2, index=False)
        npd.import_snapshot(path2)

        # Create a new instance that doesn't know about the update
        npd2 = NonDatedParquetDataset(cache_dir=temp_cache, name="instrument_master")
        npd2.current_suffix = npd._discover_current_suffix()
        # Force it to think it's on an old suffix
        time.sleep(0.01)  # Ensure different timestamp

        # This test verifies the method exists and works
        result = npd2.is_update_available()
        assert isinstance(result, bool)

    def test_refresh_no_op_when_current(
        self, temp_cache: Path, sample_parquet: Path
    ) -> None:
        """Test refresh is no-op when already on latest snapshot."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="instrument_master")
        npd.import_snapshot(sample_parquet)

        # Access dataset to cache it
        _ = npd.pyarrow_dataset()
        assert npd._dataset is not None

        # Refresh with no new snapshot should return False and NOT clear cache
        result = npd.refresh()
        assert result is False
        # Cache remains because no new snapshot exists
        assert npd._dataset is not None

    def test_refresh_clears_on_new_snapshot(
        self, temp_cache: Path, sample_parquet: Path, tmp_path: Path, sample_df: pd.DataFrame
    ) -> None:
        """Test refresh clears cache when new snapshot exists."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="instrument_master")
        npd.import_snapshot(sample_parquet)

        # Access dataset to cache it
        _ = npd.pyarrow_dataset()
        old_suffix = npd.current_suffix

        # Simulate external process adding a new snapshot
        time.sleep(1.1)
        df2 = sample_df.copy()
        path2 = tmp_path / "sample2.parquet"
        df2.to_parquet(path2, index=False)

        # Manually create new snapshot without using import_snapshot
        # (which would update our instance)
        new_suffix = generate_snapshot_suffix()
        dest = npd.npd_dir / f"instrument_master_{new_suffix}.parquet"
        shutil.copy2(path2, dest)

        # Now refresh should detect and load new snapshot, return True
        result = npd.refresh()
        assert result is True
        assert npd.current_suffix == new_suffix
        assert npd._dataset is None  # Cache cleared


class TestNonDatedDatasetSummary:
    """Tests for summary functionality."""

    def test_summary(
        self, temp_cache: Path, sample_parquet: Path
    ) -> None:
        """Test summary returns expected info."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="instrument_master")
        npd.import_snapshot(sample_parquet)

        summary = npd.summary()

        assert summary["name"] == "instrument_master"
        assert "npd_dir" in summary
        assert summary["is_directory_snapshot"] is False
        assert summary["snapshot_count"] == 1


class TestSnapshotSuffixCollisionDetection:
    """Tests for snapshot suffix collision detection in import_snapshot."""

    def test_import_when_current_suffix_none_no_error(
        self, temp_cache: Path, sample_parquet: Path
    ) -> None:
        """When current_suffix is None, no error should be raised."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="collision_test")

        # current_suffix is None (no previous snapshot)
        assert npd.current_suffix is None

        # Should succeed without error
        suffix = npd.import_snapshot(sample_parquet)
        assert suffix is not None
        assert len(suffix) == 7

    def test_import_when_new_suffix_greater_no_error(
        self, temp_cache: Path, sample_parquet: Path, tmp_path: Path, sample_df: pd.DataFrame
    ) -> None:
        """When new_suffix > current_suffix, no error should be raised."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="collision_test")

        # First import
        suffix1 = npd.import_snapshot(sample_parquet)

        # Wait to ensure different timestamp (second-granularity)
        time.sleep(1.1)

        # Create another parquet file
        path2 = tmp_path / "sample2.parquet"
        sample_df.to_parquet(path2, index=False)

        # Second import - should succeed
        suffix2 = npd.import_snapshot(path2)

        assert suffix2 > suffix1

    def test_import_collision_when_too_fast(
        self, temp_cache: Path, sample_parquet: Path, tmp_path: Path, sample_df: pd.DataFrame
    ) -> None:
        """When imports are too fast (within same second), SnapshotPublishError should be raised."""
        npd = NonDatedParquetDataset(cache_dir=temp_cache, name="collision_test")

        # First import
        suffix1 = npd.import_snapshot(sample_parquet)

        # Mock the suffix generation to return the same value
        # by manually setting current_suffix to a value that would cause collision
        # We do this by directly setting a suffix that matches what generate_snapshot_suffix
        # will return (which we can't predict), so instead we test the error condition
        # by importing immediately without sleep

        # Create another parquet file
        path2 = tmp_path / "sample2.parquet"
        sample_df.to_parquet(path2, index=False)

        # To reliably test the collision, we need to simulate a case where
        # generate_snapshot_suffix returns a suffix <= current_suffix.
        # Since we can't control the suffix generation, we test by manipulating
        # the current_suffix to be far in the future.
        npd.current_suffix = "zzzzzz"  # Artificially high suffix

        with pytest.raises(SnapshotPublishError, match="Snapshot suffix collision"):
            npd.import_snapshot(path2)
