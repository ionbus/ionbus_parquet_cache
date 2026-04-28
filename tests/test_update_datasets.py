"""Tests for update_datasets module and metadata reconstruction."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

from ionbus_parquet_cache.data_source import BucketedDataSource, DataSource
from ionbus_parquet_cache.dated_dataset import DatedParquetDataset
from ionbus_parquet_cache.partition import PartitionSpec
from ionbus_parquet_cache.update_datasets import (
    _discover_datasets_from_disk,
)


class MockDataSource(DataSource):
    """Mock data source for testing."""

    def available_dates(self) -> tuple[dt.date, dt.date]:
        return (dt.date(2024, 1, 1), dt.date(2024, 12, 31))

    def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
        """Return test data for the partition."""
        dates = pd.date_range(
            partition_spec.start_date, partition_spec.end_date
        )
        n = len(dates)
        return pd.DataFrame({
            "Date": dates,
            "price": [100.0 + i * 0.1 for i in range(n)],
        })


class BucketedInstrumentSource(BucketedDataSource):
    """BucketedDataSource for testing bucketed dataset reconstruction."""

    TICKERS = ["AAPL", "MSFT", "GOOGL", "TSLA"]

    def available_dates(self) -> tuple[dt.date, dt.date]:
        return (dt.date(2024, 1, 1), dt.date(2024, 12, 31))

    def get_instruments_for_time_period(
        self, start_date: dt.date, end_date: dt.date
    ) -> list:
        """Return all available tickers for the time period."""
        return self.TICKERS

    def get_data_for_bucket(
        self,
        instruments: list,
        start_date: dt.date,
        end_date: dt.date,
    ) -> pa.Table | None:
        """Return PyArrow table with data for the instruments in the bucket."""
        tables = []
        for ticker in instruments:
            dates = pd.date_range(start_date, end_date)
            table = pa.table({
                "Date": pa.array(dates.date, type=pa.date32()),
                "ticker": pa.array([ticker] * len(dates), type=pa.string()),
                "price": pa.array(
                    [100.0 + i * 0.1 for i in range(len(dates))],
                    type=pa.float64(),
                ),
            })
            tables.append(table)

        if not tables:
            return None
        return pa.concat_tables(tables)


@pytest.fixture
def temp_cache(tmp_path: Path) -> Path:
    """Create a temporary cache directory."""
    return tmp_path / "cache"


class TestUpdateCacheReconstruction:
    """Tests for dataset reconstruction from stored metadata."""

    def test_bucketed_dataset_reconstruction(
        self, temp_cache: Path
    ) -> None:
        """
        Bucketed DPD via update-cache: verify that num_instrument_buckets,
        instrument_column, and __instrument_bucket__ in partition_columns
        are preserved through snapshot->reconstruction.

        Test flow:
        1. Create DPD with num_instrument_buckets=4, instrument_column="ticker"
        2. Write data via update()
        3. Create snapshot metadata
        4. Call _discover_datasets_from_disk() to reconstruct
        5. Verify reconstructed DPD has correct bucketing config
        """
        # Create bucketed DPD (note: partition_columns=[] because bucketing
        # auto-injects __instrument_bucket__, and ticker is NOT a partition column)
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="bucketed_test",
            date_col="Date",
            date_partition="month",
            partition_columns=[],
            instrument_column="ticker",
            num_instrument_buckets=4,
        )

        # Write some data via update
        source = BucketedInstrumentSource(dpd)
        suffix = dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        assert suffix is not None, "Update should produce a suffix"

        # Verify metadata was created
        meta_dir = dpd.meta_dir
        meta_files = list(meta_dir.glob("*.pkl.gz"))
        assert len(meta_files) == 1, "Should have exactly one metadata file"

        # Reconstruct from disk metadata
        datasets = _discover_datasets_from_disk(temp_cache)

        # Verify reconstruction worked and has correct config
        assert "bucketed_test" in datasets
        reconstructed = datasets["bucketed_test"]

        # Check bucketing configuration
        assert reconstructed.num_instrument_buckets == 4
        assert reconstructed.instrument_column == "ticker"

        # Check partition columns include __instrument_bucket__
        assert "__instrument_bucket__" in reconstructed.partition_columns
        assert "month" in reconstructed.partition_columns

        # Verify dataset was not skipped
        assert reconstructed._metadata is not None
        assert reconstructed._metadata.suffix == suffix

    def test_row_group_size_preserved(
        self, temp_cache: Path
    ) -> None:
        """
        row_group_size preserved through update-cache: verify that
        row_group_size=5000 is stored in metadata and reconstructed.

        Test flow:
        1. Create DPD with row_group_size=5000
        2. Write data via update()
        3. Create snapshot metadata
        4. Call _discover_datasets_from_disk() to reconstruct
        5. Verify reconstructed DPD has row_group_size == 5000
        """
        # Create DPD with specific row_group_size
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="row_group_test",
            date_col="Date",
            date_partition="month",
            row_group_size=5000,
        )

        # Write some data via update
        source = MockDataSource(dpd)
        suffix = dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        assert suffix is not None, "Update should produce a suffix"

        # Verify metadata was created
        meta_dir = dpd.meta_dir
        meta_files = list(meta_dir.glob("*.pkl.gz"))
        assert len(meta_files) == 1, "Should have exactly one metadata file"

        # Reconstruct from disk metadata
        datasets = _discover_datasets_from_disk(temp_cache)

        # Verify reconstruction worked and has correct config
        assert "row_group_test" in datasets
        reconstructed = datasets["row_group_test"]

        # Check row_group_size is preserved
        assert reconstructed.row_group_size == 5000

        # Verify dataset was not skipped
        assert reconstructed._metadata is not None
        assert reconstructed._metadata.suffix == suffix

    def test_combined_bucketing_and_row_group_size(
        self, temp_cache: Path
    ) -> None:
        """
        Combined test: both bucketing config AND row_group_size
        are preserved together through reconstruction.
        """
        # Create DPD with both bucketing and row_group_size
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="combined_test",
            date_col="Date",
            date_partition="month",
            partition_columns=[],
            instrument_column="ticker",
            num_instrument_buckets=3,
            row_group_size=8000,
        )

        # Write some data via update
        source = BucketedInstrumentSource(dpd)
        suffix = dpd.update(
            source,
            start_date=dt.date(2024, 2, 1),
            end_date=dt.date(2024, 2, 29),
        )

        assert suffix is not None, "Update should produce a suffix"

        # Reconstruct from disk metadata
        datasets = _discover_datasets_from_disk(temp_cache)

        # Verify reconstruction worked with both configs
        assert "combined_test" in datasets
        reconstructed = datasets["combined_test"]

        # Check bucketing configuration
        assert reconstructed.num_instrument_buckets == 3
        assert reconstructed.instrument_column == "ticker"
        assert "__instrument_bucket__" in reconstructed.partition_columns

        # Check row_group_size
        assert reconstructed.row_group_size == 8000

        # Verify metadata is valid
        assert reconstructed._metadata is not None
        assert reconstructed._metadata.suffix == suffix
