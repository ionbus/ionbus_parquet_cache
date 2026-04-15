"""Tests for DataSource abstract class."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest

from ionbus_parquet_cache.data_source import DataSource
from ionbus_parquet_cache.dated_dataset import DatedParquetDataset
from ionbus_parquet_cache.exceptions import DataSourceError, ValidationError
from ionbus_parquet_cache.partition import PartitionSpec


class SimpleTestSource(DataSource):
    """Simple test implementation of DataSource."""

    def available_dates(self) -> tuple[dt.date, dt.date]:
        return (dt.date(2024, 1, 1), dt.date(2024, 12, 31))

    def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
        # Return simple test data
        dates = pd.date_range(partition_spec.start_date, partition_spec.end_date)
        return pd.DataFrame({"Date": dates, "value": range(len(dates))})


class InstrumentTestSource(DataSource):
    """Test source with instrument support."""

    partition_values = {"FutureRoot": ["ES", "NQ", "YM"]}

    def available_dates(self) -> tuple[dt.date, dt.date]:
        return (dt.date(2024, 1, 1), dt.date(2024, 12, 31))

    def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
        dates = pd.date_range(partition_spec.start_date, partition_spec.end_date)
        instrument = partition_spec.partition_values.get("FutureRoot", "UNKNOWN")
        return pd.DataFrame({
            "Date": dates,
            "FutureRoot": instrument,
            "value": range(len(dates)),
        })


@pytest.fixture
def temp_cache(tmp_path: Path) -> Path:
    """Create a temporary cache directory."""
    return tmp_path / "cache"


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
def instrument_dpd(temp_cache: Path) -> DatedParquetDataset:
    """Create a DPD with instrument partitioning."""
    return DatedParquetDataset(
        cache_dir=temp_cache,
        name="futures_dataset",
        date_col="Date",
        date_partition="year",
        partition_columns=["FutureRoot", "year"],
        instrument_column="FutureRoot",
    )


class TestDataSourceInit:
    """Tests for DataSource initialization."""

    def test_init_sets_dataset(self, simple_dpd: DatedParquetDataset) -> None:
        """Init should set dataset reference."""
        source = SimpleTestSource(simple_dpd)
        assert source.dataset is simple_dpd

    def test_init_sets_kwargs(self, simple_dpd: DatedParquetDataset) -> None:
        """Init should set kwargs as attributes."""
        source = SimpleTestSource(simple_dpd, api_endpoint="https://api.test.com")
        assert source.api_endpoint == "https://api.test.com"


class TestDataSourceAvailableDates:
    """Tests for available_dates()."""

    def test_returns_date_range(self, simple_dpd: DatedParquetDataset) -> None:
        """Should return start and end dates."""
        source = SimpleTestSource(simple_dpd)
        start, end = source.available_dates()

        assert start == dt.date(2024, 1, 1)
        assert end == dt.date(2024, 12, 31)


class TestDataSourcePrepare:
    """Tests for prepare()."""

    def test_prepare_sets_state(self, simple_dpd: DatedParquetDataset) -> None:
        """_do_prepare should set internal state."""
        source = SimpleTestSource(simple_dpd)
        source._do_prepare(dt.date(2024, 1, 1), dt.date(2024, 3, 31))

        assert source.start_date == dt.date(2024, 1, 1)
        assert source.end_date == dt.date(2024, 3, 31)
        assert source.prepared is True

    def test_prepare_with_instruments(
        self, instrument_dpd: DatedParquetDataset
    ) -> None:
        """Should accept instruments when instrument_column is partition column."""
        source = InstrumentTestSource(instrument_dpd)
        source._do_prepare(
            dt.date(2024, 1, 1),
            dt.date(2024, 3, 31),
            instruments=["ES", "NQ"],
        )

        assert source.instruments == ["ES", "NQ"]


class TestDataSourceGetPartitions:
    """Tests for get_partitions()."""

    def test_requires_prepare(self, simple_dpd: DatedParquetDataset) -> None:
        """Should raise if prepare() not called."""
        source = SimpleTestSource(simple_dpd)

        with pytest.raises(DataSourceError, match="prepare.*must be called"):
            source.get_partitions()

    def test_returns_partition_specs(self, simple_dpd: DatedParquetDataset) -> None:
        """Should return list of PartitionSpec objects."""
        source = SimpleTestSource(simple_dpd)
        source._do_prepare(dt.date(2024, 1, 15), dt.date(2024, 3, 15))

        specs = source.get_partitions()

        assert len(specs) == 3  # Jan, Feb, Mar (partial months)
        assert all(isinstance(s, PartitionSpec) for s in specs)

    def test_partition_values_include_date_partition(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Partition values should include date partition column."""
        source = SimpleTestSource(simple_dpd)
        source._do_prepare(dt.date(2024, 1, 1), dt.date(2024, 1, 31))

        specs = source.get_partitions()

        assert len(specs) == 1
        assert "month" in specs[0].partition_values
        assert specs[0].partition_values["month"] == "M2024-01"

    def test_with_instrument_partitions(
        self, instrument_dpd: DatedParquetDataset
    ) -> None:
        """Should create specs for each instrument/date combination."""
        source = InstrumentTestSource(instrument_dpd)
        source._do_prepare(dt.date(2024, 1, 1), dt.date(2024, 12, 31))

        specs = source.get_partitions()

        # 1 year * 3 instruments = 3 specs
        assert len(specs) == 3
        instruments = [s.partition_values["FutureRoot"] for s in specs]
        assert set(instruments) == {"ES", "NQ", "YM"}


class TestDataSourceGetData:
    """Tests for get_data()."""

    def test_returns_dataframe(self, simple_dpd: DatedParquetDataset) -> None:
        """Should return data for the partition."""
        source = SimpleTestSource(simple_dpd)
        source._do_prepare(dt.date(2024, 1, 1), dt.date(2024, 1, 31))

        specs = source.get_partitions()
        data = source.get_data(specs[0])

        assert isinstance(data, pd.DataFrame)
        assert "Date" in data.columns
        assert "value" in data.columns
        assert len(data) == 31  # January has 31 days


class TestDataSourceGetPartitionColumnValues:
    """Tests for get_partition_column_values()."""

    def test_date_partition_from_range(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Should compute date partition values from date range."""
        source = SimpleTestSource(simple_dpd)
        source._do_prepare(dt.date(2024, 1, 1), dt.date(2024, 3, 31))

        values = source.get_partition_column_values("month")

        assert len(values) == 3
        assert "M2024-01" in values
        assert "M2024-02" in values
        assert "M2024-03" in values

    def test_from_class_partition_values(
        self, instrument_dpd: DatedParquetDataset
    ) -> None:
        """Should use class-level partition_values."""
        source = InstrumentTestSource(instrument_dpd)
        source._do_prepare(dt.date(2024, 1, 1), dt.date(2024, 12, 31))

        values = source.get_partition_column_values("FutureRoot")

        assert values == ["ES", "NQ", "YM"]

    def test_unknown_column_raises(self, simple_dpd: DatedParquetDataset) -> None:
        """Should raise for unknown column with no values."""
        source = SimpleTestSource(simple_dpd)
        source._do_prepare(dt.date(2024, 1, 1), dt.date(2024, 1, 31))

        with pytest.raises(DataSourceError, match="Cannot determine values"):
            source.get_partition_column_values("unknown_column")


class TestDataSourceChunking:
    """Tests for chunked partitions."""

    def test_chunk_days_splits_partitions(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Setting chunk_days should split date ranges."""

        class ChunkedSource(SimpleTestSource):
            chunk_days = 10  # 10 business days ~ 14 calendar days

        source = ChunkedSource(simple_dpd)
        source._do_prepare(dt.date(2024, 1, 1), dt.date(2024, 1, 31))

        specs = source.get_partitions()

        # January should be split into multiple chunks
        assert len(specs) > 1
        # All should have same month partition value
        assert all(s.partition_values["month"] == "M2024-01" for s in specs)


class TestBuildPartitionsHelper:
    """Tests for build_partitions() helper method."""

    def test_build_partitions_returns_specs(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """build_partitions should return PartitionSpec list."""
        source = SimpleTestSource(simple_dpd)

        specs = source.build_partitions(
            dt.date(2024, 1, 1),
            dt.date(2024, 3, 31),
        )

        assert len(specs) == 3  # Jan, Feb, Mar
        assert all(isinstance(s, PartitionSpec) for s in specs)

    def test_build_partitions_with_max_business_days(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """build_partitions should chunk when max_business_days is set."""
        source = SimpleTestSource(simple_dpd)

        specs = source.build_partitions(
            dt.date(2024, 1, 1),
            dt.date(2024, 1, 31),
            max_business_days=10,
        )

        # Should have multiple chunks for January
        assert len(specs) > 1
        assert all(s.partition_values["month"] == "M2024-01" for s in specs)

    def test_build_partitions_with_instruments(
        self, instrument_dpd: DatedParquetDataset
    ) -> None:
        """build_partitions should create cross product with instruments."""
        source = InstrumentTestSource(instrument_dpd)
        source._do_prepare(
            dt.date(2024, 1, 1),
            dt.date(2024, 12, 31),
            instruments=["ES", "NQ"],
        )

        specs = source.build_partitions(
            dt.date(2024, 1, 1),
            dt.date(2024, 12, 31),
        )

        # Should have 2 instruments x 1 year partition = 2 specs
        assert len(specs) == 2
        roots = {s.partition_values["FutureRoot"] for s in specs}
        assert roots == {"ES", "NQ"}

    def test_build_partitions_independent_of_prepare(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """build_partitions should work without prepare() call."""
        source = SimpleTestSource(simple_dpd)

        # Don't call prepare - build_partitions should still work
        specs = source.build_partitions(
            dt.date(2024, 6, 1),
            dt.date(2024, 6, 30),
        )

        assert len(specs) == 1
        assert specs[0].partition_values["month"] == "M2024-06"


class TestChunkValues:
    """Tests for chunk_values expansion."""

    def test_chunk_values_expands_specs(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """chunk_values should expand each spec with chunk_info."""

        class ChunkedSource(SimpleTestSource):
            chunk_values = {"batch": [0, 1, 2]}

        source = ChunkedSource(simple_dpd)
        specs = source.build_partitions(
            dt.date(2024, 1, 1),
            dt.date(2024, 1, 31),
        )

        # 1 month * 3 batches = 3 specs
        assert len(specs) == 3
        # All should have same partition_values
        assert all(s.partition_values["month"] == "M2024-01" for s in specs)
        # Each should have different chunk_info
        batches = [s.chunk_info["batch"] for s in specs]
        assert sorted(batches) == [0, 1, 2]

    def test_chunk_values_multiple_dimensions(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Multiple chunk dimensions create cartesian product."""

        class MultiChunkSource(SimpleTestSource):
            chunk_values = {"exchange": ["NYSE", "NASDAQ"], "batch": [0, 1]}

        source = MultiChunkSource(simple_dpd)
        specs = source.build_partitions(
            dt.date(2024, 1, 1),
            dt.date(2024, 1, 31),
        )

        # 1 month * 2 exchanges * 2 batches = 4 specs
        assert len(specs) == 4
        # All should have same partition_values
        assert all(s.partition_values["month"] == "M2024-01" for s in specs)
        # Check all combinations exist
        combos = [(s.chunk_info["exchange"], s.chunk_info["batch"]) for s in specs]
        assert ("NYSE", 0) in combos
        assert ("NYSE", 1) in combos
        assert ("NASDAQ", 0) in combos
        assert ("NASDAQ", 1) in combos

    def test_chunk_values_with_partition_values(
        self, instrument_dpd: DatedParquetDataset
    ) -> None:
        """chunk_values should combine with partition_values."""

        class InstrumentChunkedSource(InstrumentTestSource):
            chunk_values = {"batch": [0, 1]}

        source = InstrumentChunkedSource(instrument_dpd)
        source._do_prepare(dt.date(2024, 1, 1), dt.date(2024, 12, 31))
        specs = source.build_partitions(
            dt.date(2024, 1, 1),
            dt.date(2024, 12, 31),
        )

        # 1 year * 3 instruments * 2 batches = 6 specs
        assert len(specs) == 6
        # Check partition_values are correct
        instruments = {s.partition_values["FutureRoot"] for s in specs}
        assert instruments == {"ES", "NQ", "YM"}
        # Check chunk_info
        assert all("batch" in s.chunk_info for s in specs)

    def test_empty_chunk_values_no_expansion(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Empty chunk_values should not expand specs."""
        source = SimpleTestSource(simple_dpd)
        specs = source.build_partitions(
            dt.date(2024, 1, 1),
            dt.date(2024, 1, 31),
        )

        assert len(specs) == 1
        assert specs[0].chunk_info == {}

    def test_chunk_values_instance_copy(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Instance _chunk_values should be independent copy."""

        class ChunkedSource(SimpleTestSource):
            chunk_values = {"batch": [0, 1]}

        source1 = ChunkedSource(simple_dpd)
        source2 = ChunkedSource(simple_dpd)

        # Modify instance copy
        source1._chunk_values["batch"] = [0, 1, 2, 3]

        # Class-level should be unchanged
        assert ChunkedSource.chunk_values == {"batch": [0, 1]}
        # Other instance should be unchanged
        assert source2._chunk_values == {"batch": [0, 1]}


class TestChunkExpander:
    """Tests for chunk_expander function-based expansion."""

    def test_chunk_expander_expands_specs(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """chunk_expander function should expand each spec."""

        def expand_by_batch(spec: PartitionSpec) -> list[PartitionSpec]:
            """Expand each spec into 3 batches."""
            return [
                PartitionSpec(
                    partition_values=spec.partition_values,
                    start_date=spec.start_date,
                    end_date=spec.end_date,
                    chunk_info={"batch": i},
                )
                for i in range(3)
            ]

        class ExpanderSource(SimpleTestSource):
            chunk_expander = staticmethod(expand_by_batch)

        source = ExpanderSource(simple_dpd)
        specs = source.build_partitions(
            dt.date(2024, 1, 1),
            dt.date(2024, 1, 31),
        )

        # 1 month * 3 batches = 3 specs
        assert len(specs) == 3
        batches = [s.chunk_info["batch"] for s in specs]
        assert sorted(batches) == [0, 1, 2]

    def test_chunk_expander_dynamic_expansion(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """chunk_expander can use partition info for dynamic expansion."""

        def expand_by_month(spec: PartitionSpec) -> list[PartitionSpec]:
            """Expand differently based on partition month."""
            month = spec.partition_values.get("month", "")
            if month == "M2024-01":
                # January gets 2 chunks
                num_chunks = 2
            else:
                # Other months get 1 chunk
                num_chunks = 1

            return [
                PartitionSpec(
                    partition_values=spec.partition_values,
                    start_date=spec.start_date,
                    end_date=spec.end_date,
                    chunk_info={"chunk": i},
                )
                for i in range(num_chunks)
            ]

        class DynamicExpanderSource(SimpleTestSource):
            chunk_expander = staticmethod(expand_by_month)

        source = DynamicExpanderSource(simple_dpd)
        specs = source.build_partitions(
            dt.date(2024, 1, 1),
            dt.date(2024, 2, 29),
        )

        # January: 2 chunks, February: 1 chunk = 3 total
        assert len(specs) == 3
        jan_specs = [s for s in specs if s.partition_values["month"] == "M2024-01"]
        feb_specs = [s for s in specs if s.partition_values["month"] == "M2024-02"]
        assert len(jan_specs) == 2
        assert len(feb_specs) == 1

    def test_chunk_expander_with_chunk_values(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """chunk_expander should be applied after chunk_values."""

        def add_exchange(spec: PartitionSpec) -> list[PartitionSpec]:
            """Add exchange dimension to existing chunk_info."""
            return [
                PartitionSpec(
                    partition_values=spec.partition_values,
                    start_date=spec.start_date,
                    end_date=spec.end_date,
                    chunk_info={**spec.chunk_info, "exchange": ex},
                )
                for ex in ["NYSE", "NASDAQ"]
            ]

        class CombinedSource(SimpleTestSource):
            chunk_values = {"batch": [0, 1]}
            chunk_expander = staticmethod(add_exchange)

        source = CombinedSource(simple_dpd)
        specs = source.build_partitions(
            dt.date(2024, 1, 1),
            dt.date(2024, 1, 31),
        )

        # 1 month * 2 batches * 2 exchanges = 4 specs
        assert len(specs) == 4
        # Each spec should have both batch and exchange
        for spec in specs:
            assert "batch" in spec.chunk_info
            assert "exchange" in spec.chunk_info

    def test_no_chunk_expander_no_expansion(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """None chunk_expander should not expand specs."""
        source = SimpleTestSource(simple_dpd)
        assert source.chunk_expander is None

        specs = source.build_partitions(
            dt.date(2024, 1, 1),
            dt.date(2024, 1, 31),
        )

        assert len(specs) == 1
