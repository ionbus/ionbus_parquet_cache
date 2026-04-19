"""
Tests for the instrument hash bucketing feature in DatedParquetDataset.

Covers:
1. Validation errors at construction time
2. Bucket hash assignment correctness
3. _BucketedDataSourceWrapper unit tests
4. Full integration: build, read, filter
5. Breaking-change detection (num_instrument_buckets mismatch)
6. read_data(instruments=...) on a non-bucketed dataset raises ValueError
"""

from __future__ import annotations

import datetime as dt
import zlib
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest

from ionbus_parquet_cache.data_source import DataSource
from ionbus_parquet_cache.dated_dataset import (
    INSTRUMENT_BUCKET_COL,
    DatedParquetDataset,
    _BucketedDataSourceWrapper,
)
from ionbus_parquet_cache.exceptions import ValidationError
from ionbus_parquet_cache.partition import PartitionSpec


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
YEARS = [2022, 2023]


def _make_ticker_data(ticker: str, year: int) -> pa.Table:
    """Return a tiny Arrow table for one (ticker, year) pair."""
    dates = [dt.date(year, 1, 1), dt.date(year, 6, 15), dt.date(year, 12, 31)]
    return pa.table(
        {
            "date": pa.array(dates, type=pa.date32()),
            "ticker": pa.array([ticker] * len(dates)),
            "close": pa.array([100.0 + hash(ticker + str(year)) % 200] * len(dates)),
        }
    )


class TickerYearDataSource(DataSource):
    """
    MockDataSource that returns one PartitionSpec per (ticker, year) and
    delivers test data via get_data().  The dataset must have:
        date_col="date", instrument_column="ticker"
    """

    def __init__(self, dataset: DatedParquetDataset, tickers=TICKERS, years=YEARS):
        super().__init__(dataset)
        self._tickers = tickers
        self._years = years

    def available_dates(self) -> tuple[dt.date, dt.date]:
        return (dt.date(min(self._years), 1, 1), dt.date(max(self._years), 12, 31))

    def get_partitions(self) -> list[PartitionSpec]:
        specs = []
        for ticker in sorted(self._tickers):
            for year in sorted(self._years):
                specs.append(
                    PartitionSpec(
                        partition_values={"ticker": ticker, "year": f"Y{year}"},
                        start_date=dt.date(year, 1, 1),
                        end_date=dt.date(year, 12, 31),
                    )
                )
        return specs

    def get_data(self, spec: PartitionSpec) -> pa.Table | None:
        ticker = spec.partition_values["ticker"]
        year = spec.start_date.year
        return _make_ticker_data(ticker, year)


@pytest.fixture
def temp_cache(tmp_path: Path) -> Path:
    return tmp_path / "cache"


@pytest.fixture
def bucketed_dpd(temp_cache: Path) -> DatedParquetDataset:
    """A DPD configured with instrument hash bucketing (4 buckets)."""
    return DatedParquetDataset(
        cache_dir=temp_cache,
        name="bucketed_test",
        date_col="date",
        date_partition="year",
        partition_columns=[],  # no extra; bucket + year added by model_post_init
        instrument_column="ticker",
        num_instrument_buckets=4,
    )


# ---------------------------------------------------------------------------
# 1. Validation errors
# ---------------------------------------------------------------------------


class TestValidationErrors:
    def test_buckets_only_requires_instrument_column(self, temp_cache: Path) -> None:
        """instrument_column alone (without num_instrument_buckets) is allowed
        for the non-bucketed partition-column mode.  Only the reverse is forbidden."""
        # This should NOT raise — instrument_column alone is valid
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="plain_instrument",
            date_col="date",
            date_partition="year",
            partition_columns=["ticker"],
            instrument_column="ticker",
            # num_instrument_buckets omitted intentionally
        )
        assert dpd.instrument_column == "ticker"
        assert INSTRUMENT_BUCKET_COL not in dpd.partition_columns

    def test_buckets_without_instrument_column_raises(self, temp_cache: Path) -> None:
        with pytest.raises(ValidationError, match="must both be set or both be None"):
            DatedParquetDataset(
                cache_dir=temp_cache,
                name="bad",
                date_col="date",
                date_partition="year",
                num_instrument_buckets=4,
                # instrument_column deliberately omitted
            )

    def test_instrument_column_in_partition_columns_raises(self, temp_cache: Path) -> None:
        with pytest.raises(ValidationError, match="must not be in partition_columns"):
            DatedParquetDataset(
                cache_dir=temp_cache,
                name="bad",
                date_col="date",
                date_partition="year",
                partition_columns=["ticker"],  # instrument_column in partition_columns
                instrument_column="ticker",
                num_instrument_buckets=4,
            )

    def test_bucket_col_in_partition_columns_raises(self, temp_cache: Path) -> None:
        with pytest.raises(ValidationError, match="is reserved"):
            DatedParquetDataset(
                cache_dir=temp_cache,
                name="bad",
                date_col="date",
                date_partition="year",
                partition_columns=[INSTRUMENT_BUCKET_COL],
                instrument_column="ticker",
                num_instrument_buckets=4,
            )

    def test_bucket_col_in_sort_columns_raises(self, temp_cache: Path) -> None:
        with pytest.raises(ValidationError, match="is reserved and must not appear in sort_columns"):
            DatedParquetDataset(
                cache_dir=temp_cache,
                name="bad",
                date_col="date",
                date_partition="year",
                sort_columns=[INSTRUMENT_BUCKET_COL, "date"],
                instrument_column="ticker",
                num_instrument_buckets=4,
            )


# ---------------------------------------------------------------------------
# 2. Bucket assignment
# ---------------------------------------------------------------------------


class TestBucketAssignment:
    def test_same_instrument_same_bucket(self) -> None:
        """The same ticker always maps to the same bucket."""
        n = 16
        b1 = f"{zlib.crc32('AAPL'.encode()) % n:04d}"
        b2 = f"{zlib.crc32('AAPL'.encode()) % n:04d}"
        assert b1 == b2

    def test_bucket_string_is_4_digits(self) -> None:
        for ticker in ["AAPL", "MSFT", "Z", "LOOOOOOOOONG"]:
            bucket = f"{zlib.crc32(ticker.encode()) % 256:04d}"
            assert len(bucket) == 4
            assert bucket.isdigit()

    def test_bucket_range_respected(self) -> None:
        n = 4
        for ticker in TICKERS:
            b = zlib.crc32(ticker.encode()) % n
            assert 0 <= b < n


# ---------------------------------------------------------------------------
# 3. _BucketedDataSourceWrapper unit tests
# ---------------------------------------------------------------------------


class TestBucketedDataSourceWrapper:
    """Unit tests for the wrapper in isolation."""

    def _make_wrapper(self, num_buckets: int = 4) -> tuple[_BucketedDataSourceWrapper, Any]:
        """Return (wrapper, inner_source_stub)."""
        # We don't need a real DatedParquetDataset for the wrapper itself;
        # build a minimal stub dataset just for TickerYearDataSource.
        from unittest.mock import MagicMock
        dataset = MagicMock()
        dataset.date_col = "date"
        dataset.date_partition = "year"
        dataset.partition_columns = [INSTRUMENT_BUCKET_COL, "year"]
        dataset.instrument_column = "ticker"

        inner = TickerYearDataSource(dataset, tickers=["AAPL", "MSFT"], years=[2022])
        inner.set_date_instruments(dt.date(2022, 1, 1), dt.date(2022, 12, 31))

        wrapper = _BucketedDataSourceWrapper(
            inner=inner,
            instrument_column="ticker",
            num_instrument_buckets=num_buckets,
            sort_cols=["ticker", "date"],
            instrument_bucket_col=INSTRUMENT_BUCKET_COL,
        )
        return wrapper, inner

    def test_get_partitions_groups_correctly(self) -> None:
        wrapper, _ = self._make_wrapper(num_buckets=4)
        specs = wrapper.get_partitions()
        # Each spec must have INSTRUMENT_BUCKET_COL in partition_values
        for s in specs:
            assert INSTRUMENT_BUCKET_COL in s.partition_values
        # We have 2 tickers × 1 year = 2 ticker-specs.
        # They may land in the same bucket or different buckets.
        bucket_values = {s.partition_values[INSTRUMENT_BUCKET_COL] for s in specs}
        assert len(bucket_values) >= 1

    def test_get_data_concatenates_and_sorts(self) -> None:
        # Use 1 bucket so both tickers end up in the same bucket-spec
        wrapper, _ = self._make_wrapper(num_buckets=1)
        specs = wrapper.get_partitions()
        assert len(specs) == 1, "With 1 bucket both tickers should share a spec"
        table = wrapper.get_data(specs[0])
        assert table is not None
        # Should have rows from both tickers
        tickers_in_result = set(table.column("ticker").to_pylist())
        assert "AAPL" in tickers_in_result
        assert "MSFT" in tickers_in_result
        # Should be sorted by ticker, then date
        ticker_col = table.column("ticker").to_pylist()
        date_col = table.column("date").to_pylist()
        for i in range(len(ticker_col) - 1):
            if ticker_col[i] == ticker_col[i + 1]:
                assert date_col[i] <= date_col[i + 1]
            else:
                assert ticker_col[i] <= ticker_col[i + 1]

    def test_get_data_returns_none_when_all_inner_none(self) -> None:
        from unittest.mock import MagicMock, patch

        wrapper, inner = self._make_wrapper(num_buckets=1)
        specs = wrapper.get_partitions()

        # Patch get_data on the inner source to always return None
        with patch.object(inner, "get_data", return_value=None):
            result = wrapper.get_data(specs[0])
        assert result is None

    def test_get_data_raises_when_instrument_col_missing(self) -> None:
        wrapper, inner = self._make_wrapper(num_buckets=1)
        specs = wrapper.get_partitions()

        # Patch get_data to return table WITHOUT the ticker column
        def _bad_data(spec: PartitionSpec) -> pa.Table:
            return pa.table({"date": [dt.date(2022, 1, 1)], "close": [99.0]})

        import unittest.mock as mock
        with mock.patch.object(inner, "get_data", side_effect=_bad_data):
            with pytest.raises(ValidationError, match="not found in data"):
                wrapper.get_data(specs[0])


# ---------------------------------------------------------------------------
# 4. Full integration test
# ---------------------------------------------------------------------------


class TestFullIntegration:
    def _build_dataset(self, temp_cache: Path, num_buckets: int = 4) -> DatedParquetDataset:
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="eod_bucketed",
            date_col="date",
            date_partition="year",
            partition_columns=[],
            instrument_column="ticker",
            num_instrument_buckets=num_buckets,
        )
        source = TickerYearDataSource(dpd)
        suffix = dpd.update(
            source,
            start_date=dt.date(2022, 1, 1),
            end_date=dt.date(2023, 12, 31),
        )
        assert suffix is not None
        dpd.refresh()
        return dpd

    def test_bucket_col_in_partition_columns_after_build(self, temp_cache: Path) -> None:
        dpd = self._build_dataset(temp_cache)
        assert INSTRUMENT_BUCKET_COL in dpd.partition_columns

    def test_read_data_instruments_single(self, temp_cache: Path) -> None:
        dpd = self._build_dataset(temp_cache)
        df = dpd.read_data(instruments="AAPL")
        assert len(df) > 0
        assert set(df["ticker"].unique()) == {"AAPL"}

    def test_read_data_instruments_multiple(self, temp_cache: Path) -> None:
        dpd = self._build_dataset(temp_cache)
        df = dpd.read_data(instruments=["AAPL", "MSFT"])
        assert len(df) > 0
        assert set(df["ticker"].unique()) == {"AAPL", "MSFT"}

    def test_read_data_returns_all_tickers_without_filter(self, temp_cache: Path) -> None:
        dpd = self._build_dataset(temp_cache)
        df = dpd.read_data()
        assert set(df["ticker"].unique()) == set(TICKERS)

    def test_data_sorted_instrument_then_date(self, temp_cache: Path) -> None:
        """Data within each year partition file should be sorted ticker→date.

        The sort order guarantee is per-partition-file (bucket×year).  With
        year partitioning, data from 2022 and 2023 live in separate files;
        we verify that within each year the rows are ticker→date sorted.
        """
        dpd = self._build_dataset(temp_cache, num_buckets=1)
        df = dpd.read_data(instruments=["AAPL", "MSFT"])
        df["date"] = pd.to_datetime(df["date"])
        df["year"] = df["date"].dt.year

        for year, group in df.groupby("year"):
            group = group.reset_index(drop=True)
            group_sorted = group.sort_values(["ticker", "date"]).reset_index(drop=True)
            pd.testing.assert_frame_equal(
                group[["ticker", "date"]],
                group_sorted[["ticker", "date"]],
                check_like=False,
            )

    def test_metadata_has_num_instrument_buckets(self, temp_cache: Path) -> None:
        dpd = self._build_dataset(temp_cache)
        meta = dpd._load_metadata()
        assert meta.yaml_config.get("num_instrument_buckets") == 4

    def test_read_data_pl_instruments(self, temp_cache: Path) -> None:
        dpd = self._build_dataset(temp_cache)
        df = dpd.read_data_pl(instruments="GOOGL")
        assert len(df) > 0
        assert set(df["ticker"].to_list()) == {"GOOGL"}


# ---------------------------------------------------------------------------
# 5. Breaking change: num_instrument_buckets mismatch
# ---------------------------------------------------------------------------


class TestBreakingChangeBuckets:
    def test_reopen_with_different_bucket_count_raises(self, temp_cache: Path) -> None:
        # Build with 4 buckets
        dpd4 = DatedParquetDataset(
            cache_dir=temp_cache,
            name="bucket_change_test",
            date_col="date",
            date_partition="year",
            partition_columns=[],
            instrument_column="ticker",
            num_instrument_buckets=4,
        )
        source = TickerYearDataSource(dpd4, years=[2022])
        dpd4.update(
            source,
            start_date=dt.date(2022, 1, 1),
            end_date=dt.date(2022, 12, 31),
        )

        # Re-open with 8 buckets — should raise
        dpd8 = DatedParquetDataset(
            cache_dir=temp_cache,
            name="bucket_change_test",
            date_col="date",
            date_partition="year",
            partition_columns=[],
            instrument_column="ticker",
            num_instrument_buckets=8,
        )
        with pytest.raises(ValidationError, match="num_instrument_buckets mismatch"):
            dpd8._load_metadata()


# ---------------------------------------------------------------------------
# 6. read_data(instruments=...) without bucketing → ValueError
# ---------------------------------------------------------------------------


class TestPartialInstrumentUpdateGuard:
    """update(..., instruments=...) must be blocked on bucketed datasets."""

    def test_bucketed_update_with_instruments_raises(self, temp_cache: Path) -> None:
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="eod_bucketed",
            date_col="date",
            date_partition="year",
            partition_columns=[],
            instrument_column="ticker",
            num_instrument_buckets=4,
        )
        source = TickerYearDataSource(dpd)
        with pytest.raises(ValidationError, match="Partial-instrument updates are not yet supported"):
            dpd.update(source, instruments=["AAPL"])

    def test_non_bucketed_update_with_instruments_allowed(self, temp_cache: Path) -> None:
        """Passing instruments= to a non-bucketed dataset must not raise ValidationError."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="plain",
            date_col="date",
            date_partition="year",
            partition_columns=["ticker"],
            instrument_column="ticker",
        )
        source = TickerYearDataSource(dpd)
        # Should not raise ValidationError — may raise other errors downstream
        # (e.g. no data) but the guard must not trigger
        try:
            dpd.update(source, instruments=["AAPL"])
        except ValidationError as e:
            assert "Partial-instrument" not in str(e), (
                "Non-bucketed update raised the bucketed guard unexpectedly"
            )


class TestPartialInstrumentUpdateGuard:
    """update(..., instruments=...) must be blocked on bucketed datasets."""

    def test_bucketed_update_with_instruments_raises(self, temp_cache: Path) -> None:
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="eod_bucketed",
            date_col="date",
            date_partition="year",
            partition_columns=[],
            instrument_column="ticker",
            num_instrument_buckets=4,
        )
        source = TickerYearDataSource(dpd)
        with pytest.raises(ValidationError, match="Partial-instrument updates are not yet supported"):
            dpd.update(source, instruments=["AAPL"])

    def test_non_bucketed_update_with_instruments_does_not_trigger_guard(
        self, temp_cache: Path
    ) -> None:
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="plain",
            date_col="date",
            date_partition="year",
            partition_columns=["ticker"],
            instrument_column="ticker",
        )
        source = TickerYearDataSource(dpd)
        try:
            dpd.update(source, instruments=["AAPL"])
        except ValidationError as e:
            assert "Partial-instrument" not in str(e), (
                "Non-bucketed update raised the bucketed guard unexpectedly"
            )


class TestReadDataInstrumentsWithoutBucketing:
    def test_raises_value_error_when_bucketing_not_configured(
        self, temp_cache: Path
    ) -> None:
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="plain",
            date_col="date",
            date_partition="year",
        )
        with pytest.raises(ValueError, match="requires instrument_column"):
            dpd.read_data(instruments="AAPL")

    def test_read_data_pl_raises_value_error_without_bucketing(
        self, temp_cache: Path
    ) -> None:
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="plain_pl",
            date_col="date",
            date_partition="year",
        )
        with pytest.raises(ValueError, match="requires instrument_column"):
            dpd.read_data_pl(instruments=["AAPL", "MSFT"])
