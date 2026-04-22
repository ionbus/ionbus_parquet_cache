"""
Tests for the instrument hash bucketing feature in DatedParquetDataset.

Covers:
1. Validation errors at construction time
2. Bucket hash assignment correctness
3. BucketedDataSource unit tests
4. Full integration: build, read, filter
5. Breaking-change detection (num_instrument_buckets mismatch)
6. read_data(instruments=...) on a non-bucketed dataset raises ValueError
"""

from __future__ import annotations

import datetime as dt
import zlib
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

from ionbus_parquet_cache.bucketing import INSTRUMENT_BUCKET_COL
from ionbus_parquet_cache.data_source import BucketedDataSource, DataSource
from ionbus_parquet_cache.dated_dataset import DatedParquetDataset
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
            "close": pa.array(
                [100.0 + hash(ticker + str(year)) % 200] * len(dates)
            ),
        }
    )


class MockBucketedDataSource(BucketedDataSource):
    """BucketedDataSource returning synthetic ticker data."""

    def __init__(self, dataset, tickers=TICKERS, years=YEARS):
        super().__init__(dataset)
        self._tickers = list(tickers)
        self._years = list(years)

    def available_dates(self) -> tuple[dt.date, dt.date]:
        return (
            dt.date(min(self._years), 1, 1),
            dt.date(max(self._years), 12, 31),
        )

    def get_instruments_for_time_period(
        self, start_date: dt.date, end_date: dt.date
    ) -> list:
        return self._tickers

    def get_data_for_bucket(
        self, instruments: list, start_date: dt.date, end_date: dt.date
    ) -> pa.Table | None:
        tables = []
        for ticker in instruments:
            year = start_date.year
            if year in self._years:
                t = _make_ticker_data(ticker, year)
                date_col = t.column("date")
                import pyarrow.compute as pc
                mask = pc.and_(
                    pc.greater_equal(
                        date_col, pa.scalar(start_date, type=pa.date32())
                    ),
                    pc.less_equal(
                        date_col, pa.scalar(end_date, type=pa.date32())
                    ),
                )
                t = t.filter(mask)
                if t.num_rows > 0:
                    tables.append(t)
        if not tables:
            return None
        return pa.concat_tables(tables)


# Non-bucketed source kept for tests that need it
class TickerYearDataSource(DataSource):
    def __init__(self, dataset, tickers=TICKERS, years=YEARS):
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
        partition_columns=[],
        instrument_column="ticker",
        num_instrument_buckets=4,
    )


# ---------------------------------------------------------------------------
# 1. Validation errors
# ---------------------------------------------------------------------------


class TestValidationErrors:
    def test_buckets_only_requires_instrument_column(
        self, temp_cache: Path
    ) -> None:
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="plain_instrument",
            date_col="date",
            date_partition="year",
            partition_columns=["ticker"],
            instrument_column="ticker",
        )
        assert dpd.instrument_column == "ticker"
        assert INSTRUMENT_BUCKET_COL not in dpd.partition_columns

    def test_buckets_without_instrument_column_raises(
        self, temp_cache: Path
    ) -> None:
        with pytest.raises(ValidationError, match="must both be set or both be None"):
            DatedParquetDataset(
                cache_dir=temp_cache,
                name="bad",
                date_col="date",
                date_partition="year",
                num_instrument_buckets=4,
            )

    def test_instrument_column_in_partition_columns_raises(
        self, temp_cache: Path
    ) -> None:
        with pytest.raises(ValidationError, match="must not be in partition_columns"):
            DatedParquetDataset(
                cache_dir=temp_cache,
                name="bad",
                date_col="date",
                date_partition="year",
                partition_columns=["ticker"],
                instrument_column="ticker",
                num_instrument_buckets=4,
            )

    def test_bucket_col_in_partition_columns_raises(
        self, temp_cache: Path
    ) -> None:
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
        with pytest.raises(
            ValidationError,
            match="is reserved and must not appear in sort_columns",
        ):
            DatedParquetDataset(
                cache_dir=temp_cache,
                name="bad",
                date_col="date",
                date_partition="year",
                sort_columns=[INSTRUMENT_BUCKET_COL, "date"],
                instrument_column="ticker",
                num_instrument_buckets=4,
            )

    def test_non_bucketed_source_on_bucketed_dpd_raises(
        self, temp_cache: Path
    ) -> None:
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="bucketed",
            date_col="date",
            date_partition="year",
            partition_columns=[],
            instrument_column="ticker",
            num_instrument_buckets=4,
        )
        source = TickerYearDataSource(dpd)
        with pytest.raises(ValidationError, match="not a BucketedDataSource"):
            dpd.update(source)


# ---------------------------------------------------------------------------
# 2. Bucket assignment
# ---------------------------------------------------------------------------


class TestBucketAssignment:
    def test_same_instrument_same_bucket(self) -> None:
        n = 16
        b1 = f"{zlib.crc32('AAPL'.encode()) % n:04d}"
        b2 = f"{zlib.crc32('AAPL'.encode()) % n:04d}"
        assert b1 == b2

    def test_bucket_range_respected(self) -> None:
        n = 4
        for ticker in TICKERS:
            b = zlib.crc32(ticker.encode()) % n
            assert 0 <= b < n


# ---------------------------------------------------------------------------
# 3. BucketedDataSource unit tests
# ---------------------------------------------------------------------------


class TestBucketedDataSource:
    def _make_dpd_and_source(
        self, temp_cache: Path, num_buckets: int = 4
    ) -> tuple[DatedParquetDataset, MockBucketedDataSource]:
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="bucketed_test",
            date_col="date",
            date_partition="year",
            partition_columns=[],
            instrument_column="ticker",
            num_instrument_buckets=num_buckets,
        )
        source = MockBucketedDataSource(dpd, tickers=["AAPL", "MSFT"], years=[2022])
        source._do_prepare(dt.date(2022, 1, 1), dt.date(2022, 12, 31))
        return dpd, source

    def test_get_partitions_count(self, temp_cache: Path) -> None:
        _, source = self._make_dpd_and_source(temp_cache, num_buckets=4)
        specs = source.get_partitions()
        # 4 buckets × 1 year = 4 specs
        assert len(specs) == 4

    def test_get_partitions_has_bucket_col(self, temp_cache: Path) -> None:
        _, source = self._make_dpd_and_source(temp_cache, num_buckets=4)
        for spec in source.get_partitions():
            assert INSTRUMENT_BUCKET_COL in spec.partition_values

    def test_get_data_returns_none_for_empty_bucket(
        self, temp_cache: Path
    ) -> None:
        _, source = self._make_dpd_and_source(temp_cache, num_buckets=4)
        specs = source.get_partitions()
        # With 4 buckets and 2 tickers, at least some buckets will be empty
        results = [source.get_data(s) for s in specs]
        assert any(r is None for r in results)

    def test_get_data_instruments_cached_per_period(
        self, temp_cache: Path
    ) -> None:
        _, source = self._make_dpd_and_source(temp_cache, num_buckets=1)
        specs = source.get_partitions()
        assert len(specs) == 1
        source.get_data(specs[0])
        # Cache should now have one entry
        assert len(source._period_bucket_cache) == 1

    def test_get_data_all_tickers_present_with_one_bucket(
        self, temp_cache: Path
    ) -> None:
        _, source = self._make_dpd_and_source(temp_cache, num_buckets=1)
        specs = source.get_partitions()
        table = source.get_data(specs[0])
        assert table is not None
        tickers = set(table.column("ticker").to_pylist())
        assert "AAPL" in tickers
        assert "MSFT" in tickers

    def test_prepare_clears_cache(self, temp_cache: Path) -> None:
        _, source = self._make_dpd_and_source(temp_cache, num_buckets=1)
        specs = source.get_partitions()
        source.get_data(specs[0])
        assert len(source._period_bucket_cache) == 1
        source._do_prepare(dt.date(2022, 1, 1), dt.date(2022, 12, 31))
        assert len(source._period_bucket_cache) == 0


# ---------------------------------------------------------------------------
# 4. Full integration test
# ---------------------------------------------------------------------------


class TestFullIntegration:
    def _build_dataset(
        self, temp_cache: Path, num_buckets: int = 4
    ) -> DatedParquetDataset:
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="eod_bucketed",
            date_col="date",
            date_partition="year",
            partition_columns=[],
            instrument_column="ticker",
            num_instrument_buckets=num_buckets,
        )
        source = MockBucketedDataSource(dpd)
        suffix = dpd.update(
            source,
            start_date=dt.date(2022, 1, 1),
            end_date=dt.date(2023, 12, 31),
        )
        assert suffix is not None
        dpd.refresh()
        return dpd

    def test_bucket_col_in_partition_columns_after_build(
        self, temp_cache: Path
    ) -> None:
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

    def test_read_data_returns_all_tickers_without_filter(
        self, temp_cache: Path
    ) -> None:
        dpd = self._build_dataset(temp_cache)
        df = dpd.read_data()
        assert set(df["ticker"].unique()) == set(TICKERS)

    def test_data_sorted_instrument_then_date(self, temp_cache: Path) -> None:
        dpd = self._build_dataset(temp_cache, num_buckets=1)
        df = dpd.read_data(instruments=["AAPL", "MSFT"])
        df["date"] = pd.to_datetime(df["date"])
        df["year"] = df["date"].dt.year

        for _year, group in df.groupby("year"):
            group = group.reset_index(drop=True)
            group_sorted = group.sort_values(["ticker", "date"]).reset_index(
                drop=True
            )
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
    def test_reopen_unbucketed_cache_with_bucketing_raises(
        self, temp_cache: Path
    ) -> None:
        """Opening a non-bucketed cache with num_instrument_buckets set must fail."""
        from ionbus_parquet_cache.data_source import DataSource
        from ionbus_parquet_cache.partition import PartitionSpec

        class SimpleDateSource(DataSource):
            def available_dates(self):
                return dt.date(2022, 1, 1), dt.date(2022, 12, 31)

            def get_partitions(self):
                return [PartitionSpec(
                    partition_values={"year": "Y2022"},
                    start_date=dt.date(2022, 1, 1),
                    end_date=dt.date(2022, 12, 31),
                )]

            def get_data(self, spec):
                return pa.table({"date": pa.array(
                    [dt.date(2022, 1, 3)], type=pa.date32()
                ), "ticker": ["AAPL"], "close": [150.0]})

        dpd_plain = DatedParquetDataset(
            cache_dir=temp_cache,
            name="unbucketed_then_bucketed",
            date_col="date",
            date_partition="year",
            partition_columns=["ticker"],
        )
        source = SimpleDateSource(dpd_plain)
        dpd_plain.update(
            source,
            start_date=dt.date(2022, 1, 1),
            end_date=dt.date(2022, 12, 31),
        )

        dpd_bucketed = DatedParquetDataset(
            cache_dir=temp_cache,
            name="unbucketed_then_bucketed",
            date_col="date",
            date_partition="year",
            partition_columns=[],
            instrument_column="ticker",
            num_instrument_buckets=4,
        )
        with pytest.raises(ValidationError, match="num_instrument_buckets mismatch"):
            dpd_bucketed._load_metadata()

    def test_reopen_with_different_bucket_count_raises(
        self, temp_cache: Path
    ) -> None:
        dpd4 = DatedParquetDataset(
            cache_dir=temp_cache,
            name="bucket_change_test",
            date_col="date",
            date_partition="year",
            partition_columns=[],
            instrument_column="ticker",
            num_instrument_buckets=4,
        )
        source = MockBucketedDataSource(dpd4, years=[2022])
        dpd4.update(
            source,
            start_date=dt.date(2022, 1, 1),
            end_date=dt.date(2022, 12, 31),
        )

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
# 6. Partial instrument update guard
# ---------------------------------------------------------------------------


class TestPartialInstrumentUpdateGuard:
    def test_bucketed_update_with_instruments_raises(
        self, temp_cache: Path
    ) -> None:
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="eod_bucketed",
            date_col="date",
            date_partition="year",
            partition_columns=[],
            instrument_column="ticker",
            num_instrument_buckets=4,
        )
        source = MockBucketedDataSource(dpd)
        with pytest.raises(
            ValidationError, match="Partial-instrument updates are not yet supported"
        ):
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
            assert "Partial-instrument" not in str(e)


# ---------------------------------------------------------------------------
# 7. read_data(instruments=...) without bucketing → ValueError
# ---------------------------------------------------------------------------


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
