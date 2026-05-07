"""Tests for partition.py - partition specification and utilities."""

from __future__ import annotations

import datetime as dt

import pytest

from ionbus_parquet_cache.partition import (
    PartitionSpec,
    DATE_PARTITION_GRANULARITIES,
    date_partition_column_name,
    date_partition_value,
    date_partition_range,
    date_partitions,
)


class TestPartitionSpec:
    """Tests for PartitionSpec dataclass."""

    def test_basic_creation(self) -> None:
        """Create a basic partition spec."""
        spec = PartitionSpec(
            partition_values={"FutureRoot": "ES", "year": "Y2024"},
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )
        assert spec.partition_values == {"FutureRoot": "ES", "year": "Y2024"}
        assert spec.start_date == dt.date(2024, 1, 1)
        assert spec.end_date == dt.date(2024, 1, 31)
        assert spec.temp_file_path is None

    def test_invalid_dates_raises(self) -> None:
        """start_date > end_date should raise ValueError."""
        with pytest.raises(ValueError, match="must be <="):
            PartitionSpec(
                partition_values={},
                start_date=dt.date(2024, 2, 1),
                end_date=dt.date(2024, 1, 1),
            )

    def test_partition_key(self) -> None:
        """partition_key should be hashable and consistent."""
        spec1 = PartitionSpec(
            partition_values={"FutureRoot": "ES", "year": "Y2024"},
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 15),
        )
        spec2 = PartitionSpec(
            partition_values={
                "year": "Y2024",
                "FutureRoot": "ES",
            },  # Different order
            start_date=dt.date(2024, 1, 16),
            end_date=dt.date(2024, 1, 31),
        )
        # Same partition values should have same key
        assert spec1.partition_key == spec2.partition_key

    def test_temp_file_path_excluded_from_compare(self) -> None:
        """temp_file_path should not affect equality."""
        spec1 = PartitionSpec(
            partition_values={"year": "Y2024"},
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
            temp_file_path="/tmp/file1.parquet",
        )
        spec2 = PartitionSpec(
            partition_values={"year": "Y2024"},
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
            temp_file_path="/tmp/file2.parquet",
        )
        assert spec1 == spec2

    def test_chunk_info_excluded_from_compare(self) -> None:
        """chunk_info should not affect equality comparison."""
        spec1 = PartitionSpec(
            partition_values={"year": "Y2024"},
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
            chunk_info={"tickers": ["ESH4", "ESH5"]},
        )
        spec2 = PartitionSpec(
            partition_values={"year": "Y2024"},
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
            chunk_info={"tickers": ["ESM4", "ESM5"]},
        )
        assert spec1 == spec2

    def test_chunk_info_affects_hash(self) -> None:
        """chunk_info should affect hash for distinct work items."""
        spec1 = PartitionSpec(
            partition_values={"year": "Y2024"},
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
            chunk_info={"tickers": ["ESH4"]},
        )
        spec2 = PartitionSpec(
            partition_values={"year": "Y2024"},
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
            chunk_info={"tickers": ["ESM4"]},
        )
        # Hash should be different
        assert hash(spec1) != hash(spec2)


class TestDatePartitionColumnName:
    """Tests for date_partition_column_name()."""

    def test_day_returns_date(self) -> None:
        """Day partition uses Date column by default."""
        assert date_partition_column_name("day") == "Date"

    def test_day_with_custom_date_col(self) -> None:
        """Day partition should use custom date_col when provided."""
        assert (
            date_partition_column_name("day", "PricingDate") == "PricingDate"
        )
        assert date_partition_column_name("day", "TradeDate") == "TradeDate"

    def test_other_partitions(self) -> None:
        """Other partitions use their own column names."""
        assert date_partition_column_name("week") == "week"
        assert date_partition_column_name("month") == "month"
        assert date_partition_column_name("quarter") == "quarter"
        assert date_partition_column_name("year") == "year"

    def test_other_partitions_ignore_date_col(self) -> None:
        """Non-day partitions should ignore date_col parameter."""
        assert date_partition_column_name("year", "PricingDate") == "year"
        assert date_partition_column_name("month", "TradeDate") == "month"

    def test_invalid_partition_raises(self) -> None:
        """Invalid partition should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid date_partition"):
            date_partition_column_name("invalid")


class TestDatePartitionValue:
    """Tests for date_partition_value()."""

    def test_day_partition(self) -> None:
        """Day partition returns ISO date."""
        assert (
            date_partition_value(dt.date(2024, 1, 15), "day") == "2024-01-15"
        )

    def test_week_partition(self) -> None:
        """Week partition returns ISO week."""
        # 2024-01-15 is in week 3
        assert (
            date_partition_value(dt.date(2024, 1, 15), "week") == "W2024-03"
        )

    def test_month_partition(self) -> None:
        """Month partition returns month identifier."""
        assert (
            date_partition_value(dt.date(2024, 1, 15), "month") == "M2024-01"
        )
        assert (
            date_partition_value(dt.date(2024, 12, 25), "month") == "M2024-12"
        )

    def test_quarter_partition(self) -> None:
        """Quarter partition returns quarter identifier."""
        assert (
            date_partition_value(dt.date(2024, 1, 15), "quarter") == "Q2024-1"
        )
        assert (
            date_partition_value(dt.date(2024, 4, 15), "quarter") == "Q2024-2"
        )
        assert (
            date_partition_value(dt.date(2024, 7, 15), "quarter") == "Q2024-3"
        )
        assert (
            date_partition_value(dt.date(2024, 10, 15), "quarter")
            == "Q2024-4"
        )

    def test_year_partition(self) -> None:
        """Year partition returns year identifier."""
        assert date_partition_value(dt.date(2024, 1, 15), "year") == "Y2024"
        assert date_partition_value(dt.date(2023, 12, 31), "year") == "Y2023"


class TestDatePartitionRange:
    """Tests for date_partition_range()."""

    def test_day_range(self) -> None:
        """Day partition covers single day."""
        start, end = date_partition_range("2024-01-15", "day")
        assert start == dt.date(2024, 1, 15)
        assert end == dt.date(2024, 1, 15)

    def test_month_range(self) -> None:
        """Month partition covers full month."""
        start, end = date_partition_range("M2024-01", "month")
        assert start == dt.date(2024, 1, 1)
        assert end == dt.date(2024, 1, 31)

        # February in leap year
        start, end = date_partition_range("M2024-02", "month")
        assert start == dt.date(2024, 2, 1)
        assert end == dt.date(2024, 2, 29)

    def test_quarter_range(self) -> None:
        """Quarter partition covers 3 months."""
        start, end = date_partition_range("Q2024-1", "quarter")
        assert start == dt.date(2024, 1, 1)
        assert end == dt.date(2024, 3, 31)

        start, end = date_partition_range("Q2024-4", "quarter")
        assert start == dt.date(2024, 10, 1)
        assert end == dt.date(2024, 12, 31)

    def test_year_range(self) -> None:
        """Year partition covers full year."""
        start, end = date_partition_range("Y2024", "year")
        assert start == dt.date(2024, 1, 1)
        assert end == dt.date(2024, 12, 31)


class TestDatePartitions:
    """Tests for date_partitions()."""

    def test_empty_range(self) -> None:
        """Empty range returns empty list."""
        result = date_partitions(
            dt.date(2024, 2, 1), dt.date(2024, 1, 1), "month"
        )
        assert result == []

    def test_single_day(self) -> None:
        """Single day range."""
        result = date_partitions(
            dt.date(2024, 1, 15), dt.date(2024, 1, 15), "day"
        )
        assert len(result) == 1
        assert result[0] == (
            dt.date(2024, 1, 15),
            dt.date(2024, 1, 15),
            "2024-01-15",
        )

    def test_multiple_months(self) -> None:
        """Range spanning multiple months."""
        result = date_partitions(
            dt.date(2024, 1, 15), dt.date(2024, 3, 15), "month"
        )
        assert len(result) == 3

        # First month: partial (15th to end)
        assert result[0][0] == dt.date(2024, 1, 15)
        assert result[0][1] == dt.date(2024, 1, 31)
        assert result[0][2] == "M2024-01"

        # Second month: full
        assert result[1][0] == dt.date(2024, 2, 1)
        assert result[1][1] == dt.date(2024, 2, 29)  # Leap year
        assert result[1][2] == "M2024-02"

        # Third month: partial (start to 15th)
        assert result[2][0] == dt.date(2024, 3, 1)
        assert result[2][1] == dt.date(2024, 3, 15)
        assert result[2][2] == "M2024-03"

    def test_year_partitions(self) -> None:
        """Range spanning multiple years."""
        result = date_partitions(
            dt.date(2023, 6, 1), dt.date(2024, 6, 30), "year"
        )
        assert len(result) == 2

        # First year: partial
        assert result[0][0] == dt.date(2023, 6, 1)
        assert result[0][1] == dt.date(2023, 12, 31)
        assert result[0][2] == "Y2023"

        # Second year: partial
        assert result[1][0] == dt.date(2024, 1, 1)
        assert result[1][1] == dt.date(2024, 6, 30)
        assert result[1][2] == "Y2024"

    def test_with_max_business_days(self) -> None:
        """Range with max_business_days splits partitions."""
        # A month with ~22 business days, split into chunks of 5 business days (~7 calendar days)
        result = date_partitions(
            dt.date(2024, 1, 1),
            dt.date(2024, 1, 31),
            "month",
            max_business_days=5,
        )
        # Should have multiple chunks
        assert len(result) > 1
        # All should be in same partition
        assert all(r[2] == "M2024-01" for r in result)
        # Should cover full range
        assert result[0][0] == dt.date(2024, 1, 1)
        assert result[-1][1] == dt.date(2024, 1, 31)


class TestDatePartitionsConstant:
    """Tests for DATE_PARTITIONS constant."""

    def test_valid_partitions(self) -> None:
        """All expected partition types should be present."""
        assert "day" in DATE_PARTITION_GRANULARITIES
        assert "week" in DATE_PARTITION_GRANULARITIES
        assert "month" in DATE_PARTITION_GRANULARITIES
        assert "quarter" in DATE_PARTITION_GRANULARITIES
        assert "year" in DATE_PARTITION_GRANULARITIES
        assert len(DATE_PARTITION_GRANULARITIES) == 5
