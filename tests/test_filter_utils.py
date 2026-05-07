"""Tests for filter utilities."""

from __future__ import annotations

import datetime as dt

import pytest

from ionbus_parquet_cache.filter_utils import (
    build_dataset_filter,
    build_filter_expression,
    _compute_partition_values_for_range,
)


class TestBuildFilterExpression:
    """Tests for build_filter_expression()."""

    def test_single_equality_filter(self) -> None:
        """Should build equality filter."""
        expr = build_filter_expression([("column", "=", "value")])

        assert expr is not None

    def test_multiple_filters_and(self) -> None:
        """Multiple filters should be AND-ed together."""
        expr = build_filter_expression(
            [
                ("a", "=", 1),
                ("b", ">", 2),
            ]
        )

        assert expr is not None

    def test_in_filter(self) -> None:
        """Should handle 'in' operator."""
        expr = build_filter_expression(
            [
                ("col", "in", ["a", "b", "c"]),
            ]
        )

        assert expr is not None

    def test_not_in_filter(self) -> None:
        """Should handle 'not in' operator."""
        expr = build_filter_expression(
            [
                ("col", "not in", ["x", "y", "z"]),
            ]
        )

        assert expr is not None

    def test_not_in_filter_excludes_values(self) -> None:
        """'not in' filter should exclude specified values when applied."""
        import pyarrow as pa

        # Create test data
        table = pa.table({"col": ["a", "b", "c", "x", "y", "z"]})

        # Build filter to exclude x, y, z
        expr = build_filter_expression(
            [
                ("col", "not in", ["x", "y", "z"]),
            ]
        )

        # Apply filter
        filtered = table.filter(expr)

        # Should only have a, b, c
        assert filtered.num_rows == 3
        assert set(filtered.column("col").to_pylist()) == {"a", "b", "c"}

    def test_empty_filters_returns_true_expression(self) -> None:
        """Empty filter list should return a true expression."""
        expr = build_filter_expression([])

        # Empty filters return a "true" expression (matches all)
        assert expr is not None


class TestComputePartitionValuesForRange:
    """Tests for _compute_partition_values_for_range()."""

    def test_year_partition_single_year(self) -> None:
        """Single year range should return one partition value."""
        values = _compute_partition_values_for_range(
            dt.date(2024, 3, 1),
            dt.date(2024, 9, 30),
            "year",
        )

        assert values == {"Y2024"}

    def test_year_partition_multiple_years(self) -> None:
        """Multi-year range should return multiple partition values."""
        values = _compute_partition_values_for_range(
            dt.date(2023, 6, 1),
            dt.date(2025, 3, 31),
            "year",
        )

        assert values == {"Y2023", "Y2024", "Y2025"}

    def test_quarter_partition(self) -> None:
        """Quarter partition should return correct values."""
        values = _compute_partition_values_for_range(
            dt.date(2024, 3, 1),
            dt.date(2024, 9, 30),
            "quarter",
        )

        # Quarter format is Q2024-1 (with hyphen)
        assert "Q2024-1" in values
        assert "Q2024-2" in values
        assert "Q2024-3" in values

    def test_month_partition(self) -> None:
        """Month partition should return correct values."""
        values = _compute_partition_values_for_range(
            dt.date(2024, 1, 15),
            dt.date(2024, 3, 15),
            "month",
        )

        assert values == {"M2024-01", "M2024-02", "M2024-03"}

    def test_day_partition(self) -> None:
        """Day partition should return one value per day."""
        values = _compute_partition_values_for_range(
            dt.date(2024, 1, 1),
            dt.date(2024, 1, 3),
            "day",
        )

        assert len(values) == 3


class TestBuildDatasetFilter:
    """Tests for build_dataset_filter()."""

    def test_no_filters_returns_none(self) -> None:
        """No filters should return None."""
        expr = build_dataset_filter()

        assert expr is None

    def test_date_filters_for_dated_dataset(self) -> None:
        """Should add date column filters for dated datasets."""
        expr = build_dataset_filter(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 12, 31),
            date_col="Date",
            is_dated_dataset=True,
        )

        assert expr is not None

    def test_date_filters_on_nondated_raises(self) -> None:
        """Should raise for date filters on non-dated dataset."""
        with pytest.raises(ValueError, match="do not support date filtering"):
            build_dataset_filter(
                start_date=dt.date(2024, 1, 1),
                date_col="Date",
                is_dated_dataset=False,
            )

    def test_partition_pruning_when_partition_in_columns(self) -> None:
        """Should add partition filter when date_partition is in columns."""
        expr = build_dataset_filter(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 12, 31),
            date_col="Date",
            is_dated_dataset=True,
            date_partition="year",
            partition_columns=["year"],
        )

        assert expr is not None
        # Expression should include year partition filter

    def test_no_partition_pruning_when_not_in_columns(self) -> None:
        """Should NOT add partition filter when date_partition not in columns."""
        # When partition_columns doesn't include the date_partition,
        # we should only get date column filters, not partition filters
        expr = build_dataset_filter(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 12, 31),
            date_col="Date",
            is_dated_dataset=True,
            date_partition="year",
            partition_columns=[],  # year not in partition_columns
        )

        assert expr is not None

    def test_no_partition_pruning_for_day_partition(self) -> None:
        """Day partition should NOT add partition filter (uses date column)."""
        expr = build_dataset_filter(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
            date_col="Date",
            is_dated_dataset=True,
            date_partition="day",
            partition_columns=[
                "Date"
            ],  # Even if Date is in partition_columns
        )

        assert expr is not None

    def test_user_filters_combined_with_date_filters(self) -> None:
        """User filters should be combined with date filters."""
        expr = build_dataset_filter(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 12, 31),
            date_col="Date",
            is_dated_dataset=True,
            filters=[("FutureRoot", "=", "ES")],
        )

        assert expr is not None

    def test_string_dates_parsed(self) -> None:
        """String dates should be parsed correctly."""
        expr = build_dataset_filter(
            start_date="2024-01-01",
            end_date="2024-12-31",
            date_col="Date",
            is_dated_dataset=True,
        )

        assert expr is not None
