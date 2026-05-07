"""Tests for DataCleaner abstract class."""

from __future__ import annotations

from unittest.mock import MagicMock

import duckdb
import pytest

from ionbus_parquet_cache.data_cleaner import DataCleaner, NoOpCleaner


class TestDataCleanerInit:
    """Tests for DataCleaner initialization."""

    def test_init_sets_dataset(self) -> None:
        """Init should set dataset reference."""
        mock_dataset = MagicMock()

        class TestCleaner(DataCleaner):
            def __call__(self, rel):
                return rel

        cleaner = TestCleaner(mock_dataset)
        assert cleaner.dataset is mock_dataset

    def test_init_sets_kwargs(self) -> None:
        """Init should set kwargs as attributes."""
        mock_dataset = MagicMock()

        class TestCleaner(DataCleaner):
            def __call__(self, rel):
                return rel

        cleaner = TestCleaner(mock_dataset, min_price=1.0, add_returns=True)
        assert cleaner.min_price == 1.0
        assert cleaner.add_returns is True


class TestNoOpCleaner:
    """Tests for NoOpCleaner."""

    def test_passes_through_unchanged(self) -> None:
        """NoOpCleaner should return relation unchanged."""
        mock_dataset = MagicMock()
        mock_relation = MagicMock()

        cleaner = NoOpCleaner(mock_dataset)
        result = cleaner(mock_relation)

        assert result is mock_relation


class TestDataCleanerInterface:
    """Tests for DataCleaner interface requirements."""

    def test_must_implement_call(self) -> None:
        """Subclass must implement __call__."""

        class IncompleteCleaner(DataCleaner):
            pass

        mock_dataset = MagicMock()

        with pytest.raises(TypeError, match="abstract method"):
            IncompleteCleaner(mock_dataset)

    def test_call_receives_relation(self) -> None:
        """__call__ should receive the DuckDB relation."""
        mock_dataset = MagicMock()
        received_rel = None

        class CapturingCleaner(DataCleaner):
            def __call__(self, rel):
                nonlocal received_rel
                received_rel = rel
                return rel

        mock_relation = MagicMock()
        cleaner = CapturingCleaner(mock_dataset)
        cleaner(mock_relation)

        assert received_rel is mock_relation


class TestDataCleanerWithDuckDB:
    """Tests for DataCleaner with actual DuckDB relations."""

    def test_filter_cleaner(self) -> None:
        """Test a cleaner that filters rows."""
        mock_dataset = MagicMock()

        class FilterCleaner(DataCleaner):
            def __init__(self, dataset, min_value: float = 0.0):
                super().__init__(dataset)
                self.min_value = min_value

            def __call__(self, rel):
                return rel.filter(f"value >= {self.min_value}")

        # Create test relation
        rel = duckdb.sql(
            "SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(value)"
        )

        cleaner = FilterCleaner(mock_dataset, min_value=3)
        result = cleaner(rel)

        # Should only have values >= 3
        df = result.fetchdf()
        assert len(df) == 3
        assert list(df["value"]) == [3, 4, 5]

    def test_transform_cleaner(self) -> None:
        """Test a cleaner that transforms data."""
        mock_dataset = MagicMock()

        class TransformCleaner(DataCleaner):
            def __call__(self, rel):
                return duckdb.sql(
                    "SELECT value, value * 2 AS doubled FROM rel"
                )

        # Create test relation
        rel = duckdb.sql("SELECT * FROM (VALUES (1), (2), (3)) AS t(value)")

        cleaner = TransformCleaner(mock_dataset)
        result = cleaner(rel)

        df = result.fetchdf()
        assert "doubled" in df.columns
        assert list(df["doubled"]) == [2, 4, 6]
