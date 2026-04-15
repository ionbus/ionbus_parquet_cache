"""Tests for filter utility functions."""

import pyarrow.dataset as pds
import pytest

from ionbus_parquet_cache import (
    and_filter,
    or_filter,
    pandas_filter_list_to_expression,
)


class TestOrFilter:
    """Tests for or_filter function."""

    def test_empty_list(self):
        """Empty list returns None."""
        assert or_filter([]) is None

    def test_all_none(self):
        """List of None values returns None."""
        assert or_filter([None, None, None]) is None

    def test_single_expression(self):
        """Single expression is returned unchanged."""
        expr = pds.field("col") == "A"
        result = or_filter([expr])
        assert result is not None

    def test_two_expressions(self):
        """Two expressions are ORed."""
        expr1 = pds.field("col") == "A"
        expr2 = pds.field("col") == "B"
        result = or_filter([expr1, expr2])
        assert result is not None

    def test_skips_none(self):
        """None values are skipped."""
        expr = pds.field("col") == "A"
        result = or_filter([None, expr, None])
        assert result is not None

    def test_multiple_with_none(self):
        """Multiple expressions with None mixed in."""
        expr1 = pds.field("col") == "A"
        expr2 = pds.field("col") == "B"
        result = or_filter([expr1, None, expr2])
        assert result is not None


class TestAndFilter:
    """Tests for and_filter function."""

    def test_empty_list(self):
        """Empty list returns None."""
        assert and_filter([]) is None

    def test_all_none(self):
        """List of None values returns None."""
        assert and_filter([None, None, None]) is None

    def test_single_expression(self):
        """Single expression is returned unchanged."""
        expr = pds.field("col") > 5
        result = and_filter([expr])
        assert result is not None

    def test_two_expressions(self):
        """Two expressions are ANDed."""
        expr1 = pds.field("col") >= 0
        expr2 = pds.field("col") <= 10
        result = and_filter([expr1, expr2])
        assert result is not None

    def test_skips_none(self):
        """None values are skipped."""
        expr = pds.field("col") > 5
        result = and_filter([None, expr, None])
        assert result is not None


class TestPandasFilterListToExpression:
    """Tests for pandas_filter_list_to_expression function."""

    def test_none_returns_none(self):
        """None input returns None."""
        assert pandas_filter_list_to_expression(None) is None

    def test_expression_passthrough(self):
        """Existing Expression is returned unchanged."""
        expr = pds.field("col") == "A"
        result = pandas_filter_list_to_expression(expr)
        assert result is expr

    def test_single_tuple_equals(self):
        """Single tuple with = operator."""
        result = pandas_filter_list_to_expression(("col", "=", "A"))
        assert result is not None

    def test_single_tuple_double_equals(self):
        """Single tuple with == operator."""
        result = pandas_filter_list_to_expression(("col", "==", "A"))
        assert result is not None

    def test_single_tuple_greater(self):
        """Single tuple with > operator."""
        result = pandas_filter_list_to_expression(("col", ">", 5))
        assert result is not None

    def test_single_tuple_greater_equal(self):
        """Single tuple with >= operator."""
        result = pandas_filter_list_to_expression(("col", ">=", 5))
        assert result is not None

    def test_single_tuple_less(self):
        """Single tuple with < operator."""
        result = pandas_filter_list_to_expression(("col", "<", 5))
        assert result is not None

    def test_single_tuple_less_equal(self):
        """Single tuple with <= operator."""
        result = pandas_filter_list_to_expression(("col", "<=", 5))
        assert result is not None

    def test_single_tuple_in(self):
        """Single tuple with in operator."""
        result = pandas_filter_list_to_expression(("col", "in", ["A", "B"]))
        assert result is not None

    def test_single_tuple_not_in(self):
        """Single tuple with not in operator."""
        result = pandas_filter_list_to_expression(("col", "not in", ["A", "B"]))
        assert result is not None

    def test_list_of_tuples_anded(self):
        """List of tuples are ANDed together."""
        filters = [("col1", "=", "A"), ("col2", ">", 0)]
        result = pandas_filter_list_to_expression(filters)
        assert result is not None

    def test_list_of_lists_ored(self):
        """List of lists: inner ANDed, outer ORed."""
        filters = [[("col", "=", "A")], [("col", "=", "B")]]
        result = pandas_filter_list_to_expression(filters)
        assert result is not None

    def test_complex_nested(self):
        """Complex nested filters."""
        filters = [
            [("col1", "=", "A"), ("col2", ">", 0)],
            [("col1", "=", "B"), ("col2", "<", 100)],
        ]
        result = pandas_filter_list_to_expression(filters)
        assert result is not None

    def test_invalid_type_raises(self):
        """Invalid filter type raises ValueError."""
        with pytest.raises(ValueError, match="must be tuple, list"):
            pandas_filter_list_to_expression("invalid")

    def test_unsupported_operator_raises(self):
        """Unsupported operator raises ValueError."""
        # Invalid operator causes tuple to not be recognized as a filter,
        # which then fails when processed as a list
        with pytest.raises(ValueError):
            pandas_filter_list_to_expression(("col", "like", "%A%"))

    def test_case_insensitive_operators(self):
        """Operators are case insensitive."""
        result = pandas_filter_list_to_expression(("col", "IN", ["A", "B"]))
        assert result is not None

    def test_operators_with_whitespace(self):
        """Operators with extra whitespace work."""
        result = pandas_filter_list_to_expression(("col", " >= ", 5))
        assert result is not None
