"""Filter utilities for PyArrow dataset expressions."""

from __future__ import annotations

import datetime as dt
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pds

from ionbus_utils.date_utils import date_partition_value

from ionbus_parquet_cache.partition import date_partition_column_name


def or_filter(filters: list[pds.Expression | None]) -> pds.Expression | None:
    """
    OR multiple filter expressions together.

    Args:
        filters: List of PyArrow dataset expressions (None values are skipped).

    Returns:
        Combined expression with OR, or None if no valid filters.
    """
    result = None
    for expr in filters:
        if expr is None:
            continue
        if result is None:
            result = expr
        else:
            result = result | expr
    return result


def and_filter(filters: list[pds.Expression | None]) -> pds.Expression | None:
    """
    AND multiple filter expressions together.

    Args:
        filters: List of PyArrow dataset expressions (None values are skipped).

    Returns:
        Combined expression with AND, or None if no valid filters.
    """
    result = None
    for expr in filters:
        if expr is None:
            continue
        if result is None:
            result = expr
        else:
            result = result & expr
    return result


def pandas_filter_list_to_expression(
    filters: list[list | tuple] | tuple | pds.Expression | None,
) -> pds.Expression | None:
    """
    Convert pandas-style filter list to PyArrow dataset expression.

    Supported operators: =, ==, >, >=, <, <=, in, not in

    Filter formats:
        - Single tuple: ("col", ">=", 5)
        - List of tuples (ANDed): [("col1", "=", "A"), ("col2", ">", 0)]
        - List of lists (inner ANDed, outer ORed):
          [[("col", "=", "A")], [("col", "=", "B")]]

    Args:
        filters: Filter specification in pandas format, or existing Expression.

    Returns:
        PyArrow dataset Expression, or None if no filters.

    Raises:
        ValueError: If filter format is invalid or operator unsupported.
    """
    if filters is None or isinstance(filters, pds.Expression):
        return filters

    if not isinstance(filters, (tuple, list)):
        raise ValueError("Filters must be tuple, list, or Expression")

    # Check if this is a single filter tuple: (col, op, value)
    op = _get_filter_operator(filters)
    if op is not None:
        return _tuple_to_expression(filters, op)

    # It's a list - check if all elements are filter tuples/expressions
    all_simple = all(
        _get_filter_operator(x, include_expressions=True) is not None
        for x in filters
        if x is not None
    )

    if all_simple:
        # List of simple filters -> AND them together
        return and_filter(
            [pandas_filter_list_to_expression(x) for x in filters]
        )
    else:
        # List of lists -> AND each inner, OR the results
        return or_filter(
            [pandas_filter_list_to_expression(x) for x in filters]
        )


def _get_filter_operator(
    obj: Any, include_expressions: bool = False
) -> str | None:
    """Check if obj is a valid filter tuple and return operator."""
    if obj is None:
        return None
    if isinstance(obj, pds.Expression):
        return "expression" if include_expressions else None
    if not isinstance(obj, (tuple, list)) or len(obj) != 3:
        return None
    col, op, _ = obj
    if not isinstance(col, str) or not isinstance(op, str):
        return None
    op_lower = op.lower().strip()
    if op_lower not in {"=", "==", ">", ">=", "<", "<=", "in", "not in"}:
        return None
    return op_lower


def _tuple_to_expression(
    filter_tuple: tuple | list, op: str
) -> pds.Expression:
    """Convert a single filter tuple to PyArrow expression."""
    col, _, value = filter_tuple
    field = pds.field(col)

    if op in ("=", "=="):
        return field == value
    elif op == ">":
        return field > value
    elif op == ">=":
        return field >= value
    elif op == "<":
        return field < value
    elif op == "<=":
        return field <= value
    elif op == "in":
        return field.isin(value)
    elif op == "not in":
        return ~field.isin(value)
    else:
        raise ValueError(f"Unsupported operator: {op}")


def build_filter_expression(
    filters: list[tuple[str, str, Any]],
) -> pc.Expression:
    """
    Build a PyArrow filter expression from a list of filter tuples.

    Args:
        filters: List of (column, operator, value) tuples.
            Supported operators: "=", "==", "!=", "<", "<=", ">", ">=", "in", "not in".

    Returns:
        A PyArrow compute expression for filtering.

    Raises:
        ValueError: If an unsupported operator is used.
    """
    if not filters:
        return pc.scalar(True)

    expressions = []
    for col, op, val in filters:
        field = pc.field(col)
        if op in ("=", "=="):
            expr = field == val
        elif op == "!=":
            expr = field != val
        elif op == "<":
            expr = field < val
        elif op == "<=":
            expr = field <= val
        elif op == ">":
            expr = field > val
        elif op == ">=":
            expr = field >= val
        elif op == "in":
            expr = pc.is_in(field, value_set=pa.array(val))
        elif op == "not in":
            expr = ~pc.is_in(field, value_set=pa.array(val))
        else:
            raise ValueError(f"Unsupported filter operator: {op}")
        expressions.append(expr)

    # Combine with AND
    result = expressions[0]
    for expr in expressions[1:]:
        result = result & expr

    return result


def _compute_partition_values_for_range(
    start_date: dt.date,
    end_date: dt.date,
    date_partition: str,
) -> set[str]:
    """
    Compute all partition values that a date range spans.

    Args:
        start_date: First date of the range.
        end_date: Last date of the range.
        date_partition: Partition granularity (year, quarter, month, etc.)

    Returns:
        Set of partition values that cover the date range.
    """
    values = set()
    current = start_date
    while current <= end_date:
        values.add(date_partition_value(current, date_partition))
        # Move to next partition period
        if date_partition == "day":
            current += dt.timedelta(days=1)
        elif date_partition == "week":
            current += dt.timedelta(days=7)
        elif date_partition == "month":
            # Move to first day of next month
            if current.month == 12:
                current = dt.date(current.year + 1, 1, 1)
            else:
                current = dt.date(current.year, current.month + 1, 1)
        elif date_partition == "quarter":
            # Move to first day of next quarter
            next_quarter_month = ((current.month - 1) // 3 + 1) * 3 + 1
            if next_quarter_month > 12:
                current = dt.date(current.year + 1, 1, 1)
            else:
                current = dt.date(current.year, next_quarter_month, 1)
        elif date_partition == "year":
            current = dt.date(current.year + 1, 1, 1)
        else:
            break  # Unknown partition type, just add the first value

    return values


def build_dataset_filter(
    start_date: str | dt.date | None = None,
    end_date: str | dt.date | None = None,
    date_col: str | None = None,
    is_dated_dataset: bool = False,
    filters: list[tuple[str, str, Any]] | None = None,
    date_partition: str | None = None,
    partition_columns: list[str] | None = None,
) -> pc.Expression | None:
    """
    Build a combined filter expression from dates and user filters.

    For dated datasets, converts start_date/end_date to:
    1. Partition filters (for partition pruning, if applicable)
    2. Date column filters (for row-level filtering)

    Args:
        start_date: Optional start date for filtering.
        end_date: Optional end date for filtering.
        date_col: Name of the date column (required for dated datasets).
        is_dated_dataset: Whether this is a dated dataset type.
        filters: Optional list of (column, operator, value) filter tuples.
        date_partition: Partition granularity for automatic partition pruning.
        partition_columns: List of partition columns in the dataset schema.
            Partition pruning filters are only added if the date_partition
            column is in this list.

    Returns:
        A PyArrow Expression, or None if no filters.

    Raises:
        ValueError: If dates provided for non-dated dataset.
    """
    has_dates = start_date is not None or end_date is not None

    # Validate dates for non-dated datasets
    if has_dates and not is_dated_dataset:
        raise ValueError("Non-dated datasets do not support date filtering")

    # Build filter list
    all_filters: list[tuple[str, str, Any]] = list(filters or [])

    # Add date filters for dated datasets
    if has_dates and date_col:
        # Parse dates if strings
        parsed_start = None
        parsed_end = None

        if start_date is not None:
            if isinstance(start_date, str):
                parsed_start = dt.date.fromisoformat(start_date)
            else:
                parsed_start = start_date
            all_filters.append((date_col, ">=", parsed_start))

        if end_date is not None:
            if isinstance(end_date, str):
                parsed_end = dt.date.fromisoformat(end_date)
            else:
                parsed_end = end_date
            all_filters.append((date_col, "<=", parsed_end))

        # Add automatic partition filters for partition pruning
        # Only if: not "day" partition AND partition column is in schema
        partition_cols = partition_columns or []
        # Compute actual partition column name using the abstraction
        part_col = (
            date_partition_column_name(date_partition, date_col)
            if date_partition and date_col
            else None
        )
        can_prune = (
            part_col
            and date_partition != "day"
            and part_col in partition_cols
        )
        if can_prune and parsed_start and parsed_end:
            partition_values = _compute_partition_values_for_range(
                parsed_start, parsed_end, date_partition
            )
            if partition_values:
                all_filters.append((part_col, "in", partition_values))

    # Return expression or None
    if not all_filters:
        return None

    return build_filter_expression(all_filters)
