# Filtering Data

This document describes the filtering capabilities available when reading data from datasets.

## Table of Contents

- [Basic Filter Syntax](#basic-filter-syntax)
- [Supported Operators](#supported-operators)
- [Combining Filters with AND](#combining-filters-with-and)
- [Combining Filters with OR](#combining-filters-with-or)
- [Complex Filters (AND + OR)](#complex-filters-and--or)
- [Date Filtering](#date-filtering)
- [Partition Pruning](#partition-pruning)
- [Using PyArrow Expressions Directly](#using-pyarrow-expressions-directly)
- [Helper Functions](#helper-functions)

## Basic Filter Syntax

Filters use a tuple format: `(column_name, operator, value)`

```python
from ionbus_parquet_cache import CacheRegistry

registry = CacheRegistry.instance(cache="/path/to/cache")

# Single filter
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    filters=[("Symbol", "=", "ESH24")],
)

# Filter with numeric comparison
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    filters=[("Volume", ">", 1000000)],
)
```

## Supported Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` or `==` | Equal to | `("Symbol", "=", "ES")` |
| `!=` | Not equal to | `("Exchange", "!=", "CME")` |
| `>` | Greater than | `("Price", ">", 100.0)` |
| `>=` | Greater than or equal | `("Volume", ">=", 1000)` |
| `<` | Less than | `("Price", "<", 50.0)` |
| `<=` | Less than or equal | `("Volume", "<=", 500000)` |
| `in` | Value in list | `("Symbol", "in", ["ES", "NQ", "YM"])` |
| `not in` | Value not in list | `("Exchange", "not in", ["TEST", "DEMO"])` |

### Examples

```python
# Equality
df = registry.read_data("md.prices", filters=[("Symbol", "=", "AAPL")])

# Numeric comparison
df = registry.read_data("md.prices", filters=[("Close", ">=", 150.0)])

# In a list of values
df = registry.read_data(
    "md.futures_daily",
    filters=[("FutureRoot", "in", ["ES", "NQ", "RTY"])],
)

# Not in a list
df = registry.read_data(
    "md.futures_daily",
    filters=[("Exchange", "not in", ["TEST"])],
)
```

## Combining Filters with AND

Pass multiple filter tuples in a list to AND them together. All conditions must be true.

```python
# Price between 100 and 200 AND volume over 1 million
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    filters=[
        ("Close", ">=", 100.0),
        ("Close", "<=", 200.0),
        ("Volume", ">", 1000000),
    ],
)

# Specific symbol on a specific exchange
df = registry.read_data(
    "md.futures_daily",
    filters=[
        ("FutureRoot", "=", "ES"),
        ("Exchange", "=", "CME"),
    ],
)
```

## Combining Filters with OR

To OR filters together, wrap each condition in its own list. The outer list ORs, the inner lists AND.

```python
# Symbol is ES OR Symbol is NQ
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    filters=[
        [("FutureRoot", "=", "ES")],
        [("FutureRoot", "=", "NQ")],
    ],
)
```

For simple OR on the same column, prefer the `in` operator:

```python
# Equivalent to above, but cleaner
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    filters=[("FutureRoot", "in", ["ES", "NQ"])],
)
```

## Complex Filters (AND + OR)

Combine AND and OR by nesting lists. Inner lists are ANDed, outer list is ORed.

```python
# (ES with volume > 1M) OR (NQ with volume > 500K)
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    filters=[
        [("FutureRoot", "=", "ES"), ("Volume", ">", 1000000)],
        [("FutureRoot", "=", "NQ"), ("Volume", ">", 500000)],
    ],
)

# (CME exchange AND high volume) OR (ICE exchange AND any volume)
df = registry.read_data(
    "md.futures_daily",
    filters=[
        [("Exchange", "=", "CME"), ("Volume", ">", 100000)],
        [("Exchange", "=", "ICE")],
    ],
)
```

## Date Filtering

For DatedParquetDataset, use `start_date` and `end_date` parameters. These are automatically converted to filters on the date column.

```python
# All data in Q1 2024
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    end_date="2024-03-31",
)

# From a specific date onwards
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-06-01",
)

# Up to a specific date
df = registry.read_data(
    "md.futures_daily",
    end_date="2024-06-30",
)

# Combine date range with other filters
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    end_date="2024-03-31",
    filters=[("FutureRoot", "in", ["ES", "NQ"])],
)
```

## Partition Pruning

Filters on partition columns are especially efficient because PyArrow can skip reading entire files that don't match.

If your dataset is partitioned by `FutureRoot` and `month`, filtering on these columns avoids reading irrelevant partitions:

```python
# Fast: filters on partition columns skip irrelevant files
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    end_date="2024-01-31",
    filters=[("FutureRoot", "=", "ES")],  # Partition column
)

# Also fast: date range automatically prunes month partitions
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-03-01",
    end_date="2024-03-31",
)
```

Date filtering automatically adds partition pruning filters when:
- The dataset uses month, quarter, or year partitioning
- Both start_date and end_date are provided

## Using PyArrow Expressions Directly

For advanced use cases, you can pass PyArrow expressions directly:

```python
import pyarrow.dataset as pds

# Build a custom expression
expr = (pds.field("Close") > 100) & (pds.field("Volume") > 1000000)

df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    filters=expr,
)

# Complex expression with OR
expr = (
    ((pds.field("FutureRoot") == "ES") & (pds.field("Volume") > 1000000)) |
    ((pds.field("FutureRoot") == "NQ") & (pds.field("Volume") > 500000))
)

df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    filters=expr,
)
```

Note: When using PyArrow expressions directly, you cannot combine them with `start_date`/`end_date` parameters. Include date filters in your expression instead.

## Helper Functions

The library exports helper functions for building complex filters programmatically:

```python
from ionbus_parquet_cache import (
    and_filter,
    or_filter,
    pandas_filter_list_to_expression,
)
import pyarrow.dataset as pds

# Build expressions programmatically
expr1 = pds.field("FutureRoot") == "ES"
expr2 = pds.field("Volume") > 1000000
expr3 = pds.field("FutureRoot") == "NQ"

# AND expressions together
combined_and = and_filter([expr1, expr2])

# OR expressions together
combined_or = or_filter([expr1, expr3])

# Convert pandas-style filters to expression
expr = pandas_filter_list_to_expression([
    ("FutureRoot", "in", ["ES", "NQ"]),
    ("Volume", ">", 100000),
])

# Use with read_data
dpd = registry.get_dataset("md.futures_daily")
df = dpd.read_data(start_date="2024-01-01", filters=combined_and)
```

### and_filter

Combines multiple expressions with AND:

```python
from ionbus_parquet_cache import and_filter
import pyarrow.dataset as pds

exprs = [
    pds.field("Exchange") == "CME",
    pds.field("Volume") > 100000,
    pds.field("Close") > 50,
]
combined = and_filter(exprs)
```

### or_filter

Combines multiple expressions with OR:

```python
from ionbus_parquet_cache import or_filter
import pyarrow.dataset as pds

exprs = [
    pds.field("FutureRoot") == "ES",
    pds.field("FutureRoot") == "NQ",
    pds.field("FutureRoot") == "RTY",
]
combined = or_filter(exprs)
```

### pandas_filter_list_to_expression

Converts pandas-style filter lists to PyArrow expressions:

```python
from ionbus_parquet_cache import pandas_filter_list_to_expression

# Single filter
expr = pandas_filter_list_to_expression(("Symbol", "=", "ES"))

# Multiple filters (ANDed)
expr = pandas_filter_list_to_expression([
    ("Symbol", "=", "ES"),
    ("Volume", ">", 1000000),
])

# Nested filters (inner ANDed, outer ORed)
expr = pandas_filter_list_to_expression([
    [("Symbol", "=", "ES"), ("Volume", ">", 1000000)],
    [("Symbol", "=", "NQ"), ("Volume", ">", 500000)],
])
```
