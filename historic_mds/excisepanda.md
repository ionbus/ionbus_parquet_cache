# Excise Pandas From Update Writes

## Problem

`execute_update()` currently converts Arrow tables to pandas DataFrames in
several write-path operations, then converts the DataFrames back to Arrow
before writing parquet files. That can silently widen nullable integer columns.

The observed failure case is nullable verifier columns returned by a datasource
as `pl.UInt8`:

- `convert_to_arrow()` preserves the columns as Arrow `uint8`.
- Snapshot metadata records the schema as `uint8`.
- A later Arrow -> pandas -> Arrow round trip widens nullable `uint8` columns
  to `double`.
- The physical parquet files are written with `double`, so the metadata schema
  and parquet file schema disagree.

Example affected verifier columns:

- `all_ohlcv_fields_present_v1`
- `data_exists_v1`
- `hl_spread_within_50pct_of_close_v2`
- `ohlc_in_hl_range_v2`
- `volume_gte_zero_v1`

The root issue is not pandas as a supported input type. It is using pandas as
an intermediate representation for Arrow data that will be written back to
parquet.

## Confirmed Behavior

A nullable Arrow `uint8` column with nulls stays `uint8` when sorted with
`pa.Table.sort_by(...)`.

The same column becomes `double` after:

```python
df = table.to_pandas()
df = df.sort_values(sort_cols)
table = pa.Table.from_pandas(df, preserve_index=False)
```

This explains how snapshot metadata can say `uint8` while the parquet files
store `double`: schema validation happens before the later pandas round trip.

## Scope

The fix should be surgical:

- Remove pandas round trips from `execute_update()` table transformations.
- Keep `pd.DataFrame` as an accepted datasource return type.
- Keep pandas where it is used for non-table business-day arithmetic unless a
  separate cleanup is useful later.
- Keep the Arrow/date helpers local to `update_pipeline.py` for now. We may
  later promote a general helper to `ionbus_utils`, but this fix should first
  prove the exact parquet-cache behavior in this repo.

The rule for this change is:

> Once data has been converted to `pa.Table`, any operation whose result will be
> written to parquet should stay in Arrow, DuckDB, or another schema-preserving
> representation.

## Local Helper Strategy

Do not add a new `ionbus_utils` API in this pass. The helper behavior is still
being shaped by parquet-cache update semantics, so it should live privately in
`update_pipeline.py` until the behavior is proven by tests.

The local helpers should be narrow:

```python
_sort_table(table, sort_cols) -> pa.Table
_arrow_date_range(table, date_col) -> tuple[dt.date | None, dt.date | None]
_arrow_temporal_scalar(value, arrow_type) -> pa.Scalar
_filter_new_dates(new_data, existing_data, date_col) -> pa.Table
_filter_out_date_window(existing_data, date_col, min_date, max_date) -> pa.Table
```

The scalar helper should derive the Arrow scalar type from the table column
being filtered. It should support at least `date32`, `date64`, and timestamp
types. If the date column is a timestamp, the update-window helper should make
the date-boundary behavior explicit rather than inheriting pandas' accidental
midnight comparison. The intended filter window is half-open:
`[min_date, max_date + 1 day)`.

## Code Paths To Change

All affected paths are in `update_pipeline.py`.

### 1. Chunk Sort Before Temp Write

Current behavior:

```python
df = table.to_pandas()
df = df.sort_values(sort_cols)
table = pa.Table.from_pandas(df, preserve_index=False)
```

Replace with Arrow sorting:

```python
table = table.sort_by([(col, "ascending") for col in sort_cols])
```

This is the sort that happens before writing the per-spec temp parquet file.

### 2. Backfill Date Filter

Current behavior:

```python
new_df = new_data.to_pandas()
new_df = new_df[~new_df[dataset.date_col].isin(existing_dates)]
new_data = pa.Table.from_pandas(new_df, preserve_index=False)
```

Replace with an Arrow filter:

```python
existing_dates = existing_data.column(dataset.date_col)
keep_mask = pc.invert(
    pc.is_in(new_data.column(dataset.date_col), value_set=existing_dates)
)
new_data = new_data.filter(keep_mask)
```

This keeps only new rows whose date is not already present in the existing
partition file.

### 3. Normal/Restate Update-Window Filter

Current behavior:

```python
existing_df = existing_data.to_pandas()
mask = (
    existing_df[dataset.date_col] < pd.Timestamp(min_date)
) | (
    existing_df[dataset.date_col] > pd.Timestamp(max_date)
)
existing_df = existing_df[mask]
existing_data = pa.Table.from_pandas(existing_df, preserve_index=False)
```

Replace with an Arrow filter:

```python
date_col = existing_data.column(dataset.date_col)
date_type = existing_data.schema.field(dataset.date_col).type
start = _arrow_temporal_scalar(min_date, date_type)
after_end = _arrow_temporal_scalar(
    max_date + dt.timedelta(days=1),
    date_type,
)
before_window = pc.less(date_col, start)
after_window = pc.greater_equal(date_col, after_end)
existing_data = existing_data.filter(pc.or_(before_window, after_window))
```

For date columns, the bounds are the dates themselves. For timestamp columns,
the recommended behavior is date-granular filtering: the whole `min_date`
through `max_date` range is considered inside the update window. That means the
upper bound is midnight at the start of the day after `max_date`, and rows are
kept only when they fall before `min_date` or on/after that exclusive upper
bound.

### 4. Final Combined Partition Sort

Current behavior:

```python
df = combined.to_pandas()
df = df.sort_values(sort_cols)
combined = pa.Table.from_pandas(df, preserve_index=False)
```

Replace with Arrow sorting:

```python
combined = combined.sort_by([(col, "ascending") for col in sort_cols])
```

This is the final sort before writing the consolidated partition parquet file.

### 5. Date Range Tracking

Current behavior:

```python
dates = table.column(dataset.date_col).to_pandas()
chunk_min = dates.min()
chunk_max = dates.max()
```

This is not a write-path mutation, so it is not the direct source of the
parquet widening bug. Still, it feeds the normal/restate update-window filter,
so it should be made consistent with the Arrow filtering helpers.

Replace it with a local `_arrow_date_range(table, date_col)` helper that:

- ignores nulls,
- returns `(None, None)` for empty or all-null date columns,
- supports Arrow date and timestamp columns,
- normalizes results to plain `datetime.date`,
- uses the same date interpretation expected by
  `_filter_out_date_window(...)`.

## Implementation Plan

1. Add `import pyarrow.compute as pc` to `update_pipeline.py`.
2. Add private local helpers in `update_pipeline.py`:
   - `_sort_table(table, sort_cols)`
   - `_arrow_date_range(table, date_col)`
   - `_arrow_temporal_scalar(value, arrow_type)`
   - `_filter_new_dates(new_data, existing_data, date_col)`
   - `_filter_out_date_window(existing_data, date_col, min_date, max_date)`
3. Replace both pandas sort blocks with `Table.sort_by`.
4. Replace the backfill `isin` filter with `pc.is_in` plus `pc.invert`.
5. Replace the normal/restate date-window filter with Arrow comparisons.
6. Replace date range tracking with `_arrow_date_range(...)`.
7. When reading an existing partition file, strip any partition columns that
   PyArrow materializes from the Hive path before concatenating with new data.
   This keeps virtual partition columns virtual and avoids schema mismatches in
   same-partition backfill/restate merges.
8. Keep `convert_to_arrow()` pandas input handling unchanged.
9. Keep business-day arithmetic in `compute_update_window()` unchanged for this
   fix.

## Test Plan

Add focused regression tests around `execute_update()`:

- A datasource returning `pl.DataFrame` with nullable `pl.UInt8` columns and
  `sort_columns` writes physical parquet columns as `uint8`.
- The same nullable `uint8` data remains `uint8` through backfill filtering.
- The same nullable `uint8` data remains `uint8` through normal/restate
  update-window filtering.
- Snapshot metadata schema and physical parquet schema agree for these columns.
- Date range tracking ignores nulls and returns plain `datetime.date` values.
- Date-window filtering behaves correctly for `date32` and timestamp date
  columns. Timestamp tests should cover rows on `max_date` after midnight so the
  half-open end boundary is explicit.
- Existing partition files read from Hive-style paths do not leak materialized
  partition columns into the final concatenated table.

The tests should inspect the written parquet schema directly with PyArrow, not
only the returned pandas DataFrame, because the bug is in the physical file
schema.

## Non-Goals

- Do not remove pandas as a datasource input type.
- Do not remove `read_data()` returning pandas DataFrames.
- Do not convert the whole package away from pandas.
- Do not add or require a new `ionbus_utils` date helper in this pass.
- Do not change schema-promotion rules except where needed to preserve the
  schema that already survived validation.
