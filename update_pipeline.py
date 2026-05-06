"""
Update pipeline for DatedParquetDataset.

Handles the full update flow:
1. Compute update window from source and cache state
2. Get partitions from source
3. Group specs and assign temp file paths
4. Fetch data, apply transforms, write temp files
5. Consolidate chunks into final partition files
6. Publish new snapshot
"""

from __future__ import annotations

import datetime as dt
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from ionbus_utils.logging_utils import logger

from ionbus_parquet_cache.exceptions import (
    DataSourceError,
    SchemaMismatchError,
    SnapshotNotFoundError,
    ValidationError,
)
from ionbus_parquet_cache.partition import (
    PartitionSpec,
    date_partition_column_name,
)
from ionbus_parquet_cache.snapshot import generate_snapshot_suffix
from ionbus_parquet_cache.snapshot_history import DateRange, SnapshotLineage

if TYPE_CHECKING:
    from ionbus_parquet_cache.data_cleaner import DataCleaner
    from ionbus_parquet_cache.data_source import DataSource
    from ionbus_parquet_cache.dated_dataset import (
        DatedParquetDataset,
        FileMetadata,
    )


@dataclass
class WriteGroup:
    """
    A group of PartitionSpecs that will be consolidated into one partition file.

    All specs in a group share the same partition_values.
    """

    partition_values: dict[str, Any]
    specs: list[PartitionSpec] = field(default_factory=list)
    temp_files: list[Path] = field(default_factory=list)
    final_path: Path | None = None


@dataclass
class UpdatePlan:
    """
    Complete plan for an update operation.

    Built before any data is fetched, containing:
    - All partition specs to process
    - Grouping by partition_values
    - Temp file paths for each spec
    - Final file paths for each group
    """

    suffix: str
    groups: dict[tuple, WriteGroup] = field(default_factory=dict)
    specs_in_order: list[PartitionSpec] = field(default_factory=list)


def compute_update_window(
    dataset: "DatedParquetDataset",
    source: "DataSource",
    start_date: dt.date | None = None,
    end_date: dt.date | None = None,
) -> tuple[dt.date, dt.date] | None:
    """
    Compute the update window based on source availability and cache state.

    Args:
        dataset: The DatedParquetDataset to update.
        source: The DataSource providing data.
        start_date: Optional explicit start date.
        end_date: Optional explicit end date.

    Returns:
        Tuple of (start_date, end_date) or None if no update needed.
    """
    # Get source availability
    source_start, source_end = source.available_dates()

    if start_date is None or end_date is None:
        # Load metadata if not already loaded
        if dataset._metadata is None:
            try:
                dataset._metadata = dataset._load_metadata()
            except SnapshotNotFoundError:
                pass  # No existing cache - expected for new datasets

        # Compute from cache state
        cache_max_date = None
        if dataset._metadata:
            cache_max_date = dataset._metadata.cache_end_date

        if cache_max_date:
            # Start from day after cache end
            computed_start = cache_max_date + dt.timedelta(days=1)

            # Apply repull_n_days if set
            if dataset.repull_n_days > 0:
                # Go back N business days using pandas
                computed_start = (
                    pd.Timestamp(computed_start)
                    - pd.offsets.BDay(dataset.repull_n_days)
                ).date()
        else:
            # Empty cache - start from source start
            computed_start = source_start

        if start_date is None:
            start_date = max(source_start, computed_start)
        if end_date is None:
            end_date = source_end

    # Apply start_date_str/end_date_str clamps if set
    if dataset.start_date_str:
        clamp_start = dt.date.fromisoformat(dataset.start_date_str)
        start_date = max(start_date, clamp_start)
    if dataset.end_date_str:
        clamp_end = dt.date.fromisoformat(dataset.end_date_str)
        end_date = min(end_date, clamp_end)

    # Validate window
    if start_date > end_date:
        return None  # No update needed

    return (start_date, end_date)


def build_update_plan(
    dataset: "DatedParquetDataset",
    specs: list[PartitionSpec],
    temp_dir: Path,
    suffix: str | None = None,
) -> UpdatePlan:
    """
    Build a complete update plan before fetching any data.

    Args:
        dataset: The DatedParquetDataset being updated.
        specs: List of PartitionSpecs from the DataSource.
        temp_dir: Directory for temporary files.
        suffix: Optional snapshot suffix. If None, generates a new one.

    Returns:
        UpdatePlan with all file paths assigned.
    """
    assert not dataset.is_gcs
    if suffix is None:
        suffix = generate_snapshot_suffix()
    plan = UpdatePlan(suffix=suffix)

    # Group specs by partition_values
    groups: dict[tuple, WriteGroup] = {}

    for spec in specs:
        key = spec.partition_key
        if key not in groups:
            groups[key] = WriteGroup(
                partition_values=dict(spec.partition_values)
            )
        groups[key].specs.append(spec)

    # Assign temp file paths and final paths
    # Get the date partition column name for navigation directory logic
    date_part_col = date_partition_column_name(
        dataset.date_partition, dataset.date_col
    )

    for key, group in groups.items():
        # Build partition path components with navigation directories
        path_parts = []
        for col in dataset.partition_columns:
            if col not in group.partition_values:
                continue
            val = group.partition_values[col]

            # Add navigation directories for date partition columns
            # (except year which doesn't need a nav dir)
            if col == date_part_col and dataset.date_partition != "year":
                if dataset.date_partition in ("month", "quarter", "week"):
                    # Format is X2024-NN, extract year after first char
                    if isinstance(val, str) and len(val) >= 5:
                        year = val[1:5]
                        path_parts.append(year)
                elif dataset.date_partition == "day":
                    # Format is YYYY-MM-DD
                    if isinstance(val, str) and len(val) >= 7:
                        year = val[:4]
                        month = val[5:7]
                        path_parts.append(year)
                        path_parts.append(month)

            path_parts.append(f"{col}={val}")

        # Final file path
        partition_dir = cast(Path, dataset.dataset_dir)
        for part in path_parts:
            partition_dir = partition_dir / part

        # File name based on partition values
        # Use all partition values from the group (not just
        # dataset.partition_columns)
        # to ensure unique file names even when partition_columns is empty
        name_parts = [
            f"{col}={group.partition_values[col]}"
            for col in sorted(group.partition_values.keys())
        ]
        file_base = "_".join(name_parts) if name_parts else dataset.name
        group.final_path = partition_dir / f"{file_base}_{suffix}.parquet"

        # Assign temp files with chunk numbers
        for i, spec in enumerate(group.specs):
            if len(group.specs) > 1:
                # Multiple chunks - use _01, _02 suffix
                temp_name = f"{file_base}_{suffix}_{i + 1:02d}.parquet"
            else:
                temp_name = f"{file_base}_{suffix}.parquet"

            temp_path = temp_dir / temp_name
            spec.temp_file_path = str(temp_path)
            group.temp_files.append(temp_path)

    plan.groups = groups
    plan.specs_in_order = specs

    return plan


def convert_to_arrow(
    data: pd.DataFrame | pa.Table | Any,
) -> pa.Table:
    """
    Convert various data formats to PyArrow Table.

    Args:
        data: Data from DataSource.get_data()

    Returns:
        PyArrow Table
    """
    if isinstance(data, pa.Table):
        return data

    if isinstance(data, pd.DataFrame):
        return pa.Table.from_pandas(data, preserve_index=False)

    # Try polars DataFrame
    if isinstance(data, pl.DataFrame):
        return data.to_arrow()

    # Try polars LazyFrame (collect first)
    if isinstance(data, pl.LazyFrame):
        return data.collect().to_arrow()

    # Try pyarrow dataset
    if hasattr(data, "to_table"):
        return data.to_table()

    raise ValidationError(
        f"Cannot convert {type(data).__name__} to PyArrow Table. "
        "Supported types: pa.Table, pd.DataFrame, pl.DataFrame, pl.LazyFrame, "
        "pa.dataset.Dataset"
    )


def validate_schema(
    new_schema: pa.Schema,
    existing_schema: pa.Schema | None,
    dataset_name: str,
) -> pa.Schema:
    """
    Validate schema compatibility and merge if needed.

    Args:
        new_schema: Schema of new data.
        existing_schema: Existing schema from cache (or None).
        dataset_name: Name for error messages.

    Returns:
        The validated/merged schema.

    Raises:
        SchemaMismatchError: If schemas are incompatible.
    """
    if existing_schema is None:
        return new_schema

    # Build merged schema: union of all columns from both schemas
    # For overlapping columns, use the promoted type
    merged_fields: list[pa.Field] = []
    seen_names: set[str] = set()

    # First, add all fields from new_schema (with type promotion if needed)
    for schema_field in new_schema:
        if schema_field.name in existing_schema.names:
            existing_field = existing_schema.field(schema_field.name)
            if not schema_field.type.equals(existing_field.type):
                # Check if it's a compatible promotion
                if not _types_compatible(
                    existing_field.type, schema_field.type
                ):
                    raise SchemaMismatchError(
                        f"Column '{schema_field.name}' type changed from "
                        f"{existing_field.type} to {schema_field.type}",
                        column=schema_field.name,
                        expected_type=str(existing_field.type),
                        actual_type=str(schema_field.type),
                    )
                # Use the promoted (wider) type
                merged_fields.append(schema_field)
            else:
                merged_fields.append(schema_field)
        else:
            # New column
            merged_fields.append(schema_field)
        seen_names.add(schema_field.name)

    # Then, add columns from existing_schema that aren't in new_schema
    # These columns will be null in new data but present in old files
    for schema_field in existing_schema:
        if schema_field.name not in seen_names:
            merged_fields.append(schema_field)

    return pa.schema(merged_fields)


def _types_compatible(old_type: pa.DataType, new_type: pa.DataType) -> bool:
    """Check if type change is compatible (safe promotion)."""
    # Same type is always compatible
    if old_type.equals(new_type):
        return True

    # Integer promotions
    int_types = [pa.int8(), pa.int16(), pa.int32(), pa.int64()]
    if old_type in int_types and new_type in int_types:
        return int_types.index(new_type) >= int_types.index(old_type)

    # Float promotions
    if old_type == pa.float32() and new_type == pa.float64():
        return True

    return False


def _sort_table(table: pa.Table, sort_cols: list[str]) -> pa.Table:
    """Sort an Arrow table by the subset of sort columns it contains."""
    existing_sort_cols = [c for c in sort_cols if c in table.column_names]
    if not existing_sort_cols or len(table) == 0:
        return table
    sort_keys = [(col, "ascending") for col in existing_sort_cols]
    return table.sort_by(sort_keys)


def _temporal_value_to_date(value: Any) -> dt.date | None:
    """Normalize an Arrow temporal scalar value to a plain date."""
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if hasattr(value, "date"):
        return value.date()
    raise ValidationError(
        f"Expected date-like value, got {type(value).__name__}"
    )


def _arrow_date_range(
    table: pa.Table,
    date_col: str,
) -> tuple[dt.date | None, dt.date | None]:
    """Return the non-null date range for a table date column."""
    if date_col not in table.column_names or len(table) == 0:
        return (None, None)

    dates = table.column(date_col)
    if pc.count(dates).as_py() == 0:
        return (None, None)

    return (
        _temporal_value_to_date(pc.min(dates).as_py()),
        _temporal_value_to_date(pc.max(dates).as_py()),
    )


def _arrow_temporal_scalar(
    value: dt.date,
    arrow_type: pa.DataType,
) -> pa.Scalar:
    """Build a date/timestamp scalar matching an Arrow date column type."""
    if pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
        return pa.scalar(value, type=arrow_type)

    if pa.types.is_timestamp(arrow_type):
        timestamp_value = dt.datetime.combine(value, dt.time.min)
        timezone_name = getattr(arrow_type, "tz", None)
        if timezone_name is not None:
            timestamp_value = timestamp_value.replace(
                tzinfo=_timezone_for_arrow(timezone_name)
            )
        return pa.scalar(timestamp_value, type=arrow_type)

    raise ValidationError(
        f"Date column must be an Arrow date or timestamp type, got "
        f"{arrow_type}"
    )


def _timezone_for_arrow(timezone_name: str) -> dt.tzinfo:
    """Return tzinfo for an Arrow timestamp timezone name."""
    if timezone_name.upper() == "UTC":
        return dt.timezone.utc

    from zoneinfo import ZoneInfo

    return ZoneInfo(timezone_name)


def _filter_new_dates(
    new_data: pa.Table,
    existing_data: pa.Table,
    date_col: str,
) -> pa.Table:
    """Keep rows from new_data whose date is not already in existing_data."""
    if (
        date_col not in new_data.column_names
        or date_col not in existing_data.column_names
    ):
        return new_data

    existing_dates = existing_data.column(date_col).combine_chunks()
    is_existing_date = pc.is_in(
        new_data.column(date_col),
        value_set=existing_dates,
    )
    return new_data.filter(pc.invert(is_existing_date))


def _filter_out_date_window(
    existing_data: pa.Table,
    date_col: str,
    min_date: dt.date | None,
    max_date: dt.date | None,
) -> pa.Table:
    """Keep existing rows outside the half-open date update window."""
    if (
        date_col not in existing_data.column_names
        or min_date is None
        or max_date is None
    ):
        return existing_data

    date_type = existing_data.schema.field(date_col).type
    dates = existing_data.column(date_col)
    start = _arrow_temporal_scalar(min_date, date_type)
    after_end = _arrow_temporal_scalar(
        max_date + dt.timedelta(days=1),
        date_type,
    )
    before_window = pc.less(dates, start)
    after_window = pc.greater_equal(dates, after_end)
    return existing_data.filter(pc.or_(before_window, after_window))


def _make_date_range(
    start_date: dt.date | None,
    end_date: dt.date | None,
) -> DateRange | None:
    """Build a DateRange when both endpoints are present and ordered."""
    if start_date is None or end_date is None or start_date > end_date:
        return None
    return DateRange(start_date=start_date, end_date=end_date)


def _spec_date_window(
    specs: list[PartitionSpec],
) -> tuple[dt.date | None, dt.date | None]:
    """Return the min/max requested dates across partition specs."""
    starts = [
        spec.start_date for spec in specs if spec.start_date is not None
    ]
    ends = [spec.end_date for spec in specs if spec.end_date is not None]
    return (min(starts) if starts else None, max(ends) if ends else None)


def _lineage_date_ranges(
    previous_end: dt.date | None,
    written_start: dt.date | None,
    written_end: dt.date | None,
    operation: str,
) -> tuple[list[DateRange], list[DateRange]]:
    """Split written dates into added vs rewritten lineage ranges."""
    written = _make_date_range(written_start, written_end)
    if written is None:
        return ([], [])

    if operation in ("initial", "backfill"):
        return ([written], [])
    if operation == "restate":
        return ([], [written])
    if previous_end is None or written.start_date > previous_end:
        return ([written], [])

    rewritten = _make_date_range(
        written.start_date,
        min(written.end_date, previous_end),
    )
    added = _make_date_range(
        max(written.start_date, previous_end + dt.timedelta(days=1)),
        written.end_date,
    )
    return (
        [added] if added is not None else [],
        [rewritten] if rewritten is not None else [],
    )


def _build_snapshot_lineage(
    dataset: "DatedParquetDataset",
    plan: UpdatePlan,
    previous_metadata: Any,
    requested_start: dt.date | None,
    requested_end: dt.date | None,
    written_start: dt.date | None,
    written_end: dt.date | None,
    backfill: bool,
    restate: bool,
    instruments: list[str] | None,
) -> SnapshotLineage:
    """Build SnapshotLineage for a completed update."""
    if previous_metadata is None:
        operation = "initial"
        base_snapshot = None
        first_snapshot_id = plan.suffix
        previous_end = None
    else:
        if restate:
            operation = "restate"
        elif backfill:
            operation = "backfill"
        else:
            operation = "update"
        base_snapshot = previous_metadata.suffix
        previous_lineage = getattr(previous_metadata, "lineage", None)
        first_snapshot_id = (
            getattr(previous_lineage, "first_snapshot_id", None)
            or previous_metadata.suffix
        )
        previous_end = previous_metadata.cache_end_date

    added, rewritten = _lineage_date_ranges(
        previous_end,
        written_start,
        written_end,
        operation,
    )

    if dataset.instrument_column is None:
        instrument_scope = "none"
        instrument_values = None
    elif instruments is None:
        instrument_scope = "all"
        instrument_values = None
    else:
        instrument_scope = "subset"
        instrument_values = [str(value) for value in instruments]

    return SnapshotLineage(
        base_snapshot=base_snapshot,
        first_snapshot_id=first_snapshot_id,
        operation=operation,
        requested_date_range=_make_date_range(requested_start, requested_end),
        added_date_ranges=added,
        rewritten_date_ranges=rewritten,
        instrument_column=dataset.instrument_column,
        instrument_scope=instrument_scope,
        instruments=instrument_values,
    )


def _apply_yaml_transforms(
    rel: "duckdb.DuckDBPyRelation",
    transforms: dict[str, Any] | None,
) -> "duckdb.DuckDBPyRelation":
    """
    Apply YAML-specified transforms to a DuckDB relation.

    Transforms are applied in order:
    1. columns_to_rename
    2. columns_to_drop
    3. dropna_columns
    4. dedup_columns

    Args:
        rel: The input DuckDB relation.
        transforms: Dict with transform settings (from DatasetConfig).

    Returns:
        The transformed DuckDB relation.
    """
    if not transforms:
        return rel

    # 1. Rename columns
    columns_to_rename = transforms.get("columns_to_rename", {})
    if columns_to_rename:
        current_cols = rel.columns
        other_cols = [
            f'"{c}"' for c in current_cols if c not in columns_to_rename
        ]
        renames = ", ".join(
            f'"{old}" AS "{new}"' for old, new in columns_to_rename.items()
        )
        select_clause = (
            ", ".join(other_cols + [renames]) if other_cols else renames
        )
        rel = duckdb.sql(f"SELECT {select_clause} FROM rel")

    # 2. Drop columns
    columns_to_drop = transforms.get("columns_to_drop", [])
    if columns_to_drop:
        current_cols = rel.columns
        keep_cols = [c for c in current_cols if c not in columns_to_drop]
        select_clause = ", ".join(f'"{c}"' for c in keep_cols)
        rel = duckdb.sql(f"SELECT {select_clause} FROM rel")

    # 3. Drop rows with nulls
    dropna_columns = transforms.get("dropna_columns", [])
    if dropna_columns:
        conditions = " AND ".join(
            f'"{c}" IS NOT NULL' for c in dropna_columns
        )
        rel = duckdb.sql(f"SELECT * FROM rel WHERE {conditions}")

    # 4. Deduplicate
    # Preserves input order: "first" keeps earliest row, "last" keeps latest
    # row as they appear in the source data. Uses _input_order to track
    # original row position since rowid is not available after SQL transforms.
    dedup_columns = transforms.get("dedup_columns", [])
    if dedup_columns:
        dedup_keep = transforms.get("dedup_keep", "last")
        key_cols = ", ".join(f'"{c}"' for c in dedup_columns)

        # Add row number to track input order
        rel = duckdb.sql(
            "SELECT *, ROW_NUMBER() OVER () AS _input_order FROM rel"
        )

        if dedup_keep == "first":
            rel = duckdb.sql(
                f"SELECT * FROM rel QUALIFY ROW_NUMBER() OVER "
                f"(PARTITION BY {key_cols} ORDER BY _input_order) = 1"
            )
        else:  # last
            rel = duckdb.sql(
                f"SELECT * FROM rel QUALIFY ROW_NUMBER() OVER "
                f"(PARTITION BY {key_cols} ORDER BY _input_order DESC) = 1"
            )

        # Remove helper column
        current_cols = [c for c in rel.columns if c != "_input_order"]
        select_clause = ", ".join(f'"{c}"' for c in current_cols)
        rel = duckdb.sql(f"SELECT {select_clause} FROM rel")

    return rel


def execute_update(
    dataset: "DatedParquetDataset",
    source: "DataSource",
    plan: UpdatePlan,
    cleaner: "DataCleaner | None" = None,
    dry_run: bool = False,
    backfill: bool = False,
    restate: bool = False,
    transforms: dict[str, Any] | None = None,
    yaml_config: dict[str, Any] | None = None,
    update_start: dt.date | None = None,
    update_end: dt.date | None = None,
    instruments: list[str] | None = None,
) -> str:
    """
    Execute the update plan.

    Args:
        dataset: The DatedParquetDataset to update.
        source: The DataSource providing data.
        plan: The UpdatePlan to execute.
        cleaner: Optional DataCleaner to apply.
        dry_run: If True, don't write any files.
        backfill: If True, preserve existing data and only add new rows.
        restate: If True, replace data for the requested date range.
        transforms: Optional YAML transform settings (columns_to_rename,
            columns_to_drop, dropna_columns, dedup_columns, dedup_keep).
        yaml_config: Full YAML configuration to store in snapshot metadata.
            If provided, enables updating without the original YAML file.
        update_start: Requested update start date for lineage.
        update_end: Requested update end date for lineage.
        instruments: Requested instrument subset for lineage.

    Returns:
        The new snapshot suffix.

    Raises:
        DataSourceError: If data fetch fails.
        SchemaMismatchError: If schema validation fails.
        SnapshotPublishError: If publishing fails.
    """
    assert not dataset.is_gcs
    previous_metadata = dataset._metadata
    previous_suffix: str | None = (
        previous_metadata.suffix if previous_metadata is not None else None
    )
    if instruments is None:
        instruments = getattr(source, "instruments", None)
    if update_start is None or update_end is None:
        spec_start, spec_end = _spec_date_window(plan.specs_in_order)
        update_start = update_start or spec_start
        update_end = update_end or spec_end
    merged_schema: pa.Schema | None = None
    min_date: dt.date | None = None
    max_date: dt.date | None = None
    partition_values_seen: dict[str, set] = defaultdict(set)

    logger.debug(
        f"Executing update for {dataset.name}: "
        f"{len(plan.specs_in_order)} specs, {len(plan.groups)} groups, "
        f"suffix={plan.suffix}"
    )

    try:
        # Process each spec in order
        for spec in plan.specs_in_order:
            if dry_run:
                continue

            # Fetch data
            try:
                data = source.get_data(spec)
            except Exception as e:
                raise DataSourceError(
                    f"Failed to fetch data for partition "
                    f"{spec.partition_values}: {e}",
                    source_class=source.__class__.__name__,
                    partition_info=spec.partition_values,
                ) from e

            # None means the source has no data for this partition — skip it
            if data is None:
                logger.debug(
                    f"Skipping partition {spec.partition_values}: "
                    f"source returned None"
                )
                continue

            # Convert to Arrow
            table = convert_to_arrow(data)

            # Apply YAML transforms and cleaner via DuckDB
            if transforms or cleaner:
                rel = duckdb.from_arrow(table)

                # 1. Apply YAML transforms first
                if transforms:
                    rel = _apply_yaml_transforms(rel, transforms)

                # 2. Then apply custom cleaner
                if cleaner is not None:
                    rel = cleaner(rel)

                table = rel.to_arrow_table()

            # Validate schema
            merged_schema = validate_schema(
                table.schema,
                merged_schema,
                dataset.name,
            )

            # Track date range
            if dataset.date_col in table.column_names:
                chunk_min, chunk_max = _arrow_date_range(
                    table,
                    dataset.date_col,
                )
                if chunk_min is not None:
                    if min_date is None or chunk_min < min_date:
                        min_date = chunk_min
                if chunk_max is not None:
                    if max_date is None or chunk_max > max_date:
                        max_date = chunk_max

            # Track partition values
            for col, val in spec.partition_values.items():
                partition_values_seen[col].add(val)

            # Strip partition columns from data (they are virtual per spec)
            cols_to_strip = set(spec.partition_values.keys())
            cols_in_table = set(table.column_names)
            cols_to_drop = list(cols_to_strip & cols_in_table)
            if cols_to_drop:
                table = table.drop(cols_to_drop)

            # Sort chunk by sort_columns before writing (enables efficient
            # merge)
            if dataset.sort_columns:
                table = _sort_table(table, dataset.sort_columns)

            # Write temp file
            assert spec.temp_file_path is not None
            temp_path = Path(spec.temp_file_path)
            temp_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                table,
                temp_path,
                row_group_size=dataset.row_group_size,
            )

        if dry_run:
            logger.debug(
                f"Dry run complete for {dataset.name}, suffix={plan.suffix}"
            )
            return plan.suffix

        logger.debug(f"Consolidating {len(plan.groups)} partition groups")

        # Import here to avoid circular import
        from ionbus_parquet_cache.dated_dataset import FileMetadata

        # Get existing files from metadata for merge logic
        existing_files_by_partition: dict[tuple, Path] = {}
        if previous_metadata:
            for fm in previous_metadata.files:
                full_path = cast(Path, dataset.dataset_dir) / fm.path
                if full_path.exists():
                    key = tuple(sorted(fm.partition_values.items()))
                    existing_files_by_partition[key] = full_path

        # Consolidate chunks and move to final locations
        # Build list of FileMetadata objects
        file_metadata_list: list[FileMetadata] = []
        updated_partition_keys: set[tuple] = set()

        for key, group in plan.groups.items():
            updated_partition_keys.add(key)

            # Read all new data chunks (only files that were actually written;
            # a file is absent when get_data() returned None for that spec)
            tables = [
                pq.read_table(f) for f in group.temp_files if f.exists()
            ]
            if not tables:
                continue  # Every spec in this group was skipped
            new_data = pa.concat_tables(tables)

            # Check for existing partition data
            partition_key = tuple(sorted(group.partition_values.items()))
            existing_path = existing_files_by_partition.get(partition_key)

            if existing_path and existing_path.exists():
                existing_data = pq.read_table(existing_path)
                existing_cols_to_drop = sorted(
                    set(group.partition_values)
                    & set(existing_data.column_names)
                )
                if existing_cols_to_drop:
                    existing_data = existing_data.drop(existing_cols_to_drop)

                if backfill:
                    # Backfill: keep ALL existing rows, add only new dates
                    new_data = _filter_new_dates(
                        new_data,
                        existing_data,
                        dataset.date_col,
                    )

                    combined = pa.concat_tables([existing_data, new_data])
                else:
                    # Normal/Restate: remove rows in update window from existing
                    existing_data = _filter_out_date_window(
                        existing_data,
                        dataset.date_col,
                        min_date,
                        max_date,
                    )

                    combined = pa.concat_tables([existing_data, new_data])
            else:
                combined = new_data

            # Sort by sort_columns
            if dataset.sort_columns:
                combined = _sort_table(combined, dataset.sort_columns)

            # Strip partition columns before final write (they are virtual)
            cols_to_strip = set(group.partition_values.keys())
            cols_in_table = set(combined.column_names)
            cols_to_drop = list(cols_to_strip & cols_in_table)
            if cols_to_drop:
                combined = combined.drop(cols_to_drop)

            final_path = group.final_path
            assert final_path is not None
            final_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                combined,
                final_path,
                row_group_size=dataset.row_group_size,
            )
            file_metadata_list.append(
                FileMetadata.from_path(
                    final_path,
                    cast(Path, dataset.dataset_dir),
                    dict(group.partition_values),
                )
            )

            # Clean up temp files
            for temp_file in group.temp_files:
                if temp_file.exists():
                    temp_file.unlink()

        written_min_date = min_date
        written_max_date = max_date

        # Include unchanged files from previous snapshot and merge metadata
        if previous_metadata:
            for fm in previous_metadata.files:
                key = tuple(sorted(fm.partition_values.items()))
                if key not in updated_partition_keys:
                    full_path = cast(Path, dataset.dataset_dir) / fm.path
                    if full_path.exists():
                        file_metadata_list.append(fm)
                        # Track partition values from unchanged files
                        for col, val in fm.partition_values.items():
                            partition_values_seen[col].add(val)

            # Extend date range to include previous snapshot dates
            if previous_metadata.cache_start_date:
                if (
                    min_date is None
                    or previous_metadata.cache_start_date < min_date
                ):
                    min_date = previous_metadata.cache_start_date
            if previous_metadata.cache_end_date:
                if (
                    max_date is None
                    or previous_metadata.cache_end_date > max_date
                ):
                    max_date = previous_metadata.cache_end_date

        # Build partition values dict for metadata
        pv_dict = {
            col: sorted(list(vals))
            for col, vals in partition_values_seen.items()
        }

        logger.debug(
            f"Publishing snapshot for {dataset.name}: "
            f"{len(file_metadata_list)} files, date range {min_date} to "
            f"{max_date}"
        )

        provenance_ref = None
        try:
            provenance_payload = source.get_provenance(
                plan.suffix,
                previous_suffix,
            )
            if not isinstance(provenance_payload, dict):
                raise ValidationError(
                    f"{source.__class__.__name__}.get_provenance() must "
                    f"return a dict, got {type(provenance_payload).__name__}"
                )
            if provenance_payload:
                provenance_ref = dataset._write_provenance_sidecar(
                    plan.suffix,
                    provenance_payload,
                )

            lineage = _build_snapshot_lineage(
                dataset=dataset,
                plan=plan,
                previous_metadata=previous_metadata,
                requested_start=update_start,
                requested_end=update_end,
                written_start=written_min_date,
                written_end=written_max_date,
                backfill=backfill,
                restate=restate,
                instruments=instruments,
            )

            # Use plan.suffix so metadata matches the data file names.
            dataset._publish_snapshot(
                files=file_metadata_list,
                schema=merged_schema or pa.schema([]),
                cache_start_date=min_date,
                cache_end_date=max_date,
                partition_values=pv_dict,
                yaml_config=yaml_config,
                lineage=lineage,
                provenance=provenance_ref,
                suffix=plan.suffix,
            )
        except Exception:
            dataset._delete_provenance_sidecar(provenance_ref)
            raise

        logger.info(f"Published snapshot {plan.suffix} for {dataset.name}")

        source.on_update_complete(plan.suffix, previous_suffix)

        return plan.suffix

    except Exception:
        # Clean up temp files on failure
        for group in plan.groups.values():
            for temp_file in group.temp_files:
                if temp_file.exists():
                    temp_file.unlink()
        raise
