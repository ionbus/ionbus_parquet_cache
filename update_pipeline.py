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
import tempfile
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from ionbus_utils.logging_utils import logger

from ionbus_parquet_cache.exceptions import (
    DataSourceError,
    SchemaMismatchError,
    SnapshotNotFoundError,
    SnapshotPublishError,
    ValidationError,
)
from ionbus_parquet_cache.partition import (
    PartitionSpec,
    date_partition_column_name,
    date_partition_value,
)
from ionbus_parquet_cache.snapshot import generate_snapshot_suffix

if TYPE_CHECKING:
    from ionbus_parquet_cache.data_cleaner import DataCleaner
    from ionbus_parquet_cache.data_source import DataSource
    from ionbus_parquet_cache.dated_dataset import DatedParquetDataset, FileMetadata


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
) -> UpdatePlan:
    """
    Build a complete update plan before fetching any data.

    Args:
        dataset: The DatedParquetDataset being updated.
        specs: List of PartitionSpecs from the DataSource.
        temp_dir: Directory for temporary files.

    Returns:
        UpdatePlan with all file paths assigned.
    """
    suffix = generate_snapshot_suffix()
    plan = UpdatePlan(suffix=suffix)

    # Group specs by partition_values
    groups: dict[tuple, WriteGroup] = {}

    for spec in specs:
        key = spec.partition_key
        if key not in groups:
            groups[key] = WriteGroup(partition_values=dict(spec.partition_values))
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
        partition_dir = dataset.dataset_dir
        for part in path_parts:
            partition_dir = partition_dir / part

        # File name based on partition values
        # Use all partition values from the group (not just dataset.partition_columns)
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
                temp_name = f"{file_base}_{suffix}_{i+1:02d}.parquet"
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
    for field in new_schema:
        if field.name in existing_schema.names:
            existing_field = existing_schema.field(field.name)
            if not field.type.equals(existing_field.type):
                # Check if it's a compatible promotion
                if not _types_compatible(existing_field.type, field.type):
                    raise SchemaMismatchError(
                        f"Column '{field.name}' type changed from "
                        f"{existing_field.type} to {field.type}",
                        column=field.name,
                        expected_type=str(existing_field.type),
                        actual_type=str(field.type),
                    )
                # Use the promoted (wider) type
                merged_fields.append(field)
            else:
                merged_fields.append(field)
        else:
            # New column
            merged_fields.append(field)
        seen_names.add(field.name)

    # Then, add columns from existing_schema that aren't in new_schema
    # These columns will be null in new data but present in old files
    for field in existing_schema:
        if field.name not in seen_names:
            merged_fields.append(field)

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
            f'"{c}"' for c in current_cols
            if c not in columns_to_rename
        ]
        renames = ", ".join(
            f'"{old}" AS "{new}"'
            for old, new in columns_to_rename.items()
        )
        select_clause = ", ".join(other_cols + [renames]) if other_cols else renames
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
    transforms: dict[str, Any] | None = None,
    yaml_config: dict[str, Any] | None = None,
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
        transforms: Optional YAML transform settings (columns_to_rename,
            columns_to_drop, dropna_columns, dedup_columns, dedup_keep).
        yaml_config: Full YAML configuration to store in snapshot metadata.
            If provided, enables updating without the original YAML file.

    Returns:
        The new snapshot suffix.

    Raises:
        DataSourceError: If data fetch fails.
        SchemaMismatchError: If schema validation fails.
        SnapshotPublishError: If publishing fails.
    """
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
                    f"Failed to fetch data for partition {spec.partition_values}: {e}",
                    source_class=source.__class__.__name__,
                    partition_info=spec.partition_values,
                ) from e

            # None means the source has no data for this partition — skip it
            if data is None:
                logger.debug(
                    f"Skipping partition {spec.partition_values}: source returned None"
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

                table = rel.fetch_arrow_table()

            # Validate schema
            merged_schema = validate_schema(
                table.schema,
                merged_schema,
                dataset.name,
            )

            # Track date range
            if dataset.date_col in table.column_names:
                dates = table.column(dataset.date_col).to_pandas()
                if len(dates) > 0:
                    chunk_min = dates.min()
                    chunk_max = dates.max()
                    if hasattr(chunk_min, "date"):
                        chunk_min = chunk_min.date()
                        chunk_max = chunk_max.date()
                    if min_date is None or chunk_min < min_date:
                        min_date = chunk_min
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

            # Sort chunk by sort_columns before writing (enables efficient merge)
            if dataset.sort_columns:
                sort_cols = [c for c in dataset.sort_columns if c in table.column_names]
                if sort_cols:
                    df = table.to_pandas()
                    df = df.sort_values(sort_cols)
                    table = pa.Table.from_pandas(df, preserve_index=False)

            # Write temp file
            temp_path = Path(spec.temp_file_path)
            temp_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, temp_path)

        if dry_run:
            logger.debug(f"Dry run complete for {dataset.name}, suffix={plan.suffix}")
            return plan.suffix

        logger.debug(f"Consolidating {len(plan.groups)} partition groups")

        # Import here to avoid circular import
        from ionbus_parquet_cache.dated_dataset import FileMetadata

        # Get existing files from metadata for merge logic
        existing_files_by_partition: dict[tuple, Path] = {}
        if dataset._metadata:
            for fm in dataset._metadata.files:
                full_path = dataset.dataset_dir / fm.path
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
            tables = [pq.read_table(f) for f in group.temp_files if f.exists()]
            if not tables:
                continue  # Every spec in this group was skipped
            new_data = pa.concat_tables(tables)

            # Check for existing partition data
            partition_key = tuple(sorted(group.partition_values.items()))
            existing_path = existing_files_by_partition.get(partition_key)

            if existing_path and existing_path.exists():
                existing_data = pq.read_table(existing_path)

                if backfill:
                    # Backfill: keep ALL existing rows, add only new dates
                    if dataset.date_col in new_data.column_names:
                        existing_dates = set(
                            existing_data.column(dataset.date_col).to_pylist()
                        )
                        # Filter new_data to only dates not in existing
                        new_df = new_data.to_pandas()
                        new_df = new_df[~new_df[dataset.date_col].isin(existing_dates)]
                        new_data = pa.Table.from_pandas(new_df, preserve_index=False)

                    combined = pa.concat_tables([existing_data, new_data])
                else:
                    # Normal/Restate: remove rows in update window from existing
                    if dataset.date_col in existing_data.column_names and min_date:
                        existing_df = existing_data.to_pandas()
                        # Keep rows outside the update window
                        mask = (existing_df[dataset.date_col] < pd.Timestamp(min_date)) | \
                               (existing_df[dataset.date_col] > pd.Timestamp(max_date))
                        existing_df = existing_df[mask]
                        existing_data = pa.Table.from_pandas(
                            existing_df, preserve_index=False
                        )

                    combined = pa.concat_tables([existing_data, new_data])
            else:
                combined = new_data

            # Sort by sort_columns
            if dataset.sort_columns and len(combined) > 0:
                sort_cols = [c for c in dataset.sort_columns if c in combined.column_names]
                if sort_cols:
                    df = combined.to_pandas()
                    df = df.sort_values(sort_cols)
                    combined = pa.Table.from_pandas(df, preserve_index=False)

            # Strip partition columns before final write (they are virtual)
            cols_to_strip = set(group.partition_values.keys())
            cols_in_table = set(combined.column_names)
            cols_to_drop = list(cols_to_strip & cols_in_table)
            if cols_to_drop:
                combined = combined.drop(cols_to_drop)

            final_path = group.final_path
            final_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(combined, final_path)
            file_metadata_list.append(
                FileMetadata.from_path(
                    final_path, dataset.dataset_dir, dict(group.partition_values)
                )
            )

            # Clean up temp files
            for temp_file in group.temp_files:
                if temp_file.exists():
                    temp_file.unlink()

        # Include unchanged files from previous snapshot and merge metadata
        if dataset._metadata:
            for fm in dataset._metadata.files:
                key = tuple(sorted(fm.partition_values.items()))
                if key not in updated_partition_keys:
                    full_path = dataset.dataset_dir / fm.path
                    if full_path.exists():
                        file_metadata_list.append(fm)
                        # Track partition values from unchanged files
                        for col, val in fm.partition_values.items():
                            partition_values_seen[col].add(val)

            # Extend date range to include previous snapshot dates
            if dataset._metadata.cache_start_date:
                if min_date is None or dataset._metadata.cache_start_date < min_date:
                    min_date = dataset._metadata.cache_start_date
            if dataset._metadata.cache_end_date:
                if max_date is None or dataset._metadata.cache_end_date > max_date:
                    max_date = dataset._metadata.cache_end_date

        # Build partition values dict for metadata
        pv_dict = {
            col: sorted(list(vals)) for col, vals in partition_values_seen.items()
        }

        logger.debug(
            f"Publishing snapshot for {dataset.name}: "
            f"{len(file_metadata_list)} files, date range {min_date} to {max_date}"
        )

        # Publish snapshot (use the suffix from the plan to match data files)
        dataset._publish_snapshot(
            files=file_metadata_list,
            schema=merged_schema or pa.schema([]),
            cache_start_date=min_date,
            cache_end_date=max_date,
            partition_values=pv_dict,
            yaml_config=yaml_config,
            suffix=plan.suffix,
        )

        logger.info(f"Published snapshot {plan.suffix} for {dataset.name}")

        return plan.suffix

    except Exception:
        # Clean up temp files on failure
        for group in plan.groups.values():
            for temp_file in group.temp_files:
                if temp_file.exists():
                    temp_file.unlink()
        raise
