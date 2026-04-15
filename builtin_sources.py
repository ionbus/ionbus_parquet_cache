"""
Built-in DataSource implementations.

Provides standard data sources for common use cases:
- HiveParquetSource: Read from Hive-partitioned Parquet directories
- DPDSource: Read from another DatedParquetDataset
"""

from __future__ import annotations

import datetime as dt
import re
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pds

from ionbus_parquet_cache.data_source import DataSource
from ionbus_parquet_cache.dated_dataset import DatedParquetDataset
from ionbus_parquet_cache.exceptions import ConfigurationError, DataSourceError
from ionbus_parquet_cache.partition import (
    PartitionSpec,
    date_partition_column_name,
)


class HiveParquetSource(DataSource):
    """
    DataSource that reads from Hive-partitioned Parquet directories.

    Reads from existing Parquet files, either a single file or a
    Hive-partitioned directory structure. Extracts dates from file
    paths or data content.

    Constructor parameters (via source_init_args):
        path: Base path to the Parquet source (directory or single file).
        glob_pattern: Glob pattern for finding Parquet files within path.
            Default: "**/*.parquet". Ignored if path is a single file.
        chunk_values: Optional dict mapping chunk dimension names to lists
            of values. Creates multiple PartitionSpecs with same partition_key
            but different chunk_info for parallel fetching.

    Instance Attributes:
        source_path: Path to the Parquet source.
        glob_pattern: Glob pattern for finding files.

    Example YAML:
        datasets:
          md.futures_daily:
            source_location: ""  # empty = built-in
            source_class_name: HiveParquetSource
            source_init_args:
              path: "/data/parquet/futures"
              glob_pattern: "**/*.parquet"
    """

    # Instance attributes (declared for documentation, set in __init__)
    source_path: Path
    glob_pattern: str

    # Private cached state
    _pyarrow_dataset: pds.Dataset | None
    _min_date: dt.date | None
    _max_date: dt.date | None

    def __init__(
        self,
        dataset: "DatedParquetDataset",
        path: str = "",
        glob_pattern: str = "**/*.parquet",
        chunk_values: dict[str, list[Any]] | None = None,
        **kwargs: Any,
    ):
        """
        Initialize HiveParquetSource.

        Args:
            dataset: The owning DatedParquetDataset.
            path: Base path to the Parquet source.
            glob_pattern: Glob pattern for finding files.
            chunk_values: Optional chunk dimension values for parallel fetching.
            **kwargs: Additional arguments.
        """
        super().__init__(dataset, **kwargs)

        # Apply chunk_values if provided
        if chunk_values:
            self._chunk_values = dict(chunk_values)

        if not path:
            raise ConfigurationError(
                "HiveParquetSource requires 'path' in source_init_args"
            )

        self.source_path = Path(path)
        self.glob_pattern = glob_pattern

        # Discover partition values from Hive directory structure
        self._discover_partition_values()
        self._pyarrow_dataset = None
        self._min_date = None
        self._max_date = None

    def _discover_partition_values(self) -> None:
        """Discover partition values from Hive directory structure."""
        if not self.source_path.exists() or not self.source_path.is_dir():
            return

        # Get configured partition columns
        partition_cols = self.dataset.partition_columns or []
        if not partition_cols:
            return

        # Scan directory structure for Hive partition directories
        discovered: dict[str, set[str]] = {col: set() for col in partition_cols}

        for item in self.source_path.rglob("*"):
            if item.is_dir() and "=" in item.name:
                parts = item.name.split("=", 1)
                if len(parts) == 2:
                    col_name, col_value = parts
                    if col_name in discovered:
                        discovered[col_name].add(col_value)

        # Store discovered values
        for col, values in discovered.items():
            if values:
                self._partition_values[col] = sorted(values)

    def available_dates(self) -> tuple[dt.date, dt.date]:
        """
        Return the date range available in the source.

        Scans the source to determine min/max dates.
        """
        if self._min_date is None or self._max_date is None:
            self._scan_source_dates()

        if self._min_date is None or self._max_date is None:
            raise DataSourceError(
                "Cannot determine date range from source",
                source_class="HiveParquetSource",
            )

        return (self._min_date, self._max_date)

    def _scan_source_dates(self) -> None:
        """Scan source files to determine date range."""
        if not self.source_path.exists():
            raise DataSourceError(
                f"Source path does not exist: {self.source_path}",
                source_class="HiveParquetSource",
            )

        # Try to extract dates from file paths first
        dates_from_paths = self._extract_dates_from_paths()
        if dates_from_paths:
            self._min_date = min(dates_from_paths)
            self._max_date = max(dates_from_paths)
            return

        # Fall back to reading data
        ds = self._get_pyarrow_dataset()
        if ds is None:
            return

        # Try to get date range from dataset
        date_col = self.dataset.date_col
        if date_col in ds.schema.names:
            # Read just the date column to find min/max
            table = ds.to_table(columns=[date_col])
            if table.num_rows > 0:
                dates = table.column(date_col).to_pandas()
                min_val = dates.min()
                max_val = dates.max()

                # Convert to date objects based on type
                if hasattr(min_val, "date"):
                    # Timestamp or datetime
                    self._min_date = min_val.date()
                    self._max_date = max_val.date()
                elif isinstance(min_val, str):
                    # ISO date string (YYYY-MM-DD)
                    self._min_date = dt.date.fromisoformat(min_val[:10])
                    self._max_date = dt.date.fromisoformat(max_val[:10])
                elif isinstance(min_val, dt.date):
                    self._min_date = min_val
                    self._max_date = max_val

    def _extract_dates_from_paths(self) -> list[dt.date]:
        """Extract dates from file paths using YYYY-MM-DD pattern."""
        dates = []
        date_pattern = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

        if self.source_path.is_file():
            files = [self.source_path]
        else:
            files = list(self.source_path.glob(self.glob_pattern))

        for f in files:
            match = date_pattern.search(str(f))
            if match:
                try:
                    d = dt.date(
                        int(match.group(1)),
                        int(match.group(2)),
                        int(match.group(3)),
                    )
                    dates.append(d)
                except ValueError:
                    pass

        return dates

    def _get_pyarrow_dataset(self) -> pds.Dataset | None:
        """Get or create the PyArrow dataset."""
        if self._pyarrow_dataset is not None:
            return self._pyarrow_dataset

        if not self.source_path.exists():
            return None

        if self.source_path.is_file():
            self._pyarrow_dataset = pds.dataset(
                self.source_path,
                format="parquet",
            )
        else:
            files = list(self.source_path.glob(self.glob_pattern))
            if not files:
                return None

            # Filter to only files inside Hive partition directories
            # (exclude loose parquet files at root)
            hive_files = [
                f for f in files
                if any("=" in p for p in f.relative_to(self.source_path).parts)
            ]
            if not hive_files:
                # Fall back to all files if no Hive structure detected
                hive_files = files

            # Build unified schema with string type for partition columns
            # This prevents type mismatches (string vs large_string vs dict)
            # when PyArrow combines partitions
            unified_schema = self._build_unified_schema(hive_files)

            self._pyarrow_dataset = pds.dataset(
                hive_files,
                format="parquet",
                partitioning="hive",
                schema=unified_schema,
            )

        return self._pyarrow_dataset

    def _build_unified_schema(self, files: list[Path]) -> pa.Schema | None:
        """Build a unified schema with string types for partition columns."""
        if not files:
            return None

        # Read schema from first file
        import pyarrow.parquet as pq

        try:
            base_schema = pq.read_schema(files[0])
        except Exception:
            return None

        # Get partition columns that need normalization
        partition_cols = set(self.dataset.partition_columns or [])
        date_part_col = date_partition_column_name(self.dataset.date_partition)
        if date_part_col:
            partition_cols.add(date_part_col)

        # Build new schema with string type for partition columns
        new_fields = []
        for field in base_schema:
            if field.name in partition_cols:
                # Normalize to string type
                is_large = pa.types.is_large_string(field.type)
                is_dict = pa.types.is_dictionary(field.type)
                if is_large or is_dict:
                    new_fields.append(pa.field(field.name, pa.string()))
                else:
                    new_fields.append(field)
            else:
                new_fields.append(field)

        return pa.schema(new_fields)

    def get_data(self, partition_spec: PartitionSpec) -> pa.Table:
        """
        Return data for the partition.

        Reads from the source dataset with date filtering.
        """
        ds = self._get_pyarrow_dataset()
        if ds is None:
            raise DataSourceError(
                "No data found in source",
                source_class="HiveParquetSource",
            )

        date_col = self.dataset.date_col

        # Build filter expression
        filters = None
        if date_col in ds.schema.names:
            # Create date filter
            start = partition_spec.start_date
            end = partition_spec.end_date

            # Convert to appropriate type based on schema
            date_type = ds.schema.field(date_col).type
            if pa.types.is_timestamp(date_type):
                start = pa.scalar(
                    dt.datetime.combine(start, dt.time.min),
                    type=date_type,
                )
                end = pa.scalar(
                    dt.datetime.combine(end, dt.time.max),
                    type=date_type,
                )
            elif pa.types.is_date(date_type):
                start = pa.scalar(start, type=date_type)
                end = pa.scalar(end, type=date_type)
            elif (
                pa.types.is_string(date_type)
                or pa.types.is_large_string(date_type)
            ):
                # String dates in ISO format (YYYY-MM-DD) sort correctly
                start = start.isoformat()
                end = end.isoformat()

            filters = (pds.field(date_col) >= start) & (
                pds.field(date_col) <= end
            )

        # Read with filter
        if filters is not None:
            table = ds.to_table(filter=filters)
        else:
            table = ds.to_table()

        # Normalize partition column types to string
        # PyArrow Hive partitioning can produce type mismatches
        # (string vs large_string vs dictionary) when combining partitions.
        # Partition columns are short identifiers, so string type is sufficient.
        partition_cols = set(self.dataset.partition_columns or [])
        # Add date partition column if present
        date_part_col = date_partition_column_name(self.dataset.date_partition)
        if date_part_col:
            partition_cols.add(date_part_col)

        if partition_cols:
            new_fields = []
            casts_needed = False
            for field in table.schema:
                if field.name in partition_cols:
                    # Cast large_string or dictionary to string
                    is_large = pa.types.is_large_string(field.type)
                    is_dict = pa.types.is_dictionary(field.type)
                    if is_large or is_dict:
                        new_fields.append(pa.field(field.name, pa.string()))
                        casts_needed = True
                    else:
                        new_fields.append(field)
                else:
                    new_fields.append(field)

            if casts_needed:
                table = table.cast(pa.schema(new_fields))

        return table


class DPDSource(DataSource):
    """
    DataSource that reads from another DatedParquetDataset.

    Useful for building derived datasets or chaining transformations.
    The source DPD can be in the same cache or a different cache.

    Constructor parameters (via source_init_args):
        dpd_name: Name of the source DatedParquetDataset.
        dpd_cache_name: Name of the cache in CacheRegistry containing the
            source DPD. Use this when working with CacheRegistry.
        dpd_cache_path: Path to the cache directory containing the source DPD.
            Use this for direct path access without CacheRegistry.
        chunk_values: Optional dict mapping chunk dimension names to lists
            of values. Creates multiple PartitionSpecs with same partition_key
            but different chunk_info for parallel fetching.

    If neither dpd_cache_name nor dpd_cache_path is provided, uses the same
    cache directory as the target DPD.

    Instance Attributes:
        dpd_name: Name of the source DPD.
        dpd_cache_name: Cache name for CacheRegistry lookup (or None).
        dpd_cache_path: Path to the cache directory.

    Example YAML:
        datasets:
          derived.futures_clean:
            source_location: ""  # empty = built-in
            source_class_name: DPDSource
            source_init_args:
              dpd_name: "md.futures_raw"
              dpd_cache_name: "market_data"  # use CacheRegistry
              # OR
              dpd_cache_path: "/path/to/source/cache"  # direct path
    """

    # Instance attributes (declared for documentation, set in __init__)
    dpd_name: str
    dpd_cache_name: str | None
    dpd_cache_path: Path

    # Private cached state
    _source_dpd: "DatedParquetDataset | None"

    def __init__(
        self,
        dataset: "DatedParquetDataset",
        dpd_name: str = "",
        dpd_cache_name: str | None = None,
        dpd_cache_path: str | None = None,
        chunk_values: dict[str, list[Any]] | None = None,
        **kwargs: Any,
    ):
        """
        Initialize DPDSource.

        Args:
            dataset: The owning DatedParquetDataset.
            dpd_name: Name of the source DPD.
            dpd_cache_name: Cache name for CacheRegistry lookup.
            dpd_cache_path: Direct path to source cache directory.
            chunk_values: Optional chunk dimension values for parallel fetching.
            **kwargs: Additional arguments.
        """
        super().__init__(dataset, **kwargs)

        # Apply chunk_values if provided
        if chunk_values:
            self._chunk_values = dict(chunk_values)

        if not dpd_name:
            raise ConfigurationError(
                "DPDSource requires 'dpd_name' in source_init_args"
            )

        self.dpd_name = dpd_name
        self.dpd_cache_name = dpd_cache_name
        # Resolve cache path: explicit path, or same cache as target
        if dpd_cache_path:
            self.dpd_cache_path = Path(dpd_cache_path)
        else:
            self.dpd_cache_path = dataset.cache_dir
        self._source_dpd = None

    def _get_source_dpd(self) -> DatedParquetDataset:
        """Get or create the source DPD instance."""
        if self._source_dpd is not None:
            return self._source_dpd

        # If cache name provided, use CacheRegistry
        if self.dpd_cache_name:
            from ionbus_parquet_cache.cache_registry import (
                CacheRegistry,
                DatasetType,
            )

            registry = CacheRegistry.instance()
            dpd = registry.get_dataset(
                self.dpd_name,
                cache_name=self.dpd_cache_name,
                dataset_type=DatasetType.DATED,
            )
            if dpd is None:
                raise ConfigurationError(
                    f"DPD '{self.dpd_name}' not found in cache "
                    f"'{self.dpd_cache_name}'"
                )
            # dpd is guaranteed to be DatedParquetDataset due to dataset_type
            self._source_dpd = dpd  # type: ignore[assignment]
            return self._source_dpd

        # Try to load from YAML config first
        try:
            from ionbus_parquet_cache.yaml_config import get_dataset_config

            config = get_dataset_config(self.dpd_cache_path, self.dpd_name)
            self._source_dpd = config.to_dpd()
            return self._source_dpd
        except Exception:
            pass

        # Fall back to loading config from source metadata
        # This ensures we use the source's date_col/date_partition, not target's
        from ionbus_parquet_cache.dated_dataset import SnapshotMetadata

        data_dir = self.dpd_cache_path / self.dpd_name
        meta_dir = data_dir / "_meta_data"

        if meta_dir.exists():
            # Find latest metadata file (exclude trimmed)
            meta_files = sorted(
                f for f in meta_dir.glob("*.pkl.gz") if "_trimmed" not in f.name
            )
            if meta_files:
                metadata = SnapshotMetadata.from_pickle(meta_files[-1])
                config = metadata.yaml_config
                self._source_dpd = DatedParquetDataset(
                    cache_dir=self.dpd_cache_path,
                    name=self.dpd_name,
                    date_col=config.get("date_col", "Date"),
                    date_partition=config.get("date_partition", "day"),
                    partition_columns=config.get("partition_columns", []),
                    sort_columns=config.get("sort_columns"),
                )
                return self._source_dpd

        # Last resort: use target config (may be wrong but better than nothing)
        self._source_dpd = DatedParquetDataset(
            cache_dir=self.dpd_cache_path,
            name=self.dpd_name,
            date_col=self.dataset.date_col,
            date_partition=self.dataset.date_partition,
        )

        return self._source_dpd

    def available_dates(self) -> tuple[dt.date, dt.date]:
        """
        Return the date range available in the source DPD.

        Reads from the source DPD's metadata.
        """
        source = self._get_source_dpd()

        # Try to get from metadata
        try:
            if source._metadata is None:
                source._metadata = source._load_metadata()

            min_date = source._metadata.cache_start_date
            max_date = source._metadata.cache_end_date

            if min_date and max_date:
                return (min_date, max_date)
        except Exception:
            pass

        # Fall back to scanning data
        raise DataSourceError(
            f"Cannot determine date range from source DPD '{self.dpd_name}'",
            source_class="DPDSource",
        )

    def get_data(self, partition_spec: PartitionSpec) -> pa.Table:
        """
        Return data for the partition.

        Reads from the source DPD with date filtering.
        """
        source = self._get_source_dpd()

        # Build filters for the partition
        filters = [
            (source.date_col, ">=", partition_spec.start_date),
            (source.date_col, "<=", partition_spec.end_date),
        ]

        # Add partition value filters if applicable
        # Determine the date partition column name to skip (handled by date filter)
        date_part_col = date_partition_column_name(
            source.date_partition, source.date_col
        )
        for col, val in partition_spec.partition_values.items():
            # Skip date partition column as it's handled by the date filter above
            if col == date_part_col:
                continue
            filters.append((col, "==", val))

        # Get PyArrow dataset and read with filters
        ds = source.pyarrow_dataset()

        # Convert filters to PyArrow expression
        expr = None
        for col, op, val in filters:
            if col not in ds.schema.names:
                continue

            field = pds.field(col)
            if op == ">=":
                cond = field >= val
            elif op == "<=":
                cond = field <= val
            elif op == "==":
                cond = field == val
            else:
                continue

            if expr is None:
                expr = cond
            else:
                expr = expr & cond

        if expr is not None:
            return ds.to_table(filter=expr)
        else:
            return ds.to_table()
