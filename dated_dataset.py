"""
DatedParquetDataset - date-partitioned parquet datasets with snapshot metadata.

DPDs store data partitioned by date (and optionally additional columns).
Metadata is stored in `_meta_data/` as pickled snapshot files.
"""

from __future__ import annotations

import datetime as dt
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.fs as pafs
from pydantic import PrivateAttr, field_validator

from ionbus_utils.date_utils import to_date
from ionbus_utils.file_utils import get_file_hash
from ionbus_utils.logging_utils import logger

from ionbus_parquet_cache.parquet_dataset_base import ParquetDataset
from ionbus_parquet_cache.data_cleaner import DataCleaner
from ionbus_parquet_cache.data_source import DataSource
from ionbus_parquet_cache.exceptions import (
    SnapshotNotFoundError,
    SnapshotPublishError,
    ValidationError,
)
from ionbus_parquet_cache.partition import (
    DATE_PARTITION_GRANULARITIES,
    date_partition_column_name,
)
from ionbus_parquet_cache.snapshot import (
    extract_suffix_from_filename,
    generate_snapshot_suffix,
    get_current_suffix,
)
from ionbus_parquet_cache.update_pipeline import (
    build_update_plan,
    compute_update_window,
    execute_update,
)


@dataclass
class FileMetadata:
    """Metadata for a single parquet file in a snapshot."""

    path: str  # Relative path from dataset root
    partition_values: dict[str, Any]  # All partition columns
    checksum: str  # blake2b hash of the file
    size_bytes: int  # File size in bytes

    @classmethod
    def from_path(
        cls,
        file_path: Path,
        dataset_dir: Path,
        partition_values: dict[str, Any],
    ) -> "FileMetadata":
        """Create FileMetadata from a file path."""
        return cls(
            path=str(file_path.relative_to(dataset_dir)),
            partition_values=partition_values,
            checksum=get_file_hash(file_path),
            size_bytes=file_path.stat().st_size,
        )


@dataclass
class SnapshotMetadata:
    """
    Metadata for a DPD snapshot.

    Stored as a pickled file in `_meta_data/` directory.

    Attributes:
        name: Dataset name.
        suffix: Snapshot suffix (base-62 timestamp).
        schema: PyArrow schema of the data.
        files: List of FileMetadata objects for each parquet file.
        cache_start_date: Earliest date in the cache.
        cache_end_date: Latest date in the cache.
        partition_values: Dict of partition column -> distinct values.
        created_at: Timestamp when snapshot was created.
        yaml_config: Full YAML configuration at time of snapshot.
    """

    name: str
    suffix: str
    schema: pa.Schema
    files: list[FileMetadata]
    yaml_config: dict[str, Any]  # Full YAML configuration
    cache_start_date: dt.date | None = None
    cache_end_date: dt.date | None = None
    partition_values: dict[str, list[Any]] = field(default_factory=dict)
    created_at: dt.datetime = field(default_factory=dt.datetime.now)

    def to_pickle(self, path: Path) -> None:
        """Save to pickle file."""
        pd.to_pickle(self, path)  # type: ignore

    @classmethod
    def from_pickle(cls, path: Path) -> "SnapshotMetadata":
        """Load from pickle file."""
        return pd.read_pickle(path)


class DatedParquetDataset(ParquetDataset):
    """
    Date-partitioned parquet dataset with snapshot metadata.

    DPDs store:
    - Data files in a hive-partition-like directory structure
    - Metadata in `_meta_data/` as pickled snapshot files

    Example structure:
        md.futures_daily/
            _meta_data/
                md.futures_daily_1Gz5hK.pkl.gz  <- current snapshot
            FutureRoot=ES/
                year=Y2024/
                    FutureRoot=ES_year=Y2024_1Gz5hK.parquet

    Attributes:
        date_col: Name of the date column (default "Date").
        date_partition: Granularity: "day", "week", "month", "quarter", "year".
        partition_columns: All partition columns in directory order.
        sort_columns: Sort order within a partition file.
        description: Human-readable description.
        start_date_str: Earliest date to request from source (debugging).
        end_date_str: Latest date to request from source (debugging).
        repull_n_days: Trailing business days to re-fetch on each update.
    """

    is_dated_dataset_type: ClassVar[bool] = True

    date_col: str = "Date"
    date_partition: str = "day"
    partition_columns: list[str] = []
    sort_columns: list[str] | None = None
    description: str = ""
    start_date_str: str | None = None
    end_date_str: str | None = None
    repull_n_days: int = 0
    instrument_column: str | None = None
    instruments: list[str] | None = None

    # Private attributes for cached state
    _metadata: SnapshotMetadata | None = PrivateAttr(default=None)

    @field_validator("date_partition")
    @classmethod
    def validate_date_partition(cls, v: str) -> str:
        """Validate date_partition is a valid granularity."""
        if v not in DATE_PARTITION_GRANULARITIES:
            raise ValueError(
                f"Invalid date_partition '{v}'. "
                f"Must be one of: {DATE_PARTITION_GRANULARITIES}"
            )
        return v

    def model_post_init(self, __context: Any) -> None:
        """Initialize computed values after model init."""
        super().model_post_init(__context)
        # Default sort_columns to [date_col] if not set
        if self.sort_columns is None:
            self.sort_columns = [self.date_col]

        # Auto-append date partition column if not in partition_columns
        date_part_col = date_partition_column_name(
            self.date_partition, self.date_col
        )
        if date_part_col not in self.partition_columns:
            self.partition_columns = self.partition_columns + [date_part_col]

    @property
    def dataset_dir(self) -> Path:
        """Return the dataset directory path."""
        return self.cache_dir / self.name

    @property
    def meta_dir(self) -> Path:
        """Return the metadata directory path."""
        return self.dataset_dir / "_meta_data"

    def _discover_current_suffix(self) -> str | None:
        """
        Discover the current (most recent) snapshot suffix.

        Scans the _meta_data directory for metadata files and returns
        the lexicographically largest suffix. Files ending in
        `_trimmed.pkl.gz` are excluded (staged for deletion).

        Returns:
            The current suffix, or None if no snapshots exist.
        """
        if not self.meta_dir.exists():
            return None

        suffixes = []
        for item in self.meta_dir.iterdir():
            # Skip trimmed snapshots (staged for deletion)
            if "_trimmed" in item.name:
                continue
            if item.name.endswith(".pkl.gz"):
                suffix = extract_suffix_from_filename(item.name)
                if suffix:
                    suffixes.append(suffix)

        return get_current_suffix(suffixes)

    def _load_metadata(self) -> SnapshotMetadata:
        """
        Load the current snapshot metadata.

        Returns:
            The SnapshotMetadata for the current snapshot.

        Raises:
            SnapshotNotFoundError: If no valid snapshot exists.
        """
        if self.current_suffix is None:
            self.current_suffix = self._discover_current_suffix()

        if self.current_suffix is None:
            raise SnapshotNotFoundError(
                f"No valid snapshot found for DPD '{self.name}'",
                dataset_name=self.name,
            )

        meta_path = self.meta_dir / f"{self.name}_{self.current_suffix}.pkl.gz"
        if not meta_path.exists():
            raise SnapshotNotFoundError(
                f"Metadata file not found: {meta_path}",
                dataset_name=self.name,
            )

        logger.debug(f"Loading metadata for {self.name} from {meta_path}")
        return SnapshotMetadata.from_pickle(meta_path)

    def _load_schema(self) -> pa.Schema | None:
        """Load the schema from the current snapshot metadata if needed."""
        if self._schema is None:
            if self._metadata is None:
                try:
                    self._metadata = self._load_metadata()
                except SnapshotNotFoundError:
                    return None
            self._schema = self._metadata.schema
        return self._schema

    def _build_dataset(self) -> pds.Dataset:
        """
        Build the PyArrow dataset for the current snapshot.

        Uses the file list and file_partitions from metadata to construct
        the dataset with explicit partition expressions per file.

        Returns:
            A pyarrow.dataset.Dataset object.

        Raises:
            SnapshotNotFoundError: If no valid snapshot exists.
        """
        if self._metadata is None:
            self._metadata = self._load_metadata()

        return self._build_dataset_from_metadata(
            self._metadata, self.current_suffix or "unknown"
        )

    def _build_dataset_for_snapshot(self, suffix: str) -> pds.Dataset:
        """
        Build the PyArrow dataset for a specific snapshot.

        Args:
            suffix: The snapshot suffix to load.

        Returns:
            A pyarrow.dataset.Dataset object.

        Raises:
            SnapshotNotFoundError: If the snapshot does not exist.
        """
        meta_path = self.meta_dir / f"{self.name}_{suffix}.pkl.gz"
        if not meta_path.exists():
            raise SnapshotNotFoundError(
                f"Snapshot '{suffix}' not found for DPD '{self.name}'",
                dataset_name=self.name,
            )

        metadata = SnapshotMetadata.from_pickle(meta_path)
        return self._build_dataset_from_metadata(metadata, suffix)

    def _build_dataset_from_metadata(
        self,
        metadata: SnapshotMetadata,
        suffix: str,
    ) -> pds.Dataset:
        """
        Build a PyArrow dataset from snapshot metadata.

        Args:
            metadata: The snapshot metadata.
            suffix: The snapshot suffix (for error messages).

        Returns:
            A pyarrow.dataset.Dataset object.

        Raises:
            SnapshotNotFoundError: If no data files exist.
        """
        # Build dataset from file list (FileMetadata objects)
        existing_files: list[FileMetadata] = []
        for fm in metadata.files:
            full_path = self.dataset_dir / fm.path
            if full_path.exists():
                existing_files.append(fm)

        if not existing_files:
            raise SnapshotNotFoundError(
                f"No data files found for snapshot '{suffix}'",
                dataset_name=self.name,
            )

        partition_columns = metadata.yaml_config.get("partition_columns", [])

        # Unify schema with partition columns
        schema = metadata.schema
        if partition_columns:
            existing_names = {f.name for f in schema}
            for col in partition_columns:
                if col not in existing_names:
                    schema = schema.append(pa.field(col, pa.string()))

        # Build explicit partition expressions for each file from metadata
        partitions = [
            self._build_partition_expression(fm.partition_values)
            for fm in existing_files
        ]

        return pds.FileSystemDataset.from_paths(
            paths=[str(self.dataset_dir / fm.path) for fm in existing_files],
            schema=schema,
            format=pds.ParquetFileFormat(),
            filesystem=pafs.LocalFileSystem(),
            partitions=partitions,
        )

    def _build_partition_expression(
        self,
        partition_values: dict[str, Any],
    ) -> pds.Expression:
        """
        Build a partition expression from partition values.

        Args:
            partition_values: Dict of partition column -> value.

        Returns:
            PyArrow expression for the partition values.
        """
        if not partition_values:
            return pds.scalar(True)

        expr: pds.Expression | None = None
        for col, value in partition_values.items():
            col_expr = pds.field(col) == value
            if expr is None:
                expr = col_expr
            else:
                expr = expr & col_expr

        return expr if expr is not None else pds.scalar(True)

    def get_partition_values(self, column: str) -> list[Any]:
        """
        Get the distinct values for a partition column.

        Reads from cached metadata, not by scanning files.

        Args:
            column: Name of the partition column.

        Returns:
            List of distinct values for the column.

        Raises:
            ValueError: If column is not a partition column.
            SnapshotNotFoundError: If no valid snapshot exists.
        """
        if column not in self.partition_columns:
            raise ValueError(
                f"'{column}' is not a partition column. "
                f"Partition columns are: {self.partition_columns}"
            )

        if self._metadata is None:
            self._metadata = self._load_metadata()

        return self._metadata.partition_values.get(column, [])

    def refresh(self) -> bool:
        """
        Invalidate cached dataset and load the latest snapshot.

        Also invalidates cached metadata.

        Returns:
            True if a newer snapshot was found and loaded, False otherwise.
        """
        result = super().refresh()
        if result:
            self._metadata = None
        return result

    def summary(self) -> dict[str, Any]:
        """
        Return a dict with dataset metadata.

        Returns:
            Dict containing name, date range, partition info, etc.
        """
        result = super().summary()
        result["date_col"] = self.date_col
        result["date_partition"] = self.date_partition
        result["partition_columns"] = self.partition_columns
        result["sort_columns"] = self.sort_columns
        result["description"] = self.description
        result["dataset_dir"] = str(self.dataset_dir)

        # Load metadata if needed to ensure consistent summary keys
        if self._metadata is None and self.current_suffix is not None:
            try:
                self._metadata = self._load_metadata()
            except Exception:
                pass  # Leave metadata fields out if load fails

        if self._metadata:
            result["cache_start_date"] = self._metadata.cache_start_date
            result["cache_end_date"] = self._metadata.cache_end_date
            result["file_count"] = len(self._metadata.files)

        return result

    def _publish_snapshot(
        self,
        files: list[FileMetadata],
        schema: pa.Schema,
        cache_start_date: dt.date | None = None,
        cache_end_date: dt.date | None = None,
        partition_values: dict[str, list[Any]] | None = None,
        yaml_config: dict[str, Any] | None = None,
        suffix: str | None = None,
    ) -> str:
        """
        Publish a new snapshot with the given files.

        Creates metadata and atomically promotes to current.

        Args:
            files: List of FileMetadata objects for each parquet file.
            schema: PyArrow schema for the data.
            cache_start_date: Earliest date in the cache.
            cache_end_date: Latest date in the cache.
            partition_values: Dict of partition column -> distinct values.
            yaml_config: Full YAML configuration at time of snapshot.
            suffix: Optional suffix to use. If None, generates a new one.

        Returns:
            The suffix of the new snapshot.

        Raises:
            SnapshotPublishError: If publishing fails.
        """
        if suffix is not None:
            new_suffix = suffix
        else:
            new_suffix = generate_snapshot_suffix()

        # Check for duplicate suffix (spec: update fails if same timestamp)
        current = self.current_suffix
        if current is not None and new_suffix <= current:
            raise SnapshotPublishError(
                f"Snapshot suffix collision: new suffix '{new_suffix}' is not "
                f"greater than current suffix '{current}'. "
                "This can happen if updates are too fast (within same second)."
            )

        # Build yaml_config with all DPD configuration fields if not provided
        if yaml_config is None:
            yaml_config = {
                "date_col": self.date_col,
                "date_partition": self.date_partition,
                "partition_columns": self.partition_columns,
                "sort_columns": self.sort_columns or [self.date_col],
                "description": self.description,
                "start_date_str": self.start_date_str,
                "end_date_str": self.end_date_str,
                "repull_n_days": self.repull_n_days,
                "instrument_column": self.instrument_column,
                "instruments": self.instruments,
            }

        # Build metadata
        metadata = SnapshotMetadata(
            name=self.name,
            suffix=new_suffix,
            schema=schema,
            files=files,
            yaml_config=yaml_config,
            cache_start_date=cache_start_date,
            cache_end_date=cache_end_date,
            partition_values=partition_values or {},
        )

        # Ensure metadata directory exists
        self.meta_dir.mkdir(parents=True, exist_ok=True)

        # Write metadata atomically (write to temp, then rename)
        meta_path = self.meta_dir / f"{self.name}_{new_suffix}.pkl.gz"
        temp_path = self.meta_dir / f"{self.name}_{new_suffix}.tmp.pkl.gz"

        try:
            metadata.to_pickle(temp_path)

            # Atomic rename (use replace for Windows compatibility)
            temp_path.replace(meta_path)

            logger.debug(f"Published metadata for {self.name}_{new_suffix}")

            # Update instance state
            self.current_suffix = new_suffix
            self._metadata = metadata
            self._dataset = None
            self._schema = schema

            return new_suffix

        except Exception as e:
            # Clean up temp file on failure
            if temp_path.exists():
                temp_path.unlink()
            raise SnapshotPublishError(
                f"Failed to publish snapshot for DPD '{self.name}': {e}"
            ) from e

    def create_source_from_metadata(self) -> DataSource:
        """
        Create a DataSource from the stored yaml_config in metadata.

        This allows updating the dataset without the original YAML file,
        as long as the source class can be loaded.

        Returns:
            A configured DataSource instance.

        Raises:
            SnapshotNotFoundError: If no metadata exists.
            ConfigurationError: If source class cannot be loaded or
                yaml_config is missing source configuration.
        """
        from ionbus_parquet_cache.exceptions import ConfigurationError
        from ionbus_parquet_cache.yaml_config import _load_builtin_source

        if self._metadata is None:
            self._metadata = self._load_metadata()

        config = self._metadata.yaml_config
        source_class_name = config.get("source_class_name")
        source_location = config.get("source_location", "")
        source_init_args = config.get("source_init_args", {})

        if not source_class_name:
            raise ConfigurationError(
                f"Dataset '{self.name}' metadata has no source_class_name"
            )

        # Load source class
        if not source_location:
            # Built-in source
            source_class = _load_builtin_source(source_class_name)
        else:
            # Custom source - need to load from file
            from ionbus_parquet_cache.yaml_config import _load_class_from_file

            source_path = self.cache_dir / source_location
            source_class = _load_class_from_file(
                source_path,
                source_class_name,
                DataSource,
                f"Dataset '{self.name}'",
            )

        return source_class(self, **source_init_args)

    def create_cleaner_from_metadata(self) -> "DataCleaner | None":
        """
        Create a DataCleaner from the stored yaml_config in metadata.

        Returns:
            A configured DataCleaner instance, or None if not configured.

        Raises:
            SnapshotNotFoundError: If no metadata exists.
            ConfigurationError: If cleaner class cannot be loaded.
        """
        from ionbus_parquet_cache.data_cleaner import DataCleaner
        from ionbus_parquet_cache.yaml_config import _load_class_from_file

        if self._metadata is None:
            self._metadata = self._load_metadata()

        config = self._metadata.yaml_config
        cleaner_class_name = config.get("cleaning_class_name")
        cleaner_location = config.get("cleaning_class_location")
        cleaner_init_args = config.get("cleaning_init_args", {})

        if not cleaner_class_name:
            return None

        if not cleaner_location:
            # No built-in cleaners, so location is required
            from ionbus_parquet_cache.exceptions import ConfigurationError

            raise ConfigurationError(
                f"Dataset '{self.name}' has cleaning_class_name but no "
                "cleaning_class_location"
            )

        cleaner_path = self.cache_dir / cleaner_location
        cleaner_class = _load_class_from_file(
            cleaner_path,
            cleaner_class_name,
            DataCleaner,
            f"Dataset '{self.name}'",
        )

        return cleaner_class(self, **cleaner_init_args)

    def get_transforms_from_metadata(self) -> dict[str, Any] | None:
        """
        Get YAML transform settings from the stored metadata.

        Returns:
            Dict with transform settings, or None if no transforms configured.

        Raises:
            SnapshotNotFoundError: If no metadata exists.
        """
        if self._metadata is None:
            self._metadata = self._load_metadata()

        config = self._metadata.yaml_config
        transforms = {
            "columns_to_rename": config.get("columns_to_rename", {}),
            "columns_to_drop": config.get("columns_to_drop", []),
            "dropna_columns": config.get("dropna_columns", []),
            "dedup_columns": config.get("dedup_columns", []),
            "dedup_keep": config.get("dedup_keep", "last"),
        }

        # Return None if no transforms are set
        if not any([
            transforms["columns_to_rename"],
            transforms["columns_to_drop"],
            transforms["dropna_columns"],
            transforms["dedup_columns"],
        ]):
            return None

        return transforms

    def update(
        self,
        source: DataSource,
        start_date: dt.date | str | None = None,
        end_date: dt.date | str | None = None,
        instruments: list[str] | None = None,
        dry_run: bool = False,
        cleaner: DataCleaner | None = None,
        backfill: bool = False,
        restate: bool = False,
        transforms: dict[str, Any] | None = None,
        yaml_config: dict[str, Any] | None = None,
    ) -> str | None:
        """
        Update the cache using the given DataSource.

        This method orchestrates the full update flow:
        1. Compute update window from source.available_dates() and cache state
        2. Call source.prepare() with computed dates
        3. Get partitions from source.get_partitions()
        4. Build write plan (group specs, assign temp file paths)
        5. Fetch data, apply transforms, write temp files
        6. Consolidate chunks into final partition files
        7. Publish new snapshot metadata

        Args:
            source: DataSource providing the data.
            start_date: Optional explicit start date. If None, computed from
                cache state.
            end_date: Optional explicit end date. If None, uses source end date.
            instruments: Optional list of instruments to update. Requires
                instrument_column to be set and be a partition column.
            dry_run: If True, compute plan but don't write any files.
            cleaner: Optional DataCleaner to apply to data.
            backfill: If True, extend cache backwards preserving existing data.
                Only start_date is allowed; end_date auto-set to cache_start-1.
            restate: If True, replace data for specified date range.
                Both start_date and end_date must be provided.
            transforms: Optional YAML transform settings (columns_to_rename,
                columns_to_drop, dropna_columns, dedup_columns, dedup_keep).
                Applied before the DataCleaner.
            yaml_config: Full YAML configuration to store in snapshot metadata.
                If provided, enables updating without the original YAML file.

        Returns:
            The new snapshot suffix, or None if no update was needed.

        Raises:
            DataSourceError: If data fetch fails.
            SchemaMismatchError: If schema validation fails.
            SnapshotPublishError: If publishing fails.
            ValidationError: If configuration is invalid.
        """
        # Validate backfill/restate constraints
        if backfill and restate:
            raise ValidationError("backfill and restate are mutually exclusive")

        if backfill and end_date is not None:
            raise ValidationError("end_date not allowed with backfill=True")

        if restate and (start_date is None or end_date is None):
            raise ValidationError(
                "restate requires both start_date and end_date"
            )

        # For backfill, auto-set end_date to cache_start - 1 day
        if backfill:
            if self._metadata is None:
                try:
                    self._metadata = self._load_metadata()
                except SnapshotNotFoundError:
                    pass  # No existing cache, backfill is normal update

            if self._metadata and self._metadata.cache_start_date:
                backfill_end = (
                    self._metadata.cache_start_date - dt.timedelta(days=1)
                )
                end_date = backfill_end

                # Validate backfill constraint: start must be before cache_start
                if start_date is not None:
                    start_dt = to_date(start_date)
                    cache_start = self._metadata.cache_start_date
                    if start_dt and start_dt >= cache_start:
                        raise ValidationError(
                            f"backfill start_date ({start_dt}) must be "
                            f"before cache_start_date ({cache_start})"
                        )

        # Compute update window
        window = compute_update_window(
            self, source, to_date(start_date), to_date(end_date)
        )
        if window is None:
            return None  # No update needed

        update_start, update_end = window

        # Prepare source
        source._do_prepare(update_start, update_end, instruments)

        # Get partitions
        specs = source.get_partitions()
        if not specs:
            return None  # No partitions to update

        # Build update plan
        with tempfile.TemporaryDirectory() as temp_dir:
            plan = build_update_plan(self, specs, Path(temp_dir))

            # Execute update
            suffix = execute_update(
                dataset=self,
                source=source,
                plan=plan,
                cleaner=cleaner,
                dry_run=dry_run,
                backfill=backfill,
                transforms=transforms,
                yaml_config=yaml_config,
            )

            return suffix
