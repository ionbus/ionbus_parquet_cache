"""
DatedParquetDataset - date-partitioned parquet datasets with snapshot metadata.

DPDs store data partitioned by date (and optionally additional columns).
Metadata is stored in `_meta_data/` as pickled snapshot files.
"""

from __future__ import annotations

import datetime as dt
import gzip
import pickle
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.fs as pafs
from ionbus_utils.date_utils import to_date
from ionbus_utils.file_utils import get_file_hash
from ionbus_utils.logging_utils import logger
from pydantic import PrivateAttr, field_validator

from ionbus_parquet_cache.bucketing import (
    INSTRUMENT_BUCKET_COL,
    instrument_bucket,
)
from ionbus_parquet_cache.data_cleaner import DataCleaner
from ionbus_parquet_cache.data_source import DataSource
from ionbus_parquet_cache.exceptions import (
    SnapshotNotFoundError,
    SnapshotPublishError,
    UpdateLockedError,
    ValidationError,
)
from ionbus_parquet_cache.parquet_dataset_base import ParquetDataset
from ionbus_parquet_cache.partition import (
    DATE_PARTITION_GRANULARITIES,
    date_partition_column_name,
)
from ionbus_parquet_cache.snapshot import (
    extract_suffix_from_filename,
    generate_snapshot_suffix,
    get_current_suffix,
    is_valid_suffix,
)
from ionbus_parquet_cache.snapshot_history import (
    CacheHistoryEntry,
    SnapshotLineage,
    SnapshotProvenanceRef,
)
from ionbus_parquet_cache.update_pipeline import (
    build_update_plan,
    compute_update_window,
    execute_update,
)

SNAPSHOT_METADATA_PICKLE_PROTOCOL = 4


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
        suffix: Snapshot suffix (base-36 timestamp).
        schema: PyArrow schema of the data.
        files: List of FileMetadata objects for each parquet file.
        cache_start_date: Earliest date in the cache.
        cache_end_date: Latest date in the cache.
        partition_values: Dict of partition column -> distinct values.
        lineage: How this snapshot was produced.
        provenance: Optional reference to an external provenance sidecar.
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
    lineage: SnapshotLineage | None = None
    provenance: SnapshotProvenanceRef | None = None

    def _normalize_legacy_state(self) -> None:
        """Fill fields missing from older pickles and drop retired state."""
        if not hasattr(self, "partition_values"):
            self.partition_values = {}
        if not hasattr(self, "lineage"):
            self.lineage = None
        if not hasattr(self, "provenance"):
            self.provenance = None
        self.__dict__.pop("created_at", None)

    def to_pickle(self, path: Path) -> None:
        """Save to pickle file."""
        self._normalize_legacy_state()
        pd.to_pickle(
            self,
            path,
            protocol=SNAPSHOT_METADATA_PICKLE_PROTOCOL,
        )  # type: ignore

    @classmethod
    def from_pickle(cls, path: Any) -> "SnapshotMetadata":
        """Load from pickle file."""
        if isinstance(path, (str, Path)):
            metadata = pd.read_pickle(path)
        else:
            with gzip.open(path, "rb") as gz:
                metadata = pickle.load(gz)
        if isinstance(metadata, cls):
            metadata._normalize_legacy_state()
        return metadata


class DatedParquetDataset(ParquetDataset):
    """
    Date-partitioned parquet dataset with snapshot metadata.

    DPDs store:
    - Data files in a hive-partition-like directory structure
    - Metadata in `_meta_data/` as pickled snapshot files

    Example structure:
        md.futures_daily/
            _meta_data/
                md.futures_daily_1H4DW01.pkl.gz  <- current snapshot
            FutureRoot=ES/
                year=Y2024/
                    FutureRoot=ES_year=Y2024_1H4DW01.parquet

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
    num_instrument_buckets: int | None = None
    lock_dir: Path | None = None
    use_update_lock: bool = True
    row_group_size: int | None = None

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

        # --- Instrument bucketing validation and setup ---
        # num_instrument_buckets requires instrument_column (but instrument_column
        # alone is valid for the non-bucketed partition-column use case)
        if (
            self.num_instrument_buckets is not None
            and self.instrument_column is None
        ):
            raise ValidationError(
                "instrument_column and num_instrument_buckets must both be set or both be None"
            )
        # The INSTRUMENT_BUCKET_COL reservation checks apply regardless of whether
        # bucketing is active, to prevent accidental misuse of the reserved name.
        if INSTRUMENT_BUCKET_COL in self.partition_columns:
            raise ValidationError(
                f"'{INSTRUMENT_BUCKET_COL}' is reserved by ionbus_parquet_cache"
            )
        if self.sort_columns and INSTRUMENT_BUCKET_COL in self.sort_columns:
            raise ValidationError(
                f"'{INSTRUMENT_BUCKET_COL}' is reserved and must not appear in sort_columns"
            )
        # Bucketing-specific setup: only when num_instrument_buckets is set
        if self.num_instrument_buckets is not None:
            if self.instrument_column in self.partition_columns:
                raise ValidationError(
                    f"instrument_column '{self.instrument_column}' must not be in partition_columns "
                    "— the library manages its partitioning via __instrument_bucket__"
                )
            # Inject __instrument_bucket__ before date partition col
            other_cols = [
                c for c in self.partition_columns if c != date_part_col
            ]
            self.partition_columns = (
                [INSTRUMENT_BUCKET_COL] + other_cols + [date_part_col]
            )
            # Default sort: instrument_column then date_col (if still at default)
            if self.sort_columns == [self.date_col]:
                self.sort_columns = [self.instrument_column, self.date_col]

    @property
    def dataset_dir(self) -> Path | str:
        """Return the dataset directory path (Path for local, str for GCS)."""
        if self.is_gcs:
            return f"{self.cache_dir}/{self.name}"
        return self.cache_dir / self.name  # type: ignore[operator]

    @property
    def meta_dir(self) -> Path | str:
        """Return the metadata directory path (Path for local, str for GCS)."""
        if self.is_gcs:
            return f"{self.cache_dir}/{self.name}/_meta_data"
        return self.dataset_dir / "_meta_data"  # type: ignore[union-attr]

    @property
    def provenance_dir(self) -> Path | str:
        """Return the provenance sidecar directory path."""
        if self.is_gcs:
            return f"{self.cache_dir}/{self.name}/_provenance"
        return self.dataset_dir / "_provenance"  # type: ignore[union-attr]

    def _discover_current_suffix(self) -> str | None:
        """
        Discover the current (most recent) snapshot suffix.

        Scans the _meta_data directory for metadata files and returns
        the lexicographically largest suffix. Files ending in
        `_trimmed.pkl.gz` are excluded (staged for deletion).

        Returns:
            The current suffix, or None if no snapshots exist.
        """
        if self.is_gcs:
            return self._discover_current_suffix_gcs()

        if not self.meta_dir.exists():  # type: ignore[union-attr]
            return None

        suffixes = []
        for item in self.meta_dir.iterdir():  # type: ignore[union-attr]
            # Skip trimmed snapshots (staged for deletion)
            if "_trimmed" in item.name:
                continue
            if item.name.endswith(".pkl.gz"):
                suffix = extract_suffix_from_filename(item.name)
                if suffix:
                    suffixes.append(suffix)

        return get_current_suffix(suffixes)

    def _discover_current_suffix_gcs(self) -> str | None:
        """Discover the current suffix by listing GCS metadata blobs."""
        from ionbus_parquet_cache.gcs_utils import gcs_ls

        suffixes = []
        for blob_url in gcs_ls(str(self.meta_dir)):
            name = blob_url.rstrip("/").split("/")[-1]
            if "_trimmed" in name:
                continue
            if name.endswith(".pkl.gz"):
                suffix = extract_suffix_from_filename(name)
                if suffix:
                    suffixes.append(suffix)
        return get_current_suffix(suffixes)

    def _load_metadata_for_snapshot(self, suffix: str) -> SnapshotMetadata:
        """
        Load metadata for a specific snapshot suffix.

        Args:
            suffix: Snapshot suffix to load.

        Raises:
            SnapshotNotFoundError: If the metadata file does not exist.
        """
        if self.is_gcs:
            meta_url = f"{self.meta_dir}/{self.name}_{suffix}.pkl.gz"
            from ionbus_parquet_cache.gcs_utils import gcs_exists, gcs_open

            if not gcs_exists(meta_url):
                raise SnapshotNotFoundError(
                    f"Snapshot '{suffix}' not found for DPD '{self.name}'",
                    dataset_name=self.name,
                )
            logger.debug(f"Loading metadata for {self.name} from {meta_url}")
            with gcs_open(meta_url) as f:
                return SnapshotMetadata.from_pickle(f)

        meta_path = (
            self.meta_dir / f"{self.name}_{suffix}.pkl.gz"
        )  # type: ignore[operator]
        if not meta_path.exists():
            raise SnapshotNotFoundError(
                f"Snapshot '{suffix}' not found for DPD '{self.name}'",
                dataset_name=self.name,
            )
        logger.debug(f"Loading metadata for {self.name} from {meta_path}")
        return SnapshotMetadata.from_pickle(meta_path)

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

        metadata = self._load_metadata_for_snapshot(self.current_suffix)

        # Validate num_instrument_buckets consistency
        stored_buckets = metadata.yaml_config.get("num_instrument_buckets")
        if stored_buckets != self.num_instrument_buckets:
            raise ValidationError(
                f"num_instrument_buckets mismatch: cache was built with "
                f"{stored_buckets} but current config has "
                f"{self.num_instrument_buckets}. "
                "Changing num_instrument_buckets requires a full rebuild."
            )

        return metadata

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
        metadata = self._load_metadata_for_snapshot(suffix)
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
        # For GCS, trust the metadata — files listed in the snapshot were
        # written atomically at publish time. Checking gcs_exists() per file
        # costs one HTTP round-trip each and makes reads very slow.
        existing_files: list[FileMetadata] = []
        if self.is_gcs:
            from ionbus_parquet_cache.gcs_utils import (
                gcs_join,
                gcs_pa_filesystem,
                gcs_strip_prefix,
            )

            gcs_fs = gcs_pa_filesystem()
            existing_files = list(metadata.files)
        else:
            for fm in metadata.files:
                full_path = self.dataset_dir / fm.path  # type: ignore[operator]
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

        if self.is_gcs:
            dataset_url = str(self.dataset_dir)
            paths = [
                gcs_strip_prefix(gcs_join(dataset_url, fm.path))
                for fm in existing_files
            ]
            return pds.FileSystemDataset.from_paths(
                paths=paths,
                schema=schema,
                format=pds.ParquetFileFormat(),
                filesystem=gcs_fs,
                partitions=partitions,
            )

        return pds.FileSystemDataset.from_paths(
            paths=[str(self.dataset_dir / fm.path) for fm in existing_files],  # type: ignore[operator]
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

    def invalidate_read_cache(self) -> None:
        """Invalidate all cached read state (dataset, schema, and metadata)."""
        super().invalidate_read_cache()
        self._metadata = None

    def refresh(self) -> bool:
        """
        Invalidate cached dataset and load the latest snapshot.

        Also invalidates cached metadata.

        Returns:
            True if a newer snapshot was found and loaded, False otherwise.
        """
        result = super().refresh()
        if result:
            self.invalidate_read_cache()
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

    def _default_yaml_config(self) -> dict[str, Any]:
        """Build the default captured YAML config for this DPD."""
        return {
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
            "num_instrument_buckets": self.num_instrument_buckets,
            "row_group_size": self.row_group_size,
        }

    def _load_current_metadata_if_available(self) -> None:
        """Populate _metadata when a current snapshot exists."""
        if self._metadata is not None:
            return
        try:
            self._metadata = self._load_metadata()
        except SnapshotNotFoundError:
            pass

    def _existing_annotations(self) -> dict[str, Any]:
        """Return annotations from the current snapshot, or an empty dict."""
        self._load_current_metadata_if_available()
        if self._metadata is None:
            return {}
        annotations = self._metadata.yaml_config.get("annotations", {})
        if annotations is None:
            return {}
        if not isinstance(annotations, dict):
            raise ValidationError(
                f"Existing annotations for '{self.name}' must be a dict, "
                f"got {type(annotations).__name__}"
            )
        return annotations

    def _assert_annotations_append_only(
        self,
        old: dict[str, Any],
        new: dict[str, Any],
        path: str = "annotations",
    ) -> None:
        """Reject annotation removals and changes to existing values."""
        for key, old_value in old.items():
            child_path = f"{path}.{key}"
            if key not in new:
                raise ValidationError(
                    f"{child_path} cannot be removed from annotations"
                )

            new_value = new[key]
            if isinstance(old_value, dict) and isinstance(new_value, dict):
                self._assert_annotations_append_only(
                    old_value,
                    new_value,
                    child_path,
                )
            elif new_value != old_value:
                raise ValidationError(
                    f"{child_path} cannot change in annotations: "
                    f"{old_value!r} -> {new_value!r}"
                )

    def _existing_column_descriptions(self) -> dict[str, str]:
        """Return column descriptions from current snapshot, or empty dict."""
        self._load_current_metadata_if_available()
        if self._metadata is None:
            return {}
        descriptions = self._metadata.yaml_config.get(
            "column_descriptions",
            {},
        )
        if descriptions is None:
            return {}
        self._validate_column_descriptions(
            descriptions,
            f"Existing column_descriptions for '{self.name}'",
        )
        return descriptions

    @staticmethod
    def _validate_column_descriptions(
        descriptions: Any,
        context: str,
    ) -> None:
        """Validate column_descriptions is a dict[str, str]."""
        if not isinstance(descriptions, dict):
            raise ValidationError(
                f"{context} must be a dict, "
                f"got {type(descriptions).__name__}"
            )
        for column_name, description in descriptions.items():
            if not isinstance(column_name, str) or not isinstance(
                description,
                str,
            ):
                raise ValidationError(
                    f"{context} keys and values must be strings"
                )

    def _resolve_yaml_config_column_descriptions(
        self,
        resolved: dict[str, Any],
    ) -> None:
        """
        Resolve column_descriptions in place.

        Omitting column_descriptions carries forward the previous snapshot's
        descriptions. Supplying column_descriptions may add or change text,
        but may not remove an existing column description.
        """
        existing = self._existing_column_descriptions()

        if "column_descriptions" not in resolved:
            if existing:
                resolved["column_descriptions"] = deepcopy(existing)
            return

        descriptions = resolved["column_descriptions"]
        self._validate_column_descriptions(
            descriptions,
            f"column_descriptions for '{self.name}'",
        )

        for column_name in existing:
            if column_name not in descriptions:
                raise ValidationError(
                    f"column_descriptions.{column_name} cannot be removed"
                )

    def _resolve_yaml_config_annotations(
        self,
        yaml_config: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """
        Return a metadata config with watched user metadata resolved.

        Omitting annotations carries forward the previous snapshot's value.
        Supplying annotations may add keys, but may not remove or change
        existing keys.

        Omitting column_descriptions carries forward the previous snapshot's
        value. Supplying column_descriptions may add or change descriptions,
        but may not remove existing column description keys.
        """
        resolved = (
            self._default_yaml_config()
            if yaml_config is None
            else dict(yaml_config)
        )
        existing = self._existing_annotations()

        if "annotations" not in resolved:
            if existing:
                resolved["annotations"] = deepcopy(existing)
        else:
            annotations = resolved["annotations"]
            if annotations is None or not isinstance(annotations, dict):
                raise ValidationError(
                    f"annotations for '{self.name}' must be a dict when supplied"
                )

            self._assert_annotations_append_only(existing, annotations)

        self._resolve_yaml_config_column_descriptions(resolved)
        return resolved

    def _provenance_relative_path(self, suffix: str) -> str:
        """Return the dataset-relative provenance sidecar path."""
        return f"_provenance/{self.name}_{suffix}.provenance.pkl.gz"

    def _write_provenance_sidecar(
        self,
        suffix: str,
        payload: dict[str, Any],
    ) -> SnapshotProvenanceRef:
        """Write a gzip pickle provenance sidecar for a snapshot."""
        assert not self.is_gcs
        rel_path = self._provenance_relative_path(suffix)
        sidecar_path = self.dataset_dir / rel_path  # type: ignore[operator]
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(sidecar_path, "wb") as f:
            pickle.dump(
                payload,
                f,
                protocol=SNAPSHOT_METADATA_PICKLE_PROTOCOL,
            )
        return SnapshotProvenanceRef(
            path=rel_path,
            checksum=get_file_hash(sidecar_path),
            size_bytes=sidecar_path.stat().st_size,
        )

    def _delete_provenance_sidecar(
        self,
        provenance: SnapshotProvenanceRef | None,
    ) -> None:
        """Best-effort cleanup for an unpublished provenance sidecar."""
        if self.is_gcs or provenance is None:
            return
        sidecar_path = (
            self.dataset_dir / provenance.path
        )  # type: ignore[operator]
        try:
            sidecar_path.unlink(missing_ok=True)
        except Exception:
            pass

    def read_provenance(self, snapshot: str | None = None) -> dict[str, Any]:
        """
        Load the optional external provenance sidecar for a snapshot.

        Returns an empty dict when the snapshot has no provenance sidecar.
        """
        if snapshot is None:
            if self._metadata is None:
                self._metadata = self._load_metadata()
            metadata = self._metadata
        else:
            metadata = self._load_metadata_for_snapshot(snapshot)

        provenance = metadata.provenance
        if provenance is None:
            return {}

        if self.is_gcs:
            from ionbus_parquet_cache.gcs_utils import (
                gcs_exists,
                gcs_join,
                gcs_open,
            )

            sidecar_url = gcs_join(str(self.dataset_dir), provenance.path)
            if not gcs_exists(sidecar_url):
                raise FileNotFoundError(
                    f"Provenance sidecar not found: {sidecar_url}"
                )
            with gcs_open(sidecar_url) as f:
                with gzip.open(f, "rb") as gz:
                    payload = pickle.load(gz)
        else:
            sidecar_path = (
                self.dataset_dir / provenance.path
            )  # type: ignore[operator]
            if not sidecar_path.exists():
                raise FileNotFoundError(
                    f"Provenance sidecar not found: {sidecar_path}"
                )
            if get_file_hash(sidecar_path) != provenance.checksum:
                raise ValidationError(
                    f"Provenance checksum mismatch for {sidecar_path}"
                )
            with gzip.open(sidecar_path, "rb") as f:
                payload = pickle.load(f)

        if not isinstance(payload, dict):
            raise ValidationError(
                f"Provenance sidecar for '{self.name}' must contain a dict"
            )
        return payload

    def get_column_descriptions(
        self,
        snapshot: str | None = None,
    ) -> dict[str, str]:
        """
        Return column descriptions stored in snapshot metadata.

        Args:
            snapshot: Optional snapshot suffix. If omitted, reads the current
                snapshot metadata.

        Returns:
            A copy of the column description dictionary. Returns an empty
            dictionary when the snapshot has no column descriptions.

        Raises:
            SnapshotNotFoundError: If no requested/current metadata exists.
            ValidationError: If stored column descriptions are malformed.
        """
        if snapshot is None:
            if self._metadata is None:
                self._metadata = self._load_metadata()
            metadata = self._metadata
        else:
            metadata = self._load_metadata_for_snapshot(snapshot)

        descriptions = metadata.yaml_config.get("column_descriptions", {})
        if descriptions is None:
            return {}
        self._validate_column_descriptions(
            descriptions,
            f"column_descriptions for '{self.name}'",
        )
        return deepcopy(descriptions)

    def cache_history(
        self,
        snapshot: str | None = None,
    ) -> list[CacheHistoryEntry]:
        """
        Return lineage history, walking backward from a snapshot.

        The returned list starts with the requested/current snapshot and then
        follows each lineage ``base_snapshot`` link backward.
        """
        suffix = snapshot
        if suffix is None:
            suffix = self.current_suffix or self._discover_current_suffix()
        if suffix is None:
            raise SnapshotNotFoundError(
                f"No valid snapshot found for DPD '{self.name}'",
                dataset_name=self.name,
            )

        history: list[CacheHistoryEntry] = []
        seen: set[str] = set()
        first_snapshot_id: str | None = None
        while suffix is not None:
            if suffix in seen:
                history.append(
                    CacheHistoryEntry(
                        snapshot=suffix,
                        operation="unknown",
                        base_snapshot=None,
                        first_snapshot_id=first_snapshot_id,
                        requested_date_range=None,
                        added_date_ranges=[],
                        rewritten_date_ranges=[],
                        instrument_column=None,
                        instrument_scope="unknown",
                        instruments=None,
                        status="broken_link",
                        message=f"Lineage cycle detected at {suffix}.",
                    )
                )
                break
            seen.add(suffix)

            try:
                metadata = self._load_metadata_for_snapshot(suffix)
            except SnapshotNotFoundError as e:
                history.append(
                    CacheHistoryEntry(
                        snapshot=suffix,
                        operation="unknown",
                        base_snapshot=None,
                        first_snapshot_id=first_snapshot_id,
                        requested_date_range=None,
                        added_date_ranges=[],
                        rewritten_date_ranges=[],
                        instrument_column=None,
                        instrument_scope="unknown",
                        instruments=None,
                        status="broken_link",
                        message=str(e),
                    )
                )
                break

            entry = CacheHistoryEntry.from_metadata(metadata)
            history.append(entry)
            if metadata.lineage is not None:
                first_snapshot_id = metadata.lineage.first_snapshot_id
            if (
                metadata.lineage is None
                or metadata.lineage.base_snapshot is None
            ):
                break
            suffix = metadata.lineage.base_snapshot

        return history

    def _publish_snapshot(
        self,
        files: list[FileMetadata],
        schema: pa.Schema,
        cache_start_date: dt.date | None = None,
        cache_end_date: dt.date | None = None,
        partition_values: dict[str, list[Any]] | None = None,
        yaml_config: dict[str, Any] | None = None,
        lineage: SnapshotLineage | None = None,
        provenance: SnapshotProvenanceRef | None = None,
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
            lineage: How this snapshot was produced.
            provenance: Optional external provenance sidecar reference.
            suffix: Optional suffix to use. If None, generates a new one.

        Returns:
            The suffix of the new snapshot.

        Raises:
            SnapshotPublishError: If publishing fails.
        """
        if suffix is not None:
            if not is_valid_suffix(suffix):
                raise ValueError(
                    f"Invalid snapshot suffix '{suffix}': must be 7 base-36 "
                    f"characters (0-9, A-Z)"
                )
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
            yaml_config = self._default_yaml_config()
        else:
            # Caller-supplied yaml_config may have pre-expansion partition_columns
            # (e.g. from DatasetConfig.to_yaml_config() which stores the YAML value
            # before model_post_init expands it). Always stamp the actual runtime
            # partition_columns so _build_dataset_from_metadata reads them correctly.
            yaml_config = {
                **yaml_config,
                "partition_columns": self.partition_columns,
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
            lineage=lineage,
            provenance=provenance,
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
        from ionbus_parquet_cache.yaml_config import _resolve_source_class

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

        # Load source class using unified resolution
        source_class = _resolve_source_class(
            source_location,
            source_class_name,
            self.cache_dir,
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
        if not any(
            [
                transforms["columns_to_rename"],
                transforms["columns_to_drop"],
                transforms["dropna_columns"],
                transforms["dedup_columns"],
            ]
        ):
            return None

        return transforms

    @property
    def _lock_path(self) -> Path:
        lock_root = (
            self.lock_dir if self.lock_dir is not None else self.dataset_dir
        )
        return lock_root / f"{self.name}_update.lock"

    @staticmethod
    def _pid_is_running(pid: int) -> bool | None:
        """
        Return True if the process is alive, False if dead, None if undetermined.

        Uses platform-appropriate mechanism: signal 0 on POSIX, OpenProcess on Windows.
        """
        import sys

        if sys.platform == "win32":
            import ctypes

            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = ctypes.windll.kernel32.OpenProcess(  # type: ignore[attr-defined]
                PROCESS_QUERY_LIMITED_INFORMATION, False, pid
            )
            if handle:
                ctypes.windll.kernel32.CloseHandle(handle)  # type: ignore[attr-defined]
                return True
            return False
        else:
            import os as _os

            try:
                _os.kill(pid, 0)
                return True
            except ProcessLookupError:
                return False
            except PermissionError:
                return None  # process exists but we can't signal it

    def _acquire_lock(self) -> None:
        """Write the lock file, raising UpdateLockedError if one already exists."""
        import json
        import os
        import socket
        import time

        lock_path = self._lock_path
        lock_path.parent.mkdir(parents=True, exist_ok=True)

        if lock_path.exists():
            try:
                info = json.loads(lock_path.read_text())
            except Exception:
                info = {}
            host = info.get("hostname", "unknown")
            pid = info.get("pid")
            started = info.get("started_at", "unknown")
            age = None
            try:
                age = (
                    time.time()
                    - dt.datetime.fromisoformat(started).timestamp()
                )
            except Exception:
                pass

            age_str = (
                f"{age / 60:.1f} minutes"
                if age is not None
                else "unknown age"
            )
            raise UpdateLockedError(
                f"Dataset '{self.name}' is locked for update.\n"
                f"  Lock file : {lock_path}\n"
                f"  Locked by : {host} (PID {pid})\n"
                f"  Started   : {started} ({age_str} ago)\n"
                f"\n"
                f"  To remove the lock and allow new updates:\n"
                f"    Shell : rm {lock_path}\n"
                f"    Python: dpd.clear_update_lock(force=True)",
                lock_path=str(lock_path),
                locked_by_host=host,
                locked_by_pid=pid,
                lock_age_seconds=age,
            )

        payload = {
            "dataset": self.name,
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "started_at": dt.datetime.now().isoformat(),
        }
        # Use exclusive create (atomic on POSIX) to avoid TOCTOU race
        try:
            with open(lock_path, "x") as f:
                f.write(json.dumps(payload, indent=2))
        except FileExistsError:
            # Another process beat us — re-read and raise with their info
            self._acquire_lock()

    def _release_lock(self) -> None:
        """Delete the lock file; silently ignored if already gone."""
        try:
            self._lock_path.unlink(missing_ok=True)
        except Exception:
            pass

    def clear_update_lock(self, force: bool = False) -> None:
        """
        Remove the update lock for this dataset.

        Args:
            force: If False (default), verify the locking PID is no longer
                running on this host before deleting. If True, delete
                unconditionally regardless of host or PID.

        Raises:
            UpdateLockedError: If force=False and the locking process is still
                alive on this host.
            FileNotFoundError: If no lock file exists.
        """
        import json

        lock_path = self._lock_path
        if not lock_path.exists():
            raise FileNotFoundError(
                f"No lock file found for dataset '{self.name}' at {lock_path}"
            )

        if not force:
            try:
                info = json.loads(lock_path.read_text())
                pid = info.get("pid")
                host = info.get("hostname", "")
                import socket

                if host == socket.gethostname() and pid is not None:
                    alive = self._pid_is_running(pid)
                    if alive is True or alive is None:
                        msg = (
                            f"Dataset '{self.name}' lock is held by a running process "
                            f"(PID {pid} on {host}). Use clear_update_lock(force=True) "
                            "to override."
                            if alive is True
                            else (
                                f"Dataset '{self.name}' lock is held by PID {pid} on "
                                f"{host} — cannot verify liveness. "
                                "Use clear_update_lock(force=True) to override."
                            )
                        )
                        raise UpdateLockedError(
                            msg,
                            lock_path=str(lock_path),
                            locked_by_host=host,
                            locked_by_pid=pid,
                        )
            except (json.JSONDecodeError, OSError):
                pass  # Corrupt lock file — allow clearing

        lock_path.unlink(missing_ok=True)
        logger.info(f"Cleared update lock for dataset '{self.name}'")

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
        # Bucketed datasets do not yet support partial-instrument updates.
        # Writing a subset of instruments to an existing bucket file would
        # overwrite all other instruments in that bucket. Until the read-merge
        # path is implemented, block any update that targets a specific
        # instrument list on a bucketed dataset that already has a snapshot.
        # TODO: remove this guard once _BucketedDataSourceWrapper.get_data()
        # reads and merges existing bucket data before writing.
        if (
            self.num_instrument_buckets is not None
            and instruments is not None
        ):
            raise ValidationError(
                f"Partial-instrument updates are not yet supported for bucketed "
                f"datasets (num_instrument_buckets={self.num_instrument_buckets}). "
                "Writing a subset of instruments would overwrite all other "
                "instruments already stored in the same bucket files. "
                "Track progress on this limitation in PLAN.md under "
                "'Incremental Updates for Bucketed Caches'."
            )

        # Validate backfill/restate constraints
        if backfill and restate:
            raise ValidationError(
                "backfill and restate are mutually exclusive"
            )

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
                backfill_end = self._metadata.cache_start_date - dt.timedelta(
                    days=1
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

        if self.use_update_lock:
            self._acquire_lock()
        try:
            return self._do_update(
                source=source,
                start_date=start_date,
                end_date=end_date,
                instruments=instruments,
                dry_run=dry_run,
                cleaner=cleaner,
                backfill=backfill,
                restate=restate,
                transforms=transforms,
                yaml_config=yaml_config,
            )
        finally:
            if self.use_update_lock:
                self._release_lock()

    def _do_update(
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
        """Inner update implementation (called after lock is acquired)."""
        # Compute update window
        window = compute_update_window(
            self, source, to_date(start_date), to_date(end_date)
        )
        if window is None:
            return None  # No update needed

        update_start, update_end = window

        # Validate bucketed sources use the correct base class
        if self.num_instrument_buckets is not None:
            from ionbus_parquet_cache.data_source import BucketedDataSource

            if not isinstance(source, BucketedDataSource):
                raise ValidationError(
                    f"Dataset '{self.name}' has "
                    f"num_instrument_buckets={self.num_instrument_buckets} "
                    "but the provided DataSource is not a BucketedDataSource. "
                    f"Got: {type(source).__name__}"
                )

        yaml_config = self._resolve_yaml_config_annotations(yaml_config)

        # Prepare source
        source._do_prepare(update_start, update_end, instruments)

        # Get partitions
        specs = source.get_partitions()
        if not specs:
            return None  # No partitions to update

        # Build update plan — use a visible temp dir inside the dataset directory
        # so progress can be monitored. Named with the snapshot suffix so it's
        # clear which run it belongs to. Cleaned up after publish.
        plan_suffix = generate_snapshot_suffix()
        temp_dir = self.dataset_dir / f"_tmp_{plan_suffix}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        try:
            plan = build_update_plan(
                self, specs, temp_dir, suffix=plan_suffix
            )
            # plan.suffix == plan_suffix, so temp dir name matches snapshot suffix

            # Execute update
            suffix = execute_update(
                dataset=self,
                source=source,
                plan=plan,
                cleaner=cleaner,
                dry_run=dry_run,
                backfill=backfill,
                restate=restate,
                transforms=transforms,
                yaml_config=yaml_config,
                update_start=update_start,
                update_end=update_end,
                instruments=instruments,
            )
        finally:
            # Clean up temp dir
            import shutil

            if temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)

        return suffix

    def read_data(
        self,
        start_date: str | dt.date | None = None,
        end_date: str | dt.date | None = None,
        filters: list[tuple[str, Any, Any]] | Any | None = None,
        columns: list[str] | None = None,
        snapshot: str | None = None,
        instruments: str | list[str] | set[str] | None = None,
    ) -> "pd.DataFrame":
        """
        Read data into a pandas DataFrame, with optional instrument filter.

        When ``instruments`` is provided, ``instrument_column`` must be set.
        For non-bucketed datasets, the call applies a row-level filter on
        that column. For bucketed datasets, it also pushes down to the
        correct ``__instrument_bucket__`` partition(s) before row-level
        filtering.

        Args:
            start_date: Optional start date for filtering.
            end_date: Optional end date for filtering.
            filters: Optional additional filters.
            columns: Optional list of columns to return.
            snapshot: Optional snapshot suffix.
            instruments: One or more instrument values to filter on.
                Requires ``instrument_column`` to be set.

        Raises:
            ValueError: If ``instruments`` is given but
                ``instrument_column`` is not set.
        """
        combined_filters = self._build_instrument_filters(
            instruments, filters
        )
        return super().read_data(
            start_date=start_date,
            end_date=end_date,
            filters=combined_filters,
            columns=columns,
            snapshot=snapshot,
        )

    def read_data_pl(
        self,
        start_date: str | dt.date | None = None,
        end_date: str | dt.date | None = None,
        filters: list[tuple[str, Any, Any]] | Any | None = None,
        columns: list[str] | None = None,
        snapshot: str | None = None,
        instruments: str | list[str] | set[str] | None = None,
    ) -> "pl.DataFrame":
        """
        Read data into a Polars DataFrame, with optional instrument filter.

        See :meth:`read_data` for parameter documentation.
        """
        combined_filters = self._build_instrument_filters(
            instruments, filters
        )
        return super().read_data_pl(
            start_date=start_date,
            end_date=end_date,
            filters=combined_filters,
            columns=columns,
            snapshot=snapshot,
        )

    def _build_instrument_filters(
        self,
        instruments: "str | list[str] | set[str] | None",
        existing_filters: "list[tuple] | Any | None",
    ) -> "list[tuple] | Any | None":
        """
        Build combined filters incorporating instrument bucket and row filters.

        Returns the original ``existing_filters`` unchanged if ``instruments``
        is None. Raises ``ValueError`` if ``instruments`` is given but
        ``instrument_column`` is not configured.
        """
        import pyarrow.dataset as pds

        if instruments is None:
            return existing_filters

        if self.instrument_column is None:
            raise ValueError(
                "instruments filter requires instrument_column to be set "
                "on this dataset"
            )

        # Normalise to a set. Handle a single scalar or an iterable.
        if isinstance(instruments, (str, int, float)):
            instruments_set = {instruments}
        else:
            instruments_set = set(instruments)

        instrument_values = sorted(instruments_set)
        bucket_values: list[str] | None = None

        new_filters: list[tuple] = []
        if self.num_instrument_buckets is not None:
            # Compute which buckets contain these instruments.
            bucket_values = sorted(
                instrument_bucket(t, self.num_instrument_buckets)
                for t in instruments_set
            )
            new_filters.append((INSTRUMENT_BUCKET_COL, "in", bucket_values))
        new_filters.append((self.instrument_column, "in", instrument_values))

        if existing_filters is None:
            return new_filters
        if isinstance(existing_filters, pds.Expression):
            row_expr = pds.field(self.instrument_column).isin(
                instrument_values
            )
            if bucket_values is not None:
                bucket_expr = pds.field(INSTRUMENT_BUCKET_COL).isin(
                    bucket_values
                )
                return bucket_expr & row_expr & existing_filters
            return row_expr & existing_filters
        # Assume it's a list of tuples
        return new_filters + list(existing_filters)
