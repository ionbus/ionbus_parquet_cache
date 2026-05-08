"""
Base class for parquet datasets with snapshot versioning.

ParquetDataset is the abstract base class inherited by both
DatedParquetDataset and NonDatedParquetDataset.
"""

from __future__ import annotations

import datetime as dt
import gzip
import pickle
from abc import ABC, abstractmethod
from copy import deepcopy
from pathlib import Path
from typing import Annotated, Any, ClassVar

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as pds
from ionbus_utils.file_utils import get_file_hash
from ionbus_utils.yaml_utils.pdyaml import PDYaml
from pydantic import BeforeValidator, PrivateAttr

from ionbus_parquet_cache.exceptions import (
    SnapshotNotFoundError,
    ValidationError,
)
from ionbus_parquet_cache.filter_utils import build_dataset_filter
from ionbus_parquet_cache.snapshot_history import SnapshotProvenanceRef

SNAPSHOT_INFO_PICKLE_PROTOCOL = 4
SNAPSHOT_INFO_KEYS = frozenset(
    {"notes", "annotations", "column_descriptions"}
)


def _parse_cache_dir(v: Any) -> str | Path:
    """Accept GCS strings as-is; coerce everything else to Path."""
    s = str(v)
    if s.startswith("gs://"):
        return s.rstrip("/")
    return Path(v)


CacheDirField = Annotated[str | Path, BeforeValidator(_parse_cache_dir)]


class ParquetDataset(PDYaml, ABC):
    """
    Abstract base class for parquet datasets with snapshot versioning.

    Provides the common interface for reading parquet data. Both
    DatedParquetDataset and NonDatedParquetDataset inherit from this class.

    Attributes:
        name: Unique name for this dataset (inherited from PDYaml).
        cache_dir: Root directory of the cache.
        current_suffix: Suffix of the currently loaded snapshot
            (e.g., "1H4DW01").
        schema: PyArrow schema of the dataset (lazy-loaded property).
    """

    is_dated_dataset_type: ClassVar[bool] = False
    date_col: str | None = None

    cache_dir: CacheDirField  # type: ignore[assignment]
    current_suffix: str | None = None

    @property
    def is_gcs(self) -> bool:
        """True when this dataset is backed by GCS storage."""
        return isinstance(self.cache_dir, str)

    # Private attributes for cached state (not serialized)
    _schema: pa.Schema | None = PrivateAttr(default=None)
    _dataset: pds.Dataset | None = PrivateAttr(default=None)
    _snapshot_datasets: dict[str, pds.Dataset] = PrivateAttr(
        default_factory=dict
    )

    @property
    def schema(self) -> pa.Schema | None:
        """
        Return the PyArrow schema of the dataset, or None if no snapshot exists.

        The schema is read from parquet files on demand and may be
        cached in metadata for DPDs.
        """
        return self._load_schema()

    def _load_schema(self) -> pa.Schema | None:
        """Load the schema from storage if needed."""
        return self._schema

    @abstractmethod
    def _discover_current_suffix(self) -> str | None:
        """
        Discover the current (most recent) snapshot suffix.

        Returns:
            The lexicographically largest suffix, or None if no snapshots exist.
        """
        pass

    @abstractmethod
    def _build_dataset(self) -> pds.Dataset:
        """
        Build the PyArrow dataset for the current snapshot.

        Returns:
            A pyarrow.dataset.Dataset object.

        Raises:
            SnapshotNotFoundError: If no valid snapshot exists.
        """
        pass

    @abstractmethod
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
        pass

    @property
    def _snapshot_root_dir(self) -> Path | str:
        """Return the dataset root used for snapshot sidecars."""
        if self.is_gcs:
            return f"{self.cache_dir}/{self.name}"
        return self.cache_dir / self.name  # type: ignore[operator]

    def _load_snapshot_info(
        self,
        snapshot: str | None = None,
    ) -> dict[str, Any]:
        """
        Return stored snapshot info for a snapshot.

        Subclasses override this to read from DPD metadata or NPD sidecars.
        """
        raise SnapshotNotFoundError(
            f"No snapshot info found for dataset '{self.name}'",
            dataset_name=self.name,
        )

    def _load_provenance_ref(
        self,
        snapshot: str | None = None,
    ) -> SnapshotProvenanceRef | None:
        """Return an optional provenance reference for a snapshot."""
        return None

    def _load_current_snapshot_info_if_available(self) -> dict[str, Any]:
        """Return current snapshot info, or an empty dict for a new dataset."""
        try:
            return self._load_snapshot_info()
        except SnapshotNotFoundError:
            return {}

    @staticmethod
    def _validate_snapshot_info_keys(
        info: dict[str, Any],
        context: str,
    ) -> None:
        """Reject unknown snapshot-info fields."""
        unknown = sorted(set(info) - SNAPSHOT_INFO_KEYS)
        if unknown:
            allowed = ", ".join(sorted(SNAPSHOT_INFO_KEYS))
            raise ValidationError(
                f"{context} has unknown key(s): {', '.join(unknown)}. "
                f"Allowed keys are: {allowed}"
            )

    def _existing_annotations(self) -> dict[str, Any]:
        """Return annotations from the current snapshot, or an empty dict."""
        info = self._load_current_snapshot_info_if_available()
        annotations = info.get("annotations", {})
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

    def _existing_notes(self) -> str | None:
        """Return notes from the current snapshot, or None."""
        info = self._load_current_snapshot_info_if_available()
        notes = info.get("notes")
        if notes is None:
            return None
        self._validate_notes(notes, f"Existing notes for '{self.name}'")
        return notes

    @staticmethod
    def _validate_notes(notes: Any, context: str) -> None:
        """Validate notes is a string."""
        if not isinstance(notes, str):
            raise ValidationError(
                f"{context} must be a string, got {type(notes).__name__}"
            )

    def _existing_column_descriptions(self) -> dict[str, str]:
        """Return column descriptions from current snapshot, or empty dict."""
        info = self._load_current_snapshot_info_if_available()
        descriptions = info.get("column_descriptions", {})
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

    def _resolve_snapshot_info_fields(
        self,
        supplied: dict[str, Any],
        *,
        validate_keys: bool = True,
    ) -> dict[str, Any]:
        """
        Resolve snapshot-info fields against the current snapshot.

        Missing fields carry forward. Explicit annotations are append-only,
        explicit notes may replace the previous string, and explicit column
        descriptions may add or change text but may not remove keys.
        """
        if validate_keys:
            self._validate_snapshot_info_keys(
                supplied,
                f"snapshot info for '{self.name}'",
            )
        resolved = dict(supplied)

        existing_annotations = self._existing_annotations()
        if "annotations" not in resolved:
            if existing_annotations:
                resolved["annotations"] = deepcopy(existing_annotations)
        else:
            annotations = resolved["annotations"]
            if annotations is None or not isinstance(annotations, dict):
                raise ValidationError(
                    f"annotations for '{self.name}' must be a dict when supplied"
                )
            self._assert_annotations_append_only(
                existing_annotations,
                annotations,
            )

        existing_notes = self._existing_notes()
        if "notes" not in resolved:
            if existing_notes is not None:
                resolved["notes"] = existing_notes
        else:
            self._validate_notes(
                resolved["notes"], f"notes for '{self.name}'"
            )

        existing_descriptions = self._existing_column_descriptions()
        if "column_descriptions" not in resolved:
            if existing_descriptions:
                resolved["column_descriptions"] = deepcopy(
                    existing_descriptions
                )
        else:
            descriptions = resolved["column_descriptions"]
            self._validate_column_descriptions(
                descriptions,
                f"column_descriptions for '{self.name}'",
            )
            for column_name in existing_descriptions:
                if column_name not in descriptions:
                    raise ValidationError(
                        f"column_descriptions.{column_name} cannot be removed"
                    )

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
        if self.is_gcs:
            raise ValidationError(
                "Direct GCS provenance writes are unsupported"
            )
        if not isinstance(payload, dict):
            raise ValidationError(
                f"provenance for '{self.name}' must be a dict, "
                f"got {type(payload).__name__}"
            )

        rel_path = self._provenance_relative_path(suffix)
        sidecar_path = self._snapshot_root_dir / rel_path  # type: ignore[operator]
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(sidecar_path, "wb") as f:
            pickle.dump(
                payload,
                f,
                protocol=SNAPSHOT_INFO_PICKLE_PROTOCOL,
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
        sidecar_path = self._snapshot_root_dir / provenance.path  # type: ignore[operator]
        try:
            sidecar_path.unlink(missing_ok=True)
        except Exception:
            pass

    def read_provenance(self, snapshot: str | None = None) -> dict[str, Any]:
        """
        Load the optional external provenance sidecar for a snapshot.

        Returns an empty dict when the snapshot has no provenance sidecar.
        """
        provenance = self._load_provenance_ref(snapshot)
        if provenance is None:
            return {}

        if self.is_gcs:
            from ionbus_parquet_cache.gcs_utils import (
                gcs_exists,
                gcs_join,
                gcs_open,
            )

            sidecar_url = gcs_join(
                str(self._snapshot_root_dir), provenance.path
            )
            if not gcs_exists(sidecar_url):
                raise FileNotFoundError(
                    f"Provenance sidecar not found: {sidecar_url}"
                )
            with gcs_open(sidecar_url) as f:
                with gzip.open(f, "rb") as gz:
                    payload = pickle.load(gz)
        else:
            sidecar_path = self._snapshot_root_dir / provenance.path  # type: ignore[operator]
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

    def get_annotations(
        self,
        snapshot: str | None = None,
    ) -> dict[str, Any]:
        """
        Return annotations stored for a snapshot.

        Returns a copy of the annotations dictionary. Missing annotations
        return an empty dictionary.
        """
        info = self._load_snapshot_info(snapshot)
        annotations = info.get("annotations", {})
        if annotations is None:
            return {}
        if not isinstance(annotations, dict):
            raise ValidationError(
                f"annotations for '{self.name}' must be a dict, "
                f"got {type(annotations).__name__}"
            )
        return deepcopy(annotations)

    def get_notes(
        self,
        snapshot: str | None = None,
    ) -> str | None:
        """
        Return notes stored for a snapshot.

        Returns None when the snapshot has no notes field. An explicit empty
        string is returned unchanged.
        """
        info = self._load_snapshot_info(snapshot)
        notes = info.get("notes")
        if notes is None:
            return None
        self._validate_notes(notes, f"notes for '{self.name}'")
        return notes

    def get_column_descriptions(
        self,
        snapshot: str | None = None,
    ) -> dict[str, str]:
        """
        Return column descriptions stored for a snapshot.

        Returns a copy of the dictionary, or an empty dictionary when missing.
        """
        info = self._load_snapshot_info(snapshot)
        descriptions = info.get("column_descriptions", {})
        if descriptions is None:
            return {}
        self._validate_column_descriptions(
            descriptions,
            f"column_descriptions for '{self.name}'",
        )
        return deepcopy(descriptions)

    def pyarrow_dataset(
        self,
        start_date: str | dt.date | None = None,
        end_date: str | dt.date | None = None,
        filters: list[tuple[str, str, Any]] | pds.Expression | None = None,
        snapshot: str | None = None,
    ) -> pds.Dataset:
        """
        Return the PyArrow Dataset for a snapshot.

        The unfiltered dataset is cached and reused. When filters are
        provided, returns a filtered view of the cached dataset.

        Args:
            start_date: Optional start date for filtering (DatedParquetDataset).
            end_date: Optional end date for filtering (DatedParquetDataset).
            filters: Optional filters as list of (column, operator, value)
                tuples or a PyArrow Expression.
                Supported operators: =, ==, !=, <, <=, >, >=, in, not in.
            snapshot: Optional snapshot suffix. If None, uses current snapshot.

        Returns:
            A pyarrow.dataset.Dataset object (filtered if filters provided).

        Raises:
            SnapshotNotFoundError: If no valid snapshot exists.
            ValueError: If dates provided on non-dated dataset.
        """
        # Get dataset - either from snapshot cache or current cache
        if snapshot is not None:
            # Specific snapshot requested - use dictionary cache
            if snapshot not in self._snapshot_datasets:
                self._snapshot_datasets[snapshot] = (
                    self._build_dataset_for_snapshot(snapshot)
                )
            dataset: pds.Dataset = self._snapshot_datasets[snapshot]
        else:
            # Current snapshot - use simple variable cache
            if self._dataset is None:
                if self.current_suffix is None:
                    self.current_suffix = self._discover_current_suffix()
                    if self.current_suffix is None:
                        raise SnapshotNotFoundError(
                            f"No snapshot found for dataset '{self.name}'",
                            dataset_name=self.name,
                        )
                self._dataset = self._build_dataset()
            dataset = self._dataset

        # Handle Expression filters directly
        if isinstance(filters, pds.Expression):
            if start_date is not None or end_date is not None:
                raise ValueError(
                    "Cannot combine date parameters with Expression filters"
                )
            return dataset.filter(filters)

        # Build combined filter expression
        # Pass date_partition and partition_columns for partition pruning
        date_partition = getattr(self, "date_partition", None)
        partition_columns = getattr(self, "partition_columns", None)
        expr = build_dataset_filter(
            start_date=start_date,
            end_date=end_date,
            date_col=self.date_col,
            is_dated_dataset=self.is_dated_dataset_type,
            filters=filters,
            date_partition=date_partition,
            partition_columns=partition_columns,
        )

        if expr is not None:
            return dataset.filter(expr)

        return dataset

    def to_table(
        self,
        start_date: str | dt.date | None = None,
        end_date: str | dt.date | None = None,
        filters: list[tuple[str, str, Any]] | pds.Expression | None = None,
        columns: list[str] | None = None,
        snapshot: str | None = None,
    ) -> pa.Table:
        """
        Return data as a PyArrow Table with optional filtering and projection.

        Args:
            start_date: Optional start date for filtering (DatedParquetDataset).
            end_date: Optional end date for filtering (DatedParquetDataset).
            filters: Optional filters as list of (column, operator, value)
                tuples or a PyArrow Expression.
                Supported operators: =, ==, !=, <, <=, >, >=, in, not in.
            columns: Optional list of columns to read.
            snapshot: Optional snapshot suffix. If None, uses current snapshot.

        Returns:
            A pyarrow.Table with the requested data.

        Raises:
            SnapshotNotFoundError: If no valid snapshot exists.
        """
        ds = self.pyarrow_dataset(
            start_date=start_date,
            end_date=end_date,
            filters=filters,
            snapshot=snapshot,
        )
        if columns:
            return ds.to_table(columns=columns)
        return ds.to_table()

    def read_data(
        self,
        start_date: str | dt.date | None = None,
        end_date: str | dt.date | None = None,
        filters: list[tuple[str, str, Any]] | pds.Expression | None = None,
        columns: list[str] | None = None,
        snapshot: str | None = None,
    ) -> pd.DataFrame:
        """Read data into a pandas DataFrame."""
        return self.to_table(
            start_date=start_date,
            end_date=end_date,
            filters=filters,
            columns=columns,
            snapshot=snapshot,
        ).to_pandas()

    def read_data_pl(
        self,
        start_date: str | dt.date | None = None,
        end_date: str | dt.date | None = None,
        filters: list[tuple[str, str, Any]] | pds.Expression | None = None,
        columns: list[str] | None = None,
        snapshot: str | None = None,
    ) -> pl.DataFrame:
        """Read data into a Polars DataFrame."""
        return pl.from_arrow(
            self.to_table(
                start_date=start_date,
                end_date=end_date,
                filters=filters,
                columns=columns,
                snapshot=snapshot,
            )
        )  # type: ignore[return-value]

    def is_update_available(self) -> bool:
        """
        Check if a newer snapshot exists on disk.

        Returns:
            True if a newer snapshot suffix exists, False otherwise.
        """
        latest = self._discover_current_suffix()
        if latest is None:
            return False
        if self.current_suffix is None:
            return True
        return latest > self.current_suffix

    def invalidate_read_cache(self) -> None:
        """Invalidate cached read state (dataset and schema)."""
        self._dataset = None
        self._schema = None

    def refresh(self) -> bool:
        """
        Invalidate cached dataset and load the latest snapshot.

        This method checks for a newer snapshot and reloads if one exists.
        It's a no-op if the current snapshot is already the latest.

        Returns:
            True if a newer snapshot was found and loaded, False otherwise.
        """
        latest = self._discover_current_suffix()
        if latest is None:
            # No snapshots exist
            self.current_suffix = None
            self.invalidate_read_cache()
            return False

        if latest != self.current_suffix:
            # New snapshot available - invalidate cache
            self.current_suffix = latest
            self.invalidate_read_cache()
            return True

        return False

    def summary(self) -> dict[str, Any]:
        """
        Return a dict with dataset metadata.

        Returns:
            Dict containing name, current_suffix, schema info, etc.
        """
        result: dict[str, Any] = {
            "name": self.name,
            "cache_dir": str(self.cache_dir),
            "current_suffix": self.current_suffix,
        }

        if self._schema is not None:
            result["columns"] = [f.name for f in self._schema]
            result["num_columns"] = len(self._schema)

        return result
