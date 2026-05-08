"""
NonDatedParquetDataset - parquet datasets without date partitioning.

NPDs are updated by importing complete snapshots rather than incremental
updates. All snapshots live in the `non-dated/` subdirectory of the cache.
"""

from __future__ import annotations

import gzip
import pickle
import shutil
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.dataset as pds
from pydantic import PrivateAttr, computed_field

from ionbus_parquet_cache.exceptions import (
    SnapshotNotFoundError,
    SnapshotPublishError,
    ValidationError,
)
from ionbus_parquet_cache.parquet_dataset_base import (
    SNAPSHOT_INFO_PICKLE_PROTOCOL,
    ParquetDataset,
)
from ionbus_parquet_cache.snapshot import (
    extract_suffix_from_filename,
    generate_snapshot_suffix,
    get_current_suffix,
)
from ionbus_parquet_cache.snapshot_history import SnapshotProvenanceRef


class NonDatedParquetDataset(ParquetDataset):
    """
    Parquet dataset without date-based partitioning.

    NPDs store versioned snapshots as either:
    - Single parquet files: `name_suffix.parquet`
    - Hive-partitioned directories: `name_suffix/`

    All snapshots live under `{cache_dir}/non-dated/{name}/`.

    Example structure:
        non-dated/
            instrument_master/
                instrument_master_1H4DW00.parquet  <- old snapshot
                instrument_master_1H4DW01.parquet  <- current (largest suffix)

    Attributes:
        npd_dir: Path to the NPD directory under `non-dated/`.
    """

    # Private attributes for internal state
    _is_directory_snapshot: bool | None = PrivateAttr(default=None)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def npd_dir(self) -> Path | str:
        """Return the NPD directory path (Path for local, str for GCS)."""
        if self.is_gcs:
            return f"{self.cache_dir}/non-dated/{self.name}"
        return self.cache_dir / "non-dated" / self.name  # type: ignore[operator]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def meta_dir(self) -> Path | str:
        """Return the optional NPD snapshot-info sidecar directory."""
        if self.is_gcs:
            return f"{self.npd_dir}/_meta_data"
        return self.npd_dir / "_meta_data"  # type: ignore[operator]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def provenance_dir(self) -> Path | str:
        """Return the optional NPD provenance sidecar directory."""
        if self.is_gcs:
            return f"{self.npd_dir}/_provenance"
        return self.npd_dir / "_provenance"  # type: ignore[operator]

    @property
    def _snapshot_root_dir(self) -> Path | str:
        """Return the dataset root used for NPD sidecars."""
        return self.npd_dir

    def _discover_current_suffix(self) -> str | None:
        """
        Discover the current (most recent) snapshot suffix.

        Scans the NPD directory for snapshot files/directories and returns
        the lexicographically largest suffix.

        Returns:
            The current suffix, or None if no snapshots exist.
        """
        if self.is_gcs:
            from ionbus_parquet_cache.gcs_utils import gcs_ls

            suffixes = []
            for item_url in gcs_ls(str(self.npd_dir)):
                name = item_url.rstrip("/").split("/")[-1]
                suffix = extract_suffix_from_filename(name)
                if suffix:
                    suffixes.append(suffix)
            return get_current_suffix(suffixes)

        if not self.npd_dir.exists():  # type: ignore[union-attr]
            return None

        suffixes = []
        for item in self.npd_dir.iterdir():  # type: ignore[union-attr]
            suffix = extract_suffix_from_filename(item.name)
            if suffix:
                suffixes.append(suffix)

        return get_current_suffix(suffixes)

    def _get_current_snapshot_path(self) -> Path | str | None:
        """
        Get the path to the current snapshot file or directory.

        Returns:
            Path (local) or str URL (GCS) to current snapshot, or None if
            no snapshots exist.
        """
        if self.current_suffix is None:
            self.current_suffix = self._discover_current_suffix()

        if self.current_suffix is None:
            return None

        if self.is_gcs:
            from ionbus_parquet_cache.gcs_utils import gcs_exists, gcs_join

            npd_url = str(self.npd_dir)
            dir_url = gcs_join(npd_url, f"{self.name}_{self.current_suffix}")
            if gcs_exists(dir_url):
                self._is_directory_snapshot = True
                return dir_url
            file_url = gcs_join(
                npd_url, f"{self.name}_{self.current_suffix}.parquet"
            )
            if gcs_exists(file_url):
                self._is_directory_snapshot = False
                return file_url
            return None

        # Local path handling
        dir_path = self.npd_dir / f"{self.name}_{self.current_suffix}"  # type: ignore[operator]
        if dir_path.is_dir():
            self._is_directory_snapshot = True
            return dir_path

        file_path = self.npd_dir / f"{self.name}_{self.current_suffix}.parquet"  # type: ignore[operator]
        if file_path.is_file():
            self._is_directory_snapshot = False
            return file_path

        return None

    def _load_schema(self) -> pa.Schema | None:
        """Load the schema from the current snapshot if needed."""
        if self._schema is None:
            snapshot_path = self._get_current_snapshot_path()
            if snapshot_path is None:
                return None
            # Build dataset and extract schema
            dataset = self._build_dataset()
            self._schema = dataset.schema
        return self._schema

    def _build_dataset(self) -> pds.Dataset:
        """
        Build the PyArrow dataset for the current snapshot.

        Returns:
            A pyarrow.dataset.Dataset object.

        Raises:
            SnapshotNotFoundError: If no valid snapshot exists.
        """
        if self.current_suffix is None:
            self.current_suffix = self._discover_current_suffix()
            if self.current_suffix is None:
                raise SnapshotNotFoundError(
                    f"No valid snapshot found for NPD '{self.name}'",
                    dataset_name=self.name,
                )

        return self._build_dataset_for_snapshot(self.current_suffix)

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
        if self.is_gcs:
            from ionbus_parquet_cache.gcs_utils import (
                gcs_exists,
                gcs_join,
                gcs_pa_filesystem,
                gcs_strip_prefix,
            )

            npd_url = str(self.npd_dir)
            dir_url = gcs_join(npd_url, f"{self.name}_{suffix}")
            file_url = gcs_join(npd_url, f"{self.name}_{suffix}.parquet")
            gcs_fs = gcs_pa_filesystem()
            if gcs_exists(dir_url):
                return pds.dataset(
                    gcs_strip_prefix(dir_url),
                    format="parquet",
                    partitioning="hive",
                    filesystem=gcs_fs,
                )
            if gcs_exists(file_url):
                return pds.dataset(
                    gcs_strip_prefix(file_url),
                    format="parquet",
                    filesystem=gcs_fs,
                )
            raise SnapshotNotFoundError(
                f"Snapshot '{suffix}' not found for NPD '{self.name}'",
                dataset_name=self.name,
            )

        # Local path handling
        dir_path = self.npd_dir / f"{self.name}_{suffix}"  # type: ignore[operator]
        if dir_path.is_dir():
            return pds.dataset(
                dir_path, format="parquet", partitioning="hive"
            )

        file_path = self.npd_dir / f"{self.name}_{suffix}.parquet"  # type: ignore[operator]
        if file_path.is_file():
            return pds.dataset(file_path, format="parquet")

        raise SnapshotNotFoundError(
            f"Snapshot '{suffix}' not found for NPD '{self.name}'",
            dataset_name=self.name,
        )

    def _info_sidecar_relative_path(self, suffix: str) -> str:
        """Return the NPD-root-relative info sidecar path."""
        return f"_meta_data/{self.name}_{suffix}.pkl.gz"

    def _info_sidecar_path(self, suffix: str) -> Path | str:
        """Return the info sidecar path for a suffix."""
        rel_path = self._info_sidecar_relative_path(suffix)
        if self.is_gcs:
            from ionbus_parquet_cache.gcs_utils import gcs_join

            return gcs_join(str(self.npd_dir), rel_path)
        return self.npd_dir / rel_path  # type: ignore[operator]

    def _current_or_requested_suffix(
        self,
        snapshot: str | None,
    ) -> str:
        """Return requested suffix or discover the current NPD suffix."""
        suffix = snapshot
        if suffix is None:
            suffix = self.current_suffix or self._discover_current_suffix()
        if suffix is None:
            raise SnapshotNotFoundError(
                f"No valid snapshot found for NPD '{self.name}'",
                dataset_name=self.name,
            )
        return suffix

    def _load_info_sidecar_payload(self, suffix: str) -> dict[str, Any]:
        """Load and validate an NPD info sidecar payload."""
        sidecar_path = self._info_sidecar_path(suffix)
        if self.is_gcs:
            from ionbus_parquet_cache.gcs_utils import gcs_exists, gcs_open

            if not gcs_exists(str(sidecar_path)):
                return {"info": {}, "provenance": None}
            with gcs_open(str(sidecar_path)) as f:
                with gzip.open(f, "rb") as gz:
                    payload = pickle.load(gz)
        else:
            sidecar = Path(sidecar_path)
            if not sidecar.exists():
                return {"info": {}, "provenance": None}
            with gzip.open(sidecar, "rb") as f:
                payload = pickle.load(f)

        if not isinstance(payload, dict):
            raise ValidationError(
                f"NPD info sidecar for '{self.name}' must contain a dict"
            )
        if payload.get("name") not in (None, self.name):
            raise ValidationError(
                f"NPD info sidecar name mismatch for '{self.name}'"
            )
        if payload.get("suffix") not in (None, suffix):
            raise ValidationError(
                f"NPD info sidecar suffix mismatch for '{self.name}'"
            )

        info = payload.get("info", {})
        if info is None:
            info = {}
        if not isinstance(info, dict):
            raise ValidationError(
                f"NPD info sidecar info for '{self.name}' must be a dict"
            )

        provenance = payload.get("provenance")
        if provenance is not None and not isinstance(
            provenance,
            SnapshotProvenanceRef,
        ):
            raise ValidationError(
                f"NPD info sidecar provenance for '{self.name}' must be a "
                "SnapshotProvenanceRef or None"
            )

        return {
            "name": self.name,
            "suffix": suffix,
            "info": info,
            "provenance": provenance,
        }

    def _load_snapshot_info(
        self,
        snapshot: str | None = None,
    ) -> dict[str, Any]:
        """Return snapshot info from the optional NPD sidecar."""
        suffix = self._current_or_requested_suffix(snapshot)
        payload = self._load_info_sidecar_payload(suffix)
        return payload["info"]

    def _load_provenance_ref(
        self,
        snapshot: str | None = None,
    ) -> SnapshotProvenanceRef | None:
        """Return the NPD provenance reference from the info sidecar."""
        suffix = self._current_or_requested_suffix(snapshot)
        payload = self._load_info_sidecar_payload(suffix)
        return payload["provenance"]

    def _write_info_sidecar(
        self,
        suffix: str,
        info: dict[str, Any],
        provenance: SnapshotProvenanceRef | None,
    ) -> Path:
        """Write the normalized NPD info sidecar for a snapshot."""
        assert not self.is_gcs
        sidecar_path = self.npd_dir / self._info_sidecar_relative_path(suffix)  # type: ignore[operator]
        sidecar_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = sidecar_path.parent / f"{self.name}_{suffix}_tmp.pkl.gz"
        payload = {
            "name": self.name,
            "suffix": suffix,
            "info": info,
            "provenance": provenance,
        }
        try:
            with gzip.open(temp_path, "wb") as f:
                pickle.dump(
                    payload,
                    f,
                    protocol=SNAPSHOT_INFO_PICKLE_PROTOCOL,
                )
            temp_path.replace(sidecar_path)
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise
        return sidecar_path

    def _delete_info_sidecar(self, suffix: str) -> None:
        """Best-effort cleanup for an unpublished NPD info sidecar."""
        if self.is_gcs:
            return
        sidecar_path = self.npd_dir / self._info_sidecar_relative_path(suffix)  # type: ignore[operator]
        try:
            sidecar_path.unlink(missing_ok=True)
        except Exception:
            pass

    def _resolve_import_info(
        self,
        info: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Validate and carry forward NPD snapshot info."""
        if info is None:
            supplied: dict[str, Any] = {}
        elif not isinstance(info, dict):
            raise ValidationError(
                f"snapshot info for '{self.name}' must be a dict, "
                f"got {type(info).__name__}"
            )
        else:
            supplied = dict(info)
        return self._resolve_snapshot_info_fields(supplied)

    def _normalize_import_provenance(
        self,
        provenance: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Validate NPD import provenance and normalize empty dicts to None."""
        if provenance is None:
            return None
        if not isinstance(provenance, dict):
            raise ValidationError(
                f"provenance for '{self.name}' must be a dict, "
                f"got {type(provenance).__name__}"
            )
        if not provenance:
            return None
        return dict(provenance)

    def _remove_path(self, path: Path) -> None:
        """Best-effort cleanup for files or directories."""
        try:
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            else:
                path.unlink(missing_ok=True)
        except Exception:
            pass

    def import_snapshot(
        self,
        source_path: str | Path,
        info: dict[str, Any] | None = None,
        provenance: dict[str, Any] | None = None,
    ) -> str:
        """
        Import a new snapshot from a file or directory.

        Copies the source into the NPD directory with a new snapshot suffix and
        optional snapshot info/provenance sidecars. The new snapshot becomes
        current only after all requested files are written.

        Args:
            source_path: Path to source parquet file or hive-partitioned
                directory.
            info: Optional snapshot-info fields: notes, annotations, and
                column_descriptions.
            provenance: Optional explicit per-snapshot provenance. Empty
                dictionaries are treated as absent.

        Returns:
            The suffix of the new snapshot.

        Raises:
            FileNotFoundError: If source_path does not exist.
            SnapshotPublishError: If the import fails.
        """
        if self.is_gcs:
            raise SnapshotPublishError(
                "Direct NPD imports into GCS caches are not supported. "
                "Import locally and sync to GCS."
            )

        source = Path(source_path)
        if not source.exists():
            raise FileNotFoundError(f"Source path does not exist: {source}")
        if not source.is_file() and not source.is_dir():
            raise SnapshotPublishError(
                f"Source path is not a file or directory: {source}"
            )

        current = self.current_suffix
        if current is None:
            current = self._discover_current_suffix()
            self.current_suffix = current

        # Generate new suffix
        new_suffix = generate_snapshot_suffix()

        # Check for duplicate suffix (spec: update fails if same timestamp)
        if current is not None and new_suffix <= current:
            raise SnapshotPublishError(
                f"Snapshot suffix collision: new suffix '{new_suffix}' is not "
                f"greater than current suffix '{current}'. "
                "This can happen if imports are too fast (within same second)."
            )

        resolved_info = self._resolve_import_info(info)
        provenance_payload = self._normalize_import_provenance(provenance)
        provenance_ref: SnapshotProvenanceRef | None = None

        # Ensure NPD directory exists
        self.npd_dir.mkdir(parents=True, exist_ok=True)  # type: ignore[union-attr]

        if source.is_dir():
            dest = self.npd_dir / f"{self.name}_{new_suffix}"  # type: ignore[operator]
            temp_dest = self.npd_dir / f"{self.name}_{new_suffix}_tmp"  # type: ignore[operator]
        else:
            dest = self.npd_dir / f"{self.name}_{new_suffix}.parquet"  # type: ignore[operator]
            temp_dest = self.npd_dir / f"{self.name}_{new_suffix}_tmp.parquet"  # type: ignore[operator]

        if dest.exists():
            raise SnapshotPublishError(f"Destination already exists: {dest}")
        self._remove_path(temp_dest)

        try:
            if provenance_payload is not None:
                provenance_ref = self._write_provenance_sidecar(
                    new_suffix,
                    provenance_payload,
                )
            if resolved_info or provenance_ref is not None:
                self._write_info_sidecar(
                    new_suffix,
                    resolved_info,
                    provenance_ref,
                )

            if source.is_dir():
                shutil.copytree(source, temp_dest)
                temp_dest.rename(dest)
                self._is_directory_snapshot = True
            else:
                shutil.copy2(source, temp_dest)
                temp_dest.replace(dest)
                self._is_directory_snapshot = False

            # Update current suffix and invalidate cache
            self.current_suffix = new_suffix
            self._dataset = None
            self._schema = None

            return new_suffix

        except Exception as e:
            self._remove_path(temp_dest)
            self._remove_path(dest)
            self._delete_info_sidecar(new_suffix)
            self._delete_provenance_sidecar(provenance_ref)
            raise SnapshotPublishError(
                f"Failed to import snapshot for NPD '{self.name}': {e}"
            ) from e

    def summary(self) -> dict[str, Any]:
        """
        Return a dict with dataset metadata.

        Returns:
            Dict containing name, current_suffix, npd_dir, snapshot type, etc.
        """
        result = super().summary()
        result["npd_dir"] = str(self.npd_dir)
        result["is_directory_snapshot"] = self._is_directory_snapshot

        # Count snapshots
        if self.is_gcs:
            from ionbus_parquet_cache.gcs_utils import gcs_ls

            snapshot_count = sum(
                1
                for item_url in gcs_ls(str(self.npd_dir))
                if extract_suffix_from_filename(
                    item_url.rstrip("/").split("/")[-1]
                )
            )
            result["snapshot_count"] = snapshot_count
        elif self.npd_dir.exists():  # type: ignore[union-attr]
            snapshot_count = sum(
                1
                for item in self.npd_dir.iterdir()  # type: ignore[union-attr]
                if extract_suffix_from_filename(item.name)
            )
            result["snapshot_count"] = snapshot_count

        return result
