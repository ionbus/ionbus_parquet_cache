"""
NonDatedParquetDataset - parquet datasets without date partitioning.

NPDs are updated by importing complete snapshots rather than incremental
updates. All snapshots live in the `non-dated/` subdirectory of the cache.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.dataset as pds
from pydantic import PrivateAttr, computed_field

from ionbus_parquet_cache.parquet_dataset_base import ParquetDataset
from ionbus_parquet_cache.exceptions import (
    SnapshotNotFoundError,
    SnapshotPublishError,
)
from ionbus_parquet_cache.snapshot import (
    extract_suffix_from_filename,
    generate_snapshot_suffix,
    get_current_suffix,
)


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

    def import_snapshot(self, source_path: str | Path) -> str:
        """
        Import a new snapshot from a file or directory.

        Copies the source into the NPD directory with a new snapshot suffix.
        The new snapshot becomes the current snapshot.

        Args:
            source_path: Path to source parquet file or hive-partitioned
                directory.

        Returns:
            The suffix of the new snapshot.

        Raises:
            FileNotFoundError: If source_path does not exist.
            SnapshotPublishError: If the import fails.
        """
        source = Path(source_path)
        if not source.exists():
            raise FileNotFoundError(f"Source path does not exist: {source}")

        # Generate new suffix
        new_suffix = generate_snapshot_suffix()

        # Check for duplicate suffix (spec: update fails if same timestamp)
        current = self.current_suffix
        if current is not None and new_suffix <= current:
            raise SnapshotPublishError(
                f"Snapshot suffix collision: new suffix '{new_suffix}' is not "
                f"greater than current suffix '{current}'. "
                "This can happen if imports are too fast (within same second)."
            )

        # Ensure NPD directory exists
        self.npd_dir.mkdir(parents=True, exist_ok=True)

        try:
            if source.is_dir():
                # Copy directory (hive-partitioned)
                dest = self.npd_dir / f"{self.name}_{new_suffix}"
                shutil.copytree(source, dest)
                self._is_directory_snapshot = True
            else:
                # Copy single file
                dest = self.npd_dir / f"{self.name}_{new_suffix}.parquet"
                shutil.copy2(source, dest)
                self._is_directory_snapshot = False

            # Update current suffix and invalidate cache
            self.current_suffix = new_suffix
            self._dataset = None
            self._schema = None

            return new_suffix

        except Exception as e:
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
