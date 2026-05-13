"""
CacheRegistry - manage multiple cache locations with priority search.

The registry is a singleton that searches caches in registration order
when reading data.
"""

from __future__ import annotations

import datetime as dt
import os
from enum import Enum
from pathlib import Path
from typing import Any

import pandas as pd

from ionbus_parquet_cache.dated_dataset import (
    DatedParquetDataset,
    SnapshotMetadata,
    dpd_from_metadata_config,
)
from ionbus_parquet_cache.exceptions import SnapshotNotFoundError
from ionbus_parquet_cache.non_dated_dataset import NonDatedParquetDataset
from ionbus_parquet_cache.snapshot import extract_suffix_from_filename


class DatasetType(Enum):
    """Enum for specifying dataset type in searches."""

    DATED = "dated"
    NON_DATED = "non_dated"


class CacheRegistry:
    """
    Singleton registry for managing multiple cache locations.

    Caches are searched in registration order. The first cache containing
    a requested dataset is used.

    Example:
        registry = CacheRegistry.instance(
            local="c:/Users/me/parquet_cache",
            team="w:/team/parquet_cache",
            official="w:/firm/parquet_cache",
        )
        df = registry.read_data("md.futures_daily", start_date="2024-01-01")
    """

    _instance: "CacheRegistry | None" = None

    def __init__(self) -> None:
        """Initialize an empty registry, then load from IBU_PARQUET_CACHE env var."""
        self._caches: dict[str, str | Path] = {}
        self._cache_order: list[str] = []
        self._dpd_cache: dict[tuple[str, str], DatedParquetDataset] = {}
        self._npd_cache: dict[tuple[str, str], NonDatedParquetDataset] = {}
        self._load_from_env()

    def _load_from_env(self) -> None:
        """
        Load caches from the IBU_PARQUET_CACHE environment variable.

        Format: name|location,name|location
        Example: local|/data/cache,prod|gs://my-bucket/cache
        """
        env_val = os.environ.get("IBU_PARQUET_CACHE", "").strip()
        if not env_val:
            return
        for entry in env_val.split(","):
            entry = entry.strip()
            if not entry or "|" not in entry:
                continue
            name, location = entry.split("|", 1)
            self.add_cache(name.strip(), location.strip())

    @classmethod
    def instance(cls, **named_paths: str | Path) -> "CacheRegistry":
        """
        Get or create the singleton registry.

        Args:
            **named_paths: Named cache paths to register.
                Keys are cache names, values are paths.

        Returns:
            The singleton CacheRegistry instance.

        Example:
            registry = CacheRegistry.instance(
                local="/path/to/local",
                team="/path/to/team",
            )
        """
        if cls._instance is None:
            cls._instance = cls()

        for name, path in named_paths.items():
            cls._instance.add_cache(name, path)

        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset the singleton instance (mainly for testing)."""
        cls._instance = None

    def add_cache(self, name: str, path: str | Path) -> None:
        """
        Register a cache location.

        If already registered, updates the path but preserves order.
        GCS paths (gs://) are stored as strings; local paths as Path objects.

        Args:
            name: Name for this cache location.
            path: Path or gs:// URL to the cache directory.
        """
        from ionbus_parquet_cache.gcs_utils import is_gcs_path

        path_str = str(path)
        stored: str | Path = (
            path_str.rstrip("/") if is_gcs_path(path_str) else Path(path)
        )
        if name not in self._caches:
            self._cache_order.append(name)
        self._caches[name] = stored

    def get_cache_path(self, name: str) -> str | Path | None:
        """
        Get the path for a named cache.

        Args:
            name: Cache name.

        Returns:
            Path to the cache, or None if not registered.
        """
        return self._caches.get(name)

    def discover_dpds(self, cache_path: str | Path) -> dict[str, str | Path]:
        """
        Discover DPDs in a cache by scanning for _meta_data directories.

        Args:
            cache_path: Path or gs:// URL to the cache.

        Returns:
            Dict mapping dataset name to data directory path (str for GCS, Path local).
        """
        from ionbus_parquet_cache.gcs_utils import is_gcs_path

        if is_gcs_path(str(cache_path)):
            return self._discover_dpds_gcs(str(cache_path))

        cache_path = Path(cache_path)
        dpds: dict[str, str | Path] = {}
        if not cache_path.exists():
            return dpds

        for item in cache_path.iterdir():
            if item.is_dir() and not item.name.startswith("."):
                meta_dir = item / "_meta_data"
                if meta_dir.exists() and meta_dir.is_dir():
                    dpds[item.name] = item

        return dpds

    def _discover_dpds_gcs(self, gcs_root: str) -> dict[str, str]:
        """Discover DPDs in a GCS cache using efficient two-level listing."""
        from ionbus_parquet_cache.gcs_utils import gcs_ls

        result: dict[str, str] = {}
        for item_url in gcs_ls(gcs_root):
            name = item_url.rstrip("/").split("/")[-1]
            if name.startswith(".") or name.startswith("_"):
                continue
            dataset_url = item_url.rstrip("/")
            if gcs_ls(f"{dataset_url}/_meta_data"):
                result[name] = dataset_url
        return result

    def discover_npds(self, cache_path: str | Path) -> dict[str, str | Path]:
        """
        Discover NPDs by scanning the non-dated directory.

        Args:
            cache_path: Path or gs:// URL to the cache.

        Returns:
            Dict mapping dataset name to NPD directory path (str for GCS, Path local).
        """
        from ionbus_parquet_cache.gcs_utils import is_gcs_path

        if is_gcs_path(str(cache_path)):
            return self._discover_npds_gcs(str(cache_path))

        cache_path = Path(cache_path)
        npds: dict[str, str | Path] = {}
        non_dated = cache_path / "non-dated"
        if not non_dated.exists():
            return npds

        for item in non_dated.iterdir():
            if item.is_dir():
                has_snapshots = any(
                    extract_suffix_from_filename(child.name)
                    for child in item.iterdir()
                )
                if has_snapshots:
                    npds[item.name] = item

        return npds

    def _discover_npds_gcs(self, gcs_root: str) -> dict[str, str]:
        """Discover NPDs in a GCS cache using efficient two-level listing."""
        from ionbus_parquet_cache.gcs_utils import gcs_ls

        result: dict[str, str] = {}
        non_dated_url = f"{gcs_root}/non-dated"
        for item_url in gcs_ls(non_dated_url):
            name = item_url.rstrip("/").split("/")[-1]
            dataset_url = item_url.rstrip("/")
            children = gcs_ls(dataset_url)
            if any(
                extract_suffix_from_filename(c.rstrip("/").split("/")[-1])
                for c in children
            ):
                result[name] = dataset_url
        return result

    def _get_dpd(
        self, name: str, cache_name: str
    ) -> DatedParquetDataset | None:
        """
        Get or create a DPD instance.

        Args:
            name: Dataset name.
            cache_name: Cache name.

        Returns:
            DatedParquetDataset instance, or None if not found.
        """
        key = (cache_name, name)
        if key in self._dpd_cache:
            return self._dpd_cache[key]

        cache_path = self._caches.get(cache_name)
        if not cache_path:
            return None

        dpds = self.discover_dpds(cache_path)
        if name not in dpds:
            return None

        # Load metadata to get configuration
        from ionbus_parquet_cache.gcs_utils import is_gcs_path

        if is_gcs_path(str(cache_path)):
            # GCS: stream metadata directly; DPD will discover suffix via GCS
            data_dir_str = str(dpds[name])
            meta_prefix = f"{data_dir_str}/_meta_data"
            from ionbus_parquet_cache.gcs_utils import gcs_ls, gcs_open

            suffixes_gcs = []
            for blob_url in gcs_ls(meta_prefix):
                blob_name = blob_url.rstrip("/").split("/")[-1]
                if "_trimmed" in blob_name:
                    continue
                if blob_name.endswith(".pkl.gz"):
                    suffix = extract_suffix_from_filename(blob_name)
                    if suffix:
                        suffixes_gcs.append((suffix, blob_url))
            if not suffixes_gcs:
                return None
            current_suffix, meta_url = max(suffixes_gcs, key=lambda x: x[0])
            with gcs_open(meta_url) as f:
                metadata = SnapshotMetadata.from_pickle(f)
        else:
            data_dir = dpds[name]
            meta_dir = data_dir / "_meta_data"  # type: ignore[operator]

            suffixes_local = []
            for item in meta_dir.iterdir():
                if "_trimmed" in item.name:
                    continue
                if item.name.endswith(".pkl.gz"):
                    suffix = extract_suffix_from_filename(item.name)
                    if suffix:
                        suffixes_local.append((suffix, item))

            if not suffixes_local:
                return None

            current_suffix, meta_path = max(
                suffixes_local, key=lambda x: x[0]
            )
            metadata = SnapshotMetadata.from_pickle(meta_path)
        config = metadata.yaml_config

        # Create DPD with configuration from metadata
        dpd = dpd_from_metadata_config(cache_path, name, config)

        # Set loaded state so is_update_available() and summary() work
        dpd.current_suffix = current_suffix
        dpd._metadata = metadata

        self._dpd_cache[key] = dpd
        return dpd

    def _require_dpd(
        self,
        name: str,
        cache_name: str | None = None,
    ) -> DatedParquetDataset:
        """Return a DPD or raise SnapshotNotFoundError."""
        dataset = self.get_dataset(name, cache_name, DatasetType.DATED)
        if dataset is None or not isinstance(dataset, DatedParquetDataset):
            raise SnapshotNotFoundError(
                f"DPD dataset '{name}' not found in any cache",
                dataset_name=name,
            )
        return dataset

    def _get_npd(
        self, name: str, cache_name: str
    ) -> NonDatedParquetDataset | None:
        """
        Get or create an NPD instance.

        Args:
            name: Dataset name.
            cache_name: Cache name.

        Returns:
            NonDatedParquetDataset instance, or None if not found.
        """
        key = (cache_name, name)
        if key in self._npd_cache:
            return self._npd_cache[key]

        cache_path = self._caches.get(cache_name)
        if not cache_path:
            return None

        npds = self.discover_npds(cache_path)
        if name not in npds:
            return None

        npd = NonDatedParquetDataset(cache_dir=cache_path, name=name)

        # Set loaded state so is_update_available() and summary() work
        npd.current_suffix = npd._discover_current_suffix()

        self._npd_cache[key] = npd
        return npd

    def get_dataset(
        self,
        name: str,
        cache_name: str | None = None,
        dataset_type: DatasetType | None = None,
    ) -> DatedParquetDataset | NonDatedParquetDataset | None:
        """
        Get a dataset by name.

        Searches caches in registration order unless cache_name is specified.
        By default looks for DPD first, then NPD.

        Args:
            name: Dataset name.
            cache_name: Optional specific cache to search.
            dataset_type: Optional type filter:
                - None (default): look for DPD first, then NPD
                - DatasetType.DATED: only look for DatedParquetDataset
                - DatasetType.NON_DATED: only look for NonDatedParquetDataset

        Returns:
            The dataset, or None if not found.
        """
        caches_to_search = [cache_name] if cache_name else self._cache_order

        look_for_dpd = dataset_type in (None, DatasetType.DATED)
        look_for_npd = dataset_type in (None, DatasetType.NON_DATED)

        for cn in caches_to_search:
            if cn not in self._caches:
                continue

            # Try DPD first (if looking for it)
            if look_for_dpd:
                dpd = self._get_dpd(name, cn)
                if dpd:
                    return dpd

            # Then NPD (if looking for it)
            if look_for_npd:
                npd = self._get_npd(name, cn)
                if npd:
                    return npd

        return None

    def get_latest_snapshot(
        self,
        name: str,
        cache_name: str | None = None,
    ) -> str:
        """
        Return the latest snapshot suffix available for a dataset.

        Args:
            name: Dataset name.
            cache_name: Optional specific cache to search.

        Returns:
            Snapshot suffix string (e.g. "20240501_120000").

        Raises:
            SnapshotNotFoundError: If dataset not found.
        """
        dataset = self.get_dataset(name, cache_name)
        if dataset is None:
            raise SnapshotNotFoundError(
                f"Dataset '{name}' not found in any cache",
                dataset_name=name,
            )
        return dataset.current_suffix

    def cache_history(
        self,
        name: str,
        cache_name: str | None = None,
        snapshot: str | None = None,
    ):
        """
        Return DPD snapshot lineage history.

        Args:
            name: Dataset name.
            cache_name: Optional specific cache to search.
            snapshot: Optional snapshot suffix. If None, uses current.
        """
        return self._require_dpd(name, cache_name).cache_history(snapshot)

    def read_provenance(
        self,
        name: str,
        cache_name: str | None = None,
        snapshot: str | None = None,
    ) -> dict[str, Any]:
        """
        Load optional external provenance for a dataset snapshot.

        Args:
            name: Dataset name.
            cache_name: Optional specific cache to search.
            snapshot: Optional snapshot suffix. If None, uses current.
        """
        dataset = self.get_dataset(name, cache_name)
        if dataset is None:
            raise SnapshotNotFoundError(
                f"Dataset '{name}' not found in any cache",
                dataset_name=name,
            )
        return dataset.read_provenance(snapshot)

    def read_data(
        self,
        name: str,
        start_date: str | dt.date | None = None,
        end_date: str | dt.date | None = None,
        filters: list[tuple[str, str, Any]] | None = None,
        columns: list[str] | None = None,
        cache_name: str | None = None,
        snapshot: str | None = None,
        instruments: Any = None,
    ) -> pd.DataFrame:
        """
        Read data from a dataset.

        Args:
            name: Dataset name.
            start_date: Optional start date for filtering.
            end_date: Optional end date for filtering.
            filters: Optional filters.
            columns: Optional column list.
            cache_name: Optional specific cache to use.
            snapshot: Optional snapshot suffix. If None, uses current snapshot.

        Returns:
            A pandas DataFrame.

        Raises:
            SnapshotNotFoundError: If dataset not found.
            ValueError: If dates missing for DPD.
        """
        dataset = self.get_dataset(name, cache_name)
        if dataset is None:
            raise SnapshotNotFoundError(
                f"Dataset '{name}' not found in any cache",
                dataset_name=name,
            )

        if isinstance(dataset, DatedParquetDataset):
            return dataset.read_data(
                start_date=start_date,
                end_date=end_date,
                filters=filters,
                columns=columns,
                snapshot=snapshot,
                instruments=instruments,
            )
        else:
            return dataset.read_data(
                filters=filters, columns=columns, snapshot=snapshot
            )

    def read_data_pl(
        self,
        name: str,
        start_date: str | dt.date | None = None,
        end_date: str | dt.date | None = None,
        filters: list[tuple[str, str, Any]] | None = None,
        columns: list[str] | None = None,
        cache_name: str | None = None,
        snapshot: str | None = None,
        instruments: Any = None,
    ) -> Any:
        """
        Read data from a dataset into a Polars DataFrame.

        Args:
            name: Dataset name.
            start_date: Optional start date for filtering.
            end_date: Optional end date for filtering.
            filters: Optional filters.
            columns: Optional column list.
            cache_name: Optional specific cache to use.
            snapshot: Optional snapshot suffix. If None, uses current snapshot.

        Returns:
            A Polars DataFrame.

        Raises:
            SnapshotNotFoundError: If dataset not found.
            ValueError: If dates missing for DPD.
            ImportError: If polars not installed.
        """
        dataset = self.get_dataset(name, cache_name)
        if dataset is None:
            raise SnapshotNotFoundError(
                f"Dataset '{name}' not found in any cache",
                dataset_name=name,
            )

        if isinstance(dataset, DatedParquetDataset):
            return dataset.read_data_pl(
                start_date=start_date,
                end_date=end_date,
                filters=filters,
                columns=columns,
                snapshot=snapshot,
                instruments=instruments,
            )
        else:
            return dataset.read_data_pl(
                filters=filters, columns=columns, snapshot=snapshot
            )

    def to_table(
        self,
        name: str,
        start_date: str | dt.date | None = None,
        end_date: str | dt.date | None = None,
        filters: list[tuple[str, str, Any]] | None = None,
        columns: list[str] | None = None,
        cache_name: str | None = None,
        snapshot: str | None = None,
    ):
        """
        Read data from a dataset into a PyArrow Table.

        Args:
            name: Dataset name.
            start_date: Optional start date for filtering.
            end_date: Optional end date for filtering.
            filters: Optional filters.
            columns: Optional column list.
            cache_name: Optional specific cache to use.
            snapshot: Optional snapshot suffix. If None, uses current snapshot.

        Returns:
            A PyArrow Table.

        Raises:
            SnapshotNotFoundError: If dataset not found.
        """
        dataset = self.get_dataset(name, cache_name)
        if dataset is None:
            raise SnapshotNotFoundError(
                f"Dataset '{name}' not found in any cache",
                dataset_name=name,
            )

        if isinstance(dataset, DatedParquetDataset):
            return dataset.to_table(
                start_date=start_date,
                end_date=end_date,
                filters=filters,
                columns=columns,
                snapshot=snapshot,
            )
        else:
            return dataset.to_table(
                filters=filters, columns=columns, snapshot=snapshot
            )

    def pyarrow_dataset(
        self,
        name: str,
        start_date: str | dt.date | None = None,
        end_date: str | dt.date | None = None,
        filters: list[tuple[str, str, Any]] | None = None,
        cache_name: str | None = None,
        snapshot: str | None = None,
    ):
        """
        Get the PyArrow dataset for a dataset.

        Args:
            name: Dataset name.
            start_date: Optional start date filter (DPDs only).
            end_date: Optional end date filter (DPDs only).
            filters: Optional filters.
            cache_name: Optional specific cache to use.
            snapshot: Optional snapshot suffix. If None, uses current snapshot.

        Returns:
            A pyarrow.dataset.Dataset.

        Raises:
            SnapshotNotFoundError: If dataset not found.
        """
        dataset = self.get_dataset(name, cache_name)
        if dataset is None:
            raise SnapshotNotFoundError(
                f"Dataset '{name}' not found in any cache",
                dataset_name=name,
            )

        return dataset.pyarrow_dataset(
            start_date=start_date,
            end_date=end_date,
            filters=filters,
            snapshot=snapshot,
        )

    def data_summary(
        self,
        *,
        refresh_and_possibly_change_loaded_caches: bool = False,
    ) -> pd.DataFrame:
        """
        Get a summary of all datasets across all caches.

        Args:
            refresh_and_possibly_change_loaded_caches: If False (default),
                uses cached metadata without discovering new snapshots.
                If True, discovers fresh snapshots and reloads metadata,
                which MUTATES cached DPD instances in this registry.

                CRITICAL: When set to True, this function:
                - Discovers new snapshots on disk
                - Reloads SnapshotMetadata for changed datasets
                - Clears the internal read cache (_dataset, _schema,
                  _metadata) for any reloaded DPD
                - Causes subsequent reads to return data from the NEW
                  snapshot, not the one that was originally loaded

                Only use this if you have NO active references to DPD
                instances from this registry, and you understand that
                any code holding a reference will now read from a
                different snapshot than before.

        Returns:
            DataFrame with columns:
            - cache, name, type, current_suffix (all types)
            - start_date, end_date, file_count, total_size_mb,
              partition_columns, instrument_buckets, description (DPDs only)
            - days_since_update (all types, calculated from suffix timestamp)
        """
        import datetime as dt

        rows = []

        for cache_name in self._cache_order:
            cache_path = self._caches[cache_name]

            # Discover DPDs
            for dpd_name in self.discover_dpds(cache_path):
                dpd = self._get_dpd(dpd_name, cache_name)
                if dpd:
                    try:
                        from ionbus_parquet_cache.snapshot import (
                            parse_snapshot_suffix,
                        )

                        meta = dpd._metadata
                        suffix = meta.suffix if meta else dpd.current_suffix

                        # If refresh is enabled, discover fresh suffix and
                        # reload metadata (with side effects on cached DPD)
                        if refresh_and_possibly_change_loaded_caches:
                            suffix = dpd._discover_current_suffix()
                            if meta is None or meta.suffix != suffix:
                                dpd.current_suffix = suffix
                                dpd.invalidate_read_cache()
                                meta = dpd._load_metadata()

                        if meta:
                            total_size = sum(
                                f.size_bytes for f in meta.files
                            ) / (1024 * 1024)
                            snapshot_ts = parse_snapshot_suffix(suffix)
                            snapshot_dt = dt.datetime.fromtimestamp(
                                snapshot_ts, tz=dt.timezone.utc
                            )
                            days_since = (
                                dt.datetime.now(dt.timezone.utc) - snapshot_dt
                            ).total_seconds() / 86400
                            partition_cols = meta.yaml_config.get(
                                "partition_columns", []
                            )
                            bucketing = meta.yaml_config.get(
                                "num_instrument_buckets"
                            )
                            rows.append(
                                {
                                    "cache": cache_name,
                                    "name": dpd_name,
                                    "type": "DPD",
                                    "current_suffix": suffix,
                                    "start_date": meta.cache_start_date,
                                    "end_date": meta.cache_end_date,
                                    "file_count": len(meta.files),
                                    "total_size_mb": round(total_size, 2),
                                    "partition_columns": partition_cols,
                                    "instrument_buckets": bucketing,
                                    "days_since_update": round(days_since, 2),
                                    "description": meta.yaml_config.get(
                                        "description", ""
                                    ),
                                }
                            )
                        else:
                            rows.append(
                                {
                                    "cache": cache_name,
                                    "name": dpd_name,
                                    "type": "DPD",
                                    "current_suffix": suffix,
                                }
                            )
                    except Exception as e:
                        from ionbus_utils.logging_utils import logger

                        logger.error(
                            f"Failed to load metadata for {dpd_name}: {e}"
                        )
                        rows.append(
                            {
                                "cache": cache_name,
                                "name": dpd_name,
                                "type": "DPD",
                                "current_suffix": None,
                            }
                        )

            # Discover NPDs
            for npd_name in self.discover_npds(cache_path):
                npd = self._get_npd(npd_name, cache_name)
                if npd:
                    try:
                        from ionbus_parquet_cache.snapshot import (
                            parse_snapshot_suffix,
                        )

                        # Only discover fresh suffix if refresh is enabled
                        if refresh_and_possibly_change_loaded_caches:
                            suffix = npd._discover_current_suffix()
                        else:
                            # Use cached current_suffix if available
                            suffix = npd.current_suffix
                            if suffix is None:
                                suffix = npd._discover_current_suffix()

                        if suffix:
                            snapshot_ts = parse_snapshot_suffix(suffix)
                            snapshot_dt = dt.datetime.fromtimestamp(
                                snapshot_ts, tz=dt.timezone.utc
                            )
                            days_since = (
                                dt.datetime.now(dt.timezone.utc) - snapshot_dt
                            ).total_seconds() / 86400
                            rows.append(
                                {
                                    "cache": cache_name,
                                    "name": npd_name,
                                    "type": "NPD",
                                    "current_suffix": suffix,
                                    "days_since_update": round(days_since, 2),
                                }
                            )
                        else:
                            rows.append(
                                {
                                    "cache": cache_name,
                                    "name": npd_name,
                                    "type": "NPD",
                                    "current_suffix": None,
                                }
                            )
                    except Exception as e:
                        from ionbus_utils.logging_utils import logger

                        logger.error(
                            f"Failed to discover NPD {npd_name}: {e}"
                        )
                        rows.append(
                            {
                                "cache": cache_name,
                                "name": npd_name,
                                "type": "NPD",
                                "current_suffix": None,
                            }
                        )

        return pd.DataFrame(rows)

    def discover_all_dpds(self) -> dict[str, tuple[str, Path]]:
        """
        Discover all DPDs across all registered caches.

        Returns:
            Dict mapping dataset name to (cache_name, path) tuple.
            If the same dataset exists in multiple caches, the first
            cache (in registration order) takes precedence.
        """
        result: dict[str, tuple[str, Path]] = {}
        for cache_name in self._cache_order:
            cache_path = self._caches[cache_name]
            for dpd_name, dpd_path in self.discover_dpds(cache_path).items():
                if dpd_name not in result:
                    result[dpd_name] = (cache_name, dpd_path)
        return result

    def discover_all_npds(self) -> dict[str, tuple[str, Path]]:
        """
        Discover all NPDs across all registered caches.

        Returns:
            Dict mapping dataset name to (cache_name, path) tuple.
            If the same dataset exists in multiple caches, the first
            cache (in registration order) takes precedence.
        """
        result: dict[str, tuple[str, Path]] = {}
        for cache_name in self._cache_order:
            cache_path = self._caches[cache_name]
            for npd_name, npd_path in self.discover_npds(cache_path).items():
                if npd_name not in result:
                    result[npd_name] = (cache_name, npd_path)
        return result

    def refresh_all(self) -> bool:
        """
        Refresh all cached datasets to their latest snapshots.

        Useful in long-running notebooks or processes where you want to
        pick up any new data that has been published since the datasets
        were first loaded.

        Returns:
            True if any dataset was refreshed, False if all were current.
        """
        any_refreshed = False

        # Refresh all cached DPDs
        for dpd in self._dpd_cache.values():
            if dpd.refresh():
                any_refreshed = True

        # Refresh all cached NPDs
        for npd in self._npd_cache.values():
            if npd.refresh():
                any_refreshed = True

        return any_refreshed
