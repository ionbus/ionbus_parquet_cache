"""
Base class for parquet datasets with snapshot versioning.

ParquetDataset is the abstract base class inherited by both
DatedParquetDataset and NonDatedParquetDataset.
"""

from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, ClassVar

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as pds
from pydantic import PrivateAttr

from ionbus_parquet_cache.exceptions import SnapshotNotFoundError
from ionbus_parquet_cache.filter_utils import build_dataset_filter
from ionbus_utils.yaml_utils.pdyaml import PDYaml


class ParquetDataset(PDYaml, ABC):
    """
    Abstract base class for parquet datasets with snapshot versioning.

    Provides the common interface for reading parquet data. Both
    DatedParquetDataset and NonDatedParquetDataset inherit from this class.

    Attributes:
        name: Unique name for this dataset (inherited from PDYaml).
        cache_dir: Root directory of the cache.
        current_suffix: Suffix of the currently loaded snapshot
            (e.g., "1Gz5hK").
        schema: PyArrow schema of the dataset (lazy-loaded property).
    """

    is_dated_dataset_type: ClassVar[bool] = False
    date_col: str | None = None

    cache_dir: Path
    current_suffix: str | None = None

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
            self._dataset = None
            self._schema = None
            return False

        if latest != self.current_suffix:
            # New snapshot available - invalidate cache
            self.current_suffix = latest
            self._dataset = None
            self._schema = None
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
