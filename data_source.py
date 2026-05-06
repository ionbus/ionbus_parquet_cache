"""
DataSource abstract class - interface for supplying data to DatedParquetDataset.

A DataSource is the only way to supply data to a DatedParquetDataset.
Subclasses must implement available_dates() and get_data().
The base class provides default prepare() and get_partitions() implementations.
"""

from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from itertools import product
from typing import TYPE_CHECKING, Any, Callable, Union

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.dataset as pds

from ionbus_utils.logging_utils import logger

from ionbus_parquet_cache.bucketing import (
    INSTRUMENT_BUCKET_COL,
    all_bucket_strings,
    bucket_instruments,
)
from ionbus_parquet_cache.exceptions import DataSourceError, ValidationError
from ionbus_parquet_cache.partition import (
    PartitionSpec,
    date_partition_column_name,
    date_partitions,
)

if TYPE_CHECKING:
    from ionbus_parquet_cache.dated_dataset import DatedParquetDataset

# Type alias for data returned by get_data()
DataFrameType = Union[
    pa.Table, pds.Dataset, pd.DataFrame, pl.DataFrame, pl.LazyFrame
]


class DataSource(ABC):
    """
    Abstract base class for data sources.

    A DataSource supplies data to a DatedParquetDataset during updates.
    Subclasses must implement:
    - available_dates(): Return the date range the source can provide
    - get_data(): Return data for a single partition/chunk

    The base class provides default implementations for:
    - prepare(): Calls set_date_instruments() to store state. Override only
      if custom setup logic is needed (e.g., API connections).
    - get_partitions(): Uses class attributes to build partition list.
      Override only if custom chunking logic is needed.

    Class Attributes:
        chunk_days: Business days to fetch per chunk. If None, fetches entire
            range.
        partition_order: Order to iterate partition columns. If None, uses
            date partition first, then other columns in YAML order.
        partition_values: Known values for partition columns. Used by default
            get_partition_column_values() implementation.
        chunk_values: Dict mapping chunk dimension names to lists of values.
            Creates multiple PartitionSpecs with same partition_key but
            different chunk_info. Use for splitting work without creating
            separate files (e.g., fetching by exchange or batch number).
        chunk_expander: Optional function that takes a PartitionSpec and
            returns a list of PartitionSpecs with different chunk_info.
            Use for dynamic chunk expansion that depends on the partition.
            Applied after chunk_values expansion.

    Instance Attributes:
        dataset: The owning DatedParquetDataset.
        start_date: Start date for current update (set by prepare).
        end_date: End date for current update (set by prepare).
        instruments: Instruments filter for current update (set by prepare).
        prepared: Whether prepare() has been called.

    Example:
        class MyFuturesSource(DataSource):
            chunk_days = 30  # fetch 30 business days at a time
            partition_values = {"FutureRoot": ["ES", "NQ", "YM"]}

            def available_dates(self) -> tuple[dt.date, dt.date]:
                return (dt.date(2020, 1, 1), dt.date.today() - dt.timedelta(days=1))

            def prepare(self, start_date, end_date, instruments=None):
                self.set_date_instruments(start_date, end_date, instruments)
                # Now self.start_date, self.end_date, self.instruments are set
                self.api_session = self.api.login(start_date, end_date)

            def get_data(self, partition_spec):
                # Fetch data from API/database
                return self.api.fetch(
                    start=partition_spec.start_date,
                    end=partition_spec.end_date,
                    instrument=partition_spec.partition_values.get("FutureRoot"),
                )
    """

    # Class attributes that can be overridden in subclasses
    chunk_days: int | None = None
    partition_order: list[str] | None = None
    # Note: partition_values should be overridden in subclasses, not mutated
    partition_values: dict[str, list[Any]] = {}
    # Note: chunk_values should be overridden in subclasses, not mutated
    # These create multiple PartitionSpecs with same partition_key but different
    # chunk_info, allowing work to be split without creating separate files.
    chunk_values: dict[str, list[Any]] = {}
    # Optional function for dynamic chunk expansion. Takes a PartitionSpec and
    # returns a list of PartitionSpecs with different chunk_info values.
    chunk_expander: Callable[[PartitionSpec], list[PartitionSpec]] | None = (
        None
    )

    # Instance attributes (declared for documentation, set in __init__)
    dataset: "DatedParquetDataset"
    start_date: dt.date | None
    end_date: dt.date | None
    instruments: list[str] | None
    prepared: bool
    _partition_values: dict[str, list[Any]]
    _chunk_values: dict[str, list[Any]]

    def __init__(self, dataset: "DatedParquetDataset", **kwargs: Any):
        """
        Initialize the DataSource.

        Args:
            dataset: The owning DatedParquetDataset.
            **kwargs: Additional arguments from source_init_args in YAML.
        """
        self.dataset = dataset
        self.start_date = None
        self.end_date = None
        self.instruments = None
        self.prepared = False
        # Copy class-level partition_values to instance to avoid shared mutation
        self._partition_values = dict(self.partition_values)
        # Copy class-level chunk_values to instance to avoid shared mutation
        self._chunk_values = dict(self.chunk_values)

        # Store any additional kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

    @abstractmethod
    def available_dates(self) -> tuple[dt.date, dt.date]:
        """
        Return the date range this source can provide.

        Returns:
            Tuple of (start_date, end_date) where:
            - start_date is the earliest date the source has data for
            - end_date is the most recent date data is available
              (typically the previous business day)

        Example:
            def available_dates(self) -> tuple[dt.date, dt.date]:
                # Source has data from 2020 through yesterday
                return (
                    dt.date(2020, 1, 1),
                    dt.date.today() - dt.timedelta(days=1)
                )
        """
        pass

    def set_date_instruments(
        self,
        start_date: dt.date,
        end_date: dt.date,
        instruments: list[str] | None = None,
    ) -> None:
        """
        Set the date range and instruments for this update.

        Args:
            start_date: First date to fetch.
            end_date: Last date to fetch.
            instruments: Optional list of instruments to limit the update.
        """
        self.start_date = start_date
        self.end_date = end_date
        self.instruments = instruments
        self.prepared = True

    def prepare(
        self,
        start_date: dt.date,
        end_date: dt.date,
        instruments: list[str] | None = None,
    ) -> None:
        """
        Prepare for fetching data in the given date range.

        Called internally by the update pipeline before get_partitions().
        The default implementation calls set_date_instruments() to store
        the values. Subclasses that override this should call
        set_date_instruments() to set the state (no need for super()).

        Args:
            start_date: First date to fetch.
            end_date: Last date to fetch.
            instruments: Optional list of instruments to limit the update.
                If provided, instrument_column must be a partition column.

        Raises:
            ValidationError: If instruments provided but instrument_column
                is not a partition column.
        """
        self.set_date_instruments(start_date, end_date, instruments)

    @abstractmethod
    def get_data(
        self,
        partition_spec: PartitionSpec,
    ) -> "DataFrameType | None":
        """
        Return data for a single partition or chunk.

        Called once per PartitionSpec returned by get_partitions(),
        in the exact order returned.

        Args:
            partition_spec: Contains all information needed to fetch:
                - partition_spec.start_date: Start of date range
                - partition_spec.end_date: End of date range
                - partition_spec.partition_values: Partition context
                - partition_spec.temp_file_path: Where data will be written

        Returns:
            Data as DataFrameType: pyarrow.Table, pyarrow.dataset.Dataset,
            pandas.DataFrame, polars.DataFrame, or polars.LazyFrame.
            The system converts internally.

        Note:
            The returned data does NOT need to include partition columns
            like year, month, quarter. The system derives these from the
            date column when writing files.
        """
        pass

    def get_partitions(self) -> list[PartitionSpec]:
        """
        Return the list of partitions to update.

        The default implementation calls build_partitions() with the
        prepared date range and chunk_days setting.

        Override only if custom chunking logic is needed.

        Returns:
            List of PartitionSpec objects describing partitions to write.

        Raises:
            DataSourceError: If prepare() was not called.
        """
        if not self.prepared:
            raise DataSourceError(
                "prepare() must be called before get_partitions()",
                source_class=self.__class__.__name__,
            )

        if self.start_date is None or self.end_date is None:
            return []

        return self.build_partitions(
            self.start_date,
            self.end_date,
            max_business_days=self.chunk_days,
        )

    def build_partitions(
        self,
        start_date: dt.date,
        end_date: dt.date,
        max_business_days: int | None = None,
    ) -> list[PartitionSpec]:
        """
        Build partition specs for a date range.

        Convenience method that generates all PartitionSpec objects by:
        1. Splitting date range by date_partition granularity
        2. Further splitting by max_business_days if specified
        3. Creating cartesian product with non-date partition column values
        4. Expanding with chunk_values if set (populates chunk_info)

        This method reads partition_columns and date_partition from
        self.dataset to determine the partition structure.

        Args:
            start_date: First date of the range.
            end_date: Last date of the range.
            max_business_days: Optional max business days per chunk.

        Returns:
            List of PartitionSpec objects for the date range.
        """
        # Get date partition ranges
        date_ranges = date_partitions(
            start_date,
            end_date,
            self.dataset.date_partition,
            max_business_days=max_business_days,
        )

        if not date_ranges:
            return []

        # Determine partition column order
        if self.partition_order:
            columns = self.partition_order
        else:
            # Date partition first, then other columns in order
            date_col_name = date_partition_column_name(
                self.dataset.date_partition, self.dataset.date_col
            )
            columns = [date_col_name]
            for col in self.dataset.partition_columns:
                if col not in columns:
                    columns.append(col)

        # Build partition specs
        specs = []

        for range_start, range_end, date_partition_val in date_ranges:
            # Get possible values for each non-date partition column
            date_col_name = date_partition_column_name(
                self.dataset.date_partition, self.dataset.date_col
            )
            non_date_columns = [c for c in columns if c != date_col_name]

            if non_date_columns:
                # Get all combinations of non-date partition values
                value_lists = []
                for col in non_date_columns:
                    values = self.get_partition_column_values(col)
                    value_lists.append([(col, v) for v in values])

                for combo in product(*value_lists):
                    partition_values = dict(combo)
                    # Add date partition value
                    partition_values[date_col_name] = date_partition_val

                    specs.append(
                        PartitionSpec(
                            partition_values=partition_values,
                            start_date=range_start,
                            end_date=range_end,
                        )
                    )
            else:
                # Only date partition
                specs.append(
                    PartitionSpec(
                        partition_values={date_col_name: date_partition_val},
                        start_date=range_start,
                        end_date=range_end,
                    )
                )

        # Expand with chunk_values if set
        if self._chunk_values:
            specs = self._expand_with_chunks(specs)

        # Expand with chunk_expander function if set
        if self.chunk_expander is not None:
            specs = self._apply_chunk_expander(specs)

        return specs

    def _apply_chunk_expander(
        self, specs: list[PartitionSpec]
    ) -> list[PartitionSpec]:
        """
        Apply chunk_expander function to expand partition specs.

        Each input spec is passed to chunk_expander, which returns a list
        of expanded specs. This allows dynamic chunk expansion based on
        partition-specific logic.

        Args:
            specs: List of partition specs to expand.

        Returns:
            Expanded list of partition specs.
        """
        if self.chunk_expander is None:
            return specs

        expanded = []
        for spec in specs:
            expanded.extend(self.chunk_expander(spec))

        return expanded

    def _expand_with_chunks(
        self, specs: list[PartitionSpec]
    ) -> list[PartitionSpec]:
        """
        Expand partition specs with chunk_values.

        Each input spec is expanded into multiple specs, one for each
        combination of chunk_values. The chunk values are stored in
        chunk_info, not partition_values.

        Args:
            specs: List of base partition specs.

        Returns:
            Expanded list of partition specs with chunk_info populated.
        """
        if not self._chunk_values:
            return specs

        # Build list of (key, value) pairs for cartesian product
        chunk_lists = []
        for key, values in self._chunk_values.items():
            chunk_lists.append([(key, v) for v in values])

        expanded = []
        for spec in specs:
            for combo in product(*chunk_lists):
                chunk_info = dict(combo)
                expanded.append(
                    PartitionSpec(
                        partition_values=spec.partition_values,
                        start_date=spec.start_date,
                        end_date=spec.end_date,
                        chunk_info=chunk_info,
                    )
                )

        return expanded

    def get_partition_column_values(self, column: str) -> list[Any]:
        """
        Return possible values for a partition column.

        The default implementation checks in order:
        1. If column is date partition, compute from date range
        2. If column matches instrument_column and instruments provided, use those
        3. If column is in self.partition_values, use those
        4. Fall back to dataset.get_partition_values() (from cache metadata)
        5. Raise error if still empty

        Override for dynamic lookups (e.g., API discovery of new instruments).

        Args:
            column: Name of the partition column.

        Returns:
            List of possible values for the column.

        Raises:
            DataSourceError: If no values can be determined.
        """
        # 1. Date partition column - compute from date range
        date_col_name = date_partition_column_name(
            self.dataset.date_partition, self.dataset.date_col
        )
        if column == date_col_name:
            if self.start_date and self.end_date:
                ranges = date_partitions(
                    self.start_date,
                    self.end_date,
                    self.dataset.date_partition,
                )
                return list(set(r[2] for r in ranges))
            return []

        # 2. Instrument column with provided instruments
        if (
            hasattr(self.dataset, "instrument_column")
            and column == self.dataset.instrument_column
            and self.instruments
        ):
            return self.instruments

        # 3. Instance partition_values (copied from class-level)
        if column in self._partition_values:
            return self._partition_values[column]

        # 4. Fall back to cache metadata
        try:
            values = self.dataset.get_partition_values(column)
            if values:
                return values
        except Exception:
            pass

        # 5. Error if no values found
        raise DataSourceError(
            f"Cannot determine values for partition column '{column}'. "
            f"Set partition_values in DataSource or ensure column exists in cache.",
            source_class=self.__class__.__name__,
        )

    def on_update_complete(
        self,
        suffix: str,
        previous_suffix: str | None,
    ) -> None:
        """
        Called once after all partitions have been written and the snapshot
        is published.

        Override to write external audit trails, log metadata, or do any
        other bookkeeping. Cache-local provenance that should travel with the
        snapshot belongs in ``get_provenance()``. ``self.start_date``,
        ``self.end_date``, and ``self.instruments`` are still set from
        ``prepare()`` at this point.

        Args:
            suffix: The snapshot suffix that was just published.
            previous_suffix: The suffix of the snapshot that existed before
                this update, or ``None`` if this is the first update of the
                cache.
        """

    def get_provenance(
        self,
        suffix: str,
        previous_suffix: str | None,
    ) -> dict[str, Any]:
        """
        Return optional cache-local provenance for this snapshot.

        The default empty dictionary means no provenance sidecar is written.
        Subclasses may return any non-empty dictionary of pickle-serializable
        values to store outside snapshot metadata.
        """
        return {}

    def _do_prepare(
        self,
        start_date: dt.date,
        end_date: dt.date,
        instruments: list[str] | None = None,
    ) -> None:
        """
        Internal method to set up state before calling subclass prepare().

        Called by the update pipeline.
        """
        # Validate instruments if provided
        if instruments:
            instrument_col = getattr(self.dataset, "instrument_column", None)
            if not instrument_col:
                raise ValidationError(
                    "instruments provided but dataset has no instrument_column set"
                )
            # When bucketing is active, instrument_column is not a direct
            # partition column — skip the partition-column check.
            is_bucketed = INSTRUMENT_BUCKET_COL in getattr(
                self.dataset, "partition_columns", []
            )
            if (
                not is_bucketed
                and instrument_col not in self.dataset.partition_columns
            ):
                raise ValidationError(
                    f"instrument_column '{instrument_col}' must be a partition "
                    f"column to use instruments filter. "
                    f"Partition columns: {self.dataset.partition_columns}"
                )

        # Call prepare which sets state via set_date_instruments
        self.prepare(start_date, end_date, instruments)


class BucketedDataSource(DataSource, ABC):
    """
    Base class for DataSources that work with hash-bucketed DPDs.

    The dataset must have ``instrument_column`` and ``num_instrument_buckets``
    set.  This class generates ``(bucket × date_partition)`` specs in
    ``get_partitions()`` and dispatches to ``get_data_for_bucket()`` lazily,
    caching the instrument list per date-partition period so
    ``get_instruments_for_time_period()`` is called only once per period.

    Subclasses implement:
        available_dates()
        get_instruments_for_time_period(start_date, end_date) -> list
        get_data_for_bucket(instruments, start_date, end_date)
            -> DataFrameType | None

    Do NOT override ``get_partitions()`` or ``get_data()``.
    """

    def __init__(self, dataset: "Any", **kwargs: Any) -> None:
        super().__init__(dataset, **kwargs)
        self._period_bucket_cache: dict[str, dict[str, list]] = {}

    def prepare(
        self,
        start_date: dt.date,
        end_date: dt.date,
        instruments: list[str] | None = None,
    ) -> None:
        self._period_bucket_cache = {}
        super().prepare(start_date, end_date, instruments)

    @abstractmethod
    def get_instruments_for_time_period(
        self,
        start_date: dt.date,
        end_date: dt.date,
    ) -> list:
        """
        Return instruments active in this period.

        Called once per date-partition value (e.g. once per year).
        The result is cached; subsequent specs for the same period reuse it.
        """

    @abstractmethod
    def get_data_for_bucket(
        self,
        instruments: list,
        start_date: dt.date,
        end_date: dt.date,
    ) -> "DataFrameType | None":
        """Return all data for these instruments in this date range."""

    def get_partitions(self) -> list[PartitionSpec]:
        """Generate (bucket × date_partition) specs — no instrument I/O."""
        if not self.prepared:
            raise DataSourceError(
                "prepare() must be called before get_partitions()",
                source_class=self.__class__.__name__,
            )

        num_buckets = self.dataset.num_instrument_buckets
        if num_buckets is None:
            raise ValidationError(
                f"{self.__class__.__name__} requires num_instrument_buckets "
                "to be set on the dataset"
            )

        date_part_col = date_partition_column_name(
            self.dataset.date_partition, self.dataset.date_col
        )
        date_ranges = date_partitions(
            self.start_date, self.end_date, self.dataset.date_partition
        )
        bucket_strs = all_bucket_strings(num_buckets)

        specs = []
        for range_start, range_end, date_part_val in date_ranges:
            for bucket_str in bucket_strs:
                specs.append(
                    PartitionSpec(
                        partition_values={
                            INSTRUMENT_BUCKET_COL: bucket_str,
                            date_part_col: date_part_val,
                        },
                        start_date=range_start,
                        end_date=range_end,
                    )
                )

        logger.info(
            f"{self.__class__.__name__}: {len(specs):,} specs "
            f"({num_buckets} buckets × {len(date_ranges)} periods)"
        )
        return specs

    def get_data(self, spec: PartitionSpec) -> "DataFrameType | None":
        """Lazy cache lookup then dispatch to get_data_for_bucket()."""
        num_buckets = self.dataset.num_instrument_buckets
        date_part_col = date_partition_column_name(
            self.dataset.date_partition, self.dataset.date_col
        )

        bucket_str = spec.partition_values[INSTRUMENT_BUCKET_COL]
        date_part_val = spec.partition_values[date_part_col]

        if date_part_val not in self._period_bucket_cache:
            instruments = self.get_instruments_for_time_period(
                spec.start_date, spec.end_date
            )
            self._period_bucket_cache[date_part_val] = bucket_instruments(
                instruments, num_buckets
            )
            logger.debug(
                f"{self.__class__.__name__}: cached {len(instruments):,} "
                f"instruments for {date_part_val}"
            )

        bucket_instrument_list = self._period_bucket_cache[date_part_val].get(
            bucket_str, []
        )
        if not bucket_instrument_list:
            return None

        return self.get_data_for_bucket(
            bucket_instrument_list, spec.start_date, spec.end_date
        )
