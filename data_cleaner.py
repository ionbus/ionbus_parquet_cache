"""
DataCleaner abstract class - interface for custom data transformations.

DataCleaners are applied after YAML-specified transformations (rename, drop,
dropna, dedup) and before writing to parquet. They operate on DuckDB relations
for memory-efficient lazy processing.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import duckdb

    from ionbus_parquet_cache.dated_dataset import DatedParquetDataset
    from ionbus_parquet_cache.non_dated_dataset import NonDatedParquetDataset


class DataCleaner(ABC):
    """
    Abstract base class for custom data cleaning logic.

    DataCleaners receive a DuckDB relation after all YAML-specified
    transformations and return a modified relation. Processing is lazy -
    no data is materialized until the final parquet write.

    Example:
        class EquityDataCleaner(DataCleaner):
            def __init__(self, dataset, min_price: float = 1.0):
                super().__init__(dataset)
                self.min_price = min_price

            def __call__(self, rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
                # Filter out penny stocks
                return rel.filter(f"Close >= {self.min_price}")

    Using SQL for complex transformations:
        class FuturesDataCleaner(DataCleaner):
            def __call__(self, rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
                import duckdb
                return duckdb.sql('''
                    SELECT *,
                        (DaysToExpiry = MIN(DaysToExpiry) OVER (
                            PARTITION BY RootSymbol, Date
                        )) AS IsRollDay
                    FROM rel
                    WHERE DaysToExpiry > 5
                ''')
    """

    def __init__(
        self,
        dataset: "DatedParquetDataset | NonDatedParquetDataset",
        **kwargs: Any,
    ):
        """
        Initialize the DataCleaner.

        Args:
            dataset: The owning dataset (DPD or NPD).
            **kwargs: Additional arguments from cleaning_init_args in YAML.
        """
        self.dataset = dataset

        # Store any additional kwargs as instance attributes
        for key, value in kwargs.items():
            setattr(self, key, value)

    @abstractmethod
    def __call__(
        self,
        rel: "duckdb.DuckDBPyRelation",
    ) -> "duckdb.DuckDBPyRelation":
        """
        Apply custom cleaning logic to the data.

        Called after all YAML-specified transformations (rename, drop columns,
        dropna, dedup) have been applied. The relation is lazy - no data is
        materialized until the final parquet write.

        Args:
            rel: A DuckDB relation representing the data.

        Returns:
            A DuckDB relation with cleaning applied.

        Example using relation methods:
            def __call__(self, rel):
                return rel.filter("Close >= 1.0").select("*")

        Example using SQL:
            def __call__(self, rel):
                import duckdb
                return duckdb.sql('''
                    SELECT *, Close - Open AS DailyChange
                    FROM rel
                    WHERE Volume > 0
                ''')
        """
        pass


class NoOpCleaner(DataCleaner):
    """
    A DataCleaner that does nothing - passes data through unchanged.

    Useful as a default or placeholder.
    """

    def __call__(
        self,
        rel: "duckdb.DuckDBPyRelation",
    ) -> "duckdb.DuckDBPyRelation":
        """Return the relation unchanged."""
        return rel
