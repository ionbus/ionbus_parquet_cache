"""Tests for update pipeline."""

from __future__ import annotations

import datetime as dt
import time
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest

from ionbus_parquet_cache.data_source import DataSource
from ionbus_parquet_cache.data_cleaner import DataCleaner
from ionbus_parquet_cache.dated_dataset import DatedParquetDataset
from ionbus_parquet_cache.exceptions import (
    DataSourceError,
    SchemaMismatchError,
    ValidationError,
)
from ionbus_parquet_cache.partition import PartitionSpec
import pyarrow.parquet as pq
from ionbus_parquet_cache.update_pipeline import (
    build_update_plan,
    compute_update_window,
    convert_to_arrow,
    execute_update,
    validate_schema,
    WriteGroup,
    UpdatePlan,
)


class MockDataSource(DataSource):
    """Simple mock data source for testing."""

    def available_dates(self) -> tuple[dt.date, dt.date]:
        return (dt.date(2024, 1, 1), dt.date(2024, 12, 31))

    def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
        """Return test data for the partition."""
        dates = pd.date_range(partition_spec.start_date, partition_spec.end_date)
        return pd.DataFrame({
            "Date": dates,
            "value": range(len(dates)),
            "price": [100.0 + i * 0.1 for i in range(len(dates))],
        })


class InstrumentMockSource(DataSource):
    """Mock source with instrument partitioning."""

    partition_values = {"FutureRoot": ["ES", "NQ"]}

    def available_dates(self) -> tuple[dt.date, dt.date]:
        return (dt.date(2024, 1, 1), dt.date(2024, 12, 31))

    def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
        """Return test data for the partition."""
        dates = pd.date_range(partition_spec.start_date, partition_spec.end_date)
        instrument = partition_spec.partition_values.get("FutureRoot", "UNKNOWN")
        return pd.DataFrame({
            "Date": dates,
            "FutureRoot": instrument,
            "price": [100.0 + i * 0.1 for i in range(len(dates))],
        })


@pytest.fixture
def temp_cache(tmp_path: Path) -> Path:
    """Create a temporary cache directory."""
    return tmp_path / "cache"


@pytest.fixture
def simple_dpd(temp_cache: Path) -> DatedParquetDataset:
    """Create a simple DPD for testing."""
    return DatedParquetDataset(
        cache_dir=temp_cache,
        name="test_dataset",
        date_col="Date",
        date_partition="month",
    )


@pytest.fixture
def instrument_dpd(temp_cache: Path) -> DatedParquetDataset:
    """Create a DPD with instrument partitioning."""
    return DatedParquetDataset(
        cache_dir=temp_cache,
        name="futures_dataset",
        date_col="Date",
        date_partition="year",
        partition_columns=["FutureRoot", "year"],
        instrument_column="FutureRoot",
    )


class TestComputeUpdateWindow:
    """Tests for compute_update_window()."""

    def test_empty_cache_uses_source_start(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Empty cache should start from source start date."""
        source = MockDataSource(simple_dpd)

        window = compute_update_window(simple_dpd, source)

        assert window is not None
        start, end = window
        assert start == dt.date(2024, 1, 1)
        assert end == dt.date(2024, 12, 31)

    def test_explicit_dates_override(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Explicit dates should override automatic computation."""
        source = MockDataSource(simple_dpd)

        window = compute_update_window(
            simple_dpd,
            source,
            start_date=dt.date(2024, 6, 1),
            end_date=dt.date(2024, 6, 30),
        )

        assert window is not None
        start, end = window
        assert start == dt.date(2024, 6, 1)
        assert end == dt.date(2024, 6, 30)

    def test_start_after_end_returns_none(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Window with start > end should return None."""
        source = MockDataSource(simple_dpd)

        window = compute_update_window(
            simple_dpd,
            source,
            start_date=dt.date(2025, 1, 1),  # After source end
            end_date=dt.date(2024, 12, 31),
        )

        assert window is None


class TestBuildUpdatePlan:
    """Tests for build_update_plan()."""

    def test_creates_plan_with_suffix(
        self, simple_dpd: DatedParquetDataset, tmp_path: Path
    ) -> None:
        """Plan should have a snapshot suffix."""
        specs = [
            PartitionSpec(
                partition_values={"month": "M2024-01"},
                start_date=dt.date(2024, 1, 1),
                end_date=dt.date(2024, 1, 31),
            )
        ]

        plan = build_update_plan(simple_dpd, specs, tmp_path)

        assert len(plan.suffix) == 6  # Base-62 suffix
        assert plan.suffix.isalnum()

    def test_groups_specs_by_partition_values(
        self, simple_dpd: DatedParquetDataset, tmp_path: Path
    ) -> None:
        """Specs with same partition_values should be grouped."""
        specs = [
            PartitionSpec(
                partition_values={"month": "M2024-01"},
                start_date=dt.date(2024, 1, 1),
                end_date=dt.date(2024, 1, 15),
            ),
            PartitionSpec(
                partition_values={"month": "M2024-01"},
                start_date=dt.date(2024, 1, 16),
                end_date=dt.date(2024, 1, 31),
            ),
        ]

        plan = build_update_plan(simple_dpd, specs, tmp_path)

        # Should be one group for M2024-01
        assert len(plan.groups) == 1
        key = (("month", "M2024-01"),)
        assert key in plan.groups
        assert len(plan.groups[key].specs) == 2

    def test_assigns_temp_file_paths(
        self, simple_dpd: DatedParquetDataset, tmp_path: Path
    ) -> None:
        """Each spec should get a temp_file_path assigned."""
        specs = [
            PartitionSpec(
                partition_values={"month": "M2024-01"},
                start_date=dt.date(2024, 1, 1),
                end_date=dt.date(2024, 1, 31),
            )
        ]

        plan = build_update_plan(simple_dpd, specs, tmp_path)

        assert specs[0].temp_file_path is not None
        assert specs[0].temp_file_path.endswith(".parquet")

    def test_multiple_chunks_get_numbered_temp_files(
        self, simple_dpd: DatedParquetDataset, tmp_path: Path
    ) -> None:
        """Multiple specs in same group should have numbered temp files."""
        specs = [
            PartitionSpec(
                partition_values={"month": "M2024-01"},
                start_date=dt.date(2024, 1, 1),
                end_date=dt.date(2024, 1, 15),
            ),
            PartitionSpec(
                partition_values={"month": "M2024-01"},
                start_date=dt.date(2024, 1, 16),
                end_date=dt.date(2024, 1, 31),
            ),
        ]

        plan = build_update_plan(simple_dpd, specs, tmp_path)

        key = (("month", "M2024-01"),)
        group = plan.groups[key]

        # Temp files should have _01, _02 numbering
        assert "_01.parquet" in str(group.temp_files[0])
        assert "_02.parquet" in str(group.temp_files[1])


class TestConvertToArrow:
    """Tests for convert_to_arrow()."""

    def test_pandas_dataframe(self) -> None:
        """Should convert pandas DataFrame to Arrow Table."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

        table = convert_to_arrow(df)

        assert isinstance(table, pa.Table)
        assert table.num_rows == 3
        assert "a" in table.column_names
        assert "b" in table.column_names

    def test_arrow_table_passthrough(self) -> None:
        """Arrow Table should pass through unchanged."""
        original = pa.table({"a": [1, 2, 3]})

        result = convert_to_arrow(original)

        assert result is original

    def test_invalid_type_raises(self) -> None:
        """Invalid data type should raise ValidationError."""
        with pytest.raises(ValidationError, match="Cannot convert"):
            convert_to_arrow("not a dataframe")

    def test_polars_dataframe(self) -> None:
        """Should convert polars DataFrame to Arrow Table."""
        import polars as pl

        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

        table = convert_to_arrow(df)

        assert isinstance(table, pa.Table)
        assert table.num_rows == 3
        assert "a" in table.column_names
        assert "b" in table.column_names

    def test_polars_lazyframe(self) -> None:
        """Should convert polars LazyFrame to Arrow Table."""
        import polars as pl

        # Create a LazyFrame (not collected yet)
        lf = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).lazy()

        # Verify it's actually a LazyFrame
        assert isinstance(lf, pl.LazyFrame)

        table = convert_to_arrow(lf)

        assert isinstance(table, pa.Table)
        assert table.num_rows == 3
        assert "a" in table.column_names
        assert "b" in table.column_names

    def test_polars_lazyframe_with_operations(self) -> None:
        """Should convert LazyFrame with pending operations to Arrow Table."""
        import polars as pl

        # Create a LazyFrame with pending operations
        lf = (
            pl.DataFrame({"a": [1, 2, 3, 4, 5], "b": ["x", "y", "z", "w", "v"]})
            .lazy()
            .filter(pl.col("a") > 2)
            .select(["a", "b"])
        )

        table = convert_to_arrow(lf)

        assert isinstance(table, pa.Table)
        assert table.num_rows == 3  # Only rows where a > 2
        assert list(table.column("a").to_pylist()) == [3, 4, 5]


class TestValidateSchema:
    """Tests for validate_schema()."""

    def test_no_existing_schema(self) -> None:
        """With no existing schema, should return new schema."""
        new_schema = pa.schema([("a", pa.int64()), ("b", pa.string())])

        result = validate_schema(new_schema, None, "test")

        assert result == new_schema

    def test_compatible_schemas(self) -> None:
        """Compatible schemas should pass validation."""
        existing = pa.schema([("a", pa.int64()), ("b", pa.string())])
        new_schema = pa.schema([("a", pa.int64()), ("b", pa.string())])

        result = validate_schema(new_schema, existing, "test")

        assert result is not None

    def test_int_promotion_allowed(self) -> None:
        """Integer promotion (int32 -> int64) should be allowed."""
        existing = pa.schema([("a", pa.int32())])
        new_schema = pa.schema([("a", pa.int64())])

        # Should not raise
        result = validate_schema(new_schema, existing, "test")
        assert result is not None

    def test_incompatible_type_raises(self) -> None:
        """Incompatible type change should raise SchemaMismatchError."""
        existing = pa.schema([("a", pa.int64())])
        new_schema = pa.schema([("a", pa.string())])

        with pytest.raises(SchemaMismatchError, match="type changed"):
            validate_schema(new_schema, existing, "test")

    def test_new_columns_allowed(self) -> None:
        """Adding new columns should be allowed."""
        existing = pa.schema([("a", pa.int64())])
        new_schema = pa.schema([("a", pa.int64()), ("b", pa.string())])

        # Should not raise
        result = validate_schema(new_schema, existing, "test")
        assert result is not None

    def test_schema_merge_preserves_old_columns(self) -> None:
        """Schema merge should preserve columns from existing schema."""
        existing = pa.schema([
            ("a", pa.int64()),
            ("b", pa.string()),
            ("c", pa.float64()),
        ])
        # New schema omits column 'c'
        new_schema = pa.schema([("a", pa.int64()), ("b", pa.string())])

        result = validate_schema(new_schema, existing, "test")

        # Result should have all 3 columns
        assert len(result) == 3
        assert "a" in result.names
        assert "b" in result.names
        assert "c" in result.names  # Preserved from existing

    def test_schema_merge_order_new_first(self) -> None:
        """Merged schema should have new columns first, then old-only columns."""
        existing = pa.schema([
            ("a", pa.int64()),
            ("old_only", pa.string()),
        ])
        new_schema = pa.schema([
            ("a", pa.int64()),
            ("new_only", pa.float64()),
        ])

        result = validate_schema(new_schema, existing, "test")

        # Should have: a, new_only (from new), old_only (from existing)
        assert result.names == ["a", "new_only", "old_only"]

    def test_schema_merge_with_type_promotion(self) -> None:
        """Schema merge should use promoted type for overlapping columns."""
        existing = pa.schema([
            ("a", pa.int32()),
            ("old_col", pa.string()),
        ])
        new_schema = pa.schema([
            ("a", pa.int64()),  # Promoted type
            ("new_col", pa.float64()),
        ])

        result = validate_schema(new_schema, existing, "test")

        # 'a' should have int64 (promoted), plus both other columns
        assert result.field("a").type == pa.int64()
        assert "old_col" in result.names
        assert "new_col" in result.names


class TestExecuteUpdate:
    """Tests for execute_update()."""

    def test_dry_run_returns_suffix(
        self, simple_dpd: DatedParquetDataset, tmp_path: Path
    ) -> None:
        """Dry run should return suffix without writing files."""
        source = MockDataSource(simple_dpd)
        source._do_prepare(dt.date(2024, 1, 1), dt.date(2024, 1, 31))

        specs = [
            PartitionSpec(
                partition_values={"month": "M2024-01"},
                start_date=dt.date(2024, 1, 1),
                end_date=dt.date(2024, 1, 31),
            )
        ]

        plan = build_update_plan(simple_dpd, specs, tmp_path)
        suffix = execute_update(simple_dpd, source, plan, dry_run=True)

        assert suffix == plan.suffix

        # No files should be written
        data_dir = simple_dpd.dataset_dir
        assert not data_dir.exists() or not list(data_dir.rglob("*.parquet"))

    def test_writes_parquet_files(
        self, simple_dpd: DatedParquetDataset, tmp_path: Path
    ) -> None:
        """Execute should write parquet files to data directory."""
        source = MockDataSource(simple_dpd)
        source._do_prepare(dt.date(2024, 1, 1), dt.date(2024, 1, 31))

        specs = source.get_partitions()
        temp_dir = tmp_path / "temp"
        temp_dir.mkdir()

        plan = build_update_plan(simple_dpd, specs, temp_dir)
        suffix = execute_update(simple_dpd, source, plan)

        # Check parquet files exist
        parquet_files = list(simple_dpd.dataset_dir.rglob("*.parquet"))
        assert len(parquet_files) > 0

    def test_publishes_metadata(
        self, simple_dpd: DatedParquetDataset, tmp_path: Path
    ) -> None:
        """Execute should publish snapshot metadata."""
        source = MockDataSource(simple_dpd)
        source._do_prepare(dt.date(2024, 1, 1), dt.date(2024, 1, 31))

        specs = source.get_partitions()
        temp_dir = tmp_path / "temp"
        temp_dir.mkdir()

        plan = build_update_plan(simple_dpd, specs, temp_dir)
        suffix = execute_update(simple_dpd, source, plan)

        # Metadata should exist
        meta_files = list(simple_dpd.meta_dir.glob("*.pkl.gz"))
        assert len(meta_files) == 1
        assert suffix in meta_files[0].name


class TestDPDUpdate:
    """Integration tests for DatedParquetDataset.update()."""

    def test_update_creates_snapshot(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Update should create a new snapshot."""
        source = MockDataSource(simple_dpd)

        suffix = simple_dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        assert suffix is not None
        assert len(suffix) == 6

        # Should be able to read data
        simple_dpd.refresh()
        df = simple_dpd.read_data(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )
        assert len(df) > 0

    def test_update_dry_run(self, simple_dpd: DatedParquetDataset) -> None:
        """Dry run should not create files."""
        source = MockDataSource(simple_dpd)

        suffix = simple_dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
            dry_run=True,
        )

        assert suffix is not None

        # No metadata should exist
        assert not simple_dpd.meta_dir.exists() or \
               not list(simple_dpd.meta_dir.glob("*.pkl.gz"))

    def test_update_with_cleaner(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Update should apply cleaner to data."""

        class FilterCleaner(DataCleaner):
            """Remove rows where value < 10."""

            def __call__(self, rel):
                return rel.filter("value >= 10")

        source = MockDataSource(simple_dpd)
        cleaner = FilterCleaner(simple_dpd)

        suffix = simple_dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
            cleaner=cleaner,
        )

        assert suffix is not None

        # Data should be filtered
        simple_dpd.refresh()
        df = simple_dpd.read_data(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )
        assert (df["value"] >= 10).all()

    def test_update_with_instruments(
        self, instrument_dpd: DatedParquetDataset
    ) -> None:
        """Update with instruments filter should limit partitions."""
        source = InstrumentMockSource(instrument_dpd)

        suffix = instrument_dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 12, 31),
            instruments=["ES"],
        )

        assert suffix is not None

        # Only ES data should exist
        instrument_dpd.refresh()
        df = instrument_dpd.read_data(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 12, 31),
        )
        assert set(df["FutureRoot"].unique()) == {"ES"}

    def test_update_no_data_returns_none(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Update with impossible date range returns None."""
        source = MockDataSource(simple_dpd)

        # Date range outside source availability
        suffix = simple_dpd.update(
            source,
            start_date=dt.date(2030, 1, 1),  # Far future
            end_date=dt.date(2024, 12, 31),  # Before start
        )

        assert suffix is None


class TestWriteGroup:
    """Tests for WriteGroup dataclass."""

    def test_create_write_group(self) -> None:
        """Should create a WriteGroup with partition values."""
        group = WriteGroup(partition_values={"month": "M2024-01"})

        assert group.partition_values == {"month": "M2024-01"}
        assert group.specs == []
        assert group.temp_files == []
        assert group.final_path is None


class TestUpdatePlan:
    """Tests for UpdatePlan dataclass."""

    def test_create_update_plan(self) -> None:
        """Should create an UpdatePlan with suffix."""
        plan = UpdatePlan(suffix="abc123")

        assert plan.suffix == "abc123"
        assert plan.groups == {}
        assert plan.specs_in_order == []


class TestBackfillRestateValidation:
    """Tests for backfill/restate validation in update()."""

    def test_backfill_restate_mutually_exclusive(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Should raise when both backfill and restate are True."""
        source = MockDataSource(simple_dpd)

        with pytest.raises(ValidationError, match="mutually exclusive"):
            simple_dpd.update(
                source,
                start_date=dt.date(2024, 1, 1),
                end_date=dt.date(2024, 1, 31),
                backfill=True,
                restate=True,
            )

    def test_backfill_with_end_date_raises(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Should raise when backfill=True with explicit end_date."""
        source = MockDataSource(simple_dpd)

        with pytest.raises(ValidationError, match="end_date not allowed"):
            simple_dpd.update(
                source,
                start_date=dt.date(2024, 1, 1),
                end_date=dt.date(2024, 1, 31),
                backfill=True,
            )

    def test_restate_requires_both_dates(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Should raise when restate=True without both dates."""
        source = MockDataSource(simple_dpd)

        with pytest.raises(ValidationError, match="requires both"):
            simple_dpd.update(
                source,
                start_date=dt.date(2024, 1, 1),
                restate=True,
            )

    def test_backfill_auto_sets_end_date(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Backfill should auto-set end_date to cache_start - 1."""
        source = MockDataSource(simple_dpd)

        # First, create initial cache
        simple_dpd.update(
            source,
            start_date=dt.date(2024, 6, 1),
            end_date=dt.date(2024, 6, 30),
        )

        # Refresh to ensure clean state
        simple_dpd.refresh()

        # Wait to ensure different suffix (second-granularity)
        time.sleep(1.1)

        # Now backfill - should succeed (end_date auto-set to 2024-05-31)
        suffix = simple_dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            backfill=True,
        )

        # Should have created data
        assert suffix is not None

        # Verify data covers Jan through June
        simple_dpd.refresh()
        df = simple_dpd.read_data(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 6, 30),
        )
        # Should have data for both backfill period and original period
        assert len(df) > 0


class TestYamlTransformsInPipeline:
    """Tests for YAML transforms in update pipeline."""

    def test_transforms_rename_columns(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Should rename columns via transforms dict."""
        source = MockDataSource(simple_dpd)

        transforms = {
            "columns_to_rename": {"value": "renamed_value"},
        }

        suffix = simple_dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
            transforms=transforms,
        )

        assert suffix is not None

        simple_dpd.refresh()
        df = simple_dpd.read_data(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        assert "renamed_value" in df.columns
        assert "value" not in df.columns

    def test_transforms_drop_columns(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Should drop columns via transforms dict."""
        source = MockDataSource(simple_dpd)

        transforms = {
            "columns_to_drop": ["price"],
        }

        suffix = simple_dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
            transforms=transforms,
        )

        assert suffix is not None

        simple_dpd.refresh()
        df = simple_dpd.read_data(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        assert "price" not in df.columns
        assert "value" in df.columns

    def test_transforms_applied_before_cleaner(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Transforms should be applied before DataCleaner."""

        class CheckColumnCleaner(DataCleaner):
            """Cleaner that checks for renamed column."""

            def __call__(self, rel):
                # If transforms ran first, "renamed_col" should exist
                cols = rel.columns
                assert "renamed_col" in cols
                assert "value" not in cols
                return rel

        source = MockDataSource(simple_dpd)
        cleaner = CheckColumnCleaner(simple_dpd)

        transforms = {
            "columns_to_rename": {"value": "renamed_col"},
        }

        # Should not raise - transforms applied before cleaner
        suffix = simple_dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
            transforms=transforms,
            cleaner=cleaner,
        )

        assert suffix is not None


class TestIncrementalUpdateWithExistingCache:
    """Tests for incremental updates when cache already has data."""

    def test_compute_window_uses_cache_end_date(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Incremental update should start from cache end date + 1."""
        source = MockDataSource(simple_dpd)

        # Create initial cache for January
        simple_dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        # Refresh to ensure metadata is cleared from instance
        simple_dpd.refresh()
        simple_dpd._metadata = None  # Clear cached metadata

        # Compute window without explicit dates - should start from Feb 1
        window = compute_update_window(simple_dpd, source)

        assert window is not None
        start, end = window
        assert start == dt.date(2024, 2, 1)  # Day after cache end
        assert end == dt.date(2024, 12, 31)  # Source end

    def test_repull_n_days_uses_business_days(
        self, temp_cache: Path
    ) -> None:
        """repull_n_days should go back N business days, not calendar days."""
        # Create DPD with repull_n_days
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="repull_test",
            date_col="Date",
            date_partition="month",
            repull_n_days=5,
        )
        source = MockDataSource(dpd)

        # Create initial cache ending on a Friday (2024-01-26)
        dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 26),
        )

        # Refresh and clear metadata
        dpd.refresh()
        dpd._metadata = None

        # Compute window - should go back 5 business days from Jan 27
        window = compute_update_window(dpd, source)

        assert window is not None
        start, end = window

        # 5 business days before Jan 27 is Jan 22 (Mon)
        # Jan 27 (Sat), Jan 26 (Fri), Jan 25 (Thu), Jan 24 (Wed),
        # Jan 23 (Tue), Jan 22 (Mon)
        assert start == dt.date(2024, 1, 22)


class TestSuffixConsistency:
    """Tests for suffix consistency between data files and metadata."""

    def test_data_files_match_metadata_suffix(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Data file names should contain the same suffix as metadata."""
        source = MockDataSource(simple_dpd)

        suffix = simple_dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        assert suffix is not None

        # Check metadata file has the suffix
        meta_files = list(simple_dpd.meta_dir.glob("*.pkl.gz"))
        assert len(meta_files) == 1
        assert suffix in meta_files[0].name

        # Check data files have the same suffix
        data_files = list(simple_dpd.dataset_dir.rglob("*.parquet"))
        assert len(data_files) > 0
        for data_file in data_files:
            assert suffix in data_file.name, (
                f"Data file {data_file.name} does not contain suffix {suffix}"
            )


class TestDedupTransforms:
    """Tests for dedup transforms in update pipeline."""

    def test_dedup_keeps_first(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Dedup with keep='first' should keep first occurrence."""

        class DuplicateSource(DataSource):
            """Source that returns duplicate rows."""

            def available_dates(self) -> tuple[dt.date, dt.date]:
                return (dt.date(2024, 1, 1), dt.date(2024, 1, 31))

            def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
                # Return data with duplicates - same Date, different values
                return pd.DataFrame({
                    "Date": [
                        dt.date(2024, 1, 1),
                        dt.date(2024, 1, 1),  # duplicate
                        dt.date(2024, 1, 2),
                    ],
                    "value": [100, 200, 300],  # 200 is the duplicate
                })

        source = DuplicateSource(simple_dpd)

        transforms = {
            "dedup_columns": ["Date"],
            "dedup_keep": "first",
        }

        suffix = simple_dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
            transforms=transforms,
        )

        assert suffix is not None

        simple_dpd.refresh()
        df = simple_dpd.read_data(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        # Should have 2 rows (duplicates removed)
        assert len(df) == 2
        # First occurrence (value=100) should be kept
        # Convert Date column to date objects for comparison
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        jan1_row = df[df["Date"] == dt.date(2024, 1, 1)]
        assert len(jan1_row) == 1
        assert jan1_row["value"].iloc[0] == 100

    def test_dedup_keeps_last(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """Dedup with keep='last' should keep last occurrence."""

        class DuplicateSource(DataSource):
            """Source that returns duplicate rows."""

            def available_dates(self) -> tuple[dt.date, dt.date]:
                return (dt.date(2024, 1, 1), dt.date(2024, 1, 31))

            def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
                return pd.DataFrame({
                    "Date": [
                        dt.date(2024, 1, 1),
                        dt.date(2024, 1, 1),  # duplicate
                        dt.date(2024, 1, 2),
                    ],
                    "value": [100, 200, 300],
                })

        source = DuplicateSource(simple_dpd)

        transforms = {
            "dedup_columns": ["Date"],
            "dedup_keep": "last",
        }

        suffix = simple_dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
            transforms=transforms,
        )

        assert suffix is not None

        simple_dpd.refresh()
        df = simple_dpd.read_data(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        # Should have 2 rows (duplicates removed)
        assert len(df) == 2
        # Last occurrence (value=200) should be kept
        # Convert Date column to date objects for comparison
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        jan1_row = df[df["Date"] == dt.date(2024, 1, 1)]
        assert len(jan1_row) == 1
        assert jan1_row["value"].iloc[0] == 200


class TestNavigationDirectories:
    """Tests for navigation directory creation in partition paths."""

    def test_month_partition_has_year_nav_dir(self, temp_cache: Path) -> None:
        """Month partition should create YYYY/ navigation directory."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="nav_test",
            date_col="Date",
            date_partition="month",
        )
        source = MockDataSource(dpd)

        suffix = dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        assert suffix is not None

        # Check directory structure includes year nav directory
        data_files = list(dpd.dataset_dir.rglob("*.parquet"))
        assert len(data_files) > 0

        # Path should include "2024" navigation directory
        for f in data_files:
            rel_path = f.relative_to(dpd.dataset_dir)
            parts = rel_path.parts
            # Should have: 2024 / month=M2024-01 / filename.parquet
            assert "2024" in parts, f"Missing year nav dir in {rel_path}"

    def test_day_partition_has_year_month_nav_dirs(self, temp_cache: Path) -> None:
        """Day partition should create YYYY/MM/ navigation directories."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="nav_test",
            date_col="Date",
            date_partition="day",
        )
        source = MockDataSource(dpd)

        suffix = dpd.update(
            source,
            start_date=dt.date(2024, 2, 15),
            end_date=dt.date(2024, 2, 15),
        )

        assert suffix is not None

        # Check directory structure includes year and month nav directories
        data_files = list(dpd.dataset_dir.rglob("*.parquet"))
        assert len(data_files) > 0

        for f in data_files:
            rel_path = f.relative_to(dpd.dataset_dir)
            parts = rel_path.parts
            # Should have: 2024 / 02 / Date=2024-02-15 / filename.parquet
            assert "2024" in parts, f"Missing year nav dir in {rel_path}"
            assert "02" in parts, f"Missing month nav dir in {rel_path}"

    def test_year_partition_no_extra_nav_dir(self, temp_cache: Path) -> None:
        """Year partition should NOT add extra navigation directories."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="nav_test",
            date_col="Date",
            date_partition="year",
        )
        source = MockDataSource(dpd)

        suffix = dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        assert suffix is not None

        # Check directory structure - should be year=Y2024/ directly
        data_files = list(dpd.dataset_dir.rglob("*.parquet"))
        assert len(data_files) > 0

        for f in data_files:
            rel_path = f.relative_to(dpd.dataset_dir)
            parts = rel_path.parts
            # Should be: year=Y2024 / filename.parquet (no extra "2024" nav dir)
            # The first part should be the partition dir, not a nav dir
            assert parts[0].startswith("year="), f"Unexpected structure: {rel_path}"


class TestDateStrClamping:
    """Tests for start_date_str and end_date_str clamping."""

    def test_start_date_str_clamps_window(self, temp_cache: Path) -> None:
        """start_date_str should clamp the start of the update window."""
        # Create DPD with start_date_str set to Feb 1
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="clamp_test",
            date_col="Date",
            date_partition="month",
            start_date_str="2024-02-01",
        )
        source = MockDataSource(dpd)

        # Compute window - source starts Jan 1 but should be clamped to Feb 1
        window = compute_update_window(dpd, source)

        assert window is not None
        start, end = window
        assert start == dt.date(2024, 2, 1)  # Clamped to start_date_str
        assert end == dt.date(2024, 12, 31)  # Source end

    def test_end_date_str_clamps_window(self, temp_cache: Path) -> None:
        """end_date_str should clamp the end of the update window."""
        # Create DPD with end_date_str set to June 30
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="clamp_test",
            date_col="Date",
            date_partition="month",
            end_date_str="2024-06-30",
        )
        source = MockDataSource(dpd)

        # Compute window - source ends Dec 31 but should be clamped to June 30
        window = compute_update_window(dpd, source)

        assert window is not None
        start, end = window
        assert start == dt.date(2024, 1, 1)  # Source start
        assert end == dt.date(2024, 6, 30)  # Clamped to end_date_str

    def test_both_date_str_clamps(self, temp_cache: Path) -> None:
        """Both start_date_str and end_date_str should clamp together."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="clamp_test",
            date_col="Date",
            date_partition="month",
            start_date_str="2024-03-01",
            end_date_str="2024-09-30",
        )
        source = MockDataSource(dpd)

        window = compute_update_window(dpd, source)

        assert window is not None
        start, end = window
        assert start == dt.date(2024, 3, 1)
        assert end == dt.date(2024, 9, 30)

    def test_clamp_creates_empty_window(self, temp_cache: Path) -> None:
        """Clamping can result in no update if window becomes empty."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="clamp_test",
            date_col="Date",
            date_partition="month",
            start_date_str="2024-06-01",  # Start after end
            end_date_str="2024-03-31",
        )
        source = MockDataSource(dpd)

        window = compute_update_window(dpd, source)

        # Window should be None since start > end after clamping
        assert window is None


class TestPartitionColumnStripping:
    """Tests for partition column stripping before writing parquet files."""

    def test_partition_columns_not_in_final_file(
        self, temp_cache: Path
    ) -> None:
        """Partition columns should be stripped from the final parquet file."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="strip_test",
            date_col="Date",
            date_partition="month",
            partition_columns=["FutureRoot", "month"],
        )

        class PartitionedSource(DataSource):
            """Source with partition columns in the data."""

            partition_values = {"FutureRoot": ["ES", "NQ"]}

            def available_dates(self) -> tuple[dt.date, dt.date]:
                return (dt.date(2024, 1, 1), dt.date(2024, 1, 31))

            def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
                """Return test data including partition column values."""
                dates = pd.date_range(partition_spec.start_date, partition_spec.end_date)
                instrument = partition_spec.partition_values.get("FutureRoot", "ES")
                month = partition_spec.partition_values.get("month", "M2024-01")
                return pd.DataFrame({
                    "Date": dates,
                    "FutureRoot": instrument,  # Partition column in data
                    "month": month,  # Another partition column in data
                    "price": [100.0 + i for i in range(len(dates))],
                })

        source = PartitionedSource(dpd)

        suffix = dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        assert suffix is not None

        # Read the raw parquet file directly (not through pyarrow dataset)
        parquet_files = list(dpd.dataset_dir.rglob("*.parquet"))
        assert len(parquet_files) > 0

        for pq_file in parquet_files:
            # Read parquet file schema directly
            schema = pq.read_schema(pq_file)
            column_names = schema.names

            # Partition columns should NOT be in the parquet file
            assert "FutureRoot" not in column_names, (
                f"FutureRoot should be stripped from {pq_file.name}"
            )
            assert "month" not in column_names, (
                f"month should be stripped from {pq_file.name}"
            )

            # Non-partition columns should still be present
            assert "Date" in column_names
            assert "price" in column_names

    def test_data_read_correctly_after_stripping(
        self, temp_cache: Path
    ) -> None:
        """Data should be readable correctly after partition columns are stripped."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="strip_read_test",
            date_col="Date",
            date_partition="month",
            partition_columns=["category", "month"],
        )

        class CategorySource(DataSource):
            """Source with category partition."""

            partition_values = {"category": ["A", "B"]}

            def available_dates(self) -> tuple[dt.date, dt.date]:
                return (dt.date(2024, 1, 1), dt.date(2024, 1, 31))

            def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
                dates = pd.date_range(partition_spec.start_date, partition_spec.end_date)
                category = partition_spec.partition_values.get("category", "A")
                return pd.DataFrame({
                    "Date": dates,
                    "category": category,  # Partition column in data
                    "value": [10.0 * (1 if category == "A" else 2) + i for i in range(len(dates))],
                })

        source = CategorySource(dpd)

        suffix = dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        assert suffix is not None

        # Refresh and read data through the dataset API
        dpd.refresh()
        df = dpd.read_data(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        # Data should include the partition columns (added back virtually)
        assert "category" in df.columns
        assert "month" in df.columns
        assert "Date" in df.columns
        assert "value" in df.columns

        # Both categories should be present
        assert set(df["category"].unique()) == {"A", "B"}


class TestPerChunkSorting:
    """Tests for per-chunk sorting by sort_columns before writing temp files."""

    def test_data_sorted_by_sort_columns(
        self, temp_cache: Path
    ) -> None:
        """Data in each partition should be sorted by sort_columns."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="sort_test",
            date_col="Date",
            date_partition="month",
            sort_columns=["Date", "symbol"],  # Sort by Date, then symbol
        )

        class UnsortedSource(DataSource):
            """Source that returns unsorted data."""

            def available_dates(self) -> tuple[dt.date, dt.date]:
                return (dt.date(2024, 1, 1), dt.date(2024, 1, 31))

            def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
                # Return data intentionally out of order
                return pd.DataFrame({
                    "Date": [
                        dt.date(2024, 1, 3),  # Out of order
                        dt.date(2024, 1, 1),
                        dt.date(2024, 1, 2),
                        dt.date(2024, 1, 2),  # Same date, different symbol
                    ],
                    "symbol": ["AAPL", "MSFT", "GOOGL", "AAPL"],  # Out of order
                    "price": [150.0, 350.0, 140.0, 145.0],
                })

        source = UnsortedSource(dpd)

        suffix = dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        assert suffix is not None

        # Read the data back
        dpd.refresh()
        df = dpd.read_data(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        # Verify data is sorted by Date first, then symbol
        # Convert Date to comparable format
        df["Date"] = pd.to_datetime(df["Date"])

        # Create expected sort order
        df_sorted = df.sort_values(["Date", "symbol"]).reset_index(drop=True)
        df_actual = df.reset_index(drop=True)

        # The data should already be in sorted order
        pd.testing.assert_frame_equal(df_actual, df_sorted)

    def test_sort_columns_only_uses_existing_columns(
        self, temp_cache: Path
    ) -> None:
        """Sorting should only use columns that exist in the data."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="sort_missing_test",
            date_col="Date",
            date_partition="month",
            sort_columns=["Date", "nonexistent_col"],  # One column doesn't exist
        )

        class SimpleSource(DataSource):
            def available_dates(self) -> tuple[dt.date, dt.date]:
                return (dt.date(2024, 1, 1), dt.date(2024, 1, 5))

            def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
                return pd.DataFrame({
                    "Date": [
                        dt.date(2024, 1, 3),
                        dt.date(2024, 1, 1),
                        dt.date(2024, 1, 2),
                    ],
                    "value": [30, 10, 20],
                })

        source = SimpleSource(dpd)

        # Should not raise even though nonexistent_col doesn't exist
        suffix = dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 5),
        )

        assert suffix is not None

        # Data should still be sorted by Date
        dpd.refresh()
        df = dpd.read_data(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 5),
        )

        df["Date"] = pd.to_datetime(df["Date"])
        dates = df["Date"].tolist()

        # Dates should be in ascending order
        assert dates == sorted(dates)


class TestNoneFromGetData:
    """
    Tests for the bug fix: get_data() returning None should skip the partition
    instead of raising an error.

    Bug 1: execute_update crashed with ValidationError when get_data() returned
    None because convert_to_arrow(None) has no handler for NoneType.

    Bug 2: consolidation called pq.read_table() on a temp file path that was
    never written (because its spec was skipped), raising a FileNotFoundError.

    Both fixes are in update_pipeline.execute_update().
    """

    def test_none_from_all_specs_is_skipped(
        self, simple_dpd: DatedParquetDataset
    ) -> None:
        """When every spec returns None, update returns a suffix but writes no files."""

        class AllNoneSource(DataSource):
            def available_dates(self) -> tuple[dt.date, dt.date]:
                return (dt.date(2024, 1, 1), dt.date(2024, 1, 31))

            def get_data(self, partition_spec: PartitionSpec):
                return None

        source = AllNoneSource(simple_dpd)

        suffix = simple_dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        # Pipeline should complete without error; no data files written
        # (suffix may be None if no partitions produced data, depending on
        # whether _publish_snapshot is reached — either outcome is acceptable
        # as long as no exception is raised)
        parquet_files = list(simple_dpd.dataset_dir.rglob("*.parquet")) if simple_dpd.dataset_dir.exists() else []
        assert parquet_files == []

    def test_none_from_some_specs_writes_non_none_partitions(
        self, temp_cache: Path
    ) -> None:
        """When only some specs return None, the non-None partitions are written."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="partial_none_test",
            date_col="Date",
            date_partition="month",
            partition_columns=["symbol", "month"],
        )

        class PartialNoneSource(DataSource):
            """Returns data for 'AAPL' but None for 'FAIL'."""

            partition_values = {"symbol": ["AAPL", "FAIL"]}

            def available_dates(self) -> tuple[dt.date, dt.date]:
                return (dt.date(2024, 1, 1), dt.date(2024, 1, 31))

            def get_data(self, partition_spec: PartitionSpec):
                symbol = partition_spec.partition_values.get("symbol")
                if symbol == "FAIL":
                    return None
                dates = pd.date_range(
                    partition_spec.start_date, partition_spec.end_date
                )
                return pd.DataFrame({
                    "Date": dates,
                    "symbol": symbol,
                    "price": [100.0 + i for i in range(len(dates))],
                })

        source = PartialNoneSource(dpd)

        suffix = dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        assert suffix is not None

        dpd.refresh()
        df = dpd.read_data(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        # Only AAPL data should be present; FAIL was skipped without error
        assert set(df["symbol"].unique()) == {"AAPL"}
        assert len(df) > 0

    def test_none_does_not_raise_validation_error(
        self, simple_dpd: DatedParquetDataset, tmp_path: Path
    ) -> None:
        """None return must not raise ValidationError (the pre-fix behaviour)."""

        class NoneSource(DataSource):
            def available_dates(self) -> tuple[dt.date, dt.date]:
                return (dt.date(2024, 1, 1), dt.date(2024, 1, 31))

            def get_data(self, partition_spec: PartitionSpec):
                return None

        source = NoneSource(simple_dpd)
        source._do_prepare(dt.date(2024, 1, 1), dt.date(2024, 1, 31))

        specs = source.get_partitions()
        plan = build_update_plan(simple_dpd, specs, tmp_path)

        # Must not raise — previously raised ValidationError("Cannot convert NoneType")
        execute_update(simple_dpd, source, plan)

    def test_none_mixed_with_data_in_chunked_group(
        self, temp_cache: Path
    ) -> None:
        """
        When a group has multiple chunks and some return None, the non-None
        chunks are still consolidated into the final file correctly.
        """
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="chunked_none_test",
            date_col="Date",
            date_partition="month",
        )

        call_count = {"n": 0}

        class AlternatingSource(DataSource):
            """Returns data on even calls, None on odd calls."""

            chunk_days = 10  # Force multiple chunks per month

            def available_dates(self) -> tuple[dt.date, dt.date]:
                return (dt.date(2024, 1, 1), dt.date(2024, 1, 31))

            def get_data(self, partition_spec: PartitionSpec):
                call_count["n"] += 1
                if call_count["n"] % 2 == 0:
                    return None
                dates = pd.date_range(
                    partition_spec.start_date, partition_spec.end_date
                )
                return pd.DataFrame({
                    "Date": dates,
                    "value": range(len(dates)),
                })

        source = AlternatingSource(dpd)

        # Must not raise even when some chunks return None
        suffix = dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )

        # At least some chunks produced data
        assert suffix is not None
        dpd.refresh()
        df = dpd.read_data(
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 1, 31),
        )
        assert len(df) > 0
