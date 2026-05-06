"""Tests for DatedParquetDataset."""

from __future__ import annotations

import datetime as dt
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

from ionbus_parquet_cache.dated_dataset import (
    DatedParquetDataset,
    FileMetadata,
    SnapshotMetadata,
)
from ionbus_parquet_cache.exceptions import (
    SnapshotNotFoundError,
    SnapshotPublishError,
    ValidationError,
)
from ionbus_parquet_cache.partition import DATE_PARTITION_GRANULARITIES
from ionbus_parquet_cache.snapshot_history import (
    DateRange,
    SnapshotLineage,
)


@pytest.fixture
def temp_cache(tmp_path: Path) -> Path:
    """Create a temporary cache directory."""
    return tmp_path / "cache"


class TestDatedDatasetInit:
    """Tests for DatedParquetDataset initialization."""

    def test_basic_init(self, temp_cache: Path) -> None:
        """Test basic initialization."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="md.futures_daily",
            date_col="Date",
            date_partition="year",
        )
        assert dpd.name == "md.futures_daily"
        assert dpd.date_col == "Date"
        assert dpd.date_partition == "year"
        assert dpd.dataset_dir == temp_cache / "md.futures_daily"
        assert dpd.meta_dir == temp_cache / "md.futures_daily" / "_meta_data"

    def test_init_with_all_params(self, temp_cache: Path) -> None:
        """Test initialization with all parameters."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="md.futures_daily",
            date_col="PricingDate",
            date_partition="month",
            partition_columns=["FutureRoot", "month"],
            sort_columns=["FutureRoot", "Date"],
            description="Daily futures data",
            start_date_str="2020-01-01",
            end_date_str="2024-12-31",
            repull_n_days=5,
            instrument_column="FutureRoot",
            instruments=["ES", "NQ"],
        )
        assert dpd.date_col == "PricingDate"
        assert dpd.date_partition == "month"
        assert dpd.partition_columns == ["FutureRoot", "month"]
        assert dpd.sort_columns == ["FutureRoot", "Date"]
        assert dpd.description == "Daily futures data"

    def test_invalid_date_partition_raises(self, temp_cache: Path) -> None:
        """Invalid date_partition should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid date_partition"):
            DatedParquetDataset(
                cache_dir=temp_cache,
                name="test",
                date_partition="invalid",
            )

    def test_all_date_partitions_valid(self, temp_cache: Path) -> None:
        """All DATE_PARTITIONS should be valid."""
        for dp in DATE_PARTITION_GRANULARITIES:
            dpd = DatedParquetDataset(
                cache_dir=temp_cache,
                name=f"test_{dp}",
                date_partition=dp,
            )
            assert dpd.date_partition == dp


class TestSnapshotMetadata:
    """Tests for SnapshotMetadata."""

    def test_pickle_roundtrip(self, temp_cache: Path) -> None:
        """Test serialization roundtrip."""
        schema = pa.schema([("Date", pa.date32()), ("value", pa.float64())])

        yaml_config = {
            "date_col": "Date",
            "date_partition": "year",
            "partition_columns": ["year"],
            "sort_columns": ["Date"],
            "description": "Test dataset",
            "instrument_column": None,
            "instruments": None,
        }

        files = [
            FileMetadata(
                path="year=Y2024/test_1GZ5HK0.parquet",
                partition_values={"year": "Y2024"},
                checksum="abc123",
                size_bytes=1024,
            )
        ]

        metadata = SnapshotMetadata(
            name="test",
            suffix="1GZ5HK0",
            schema=schema,
            files=files,
            yaml_config=yaml_config,
            cache_start_date=dt.date(2024, 1, 1),
            cache_end_date=dt.date(2024, 12, 31),
            partition_values={"year": ["Y2024"]},
        )

        # Serialize and deserialize
        temp_cache.mkdir(parents=True, exist_ok=True)
        pickle_path = temp_cache / "test_metadata.pkl.gz"
        metadata.to_pickle(pickle_path)
        loaded = SnapshotMetadata.from_pickle(pickle_path)

        assert loaded.name == metadata.name
        assert loaded.suffix == metadata.suffix
        assert loaded.yaml_config == metadata.yaml_config
        assert len(loaded.files) == len(metadata.files)
        assert loaded.files[0].path == metadata.files[0].path
        assert loaded.cache_start_date == metadata.cache_start_date
        assert loaded.partition_values == metadata.partition_values
        assert loaded.schema.equals(metadata.schema)
        assert loaded.lineage is None
        assert loaded.provenance is None

    def test_legacy_pickle_drops_created_at(self, temp_cache: Path) -> None:
        """Older metadata pickles should load without created_at."""
        schema = pa.schema([("Date", pa.date32())])
        metadata = SnapshotMetadata(
            name="test",
            suffix="1GZ5HK0",
            schema=schema,
            files=[],
            yaml_config={},
        )
        metadata.created_at = dt.datetime(2024, 1, 1)
        del metadata.lineage
        del metadata.provenance

        temp_cache.mkdir(parents=True, exist_ok=True)
        pickle_path = temp_cache / "legacy.pkl.gz"
        pd.to_pickle(metadata, pickle_path, compression="gzip")

        loaded = SnapshotMetadata.from_pickle(pickle_path)

        assert not hasattr(loaded, "created_at")
        assert loaded.lineage is None
        assert loaded.provenance is None


class TestDatedDatasetNoSnapshot:
    """Tests for DatedParquetDataset with no snapshot."""

    def test_no_snapshot_discover_returns_none(
        self, temp_cache: Path
    ) -> None:
        """No snapshots should return None."""
        dpd = DatedParquetDataset(cache_dir=temp_cache, name="empty")
        assert dpd._discover_current_suffix() is None

    def test_no_snapshot_pyarrow_dataset_raises(
        self, temp_cache: Path
    ) -> None:
        """Accessing dataset with no snapshot should raise."""
        dpd = DatedParquetDataset(cache_dir=temp_cache, name="empty")
        with pytest.raises(SnapshotNotFoundError):
            dpd.pyarrow_dataset()


class TestDatedDatasetWithSnapshot:
    """Tests for DatedParquetDataset with a published snapshot."""

    @pytest.fixture
    def dpd_with_data(self, temp_cache: Path) -> DatedParquetDataset:
        """Create a DPD with some test data."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="test_dataset",
            date_col="Date",
            date_partition="year",
            partition_columns=["year"],
            sort_columns=["Date"],
        )

        # Create test data
        df = pd.DataFrame(
            {
                "Date": pd.to_datetime(
                    ["2024-01-15", "2024-02-20", "2024-03-25"]
                ),
                "value": [1.0, 2.0, 3.0],
            }
        )

        # Create data directory structure
        data_dir = dpd.dataset_dir
        year_dir = data_dir / "year=Y2024"
        year_dir.mkdir(parents=True)

        # Write parquet file
        file_path = year_dir / "test_dataset_1GZ5HK0.parquet"
        df.to_parquet(file_path, index=False)

        # Create schema
        schema = pa.schema(
            [
                ("Date", pa.timestamp("ns")),
                ("value", pa.float64()),
                ("year", pa.string()),
            ]
        )

        # Create FileMetadata for the file
        file_metadata = FileMetadata(
            path="year=Y2024/test_dataset_1GZ5HK0.parquet",
            partition_values={"year": "Y2024"},
            checksum="dummy_checksum_for_test",
            size_bytes=file_path.stat().st_size,
        )

        # Publish snapshot
        dpd._publish_snapshot(
            files=[file_metadata],
            schema=schema,
            cache_start_date=dt.date(2024, 1, 15),
            cache_end_date=dt.date(2024, 3, 25),
            partition_values={"year": ["Y2024"]},
        )

        return dpd

    def test_discover_suffix(
        self, dpd_with_data: DatedParquetDataset
    ) -> None:
        """Should discover the published snapshot."""
        suffix = dpd_with_data._discover_current_suffix()
        assert suffix is not None
        assert len(suffix) == 7

    def test_read_data_without_dates(
        self, dpd_with_data: DatedParquetDataset
    ) -> None:
        """read_data without dates returns full dataset."""
        df = dpd_with_data.read_data()
        assert len(df) > 0

        # Can also pass just start_date or just end_date
        df = dpd_with_data.read_data(start_date="2024-01-01")
        assert len(df) > 0

    def test_get_partition_values(
        self, dpd_with_data: DatedParquetDataset
    ) -> None:
        """Test getting partition values."""
        values = dpd_with_data.get_partition_values("year")
        assert values == ["Y2024"]

    def test_get_partition_values_invalid_column(
        self, dpd_with_data: DatedParquetDataset
    ) -> None:
        """Getting values for non-partition column should raise."""
        with pytest.raises(ValueError, match="not a partition column"):
            dpd_with_data.get_partition_values("invalid_column")

    def test_summary(self, dpd_with_data: DatedParquetDataset) -> None:
        """Test summary includes DPD-specific info."""
        summary = dpd_with_data.summary()

        assert summary["name"] == "test_dataset"
        assert summary["date_col"] == "Date"
        assert summary["date_partition"] == "year"
        assert summary["partition_columns"] == ["year"]
        assert "cache_start_date" in summary
        assert "cache_end_date" in summary


class TestDatedDatasetAnnotations:
    """Tests for watched user annotations."""

    def test_annotations_carry_forward_when_omitted(
        self,
        temp_cache: Path,
    ) -> None:
        """Omitted annotations should inherit current metadata annotations."""
        dpd = DatedParquetDataset(cache_dir=temp_cache, name="test")
        schema = pa.schema([("Date", pa.date32())])
        dpd._publish_snapshot(
            files=[],
            schema=schema,
            yaml_config={"annotations": {"flags": {"1": "active"}}},
            suffix="1GZ5HK0",
        )

        resolved = dpd._resolve_yaml_config_annotations({"date_col": "Date"})

        assert resolved["annotations"] == {"flags": {"1": "active"}}

    def test_annotations_may_add_but_not_change_or_remove(
        self,
        temp_cache: Path,
    ) -> None:
        """Existing annotation keys are append-only."""
        dpd = DatedParquetDataset(cache_dir=temp_cache, name="test")
        schema = pa.schema([("Date", pa.date32())])
        dpd._publish_snapshot(
            files=[],
            schema=schema,
            yaml_config={
                "annotations": {
                    "flags": {"1": "active"},
                    "unit": "usd",
                }
            },
            suffix="1GZ5HK0",
        )

        resolved = dpd._resolve_yaml_config_annotations(
            {
                "annotations": {
                    "flags": {"1": "active", "2": "stale"},
                    "unit": "usd",
                    "owner": "research",
                }
            }
        )
        assert resolved["annotations"]["flags"]["2"] == "stale"

        with pytest.raises(ValidationError, match="cannot change"):
            dpd._resolve_yaml_config_annotations(
                {"annotations": {"flags": {"1": "inactive"}, "unit": "usd"}}
            )

        with pytest.raises(ValidationError, match="cannot be removed"):
            dpd._resolve_yaml_config_annotations(
                {"annotations": {"flags": {"1": "active"}}}
            )


class TestDatedDatasetProvenanceAndHistory:
    """Tests for provenance sidecars and lineage history."""

    def test_read_provenance_and_cache_history(
        self,
        temp_cache: Path,
    ) -> None:
        """Provenance sidecars are explicit reads and history follows lineage."""
        dpd = DatedParquetDataset(cache_dir=temp_cache, name="test")
        schema = pa.schema([("Date", pa.date32())])
        initial_lineage = SnapshotLineage(
            base_snapshot=None,
            first_snapshot_id="1GZ5HK0",
            operation="initial",
            requested_date_range=DateRange(
                dt.date(2024, 1, 1),
                dt.date(2024, 1, 31),
            ),
            added_date_ranges=[
                DateRange(dt.date(2024, 1, 1), dt.date(2024, 1, 31))
            ],
        )
        dpd._publish_snapshot(
            files=[],
            schema=schema,
            lineage=initial_lineage,
            suffix="1GZ5HK0",
        )
        provenance = dpd._write_provenance_sidecar(
            "1GZ5HK1",
            {"created": dt.datetime(2024, 2, 1), "inputs": ["a", "b"]},
        )
        dpd._publish_snapshot(
            files=[],
            schema=schema,
            lineage=SnapshotLineage(
                base_snapshot="1GZ5HK0",
                first_snapshot_id="1GZ5HK0",
                operation="update",
                requested_date_range=DateRange(
                    dt.date(2024, 2, 1),
                    dt.date(2024, 2, 29),
                ),
                added_date_ranges=[
                    DateRange(dt.date(2024, 2, 1), dt.date(2024, 2, 29))
                ],
            ),
            provenance=provenance,
            suffix="1GZ5HK1",
        )

        assert dpd.read_provenance()["inputs"] == ["a", "b"]

        history = dpd.cache_history()
        assert [entry.snapshot for entry in history] == ["1GZ5HK1", "1GZ5HK0"]
        assert [entry.operation for entry in history] == ["update", "initial"]
        assert all(entry.status == "ok" for entry in history)


class TestDatedDatasetPublish:
    """Tests for snapshot publishing."""

    def test_publish_creates_metadata(self, temp_cache: Path) -> None:
        """Publishing should create metadata file."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="test",
            date_partition="year",
            partition_columns=["year"],
        )

        schema = pa.schema([("Date", pa.date32()), ("value", pa.float64())])

        file_metadata = FileMetadata(
            path="year=Y2024/test.parquet",
            partition_values={"year": "Y2024"},
            checksum="dummy_checksum",
            size_bytes=1024,
        )

        suffix = dpd._publish_snapshot(
            files=[file_metadata],
            schema=schema,
        )

        # Check metadata file exists
        meta_path = dpd.meta_dir / f"test_{suffix}.pkl.gz"
        assert meta_path.exists()

        # Check instance state updated
        assert dpd.current_suffix == suffix
        assert dpd._metadata is not None
        assert dpd._metadata.suffix == suffix

    def test_publish_multiple_snapshots(self, temp_cache: Path) -> None:
        """Publishing multiple times creates multiple metadata files."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="test",
            date_partition="year",
        )

        schema = pa.schema([("Date", pa.date32())])

        file1 = FileMetadata(
            path="f1.parquet",
            partition_values={},
            checksum="checksum1",
            size_bytes=1024,
        )
        suffix1 = dpd._publish_snapshot(files=[file1], schema=schema)

        # Wait to ensure different timestamp (suffix is second-granularity)
        time.sleep(1.1)

        file2 = FileMetadata(
            path="f2.parquet",
            partition_values={},
            checksum="checksum2",
            size_bytes=1024,
        )
        suffix2 = dpd._publish_snapshot(files=[file2], schema=schema)

        assert suffix2 > suffix1
        assert dpd.current_suffix == suffix2

        # Both metadata files should exist
        assert (dpd.meta_dir / f"test_{suffix1}.pkl.gz").exists()
        assert (dpd.meta_dir / f"test_{suffix2}.pkl.gz").exists()


class TestDatedDatasetRefresh:
    """Tests for refresh functionality."""

    def test_refresh_clears_metadata_cache_on_new_snapshot(
        self, temp_cache: Path
    ) -> None:
        """Refresh should clear cached metadata when new snapshot found."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="test",
            date_partition="year",
        )

        schema = pa.schema([("Date", pa.date32())])
        file_metadata = FileMetadata(
            path="f1.parquet",
            partition_values={},
            checksum="dummy",
            size_bytes=512,
        )
        dpd._publish_snapshot(files=[file_metadata], schema=schema)

        # Load metadata
        _ = dpd._load_metadata()
        assert dpd._metadata is not None
        old_suffix = dpd.current_suffix

        # Wait and create new snapshot
        time.sleep(1.1)
        file_metadata2 = FileMetadata(
            path="f2.parquet",
            partition_values={},
            checksum="dummy2",
            size_bytes=1024,
        )
        dpd._publish_snapshot(files=[file_metadata2], schema=schema)

        # Reset to old suffix to simulate external update
        dpd.current_suffix = old_suffix

        # Refresh should find new snapshot and clear metadata
        result = dpd.refresh()
        assert result is True
        assert dpd._metadata is None

    def test_refresh_preserves_metadata_when_current(
        self, temp_cache: Path
    ) -> None:
        """Refresh should preserve metadata when already on latest snapshot."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="test",
            date_partition="year",
        )

        schema = pa.schema([("Date", pa.date32())])
        file_metadata = FileMetadata(
            path="f1.parquet",
            partition_values={},
            checksum="dummy",
            size_bytes=512,
        )
        dpd._publish_snapshot(files=[file_metadata], schema=schema)

        # Load metadata
        _ = dpd._load_metadata()
        assert dpd._metadata is not None

        # Refresh with no new snapshot should preserve metadata
        result = dpd.refresh()
        assert result is False
        assert dpd._metadata is not None

    def test_summary_after_refresh_has_all_keys(
        self, temp_cache: Path
    ) -> None:
        """summary() should have all keys even after refresh() clears _metadata."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="test",
            date_partition="year",
        )

        schema = pa.schema([("Date", pa.date32())])
        file_metadata = FileMetadata(
            path="f1.parquet",
            partition_values={},
            checksum="dummy",
            size_bytes=512,
        )
        dpd._publish_snapshot(
            files=[file_metadata],
            schema=schema,
            cache_start_date=dt.date(2024, 1, 1),
            cache_end_date=dt.date(2024, 12, 31),
        )

        # Wait and create new snapshot
        time.sleep(1.1)
        file_metadata2 = FileMetadata(
            path="f2.parquet",
            partition_values={},
            checksum="dummy2",
            size_bytes=1024,
        )
        dpd._publish_snapshot(
            files=[file_metadata2],
            schema=schema,
            cache_start_date=dt.date(2024, 1, 1),
            cache_end_date=dt.date(2024, 12, 31),
        )

        # Reset to old suffix to simulate stale state
        dpd.current_suffix = "000000"  # Force refresh to find new snapshot
        dpd._metadata = None  # Clear metadata

        # Refresh finds new snapshot and clears _metadata
        result = dpd.refresh()
        assert result is True
        assert dpd._metadata is None  # Cleared by refresh

        # summary() should still have all keys (loads metadata if needed)
        summary = dpd.summary()
        assert "cache_start_date" in summary
        assert "cache_end_date" in summary
        assert "file_count" in summary


class TestCreateSourceFromMetadata:
    """Tests for creating sources from stored metadata."""

    def test_create_source_from_metadata(self, temp_cache: Path) -> None:
        """Should create source from stored yaml_config."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="test",
            date_col="Date",
            date_partition="month",
        )

        # Create metadata with source config
        schema = pa.schema([("Date", pa.date32()), ("value", pa.int64())])
        file_metadata = FileMetadata(
            path="f.parquet",
            partition_values={},
            checksum="dummy",
            size_bytes=512,
        )
        yaml_config = {
            "date_col": "Date",
            "date_partition": "month",
            "source_class_name": "HiveParquetSource",
            "source_location": "",
            "source_init_args": {"path": "/data/test"},
        }
        dpd._publish_snapshot(
            files=[file_metadata],
            schema=schema,
            yaml_config=yaml_config,
        )

        # Create source from metadata
        source = dpd.create_source_from_metadata()

        from ionbus_parquet_cache.builtin_sources import HiveParquetSource

        assert isinstance(source, HiveParquetSource)
        assert source.source_path.as_posix() == "/data/test"

    def test_create_source_missing_class_name_raises(
        self, temp_cache: Path
    ) -> None:
        """Should raise if yaml_config has no source_class_name."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="test",
            date_col="Date",
            date_partition="month",
        )

        # Create metadata without source config
        schema = pa.schema([("Date", pa.date32())])
        file_metadata = FileMetadata(
            path="f.parquet",
            partition_values={},
            checksum="dummy",
            size_bytes=512,
        )
        yaml_config = {"date_col": "Date"}  # No source_class_name
        dpd._publish_snapshot(
            files=[file_metadata],
            schema=schema,
            yaml_config=yaml_config,
        )

        from ionbus_parquet_cache.exceptions import ConfigurationError

        with pytest.raises(ConfigurationError, match="no source_class_name"):
            dpd.create_source_from_metadata()

    def test_get_transforms_from_metadata(self, temp_cache: Path) -> None:
        """Should return transforms from stored yaml_config."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="test",
            date_col="Date",
            date_partition="month",
        )

        schema = pa.schema([("Date", pa.date32())])
        file_metadata = FileMetadata(
            path="f.parquet",
            partition_values={},
            checksum="dummy",
            size_bytes=512,
        )
        yaml_config = {
            "date_col": "Date",
            "columns_to_rename": {"old": "new"},
            "columns_to_drop": ["unwanted"],
            "dedup_columns": ["Date"],
            "dedup_keep": "first",
        }
        dpd._publish_snapshot(
            files=[file_metadata],
            schema=schema,
            yaml_config=yaml_config,
        )

        transforms = dpd.get_transforms_from_metadata()

        assert transforms is not None
        assert transforms["columns_to_rename"] == {"old": "new"}
        assert transforms["columns_to_drop"] == ["unwanted"]
        assert transforms["dedup_columns"] == ["Date"]
        assert transforms["dedup_keep"] == "first"

    def test_get_transforms_returns_none_when_empty(
        self, temp_cache: Path
    ) -> None:
        """Should return None if no transforms configured."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="test",
            date_col="Date",
            date_partition="month",
        )

        schema = pa.schema([("Date", pa.date32())])
        file_metadata = FileMetadata(
            path="f.parquet",
            partition_values={},
            checksum="dummy",
            size_bytes=512,
        )
        yaml_config = {"date_col": "Date"}  # No transforms
        dpd._publish_snapshot(
            files=[file_metadata],
            schema=schema,
            yaml_config=yaml_config,
        )

        transforms = dpd.get_transforms_from_metadata()
        assert transforms is None


class TestBackfillValidation:
    """Tests for backfill start_date validation."""

    def test_backfill_raises_when_start_after_cache_start(
        self, temp_cache: Path
    ) -> None:
        """Backfill should raise ValidationError if start_date >= cache_start_date."""
        from ionbus_parquet_cache.data_source import DataSource
        from ionbus_parquet_cache.partition import PartitionSpec
        from ionbus_parquet_cache.exceptions import ValidationError

        class MockSource(DataSource):
            """Mock source for testing."""

            def available_dates(self) -> tuple[dt.date, dt.date]:
                return (dt.date(2023, 1, 1), dt.date(2024, 12, 31))

            def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
                dates = pd.date_range(
                    partition_spec.start_date, partition_spec.end_date
                )
                return pd.DataFrame(
                    {"Date": dates, "value": range(len(dates))}
                )

        # Create DPD
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="backfill_test",
            date_col="Date",
            date_partition="month",
        )

        source = MockSource(dpd)

        # Create initial cache starting at 2024-01-01
        dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 6, 30),
        )

        # Try backfill with start_date AFTER cache_start - should raise
        # (cache_start is 2024-01-01, we're trying 2024-06-01)
        with pytest.raises(
            ValidationError, match="backfill start_date.*must be before"
        ):
            dpd.update(
                source,
                start_date=dt.date(2024, 6, 1),  # After cache_start
                backfill=True,
            )

    def test_backfill_raises_when_start_equals_cache_start(
        self, temp_cache: Path
    ) -> None:
        """Backfill should raise if start_date equals cache_start_date."""
        from ionbus_parquet_cache.data_source import DataSource
        from ionbus_parquet_cache.partition import PartitionSpec
        from ionbus_parquet_cache.exceptions import ValidationError

        class MockSource(DataSource):
            """Mock source for testing."""

            def available_dates(self) -> tuple[dt.date, dt.date]:
                return (dt.date(2023, 1, 1), dt.date(2024, 12, 31))

            def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
                dates = pd.date_range(
                    partition_spec.start_date, partition_spec.end_date
                )
                return pd.DataFrame(
                    {"Date": dates, "value": range(len(dates))}
                )

        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="backfill_equals_test",
            date_col="Date",
            date_partition="month",
        )

        source = MockSource(dpd)

        # Create initial cache starting at 2024-01-01
        dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),
            end_date=dt.date(2024, 6, 30),
        )

        # Try backfill with start_date EQUAL to cache_start - should raise
        with pytest.raises(
            ValidationError, match="backfill start_date.*must be before"
        ):
            dpd.update(
                source,
                start_date=dt.date(2024, 1, 1),  # Equal to cache_start
                backfill=True,
            )

    def test_backfill_succeeds_when_start_before_cache_start(
        self, temp_cache: Path
    ) -> None:
        """Backfill should succeed when start_date < cache_start_date."""
        from ionbus_parquet_cache.data_source import DataSource
        from ionbus_parquet_cache.partition import PartitionSpec

        class MockSource(DataSource):
            """Mock source for testing."""

            def available_dates(self) -> tuple[dt.date, dt.date]:
                return (dt.date(2023, 1, 1), dt.date(2024, 12, 31))

            def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
                dates = pd.date_range(
                    partition_spec.start_date, partition_spec.end_date
                )
                return pd.DataFrame(
                    {"Date": dates, "value": range(len(dates))}
                )

        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="backfill_success_test",
            date_col="Date",
            date_partition="month",
        )

        source = MockSource(dpd)

        # Create initial cache starting at 2024-06-01
        dpd.update(
            source,
            start_date=dt.date(2024, 6, 1),
            end_date=dt.date(2024, 6, 30),
        )

        # Wait to ensure different suffix (second-granularity)
        time.sleep(1.1)

        # Backfill with start_date BEFORE cache_start - should succeed
        suffix = dpd.update(
            source,
            start_date=dt.date(2024, 1, 1),  # Before cache_start
            backfill=True,
        )

        # Should return a valid suffix
        assert suffix is not None
        assert len(suffix) == 7


class TestSnapshotSuffixCollisionDetection:
    """Tests for snapshot suffix collision detection in _publish_snapshot."""

    def test_publish_when_current_suffix_none_no_error(
        self, temp_cache: Path
    ) -> None:
        """When current_suffix is None, no error should be raised."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="collision_test",
            date_partition="year",
        )

        schema = pa.schema([("Date", pa.date32())])
        file_metadata = FileMetadata(
            path="f1.parquet",
            partition_values={},
            checksum="checksum1",
            size_bytes=1024,
        )

        # current_suffix is None (no previous snapshot)
        assert dpd.current_suffix is None

        # Should succeed without error
        suffix = dpd._publish_snapshot(files=[file_metadata], schema=schema)
        assert suffix is not None
        assert len(suffix) == 7

    def test_publish_when_new_suffix_greater_no_error(
        self, temp_cache: Path
    ) -> None:
        """When new_suffix > current_suffix, no error should be raised."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="collision_test",
            date_partition="year",
        )

        schema = pa.schema([("Date", pa.date32())])
        file1 = FileMetadata(
            path="f1.parquet",
            partition_values={},
            checksum="checksum1",
            size_bytes=1024,
        )

        # First publish
        suffix1 = dpd._publish_snapshot(files=[file1], schema=schema)

        # Wait to ensure different timestamp (second-granularity)
        time.sleep(1.1)

        # Second publish - should succeed
        file2 = FileMetadata(
            path="f2.parquet",
            partition_values={},
            checksum="checksum2",
            size_bytes=1024,
        )
        suffix2 = dpd._publish_snapshot(files=[file2], schema=schema)

        assert suffix2 > suffix1

    def test_publish_with_same_suffix_raises_collision_error(
        self, temp_cache: Path
    ) -> None:
        """When new_suffix == current_suffix, SnapshotPublishError should be raised."""
        dpd = DatedParquetDataset(
            cache_dir=temp_cache,
            name="collision_test",
            date_partition="year",
        )

        schema = pa.schema([("Date", pa.date32())])
        file1 = FileMetadata(
            path="f1.parquet",
            partition_values={},
            checksum="checksum1",
            size_bytes=1024,
        )

        # First publish
        suffix1 = dpd._publish_snapshot(files=[file1], schema=schema)

        # Try to publish again with the SAME explicit suffix
        file2 = FileMetadata(
            path="f2.parquet",
            partition_values={},
            checksum="checksum2",
            size_bytes=1024,
        )

        with pytest.raises(
            SnapshotPublishError, match="Snapshot suffix collision"
        ):
            dpd._publish_snapshot(
                files=[file2], schema=schema, suffix=suffix1
            )
