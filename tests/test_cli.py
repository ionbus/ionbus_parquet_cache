"""Tests for CLI tools."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from ionbus_utils.file_utils import format_size

from ionbus_parquet_cache.cleanup_cache import (
    _discover_dpd_snapshots,
    _discover_npd_snapshots,
    _generate_cleanup_script,
    _generate_trim_delete_script,
    _generate_trim_undo_script,
    cleanup_cache_main,
)
from ionbus_parquet_cache.import_npd import import_npd_main
from ionbus_parquet_cache.non_dated_dataset import NonDatedParquetDataset
from ionbus_parquet_cache.snapshot import SUFFIX_PATTERN
from ionbus_parquet_cache.sync_cache import sync_cache_main
from ionbus_parquet_cache.update_datasets import update_cache_main
from ionbus_parquet_cache.yaml_create_datasets import create_cache_main


@pytest.fixture
def temp_cache(tmp_path: Path) -> Path:
    """Create a temporary cache with yaml/ and code/ directories."""
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "yaml").mkdir()
    (cache / "code").mkdir()
    return cache


@pytest.fixture
def configured_cache(temp_cache: Path) -> Path:
    """Create a cache with YAML config and source code."""
    # Create YAML config
    yaml_content = """
datasets:
    test_dataset:
        description: Test dataset
        date_col: Date
        date_partition: month
        source_location: code/test_source.py
        source_class_name: TestSource
"""
    (temp_cache / "yaml" / "test.yaml").write_text(yaml_content)

    # Create source file
    source_content = """
from __future__ import annotations
import datetime as dt
import pandas as pd
from ionbus_parquet_cache.data_source import DataSource
from ionbus_parquet_cache.partition import PartitionSpec


class TestSource(DataSource):
    def available_dates(self) -> tuple[dt.date, dt.date]:
        return (dt.date(2024, 1, 1), dt.date(2024, 12, 31))

    def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
        dates = pd.date_range(partition_spec.start_date, partition_spec.end_date)
        return pd.DataFrame({"Date": dates, "value": range(len(dates))})
"""
    (temp_cache / "code" / "test_source.py").write_text(source_content)

    return temp_cache


class TestCreateCacheMain:
    """Tests for yaml-create-datasets CLI (YAML-based)."""

    def test_missing_yaml_file(self, tmp_path: Path) -> None:
        """Should fail with missing YAML file."""
        result = create_cache_main([str(tmp_path / "nonexistent.yaml")])
        assert result == 1

    def test_empty_yaml(self, temp_cache: Path) -> None:
        """Should fail with empty YAML file."""
        yaml_file = temp_cache / "yaml" / "empty.yaml"
        yaml_file.write_text("datasets: {}")
        result = create_cache_main([str(yaml_file)])
        assert result == 1

    def test_missing_dataset(self, configured_cache: Path) -> None:
        """Should fail with nonexistent dataset name."""
        yaml_file = configured_cache / "yaml" / "test.yaml"
        result = create_cache_main([str(yaml_file), "nonexistent"])
        assert result == 1

    def test_backfill_with_end_date(self, configured_cache: Path) -> None:
        """Should fail when --backfill used with --end-date."""
        yaml_file = configured_cache / "yaml" / "test.yaml"
        result = create_cache_main(
            [
                str(yaml_file),
                "--backfill",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-06-30",
            ]
        )
        assert result == 1

    def test_backfill_restate_mutually_exclusive(
        self, configured_cache: Path
    ) -> None:
        """Should fail when --backfill and --restate both specified."""
        yaml_file = configured_cache / "yaml" / "test.yaml"
        result = create_cache_main(
            [
                str(yaml_file),
                "--backfill",
                "--restate",
                "--start-date",
                "2024-01-01",
            ]
        )
        assert result == 1

    def test_restate_requires_dates(self, configured_cache: Path) -> None:
        """Should fail when --restate used without both dates."""
        yaml_file = configured_cache / "yaml" / "test.yaml"
        result = create_cache_main(
            [
                str(yaml_file),
                "--restate",
                "--start-date",
                "2024-01-01",
            ]
        )
        assert result == 1

    def test_invalid_date_format(self, configured_cache: Path) -> None:
        """Should fail with invalid date format."""
        yaml_file = configured_cache / "yaml" / "test.yaml"
        result = create_cache_main(
            [
                str(yaml_file),
                "--start-date",
                "01-01-2024",  # Wrong format
            ]
        )
        assert result == 1

    def test_dry_run_no_files(self, configured_cache: Path) -> None:
        """Dry run should not create any files."""
        yaml_file = configured_cache / "yaml" / "test.yaml"
        result = create_cache_main(
            [
                str(yaml_file),
                "test_dataset",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
                "--dry-run",
            ]
        )
        assert result == 0

        # No metadata should be created
        meta_dir = configured_cache / "test_dataset" / "_meta_data"
        assert not meta_dir.exists() or not list(meta_dir.glob("*.pkl.gz"))

    def test_create_creates_snapshot(self, configured_cache: Path) -> None:
        """Successful create should create snapshot."""
        yaml_file = configured_cache / "yaml" / "test.yaml"
        result = create_cache_main(
            [
                str(yaml_file),
                "test_dataset",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ]
        )
        assert result == 0

        # Metadata should exist
        meta_dir = configured_cache / "test_dataset" / "_meta_data"
        assert meta_dir.exists()
        assert len(list(meta_dir.glob("*.pkl.gz"))) == 1


class TestUpdateCacheMain:
    """Tests for update-cache CLI (metadata-based)."""

    def test_missing_cache_dir(self, tmp_path: Path) -> None:
        """Should fail with missing cache directory."""
        result = update_cache_main([str(tmp_path / "nonexistent")])
        assert result == 1

    def test_no_metadata(self, temp_cache: Path) -> None:
        """Should fail with no datasets having metadata."""
        result = update_cache_main([str(temp_cache)])
        assert result == 1

    def test_backfill_with_end_date(self, temp_cache: Path) -> None:
        """Should fail when --backfill used with --end-date."""
        result = update_cache_main(
            [
                str(temp_cache),
                "--backfill",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-06-30",
            ]
        )
        assert result == 1

    def test_backfill_restate_mutually_exclusive(
        self, temp_cache: Path
    ) -> None:
        """Should fail when --backfill and --restate both specified."""
        result = update_cache_main(
            [
                str(temp_cache),
                "--backfill",
                "--restate",
                "--start-date",
                "2024-01-01",
            ]
        )
        assert result == 1

    def test_restate_requires_dates(self, temp_cache: Path) -> None:
        """Should fail when --restate used without both dates."""
        result = update_cache_main(
            [
                str(temp_cache),
                "--restate",
                "--start-date",
                "2024-01-01",
            ]
        )
        assert result == 1

    def test_invalid_date_format(self, temp_cache: Path) -> None:
        """Should fail with invalid date format."""
        result = update_cache_main(
            [
                str(temp_cache),
                "--start-date",
                "01-01-2024",  # Wrong format
            ]
        )
        assert result == 1


class TestImportNpdMain:
    """Tests for import-npd CLI."""

    def test_missing_cache_dir(self, tmp_path: Path) -> None:
        """Should fail with missing cache directory."""
        source = tmp_path / "source.parquet"
        pd.DataFrame({"symbol": ["AAPL"]}).to_parquet(source, index=False)

        result = import_npd_main(
            [
                str(tmp_path / "missing_cache"),
                "ref.instrument_master",
                str(source),
            ]
        )

        assert result == 1

    def test_import_single_file(
        self, temp_cache: Path, tmp_path: Path
    ) -> None:
        """Should import a single parquet file as an NPD snapshot."""
        source = tmp_path / "instruments.parquet"
        expected = pd.DataFrame({"symbol": ["AAPL", "MSFT"]})
        expected.to_parquet(source, index=False)

        result = import_npd_main(
            [
                str(temp_cache),
                "ref.instrument_master",
                str(source),
            ]
        )

        assert result == 0

        npd = NonDatedParquetDataset(
            cache_dir=temp_cache,
            name="ref.instrument_master",
        )
        actual = npd.read_data().reset_index(drop=True)
        pd.testing.assert_frame_equal(actual, expected)

        snapshot_files = list(
            (temp_cache / "non-dated" / "ref.instrument_master").glob(
                "ref.instrument_master_*.parquet"
            )
        )
        assert len(snapshot_files) == 1

    def test_import_directory(self, temp_cache: Path, tmp_path: Path) -> None:
        """Should import a directory containing parquet files."""
        source = tmp_path / "calendar"
        nyse = source / "exchange=NYSE"
        nasdaq = source / "exchange=NASDAQ"
        nyse.mkdir(parents=True)
        nasdaq.mkdir(parents=True)
        pd.DataFrame({"symbol": ["IBM"]}).to_parquet(
            nyse / "data.parquet",
            index=False,
        )
        pd.DataFrame({"symbol": ["AAPL"]}).to_parquet(
            nasdaq / "data.parquet",
            index=False,
        )

        result = import_npd_main(
            [
                str(temp_cache),
                "ref.exchange_calendar",
                str(source),
            ]
        )

        assert result == 0

        npd = NonDatedParquetDataset(
            cache_dir=temp_cache,
            name="ref.exchange_calendar",
        )
        actual = npd.read_data()
        assert len(actual) == 2

        snapshot_dirs = [
            path
            for path in (
                temp_cache / "non-dated" / "ref.exchange_calendar"
            ).iterdir()
            if path.is_dir()
        ]
        assert len(snapshot_dirs) == 1

    def test_dry_run_does_not_write(
        self, temp_cache: Path, tmp_path: Path
    ) -> None:
        """Dry run should validate but not create the NPD directory."""
        source = tmp_path / "instruments.parquet"
        pd.DataFrame({"symbol": ["AAPL"]}).to_parquet(source, index=False)

        result = import_npd_main(
            [
                str(temp_cache),
                "ref.instrument_master",
                str(source),
                "--dry-run",
            ]
        )

        assert result == 0
        assert not (temp_cache / "non-dated").exists()

    def test_rejects_invalid_source(
        self, temp_cache: Path, tmp_path: Path
    ) -> None:
        """Should fail before writing when source is not parquet."""
        source = tmp_path / "bad.parquet"
        source.write_text("not parquet")

        result = import_npd_main(
            [
                str(temp_cache),
                "ref.instrument_master",
                str(source),
            ]
        )

        assert result == 1
        assert not (temp_cache / "non-dated").exists()

    def test_skip_validation_imports_unreadable_parquet(
        self, temp_cache: Path, tmp_path: Path
    ) -> None:
        """--skip-validation should bypass PyArrow dataset validation."""
        source = tmp_path / "bad.parquet"
        source.write_text("not parquet")

        result = import_npd_main(
            [
                str(temp_cache),
                "ref.instrument_master",
                str(source),
                "--skip-validation",
            ]
        )

        assert result == 0
        snapshot_files = list(
            (temp_cache / "non-dated" / "ref.instrument_master").glob(
                "ref.instrument_master_*.parquet"
            )
        )
        assert len(snapshot_files) == 1
        assert snapshot_files[0].read_text() == "not parquet"

    def test_import_with_info_and_provenance_files(
        self, temp_cache: Path, tmp_path: Path
    ) -> None:
        """--info-file and --provenance-file should attach NPD sidecars."""
        source = tmp_path / "instruments.parquet"
        pd.DataFrame({"symbol": ["AAPL"]}).to_parquet(source, index=False)
        info_file = tmp_path / "instruments.info.yaml"
        info_file.write_text("""
notes: Instrument notes.
annotations:
  vendor: example
column_descriptions:
  symbol: Vendor ticker-like symbol.
""")
        provenance_file = tmp_path / "instruments.provenance.yaml"
        provenance_file.write_text("""
source: unit-test
batch: 3
""")

        result = import_npd_main(
            [
                str(temp_cache),
                "ref.instrument_master",
                str(source),
                "--info-file",
                str(info_file),
                "--provenance-file",
                str(provenance_file),
            ]
        )

        assert result == 0
        npd = NonDatedParquetDataset(
            cache_dir=temp_cache,
            name="ref.instrument_master",
        )
        assert npd.get_notes() == "Instrument notes."
        assert npd.get_annotations() == {"vendor": "example"}
        assert npd.get_column_descriptions() == {
            "symbol": "Vendor ticker-like symbol.",
        }
        assert npd.read_provenance() == {"source": "unit-test", "batch": 3}

    def test_import_info_file_rejects_unknown_keys(
        self, temp_cache: Path, tmp_path: Path
    ) -> None:
        """Unknown --info-file keys should fail before writing."""
        source = tmp_path / "instruments.parquet"
        pd.DataFrame({"symbol": ["AAPL"]}).to_parquet(source, index=False)
        info_file = tmp_path / "bad.info.yaml"
        info_file.write_text("owner: quiet\n")

        result = import_npd_main(
            [
                str(temp_cache),
                "ref.instrument_master",
                str(source),
                "--info-file",
                str(info_file),
            ]
        )

        assert result == 1
        assert not (temp_cache / "non-dated").exists()

    def test_rejects_directory_without_parquet(
        self, temp_cache: Path, tmp_path: Path
    ) -> None:
        """Should reject source directories with no parquet files."""
        source = tmp_path / "empty_source"
        source.mkdir()

        result = import_npd_main(
            [
                str(temp_cache),
                "ref.empty",
                str(source),
            ]
        )

        assert result == 1
        assert not (temp_cache / "non-dated").exists()

    def test_rejects_gcs_cache(self, tmp_path: Path) -> None:
        """Direct GCS imports should fail."""
        source = tmp_path / "instruments.parquet"
        pd.DataFrame({"symbol": ["AAPL"]}).to_parquet(source, index=False)

        result = import_npd_main(
            [
                "gs://bucket/cache",
                "ref.instrument_master",
                str(source),
            ]
        )

        assert result == 1

    def test_rejects_dpd_name_collision(
        self, temp_cache: Path, tmp_path: Path
    ) -> None:
        """Should not create an NPD with the same name as an existing DPD."""
        source = tmp_path / "instruments.parquet"
        pd.DataFrame({"symbol": ["AAPL"]}).to_parquet(source, index=False)
        (temp_cache / "ref.instrument_master" / "_meta_data").mkdir(
            parents=True,
        )

        result = import_npd_main(
            [
                str(temp_cache),
                "ref.instrument_master",
                str(source),
            ]
        )

        assert result == 1
        assert not (temp_cache / "non-dated").exists()

    def test_rejects_path_like_dataset_name(
        self, temp_cache: Path, tmp_path: Path
    ) -> None:
        """Dataset names must not be paths."""
        source = tmp_path / "instruments.parquet"
        pd.DataFrame({"symbol": ["AAPL"]}).to_parquet(source, index=False)

        result = import_npd_main(
            [
                str(temp_cache),
                "../bad",
                str(source),
            ]
        )

        assert result == 1
        assert not (temp_cache / "non-dated").exists()


class TestCleanupCacheMain:
    """Tests for cleanup-cache CLI."""

    def test_missing_cache_dir(self, tmp_path: Path) -> None:
        """Should fail with missing cache directory."""
        result = cleanup_cache_main([str(tmp_path / "nonexistent")])
        assert result == 1

    def test_mutually_exclusive_modes(self, temp_cache: Path) -> None:
        """Should fail with multiple mode options."""
        result = cleanup_cache_main(
            [
                str(temp_cache),
                "--dpd-only",
                "--npd-only",
            ]
        )
        assert result == 1

    def test_empty_cache(self, temp_cache: Path) -> None:
        """Should handle empty cache gracefully."""
        result = cleanup_cache_main([str(temp_cache)])
        assert result == 0

    def test_trim_empty_cache(self, temp_cache: Path) -> None:
        """Trim on empty cache should succeed with no files to trim."""
        result = cleanup_cache_main(
            [
                str(temp_cache),
                "--keep-days",
                "30",
            ]
        )
        assert result == 0

    def test_single_snapshot_message_without_verbose(
        self, configured_cache: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """cleanup-cache should list single-snapshot datasets without --verbose."""
        # Create a single snapshot
        create_cache_main(
            [
                str(configured_cache / "yaml" / "test.yaml"),
                "test_dataset",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ]
        )

        # Run cleanup without --verbose flag
        result = cleanup_cache_main([str(configured_cache)])
        assert result == 0

        # Should mention "only current snapshot" even without --verbose
        assert "only current snapshot" in caplog.text


class TestSyncCacheMain:
    """Tests for sync-cache CLI."""

    def test_missing_subcommand(self) -> None:
        """Should fail without push/pull subcommand."""
        with pytest.raises(SystemExit) as exc_info:
            sync_cache_main([])
        assert exc_info.value.code != 0

    def test_missing_source(self, tmp_path: Path) -> None:
        """Should fail with missing source."""
        result = sync_cache_main(
            [
                "push",
                str(tmp_path / "nonexistent"),
                str(tmp_path / "dest"),
            ]
        )
        assert result == 1

    def test_s3_not_implemented(
        self, temp_cache: Path, tmp_path: Path
    ) -> None:
        """S3 sync should return error (not implemented)."""
        result = sync_cache_main(
            [
                "push",
                str(temp_cache),
                "s3://bucket/path",
            ]
        )
        assert result == 1

    def test_mutually_exclusive_modes(
        self, temp_cache: Path, tmp_path: Path
    ) -> None:
        """Should fail with multiple mode options."""
        result = sync_cache_main(
            [
                "push",
                str(temp_cache),
                str(tmp_path / "dest"),
                "--dpd-only",
                "--npd-only",
            ]
        )
        assert result == 1

    def test_push_empty_cache(self, temp_cache: Path, tmp_path: Path) -> None:
        """Should handle empty cache gracefully."""
        dest = tmp_path / "dest"
        result = sync_cache_main(
            [
                "push",
                str(temp_cache),
                str(dest),
            ]
        )
        assert result == 0

    def test_push_creates_destination(
        self, temp_cache: Path, tmp_path: Path
    ) -> None:
        """Push should create destination directory."""
        dest = tmp_path / "new_dest"
        assert not dest.exists()

        result = sync_cache_main(
            [
                "push",
                str(temp_cache),
                str(dest),
            ]
        )
        assert result == 0
        assert dest.exists()


class TestDiscoverSnapshots:
    """Tests for snapshot discovery functions."""

    def test_discover_dpd_no_datasets(self, temp_cache: Path) -> None:
        """Should return empty dict for cache with no DPDs."""
        result = _discover_dpd_snapshots(temp_cache, None)
        assert result == {}

    def test_discover_dpd_with_snapshot(self, configured_cache: Path) -> None:
        """Should discover DPD snapshots."""
        # Create a snapshot first
        create_cache_main(
            [
                str(configured_cache / "yaml" / "test.yaml"),
                "test_dataset",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ]
        )

        result = _discover_dpd_snapshots(configured_cache, None)
        assert "test_dataset" in result
        assert len(result["test_dataset"]) == 1

    def test_discover_npd_no_datasets(self, temp_cache: Path) -> None:
        """Should return empty dict for cache with no NPDs."""
        result = _discover_npd_snapshots(temp_cache, None)
        assert result == {}

    def test_filter_by_name(self, configured_cache: Path) -> None:
        """Should filter by dataset name."""
        create_cache_main(
            [
                str(configured_cache / "yaml" / "test.yaml"),
                "test_dataset",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ]
        )

        # Filter to non-existent name
        result = _discover_dpd_snapshots(configured_cache, ["other_dataset"])
        assert result == {}

        # Filter to correct name
        result = _discover_dpd_snapshots(configured_cache, ["test_dataset"])
        assert "test_dataset" in result


class TestFormatSize:
    """Tests for size formatting."""

    def test_bytes(self) -> None:
        assert format_size(500) == "500 B"

    def test_kilobytes(self) -> None:
        assert format_size(2048) == "2.0 KB"

    def test_megabytes(self) -> None:
        assert format_size(1024 * 1024 * 5) == "5.0 MB"

    def test_gigabytes(self) -> None:
        assert format_size(1024 * 1024 * 1024 * 2) == "2.0 GB"


class TestSyncPushPull:
    """Tests for sync push/pull operations."""

    def test_push_syncs_dpd(
        self, configured_cache: Path, tmp_path: Path
    ) -> None:
        """Push should sync DPD data files."""
        # Create a snapshot
        create_cache_main(
            [
                str(configured_cache / "yaml" / "test.yaml"),
                "test_dataset",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ]
        )

        # Push to destination
        dest = tmp_path / "dest"
        result = sync_cache_main(
            [
                "push",
                str(configured_cache),
                str(dest),
            ]
        )
        assert result == 0

        # Check files were copied
        dest_meta = dest / "test_dataset" / "_meta_data"
        assert dest_meta.exists()
        assert len(list(dest_meta.glob("*.pkl.gz"))) == 1

    def test_dry_run_no_copy(
        self, configured_cache: Path, tmp_path: Path
    ) -> None:
        """Dry run should not copy files."""
        # Create a snapshot
        create_cache_main(
            [
                str(configured_cache / "yaml" / "test.yaml"),
                "test_dataset",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ]
        )

        dest = tmp_path / "dest"
        result = sync_cache_main(
            [
                "push",
                str(configured_cache),
                str(dest),
                "--dry-run",
            ]
        )
        assert result == 0
        assert not dest.exists()

    def test_delete_removes_extra_files(
        self, configured_cache: Path, tmp_path: Path
    ) -> None:
        """--delete should remove files at destination not in source."""
        # Create a snapshot first
        create_cache_main(
            [
                str(configured_cache / "yaml" / "test.yaml"),
                "test_dataset",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ]
        )

        # Push to destination
        dest = tmp_path / "dest"
        result = sync_cache_main(
            [
                "push",
                str(configured_cache),
                str(dest),
            ]
        )
        assert result == 0

        # Create an extra file at destination that shouldn't be there
        extra_file = dest / "test_dataset" / "extra_file.parquet"
        extra_file.parent.mkdir(parents=True, exist_ok=True)
        extra_file.write_text("fake data")
        assert extra_file.exists()

        # Sync again with --delete
        result = sync_cache_main(
            [
                "push",
                str(configured_cache),
                str(dest),
                "--delete",
            ]
        )
        assert result == 0

        # Extra file should be deleted
        assert not extra_file.exists()

    def test_sync_skips_unchanged_files(
        self, configured_cache: Path, tmp_path: Path
    ) -> None:
        """Sync should skip files that already exist with matching content."""
        # Create a snapshot first
        create_cache_main(
            [
                str(configured_cache / "yaml" / "test.yaml"),
                "test_dataset",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ]
        )

        # Push to destination
        dest = tmp_path / "dest"
        result = sync_cache_main(
            [
                "push",
                str(configured_cache),
                str(dest),
            ]
        )
        assert result == 0

        # Get modification time of a synced file
        data_files = list(dest.rglob("*.parquet"))
        assert len(data_files) > 0
        original_mtime = data_files[0].stat().st_mtime

        # Sync again - files should be skipped (unchanged)
        import time

        time.sleep(0.1)  # Ensure time passes

        result = sync_cache_main(
            [
                "push",
                str(configured_cache),
                str(dest),
            ]
        )
        assert result == 0

        # File should not have been modified (was skipped)
        assert data_files[0].stat().st_mtime == original_mtime

    def test_dry_run_reports_pending_work(
        self,
        configured_cache: Path,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Dry-run sync should report files that would actually be copied."""
        create_cache_main(
            [
                str(configured_cache / "yaml" / "test.yaml"),
                "test_dataset",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ]
        )

        dest = tmp_path / "dest"
        result = sync_cache_main(
            [
                "push",
                str(configured_cache),
                str(dest),
                "--dry-run",
            ]
        )
        assert result == 0
        assert not dest.exists()

        assert "Would sync 0 files" not in caplog.text
        assert "Would sync " in caplog.text

    def test_sync_reports_skipped_unchanged_files(
        self,
        configured_cache: Path,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A repeat sync should report skipped unchanged files in the summary."""
        create_cache_main(
            [
                str(configured_cache / "yaml" / "test.yaml"),
                "test_dataset",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ]
        )

        dest = tmp_path / "dest"
        sync_cache_main(
            [
                "push",
                str(configured_cache),
                str(dest),
            ]
        )

        sync_cache_main(
            [
                "push",
                str(configured_cache),
                str(dest),
            ]
        )

        assert "skipped " in caplog.text
        assert "unchanged" in caplog.text


class TestTrimUndoScript:
    """Tests for trim undo script generation."""

    def test_undo_script_deletes_new_metadata(self, tmp_path: Path) -> None:
        """Undo script should explicitly delete new metadata created by trim."""
        cache_path = tmp_path / "cache"
        cache_path.mkdir()

        trimmed_files = [
            cache_path
            / "dataset"
            / "_meta_data"
            / "dataset_old_trimmed.pkl.gz",
            cache_path
            / "dataset"
            / "year=Y2024"
            / "data_old_trimmed.parquet",
        ]
        new_meta_files = [
            cache_path / "dataset" / "_meta_data" / "dataset_new.pkl.gz",
        ]

        script_path = _generate_trim_undo_script(
            cache_path=cache_path,
            files=trimmed_files,
            new_meta_files=new_meta_files,
            suffix="1H4DW00",
        )

        content = script_path.read_text()
        assert str(new_meta_files[0]) in content
        assert "dataset_old_trimmed.pkl.gz" in content
        assert "data_old_trimmed.parquet" in content


class TestCleanupScriptNaming:
    """Tests for cleanup script filename base-36 suffix format."""

    def test_generate_cleanup_script_suffix_format(
        self, tmp_path: Path
    ) -> None:
        """_generate_cleanup_script produces filename with base-36 suffix pattern."""
        cache_path = tmp_path / "cache"
        cache_path.mkdir()
        files = [cache_path / "test.parquet"]

        script_path = _generate_cleanup_script(cache_path, files)

        # Check filename pattern: _cleanup_{7-char-suffix}.bat or .sh
        name = script_path.name
        assert name.startswith("_cleanup_")
        # Extract suffix (between _cleanup_ and extension)
        if name.endswith(".bat"):
            suffix = name[len("_cleanup_") : -len(".bat")]
        else:
            suffix = name[len("_cleanup_") : -len(".sh")]

        # Verify suffix is exactly 7 characters
        assert len(suffix) == 7, f"Suffix '{suffix}' is not 7 characters"
        # Verify suffix contains only base-36 characters
        assert SUFFIX_PATTERN.match(
            suffix
        ), f"Suffix '{suffix}' does not match base-36 pattern"

    def test_generate_trim_delete_script_suffix_format(
        self, tmp_path: Path
    ) -> None:
        """_generate_trim_delete_script produces filename with base-36 suffix pattern."""
        cache_path = tmp_path / "cache"
        cache_path.mkdir()
        files = [cache_path / "test_trimmed.parquet"]
        suffix = "1H4DW00"  # Example base-36 suffix

        script_path = _generate_trim_delete_script(cache_path, files, suffix)

        # Check filename pattern: _cleanup_{7-char-suffix}_delete.bat or .sh
        name = script_path.name
        assert name.startswith("_cleanup_")
        assert "_delete" in name
        # Extract suffix (between _cleanup_ and _delete)
        if name.endswith(".bat"):
            extracted_suffix = name[len("_cleanup_") : name.index("_delete")]
        else:
            extracted_suffix = name[len("_cleanup_") : name.index("_delete")]

        # Verify suffix is exactly 7 characters
        assert (
            len(extracted_suffix) == 7
        ), f"Suffix '{extracted_suffix}' is not 7 characters"
        # Verify suffix contains only base-36 characters
        assert SUFFIX_PATTERN.match(
            extracted_suffix
        ), f"Suffix '{extracted_suffix}' does not match base-36 pattern"
        # Verify it matches the provided suffix
        assert extracted_suffix == suffix

    def test_generate_trim_undo_script_suffix_format(
        self, tmp_path: Path
    ) -> None:
        """_generate_trim_undo_script produces filename with base-36 suffix pattern."""
        cache_path = tmp_path / "cache"
        cache_path.mkdir()
        files = [cache_path / "test_trimmed.parquet"]
        new_meta_files: list[Path] = []
        suffix = "2ABCDE0"  # Example base-36 suffix

        script_path = _generate_trim_undo_script(
            cache_path, files, new_meta_files, suffix
        )

        # Check filename pattern: _cleanup_{7-char-suffix}_undo.bat or .sh
        name = script_path.name
        assert name.startswith("_cleanup_")
        assert "_undo" in name
        # Extract suffix (between _cleanup_ and _undo)
        if name.endswith(".bat"):
            extracted_suffix = name[len("_cleanup_") : name.index("_undo")]
        else:
            extracted_suffix = name[len("_cleanup_") : name.index("_undo")]

        # Verify suffix is exactly 7 characters
        assert (
            len(extracted_suffix) == 7
        ), f"Suffix '{extracted_suffix}' is not 7 characters"
        # Verify suffix contains only base-36 characters
        assert SUFFIX_PATTERN.match(
            extracted_suffix
        ), f"Suffix '{extracted_suffix}' does not match base-36 pattern"
        # Verify it matches the provided suffix
        assert extracted_suffix == suffix

    def test_suffix_only_contains_base36_characters(
        self, tmp_path: Path
    ) -> None:
        """Verify suffix contains only 0-9, A-Z characters."""
        cache_path = tmp_path / "cache"
        cache_path.mkdir()
        files = [cache_path / "test.parquet"]

        # Generate multiple scripts to test suffix randomness/validity
        for _ in range(5):
            script_path = _generate_cleanup_script(cache_path, files)
            name = script_path.name

            if name.endswith(".bat"):
                suffix = name[len("_cleanup_") : -len(".bat")]
            else:
                suffix = name[len("_cleanup_") : -len(".sh")]

            # Verify all characters are valid base-36
            valid_chars = set("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
            for char in suffix:
                assert (
                    char in valid_chars
                ), f"Character '{char}' in suffix '{suffix}' is not base-36"

            # Cleanup for next iteration
            script_path.unlink()


class TestTrimmedFileFiltering:
    """Tests for _trimmed file filtering in discovery functions."""

    def test_discover_datasets_ignores_trimmed_metadata(
        self, configured_cache: Path
    ) -> None:
        """_discover_datasets_from_disk should ignore _trimmed.pkl.gz files."""
        from ionbus_parquet_cache.update_datasets import (
            _discover_datasets_from_disk,
        )

        # Create initial snapshot
        create_cache_main(
            [
                str(configured_cache / "yaml" / "test.yaml"),
                "test_dataset",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ]
        )

        # Get the actual metadata file suffix
        meta_dir = configured_cache / "test_dataset" / "_meta_data"
        real_meta_files = list(meta_dir.glob("*.pkl.gz"))
        assert len(real_meta_files) == 1
        real_suffix = (
            real_meta_files[0].name.split("_")[-1].replace(".pkl.gz", "")
        )

        # Create a _trimmed metadata file with a higher suffix
        trimmed_meta = meta_dir / "test_dataset_zzzzzz_trimmed.pkl.gz"
        trimmed_meta.write_bytes(b"fake trimmed metadata")
        assert trimmed_meta.exists()

        # Discover datasets - should use real metadata, not trimmed
        datasets = _discover_datasets_from_disk(configured_cache)

        assert "test_dataset" in datasets
        dpd = datasets["test_dataset"]
        # The loaded suffix should be the real one, not the trimmed one
        assert dpd._metadata is not None
        assert dpd._metadata.suffix == real_suffix
        assert "_trimmed" not in dpd._metadata.suffix


class TestFindOrphansScriptGeneration:
    """Tests for --find-orphans script generation."""

    def test_find_orphans_generates_script(
        self, configured_cache: Path
    ) -> None:
        """--find-orphans should generate a cleanup script for orphaned files."""
        import sys

        # Create a snapshot first
        create_cache_main(
            [
                str(configured_cache / "yaml" / "test.yaml"),
                "test_dataset",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ]
        )

        # Create an orphaned parquet file (not referenced by any snapshot)
        orphan_dir = configured_cache / "test_dataset" / "year=Y2024"
        orphan_dir.mkdir(parents=True, exist_ok=True)
        orphan_file = orphan_dir / "orphan_file_XyZabc.parquet"
        orphan_file.write_bytes(b"fake parquet data")
        assert orphan_file.exists()

        # Run cleanup with find_orphans=True
        result = cleanup_cache_main(
            [
                str(configured_cache),
                "--find-orphans",
            ]
        )
        assert result == 0

        # Check that a cleanup script was generated
        if sys.platform == "win32":
            scripts = list(configured_cache.glob("_cleanup_*.bat"))
        else:
            scripts = list(configured_cache.glob("_cleanup_*.sh"))

        assert (
            len(scripts) >= 1
        ), "No cleanup script was generated for orphaned files"

        # The script should contain the orphan file path
        script_content = scripts[0].read_text()
        assert "orphan_file_XyZabc.parquet" in script_content

    def test_find_orphans_includes_npd_sidecars(
        self,
        temp_cache: Path,
        tmp_path: Path,
    ) -> None:
        """NPD sidecars without a matching snapshot should be reported."""
        import sys

        source = tmp_path / "ref.parquet"
        pd.DataFrame({"symbol": ["AAPL"]}).to_parquet(source, index=False)
        npd = NonDatedParquetDataset(
            cache_dir=temp_cache,
            name="ref.instrument_master",
        )
        npd.import_snapshot(source, info={"notes": "Current notes."})

        orphan = (
            temp_cache
            / "non-dated"
            / "ref.instrument_master"
            / "_meta_data"
            / "ref.instrument_master_1ZZZZZZ.pkl.gz"
        )
        orphan.write_bytes(b"not a real sidecar")

        result = cleanup_cache_main([str(temp_cache), "--find-orphans"])
        assert result == 0

        if sys.platform == "win32":
            scripts = list(temp_cache.glob("_cleanup_*.bat"))
        else:
            scripts = list(temp_cache.glob("_cleanup_*.sh"))
        assert scripts
        script_content = scripts[0].read_text()
        assert "ref.instrument_master_1ZZZZZZ.pkl.gz" in script_content

    def test_find_orphans_no_script_when_no_orphans(
        self, configured_cache: Path
    ) -> None:
        """--find-orphans should not generate script when no orphans found."""
        import sys

        # Create a snapshot (no orphans)
        create_cache_main(
            [
                str(configured_cache / "yaml" / "test.yaml"),
                "test_dataset",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ]
        )

        # Count existing scripts before
        if sys.platform == "win32":
            scripts_before = list(configured_cache.glob("_cleanup_*.bat"))
        else:
            scripts_before = list(configured_cache.glob("_cleanup_*.sh"))

        # Run cleanup with find_orphans=True but no orphans exist
        result = cleanup_cache_main(
            [
                str(configured_cache),
                "--find-orphans",
            ]
        )
        assert result == 0

        # Count scripts after - should be the same (no new script)
        if sys.platform == "win32":
            scripts_after = list(configured_cache.glob("_cleanup_*.bat"))
        else:
            scripts_after = list(configured_cache.glob("_cleanup_*.sh"))

        assert len(scripts_after) == len(scripts_before)
