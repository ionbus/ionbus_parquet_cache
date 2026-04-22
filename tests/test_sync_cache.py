"""Tests for sync_cache CLI tool."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ionbus_parquet_cache.sync_cache import (
    sync_cache_main,
    _should_copy_file,
)
from ionbus_parquet_cache.dated_dataset import DatedParquetDataset, FileMetadata


@pytest.fixture
def source_cache(tmp_path: Path) -> Path:
    """Create a source cache with test data."""
    cache = tmp_path / "source"
    cache.mkdir()
    return cache


@pytest.fixture
def dest_cache(tmp_path: Path) -> Path:
    """Create a destination cache directory."""
    cache = tmp_path / "dest"
    cache.mkdir()
    return cache


@pytest.fixture
def source_with_dpd(source_cache: Path) -> Path:
    """Create a source cache with a DPD containing test data."""
    from ionbus_parquet_cache.snapshot import generate_snapshot_suffix

    # Create a DPD
    dpd = DatedParquetDataset(
        cache_dir=source_cache,
        name="test_dataset",
        date_col="Date",
        date_partition="month",
    )

    # Create test data
    df = pd.DataFrame({
        "Date": pd.date_range("2024-01-01", "2024-01-31"),
        "value": range(31),
    })
    table = pa.Table.from_pandas(df)

    # Generate a snapshot suffix (sync discovers files by suffix pattern)
    suffix = generate_snapshot_suffix()

    # Write data file with proper suffix naming
    data_dir = source_cache / "test_dataset"
    data_dir.mkdir(parents=True)
    (data_dir / "month=M2024-01").mkdir()
    data_file = data_dir / "month=M2024-01" / f"data_{suffix}.parquet"
    pq.write_table(table, data_file)

    # Publish snapshot with matching suffix
    file_metadata = FileMetadata(
        path=f"month=M2024-01/data_{suffix}.parquet",
        partition_values={"month": "M2024-01"},
        checksum="dummy_checksum",
        size_bytes=data_file.stat().st_size,
    )
    dpd._publish_snapshot(
        files=[file_metadata],
        schema=table.schema,
        cache_start_date=dt.date(2024, 1, 1),
        cache_end_date=dt.date(2024, 1, 31),
        partition_values={"month": ["M2024-01"]},
        suffix=suffix,
    )

    return source_cache


class TestSyncCacheMain:
    """Tests for sync_cache_main CLI."""

    def test_push_missing_source(self, tmp_path: Path) -> None:
        """Push with missing source should fail."""
        result = sync_cache_main([
            "push",
            str(tmp_path / "nonexistent"),
            str(tmp_path / "dest"),
        ])
        assert result == 1

    def test_pull_missing_source(self, tmp_path: Path) -> None:
        """Pull with missing source should fail."""
        result = sync_cache_main([
            "pull",
            str(tmp_path / "nonexistent"),
            str(tmp_path / "dest"),
        ])
        assert result == 1

    def test_s3_source_not_implemented(self, tmp_path: Path) -> None:
        """S3 source path should fail with not implemented."""
        result = sync_cache_main([
            "push",
            "s3://bucket/cache",
            str(tmp_path / "dest"),
        ])
        assert result == 1

    def test_s3_destination_not_implemented(self, source_cache: Path) -> None:
        """S3 destination path should fail with not implemented."""
        result = sync_cache_main([
            "push",
            str(source_cache),
            "s3://bucket/cache",
        ])
        assert result == 1

    def test_mutually_exclusive_dataset_dpd_only(
        self, source_cache: Path, dest_cache: Path
    ) -> None:
        """--dataset and --dpd-only are mutually exclusive."""
        result = sync_cache_main([
            "push",
            str(source_cache),
            str(dest_cache),
            "--dataset", "foo",
            "--dpd-only",
        ])
        assert result == 1

    def test_mutually_exclusive_dataset_npd_only(
        self, source_cache: Path, dest_cache: Path
    ) -> None:
        """--dataset and --npd-only are mutually exclusive."""
        result = sync_cache_main([
            "push",
            str(source_cache),
            str(dest_cache),
            "--dataset", "foo",
            "--npd-only",
        ])
        assert result == 1

    def test_mutually_exclusive_dpd_npd_only(
        self, source_cache: Path, dest_cache: Path
    ) -> None:
        """--dpd-only and --npd-only are mutually exclusive."""
        result = sync_cache_main([
            "push",
            str(source_cache),
            str(dest_cache),
            "--dpd-only",
            "--npd-only",
        ])
        assert result == 1


class TestSyncPush:
    """Tests for push sync operation."""

    def test_push_empty_cache(
        self, source_cache: Path, dest_cache: Path
    ) -> None:
        """Push from empty cache should succeed."""
        result = sync_cache_main([
            "push",
            str(source_cache),
            str(dest_cache),
        ])
        assert result == 0

    def test_push_creates_destination(
        self, source_with_dpd: Path, tmp_path: Path
    ) -> None:
        """Push should create destination directory if it doesn't exist."""
        dest = tmp_path / "new_dest"
        assert not dest.exists()

        result = sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest),
        ])
        assert result == 0
        assert dest.exists()

    def test_push_copies_dpd_files(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push should copy DPD data and metadata files."""
        result = sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest_cache),
        ])
        assert result == 0

        # Check metadata was copied
        meta_dir = dest_cache / "test_dataset" / "_meta_data"
        assert meta_dir.exists()
        meta_files = list(meta_dir.glob("*.pkl.gz"))
        assert len(meta_files) == 1

        # Check data was copied
        data_dir = dest_cache / "test_dataset" / "month=M2024-01"
        assert data_dir.exists()
        data_files = list(data_dir.glob("*.parquet"))
        assert len(data_files) == 1

    def test_push_dry_run_no_copy(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push --dry-run should not copy files."""
        result = sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest_cache),
            "--dry-run",
        ])
        assert result == 0

        # Nothing should be copied
        assert not (dest_cache / "test_dataset").exists()

    def test_push_skips_unchanged_files(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push should skip files that haven't changed."""
        # First sync
        result1 = sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest_cache),
        ])
        assert result1 == 0

        # Second sync should skip all files (unchanged)
        result2 = sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest_cache),
            "--verbose",
        ])
        assert result2 == 0

    def test_push_dataset_filter(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push --dataset should only sync matching datasets."""
        # Try to sync non-existent dataset
        result = sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest_cache),
            "--dataset", "nonexistent",
        ])
        assert result == 0

        # Nothing should be synced
        assert not (dest_cache / "test_dataset").exists()

    def test_push_dpd_only(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push --dpd-only should only sync DPDs."""
        from ionbus_parquet_cache.snapshot import generate_snapshot_suffix

        # Create an NPD in source with proper suffix naming
        suffix = generate_snapshot_suffix()
        npd_dir = source_with_dpd / "non-dated" / "test_npd"
        npd_dir.mkdir(parents=True)
        npd_file = npd_dir / f"test_npd_{suffix}.parquet"
        df = pd.DataFrame({"col": [1, 2, 3]})
        pq.write_table(pa.Table.from_pandas(df), npd_file)

        result = sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest_cache),
            "--dpd-only",
        ])
        assert result == 0

        # DPD should be synced
        assert (dest_cache / "test_dataset").exists()
        # NPD should NOT be synced
        assert not (dest_cache / "non-dated").exists()

    def test_push_npd_only(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push --npd-only should only sync NPDs."""
        from ionbus_parquet_cache.snapshot import generate_snapshot_suffix

        # Create an NPD in source with proper suffix naming
        suffix = generate_snapshot_suffix()
        npd_dir = source_with_dpd / "non-dated" / "test_npd"
        npd_dir.mkdir(parents=True)
        npd_file = npd_dir / f"test_npd_{suffix}.parquet"
        df = pd.DataFrame({"col": [1, 2, 3]})
        pq.write_table(pa.Table.from_pandas(df), npd_file)

        result = sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest_cache),
            "--npd-only",
        ])
        assert result == 0

        # DPD should NOT be synced
        assert not (dest_cache / "test_dataset").exists()
        # NPD should be synced
        assert (dest_cache / "non-dated" / "test_npd").exists()

    def test_push_delete_removes_extra_files(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push --delete should remove files at dest not in source."""
        # First sync
        result1 = sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest_cache),
        ])
        assert result1 == 0

        # Create extra file at destination
        extra_file = dest_cache / "test_dataset" / "extra.parquet"
        extra_file.write_bytes(b"extra data")
        assert extra_file.exists()

        # Sync with --delete
        result2 = sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest_cache),
            "--delete",
        ])
        assert result2 == 0

        # Extra file should be deleted
        assert not extra_file.exists()

    def test_push_delete_dry_run_no_delete(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push --delete --dry-run should not delete files."""
        # First sync
        sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest_cache),
        ])

        # Create extra file at destination
        extra_file = dest_cache / "test_dataset" / "extra.parquet"
        extra_file.write_bytes(b"extra data")

        # Dry run with --delete
        result = sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest_cache),
            "--delete",
            "--dry-run",
        ])
        assert result == 0

        # Extra file should NOT be deleted
        assert extra_file.exists()

    def test_push_delete_preserves_yaml_code(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push --delete should preserve yaml/ and code/ directories."""
        # First sync
        sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest_cache),
        ])

        # Create files in yaml/ and code/ at destination
        yaml_dir = dest_cache / "yaml"
        yaml_dir.mkdir()
        yaml_file = yaml_dir / "config.yaml"
        yaml_file.write_text("datasets: {}")

        code_dir = dest_cache / "code"
        code_dir.mkdir()
        code_file = code_dir / "source.py"
        code_file.write_text("# custom source")

        # Sync with --delete
        result = sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest_cache),
            "--delete",
        ])
        assert result == 0

        # yaml/ and code/ should be preserved
        assert yaml_file.exists()
        assert code_file.exists()


class TestSyncPull:
    """Tests for pull sync operation."""

    def test_pull_basic(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Pull should copy files from source to destination."""
        result = sync_cache_main([
            "pull",
            str(source_with_dpd),
            str(dest_cache),
        ])
        assert result == 0

        # Check data was copied
        assert (dest_cache / "test_dataset").exists()


class TestShouldCopyFile:
    """Tests for _should_copy_file function."""

    def test_dest_not_exists(self, tmp_path: Path) -> None:
        """Should copy if destination doesn't exist."""
        source = tmp_path / "source.txt"
        source.write_text("content")
        dest = tmp_path / "dest.txt"

        assert _should_copy_file(source, dest) is True

    def test_size_mismatch(self, tmp_path: Path) -> None:
        """Should copy if sizes differ."""
        source = tmp_path / "source.txt"
        source.write_text("longer content")
        dest = tmp_path / "dest.txt"
        dest.write_text("short")

        assert _should_copy_file(source, dest) is True

    def test_same_size_different_content(self, tmp_path: Path) -> None:
        """Should copy if same size but different content."""
        source = tmp_path / "source.txt"
        source.write_text("content1")
        dest = tmp_path / "dest.txt"
        dest.write_text("content2")

        assert _should_copy_file(source, dest) is True

    def test_identical_files(self, tmp_path: Path) -> None:
        """Should not copy if files are identical."""
        source = tmp_path / "source.txt"
        source.write_text("same content")
        dest = tmp_path / "dest.txt"
        dest.write_text("same content")

        assert _should_copy_file(source, dest) is False


class TestAllSnapshots:
    """Tests for --all-snapshots flag."""

    def test_all_snapshots_syncs_multiple(
        self, source_cache: Path, dest_cache: Path
    ) -> None:
        """--all-snapshots should sync all snapshots, not just current."""
        import time

        # Create DPD with multiple snapshots
        dpd = DatedParquetDataset(
            cache_dir=source_cache,
            name="multi_snap",
            date_col="Date",
            date_partition="month",
        )

        # Use explicit different suffixes to ensure they're distinct
        suffix1 = "1AAAAAA"
        suffix2 = "1BBBBB0"

        # Create first snapshot
        df1 = pd.DataFrame({
            "Date": pd.date_range("2024-01-01", "2024-01-15"),
            "value": range(15),
        })
        table1 = pa.Table.from_pandas(df1)

        data_dir = source_cache / "multi_snap"
        data_dir.mkdir(parents=True)
        (data_dir / "month=M2024-01").mkdir()

        data_file1 = data_dir / "month=M2024-01" / f"data_{suffix1}.parquet"
        pq.write_table(table1, data_file1)

        fm1 = FileMetadata(
            path=f"month=M2024-01/data_{suffix1}.parquet",
            partition_values={"month": "M2024-01"},
            checksum="checksum1",
            size_bytes=data_file1.stat().st_size,
        )
        dpd._publish_snapshot(
            files=[fm1],
            schema=table1.schema,
            cache_start_date=dt.date(2024, 1, 1),
            cache_end_date=dt.date(2024, 1, 15),
            partition_values={"month": ["M2024-01"]},
            suffix=suffix1,
        )

        # Create second snapshot with different suffix
        df2 = pd.DataFrame({
            "Date": pd.date_range("2024-01-01", "2024-01-31"),
            "value": range(31),
        })
        table2 = pa.Table.from_pandas(df2)

        data_file2 = data_dir / "month=M2024-01" / f"data_{suffix2}.parquet"
        pq.write_table(table2, data_file2)

        fm2 = FileMetadata(
            path=f"month=M2024-01/data_{suffix2}.parquet",
            partition_values={"month": "M2024-01"},
            checksum="checksum2",
            size_bytes=data_file2.stat().st_size,
        )
        dpd._publish_snapshot(
            files=[fm2],
            schema=table2.schema,
            cache_start_date=dt.date(2024, 1, 1),
            cache_end_date=dt.date(2024, 1, 31),
            partition_values={"month": ["M2024-01"]},
            suffix=suffix2,
        )

        # Sync with --all-snapshots
        result = sync_cache_main([
            "push",
            str(source_cache),
            str(dest_cache),
            "--all-snapshots",
        ])
        assert result == 0

        # Both metadata files should be synced
        meta_dir = dest_cache / "multi_snap" / "_meta_data"
        meta_files = list(meta_dir.glob("*.pkl.gz"))
        assert len(meta_files) == 2

    def test_without_all_snapshots_only_current(
        self, source_cache: Path, dest_cache: Path
    ) -> None:
        """Without --all-snapshots, only current snapshot is synced."""
        # Create DPD with multiple snapshots
        dpd = DatedParquetDataset(
            cache_dir=source_cache,
            name="multi_snap",
            date_col="Date",
            date_partition="month",
        )

        # Use explicit different suffixes (lexicographically ordered)
        suffix1 = "1AAAAAA"  # older
        suffix2 = "1BBBBB0"  # newer (current)

        df1 = pd.DataFrame({
            "Date": pd.date_range("2024-01-01", "2024-01-15"),
            "value": range(15),
        })
        table1 = pa.Table.from_pandas(df1)

        data_dir = source_cache / "multi_snap"
        data_dir.mkdir(parents=True)
        (data_dir / "month=M2024-01").mkdir()

        data_file1 = data_dir / "month=M2024-01" / f"data_{suffix1}.parquet"
        pq.write_table(table1, data_file1)

        fm1 = FileMetadata(
            path=f"month=M2024-01/data_{suffix1}.parquet",
            partition_values={"month": "M2024-01"},
            checksum="checksum1",
            size_bytes=data_file1.stat().st_size,
        )
        dpd._publish_snapshot(
            files=[fm1],
            schema=table1.schema,
            cache_start_date=dt.date(2024, 1, 1),
            cache_end_date=dt.date(2024, 1, 15),
            partition_values={"month": ["M2024-01"]},
            suffix=suffix1,
        )

        # Create second snapshot (newer)
        df2 = pd.DataFrame({
            "Date": pd.date_range("2024-01-01", "2024-01-31"),
            "value": range(31),
        })
        table2 = pa.Table.from_pandas(df2)

        data_file2 = data_dir / "month=M2024-01" / f"data_{suffix2}.parquet"
        pq.write_table(table2, data_file2)

        fm2 = FileMetadata(
            path=f"month=M2024-01/data_{suffix2}.parquet",
            partition_values={"month": "M2024-01"},
            checksum="checksum2",
            size_bytes=data_file2.stat().st_size,
        )
        dpd._publish_snapshot(
            files=[fm2],
            schema=table2.schema,
            cache_start_date=dt.date(2024, 1, 1),
            cache_end_date=dt.date(2024, 1, 31),
            partition_values={"month": ["M2024-01"]},
            suffix=suffix2,
        )

        # Sync WITHOUT --all-snapshots
        result = sync_cache_main([
            "push",
            str(source_cache),
            str(dest_cache),
        ])
        assert result == 0

        # Only one metadata file (current) should be synced
        meta_dir = dest_cache / "multi_snap" / "_meta_data"
        meta_files = list(meta_dir.glob("*.pkl.gz"))
        assert len(meta_files) == 1


class TestRename:
    """Tests for --rename option."""

    def test_rename_invalid_format(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """--rename without colon should fail."""
        result = sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest_cache),
            "--rename", "invalid_format",
        ])
        assert result == 1

    def test_rename_copies_to_new_name(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """--rename should copy dataset with new name."""
        result = sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest_cache),
            "--rename", "test_dataset:renamed_dataset",
        ])
        assert result == 0

        # Source name should NOT exist at destination
        assert not (dest_cache / "test_dataset").exists()

        # Renamed dataset should exist
        assert (dest_cache / "renamed_dataset").exists()
        assert (dest_cache / "renamed_dataset" / "_meta_data").exists()

        # Metadata file should have new name
        meta_files = list(
            (dest_cache / "renamed_dataset" / "_meta_data").glob("renamed_dataset_*.pkl.gz")
        )
        assert len(meta_files) == 1

    def test_rename_with_dataset_filter(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """--rename should auto-filter to source dataset."""
        # Create a second dataset in source
        dpd2 = DatedParquetDataset(
            cache_dir=source_with_dpd,
            name="other_dataset",
            date_col="Date",
            date_partition="month",
        )
        df = pd.DataFrame({
            "Date": pd.date_range("2024-02-01", "2024-02-28"),
            "value": range(28),
        })
        table = pa.Table.from_pandas(df)
        suffix = "1CCC000"

        data_dir = source_with_dpd / "other_dataset"
        data_dir.mkdir(parents=True)
        (data_dir / "month=M2024-02").mkdir()
        data_file = data_dir / "month=M2024-02" / f"data_{suffix}.parquet"
        pq.write_table(table, data_file)

        fm = FileMetadata(
            path=f"month=M2024-02/data_{suffix}.parquet",
            partition_values={"month": "M2024-02"},
            checksum="dummy",
            size_bytes=data_file.stat().st_size,
        )
        dpd2._publish_snapshot(
            files=[fm],
            schema=table.schema,
            cache_start_date=dt.date(2024, 2, 1),
            cache_end_date=dt.date(2024, 2, 28),
            partition_values={"month": ["M2024-02"]},
            suffix=suffix,
        )

        # Rename only test_dataset
        result = sync_cache_main([
            "push",
            str(source_with_dpd),
            str(dest_cache),
            "--rename", "test_dataset:renamed_dataset",
        ])
        assert result == 0

        # Only the renamed dataset should be at destination
        assert (dest_cache / "renamed_dataset").exists()
        # other_dataset should NOT be synced (filtered out)
        assert not (dest_cache / "other_dataset").exists()
