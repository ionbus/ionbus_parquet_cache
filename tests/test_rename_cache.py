"""Tests for rename_cache.py"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pytest

from ionbus_parquet_cache.rename_cache import rename_cache


def _make_snapshot_metadata(name: str, suffix: str):
    """Build a minimal SnapshotMetadata for testing without importing DPD."""
    from ionbus_parquet_cache.dated_dataset import SnapshotMetadata

    return SnapshotMetadata(
        name=name,
        suffix=suffix,
        schema=pa.schema(
            [pa.field("date", pa.date32()), pa.field("value", pa.float64())]
        ),
        files=[],
        yaml_config={},
        cache_start_date=dt.date(2024, 1, 1),
        cache_end_date=dt.date(2024, 12, 31),
        partition_values={"ticker": ["AAPL", "GOOG"]},
    )


def _create_fake_dataset(
    cache_dir: Path, name: str, suffixes: list[str]
) -> None:
    """Create a fake DPD directory structure with metadata files."""
    dataset_dir = cache_dir / name
    meta_dir = dataset_dir / "_meta_data"
    meta_dir.mkdir(parents=True)

    # Create a fake data parquet file (name doesn't embed dataset name)
    data_dir = dataset_dir / "year=Y2024"
    data_dir.mkdir()
    (data_dir / "part-0.parquet").write_bytes(b"fake parquet data")

    for suffix in suffixes:
        meta = _make_snapshot_metadata(name, suffix)
        pkl_path = meta_dir / f"{name}_{suffix}.pkl.gz"
        meta.to_pickle(pkl_path)


class TestRenameValidation:
    def test_raises_if_old_dir_missing(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="not found"):
            rename_cache(tmp_path, "missing", "new_name")

    def test_raises_if_no_meta_data_dir(self, tmp_path):
        (tmp_path / "old_name").mkdir()
        with pytest.raises(FileNotFoundError, match="_meta_data"):
            rename_cache(tmp_path, "old_name", "new_name")

    def test_raises_if_new_dir_exists(self, tmp_path):
        _create_fake_dataset(tmp_path, "old_name", ["1H4DW00"])
        (tmp_path / "new_name").mkdir()
        with pytest.raises(FileExistsError, match="already exists"):
            rename_cache(tmp_path, "old_name", "new_name")

    def test_raises_if_same_name(self, tmp_path):
        _create_fake_dataset(tmp_path, "my_cache", ["1H4DW00"])
        with pytest.raises(ValueError, match="same"):
            rename_cache(tmp_path, "my_cache", "my_cache")

    def test_raises_if_empty_old_name(self, tmp_path):
        with pytest.raises(ValueError, match="non-empty"):
            rename_cache(tmp_path, "", "new_name")

    def test_raises_if_empty_new_name(self, tmp_path):
        with pytest.raises(ValueError, match="non-empty"):
            rename_cache(tmp_path, "old_name", "")

    def test_raises_if_no_pkl_files(self, tmp_path):
        dataset_dir = tmp_path / "old_name"
        (dataset_dir / "_meta_data").mkdir(parents=True)
        with pytest.raises(FileNotFoundError, match="No snapshot metadata"):
            rename_cache(tmp_path, "old_name", "new_name")


class TestRenameSuccess:
    def test_directory_renamed(self, tmp_path):
        _create_fake_dataset(tmp_path, "old_name", ["1H4DW00"])
        rename_cache(tmp_path, "old_name", "new_name")
        assert (tmp_path / "new_name").exists()
        assert not (tmp_path / "old_name").exists()

    def test_new_metadata_file_created(self, tmp_path):
        _create_fake_dataset(tmp_path, "old_name", ["1H4DW00"])
        rename_cache(tmp_path, "old_name", "new_name")
        new_meta_dir = tmp_path / "new_name" / "_meta_data"
        assert (new_meta_dir / "new_name_1H4DW00.pkl.gz").exists()

    def test_old_metadata_file_deleted(self, tmp_path):
        _create_fake_dataset(tmp_path, "old_name", ["1H4DW00"])
        rename_cache(tmp_path, "old_name", "new_name")
        new_meta_dir = tmp_path / "new_name" / "_meta_data"
        assert not (new_meta_dir / "old_name_1H4DW00.pkl.gz").exists()

    def test_metadata_name_field_updated(self, tmp_path):
        _create_fake_dataset(tmp_path, "old_name", ["1H4DW00"])
        rename_cache(tmp_path, "old_name", "new_name")
        from ionbus_parquet_cache.dated_dataset import SnapshotMetadata

        pkl_path = (
            tmp_path / "new_name" / "_meta_data" / "new_name_1H4DW00.pkl.gz"
        )
        meta = SnapshotMetadata.from_pickle(pkl_path)
        assert meta.name == "new_name"

    def test_data_files_preserved(self, tmp_path):
        _create_fake_dataset(tmp_path, "old_name", ["1H4DW00"])
        rename_cache(tmp_path, "old_name", "new_name")
        assert (
            tmp_path / "new_name" / "year=Y2024" / "part-0.parquet"
        ).exists()

    def test_multiple_snapshots(self, tmp_path):
        suffixes = ["1AAA000", "1BBB000", "1CCC000"]
        _create_fake_dataset(tmp_path, "old_name", suffixes)
        rename_cache(tmp_path, "old_name", "new_name")
        new_meta_dir = tmp_path / "new_name" / "_meta_data"
        for s in suffixes:
            assert (new_meta_dir / f"new_name_{s}.pkl.gz").exists()
            assert not (new_meta_dir / f"old_name_{s}.pkl.gz").exists()

    def test_all_new_metadata_names_updated(self, tmp_path):
        suffixes = ["1AAA000", "1BBB000"]
        _create_fake_dataset(tmp_path, "old_name", suffixes)
        rename_cache(tmp_path, "old_name", "new_name")
        from ionbus_parquet_cache.dated_dataset import SnapshotMetadata

        new_meta_dir = tmp_path / "new_name" / "_meta_data"
        for s in suffixes:
            meta = SnapshotMetadata.from_pickle(
                new_meta_dir / f"new_name_{s}.pkl.gz"
            )
            assert meta.name == "new_name"


class TestDryRun:
    def test_dry_run_does_not_rename_directory(self, tmp_path):
        _create_fake_dataset(tmp_path, "old_name", ["1H4DW00"])
        rename_cache(tmp_path, "old_name", "new_name", dry_run=True)
        assert (tmp_path / "old_name").exists()
        assert not (tmp_path / "new_name").exists()

    def test_dry_run_does_not_write_new_metadata(self, tmp_path):
        _create_fake_dataset(tmp_path, "old_name", ["1H4DW00"])
        rename_cache(tmp_path, "old_name", "new_name", dry_run=True)
        meta_dir = tmp_path / "old_name" / "_meta_data"
        assert not (meta_dir / "new_name_1H4DW00.pkl.gz").exists()

    def test_dry_run_does_not_delete_old_metadata(self, tmp_path):
        _create_fake_dataset(tmp_path, "old_name", ["1H4DW00"])
        rename_cache(tmp_path, "old_name", "new_name", dry_run=True)
        meta_dir = tmp_path / "old_name" / "_meta_data"
        assert (meta_dir / "old_name_1H4DW00.pkl.gz").exists()


class TestTrimmedSnapshotsIgnored:
    def test_trimmed_snapshots_not_renamed(self, tmp_path):
        """Trimmed snapshots should be left in place (not renamed)."""
        _create_fake_dataset(tmp_path, "old_name", ["1H4DW00"])
        # Add a trimmed snapshot
        meta_dir = tmp_path / "old_name" / "_meta_data"
        trimmed_path = meta_dir / "old_name_1H4DW00_trimmed.pkl.gz"
        trimmed_path.write_bytes(b"trimmed metadata")

        rename_cache(tmp_path, "old_name", "new_name")

        new_meta_dir = tmp_path / "new_name" / "_meta_data"
        # Trimmed file should still exist under its original name
        assert (new_meta_dir / "old_name_1H4DW00_trimmed.pkl.gz").exists()
        # No new trimmed file should have been created
        assert not (new_meta_dir / "new_name_1H4DW00_trimmed.pkl.gz").exists()
