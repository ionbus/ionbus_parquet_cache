"""Tests for sync_cache CLI tool."""

from __future__ import annotations

import datetime as dt
import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ionbus_parquet_cache.dated_dataset import (
    DatedParquetDataset,
    FileMetadata,
)
from ionbus_parquet_cache.non_dated_dataset import NonDatedParquetDataset
from ionbus_parquet_cache.snapshot_history import SnapshotProvenanceRef
from ionbus_parquet_cache.sync_cache import (
    _collect_gcs_sync_blobs,
    _collect_selected_local_snapshots,
    _local_sync_pairs_from_selected,
    _should_copy_file,
    _sync_function_targets_from_selected,
    sync_cache_main,
)

GCS_CACHE = "gs://bucket/cache"


def _gcs_urls(*relative_paths: str) -> list[str]:
    """Build GCS blob URLs under the test cache prefix."""
    return [f"{GCS_CACHE}/{rel}" for rel in relative_paths]


def _write_sync_hooks(cache: Path) -> Path:
    """Create a cache-local sync function module used by tests."""
    code_dir = cache / "code"
    code_dir.mkdir(exist_ok=True)
    hook_file = code_dir / "sync_functions.py"
    hook_file.write_text("""
from pathlib import Path


def _append(path, line):
    existing = path.read_text() if path.exists() else ""
    path.write_text(existing + line + "\\n")


def record_sync(
    source_dataset_name,
    dest_dataset_name,
    dataset_type,
    snapshot_id,
    source_location,
    dest_location,
):
    marker = Path(dest_location) / "_sync_function_calls.txt"
    _append(
        marker,
        "|".join([
            dataset_type,
            source_dataset_name,
            dest_dataset_name,
            snapshot_id,
            source_location,
            dest_location,
        ]),
    )


def record_to_source(
    source_dataset_name,
    dest_dataset_name,
    dataset_type,
    snapshot_id,
    source_location,
    dest_location,
):
    marker = Path(source_location) / "_gcs_sync_function_calls.txt"
    _append(marker, f"{dataset_type}|{dest_location}|{snapshot_id}")


def failing_sync(
    source_dataset_name,
    dest_dataset_name,
    dataset_type,
    snapshot_id,
    source_location,
    dest_location,
):
    raise RuntimeError("sync hook failed")


class ClassRecorder:
    def __init__(self, marker):
        self.marker = marker

    def __call__(
        self,
        source_dataset_name,
        dest_dataset_name,
        dataset_type,
        snapshot_id,
        source_location,
        dest_location,
    ):
        marker = Path(dest_location) / self.marker
        _append(marker, f"{dataset_type}|{dest_dataset_name}|{snapshot_id}")
""")
    return hook_file


def _write_yaml_sync_config(
    cache: Path,
    dataset_name: str,
    function_name: str = "record_sync",
) -> None:
    """Create YAML sync-function config for a dataset."""
    yaml_dir = cache / "yaml"
    yaml_dir.mkdir(exist_ok=True)
    (yaml_dir / "sync_function.yaml").write_text(f"""
datasets:
    {dataset_name}:
        sync_function_location: code/sync_functions.py
        sync_function_name: {function_name}
""")


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
    df = pd.DataFrame(
        {
            "Date": pd.date_range("2024-01-01", "2024-01-31"),
            "value": range(31),
        }
    )
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
        result = sync_cache_main(
            [
                "push",
                str(tmp_path / "nonexistent"),
                str(tmp_path / "dest"),
            ]
        )
        assert result == 1

    def test_pull_missing_source(self, tmp_path: Path) -> None:
        """Pull with missing source should fail."""
        result = sync_cache_main(
            [
                "pull",
                str(tmp_path / "nonexistent"),
                str(tmp_path / "dest"),
            ]
        )
        assert result == 1

    def test_s3_source_not_implemented(self, tmp_path: Path) -> None:
        """S3 source path should fail with not implemented."""
        result = sync_cache_main(
            [
                "push",
                "s3://bucket/cache",
                str(tmp_path / "dest"),
            ]
        )
        assert result == 1

    def test_s3_destination_not_implemented(self, source_cache: Path) -> None:
        """S3 destination path should fail with not implemented."""
        result = sync_cache_main(
            [
                "push",
                str(source_cache),
                "s3://bucket/cache",
            ]
        )
        assert result == 1

    def test_local_snapshot_selection_excludes_update_lock(
        self,
        source_with_dpd: Path,
    ) -> None:
        """Selected snapshot sync should not include update lock files."""
        lock_file = (
            source_with_dpd / "test_dataset" / "test_dataset_update.lock"
        )
        lock_file.write_text("stale lock")

        selected = _collect_selected_local_snapshots(
            source_path=source_with_dpd,
            dataset_names=None,
            ignore_dataset_names=None,
            dpd_only=False,
            npd_only=False,
            all_snapshots=False,
            snapshot_suffixes=None,
        )
        pairs = _local_sync_pairs_from_selected(source_with_dpd, selected, {})

        synced_rel_paths = {rel for _path, rel in pairs}
        assert "test_dataset/test_dataset_update.lock" not in synced_rel_paths

    def test_mutually_exclusive_dataset_dpd_only(
        self, source_cache: Path, dest_cache: Path
    ) -> None:
        """--dataset and --dpd-only are mutually exclusive."""
        result = sync_cache_main(
            [
                "push",
                str(source_cache),
                str(dest_cache),
                "--dataset",
                "foo",
                "--dpd-only",
            ]
        )
        assert result == 1

    def test_mutually_exclusive_dataset_npd_only(
        self, source_cache: Path, dest_cache: Path
    ) -> None:
        """--dataset and --npd-only are mutually exclusive."""
        result = sync_cache_main(
            [
                "push",
                str(source_cache),
                str(dest_cache),
                "--dataset",
                "foo",
                "--npd-only",
            ]
        )
        assert result == 1

    def test_mutually_exclusive_dpd_npd_only(
        self, source_cache: Path, dest_cache: Path
    ) -> None:
        """--dpd-only and --npd-only are mutually exclusive."""
        result = sync_cache_main(
            [
                "push",
                str(source_cache),
                str(dest_cache),
                "--dpd-only",
                "--npd-only",
            ]
        )
        assert result == 1


class TestSyncPush:
    """Tests for push sync operation."""

    def test_push_empty_cache(
        self, source_cache: Path, dest_cache: Path
    ) -> None:
        """Push from empty cache should succeed."""
        result = sync_cache_main(
            [
                "push",
                str(source_cache),
                str(dest_cache),
            ]
        )
        assert result == 0

    def test_push_creates_destination(
        self, source_with_dpd: Path, tmp_path: Path
    ) -> None:
        """Push should create destination directory if it doesn't exist."""
        dest = tmp_path / "new_dest"
        assert not dest.exists()

        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest),
            ]
        )
        assert result == 0
        assert dest.exists()

    def test_push_copies_dpd_files(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push should copy DPD data and metadata files."""
        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
            ]
        )
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

    def test_push_copies_dpd_provenance_sidecar(
        self,
        source_with_dpd: Path,
        dest_cache: Path,
    ) -> None:
        """Push should copy provenance sidecars referenced by metadata."""
        dpd = DatedParquetDataset(
            cache_dir=source_with_dpd,
            name="test_dataset",
            date_col="Date",
            date_partition="month",
        )
        metadata = dpd._load_metadata()
        provenance = dpd._write_provenance_sidecar(
            metadata.suffix,
            {"inputs": ["raw_a", "raw_b"]},
        )
        metadata.provenance = provenance
        meta_path = dpd.meta_dir / f"test_dataset_{metadata.suffix}.pkl.gz"
        metadata.to_pickle(meta_path)

        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
            ]
        )

        assert result == 0
        sidecars = list(
            (dest_cache / "test_dataset" / "_provenance").glob(
                "*.provenance.pkl.gz"
            )
        )
        assert len(sidecars) == 1

    def test_push_ignores_nonstandard_provenance_sidecar_name(
        self,
        source_with_dpd: Path,
        dest_cache: Path,
    ) -> None:
        """Sync only guarantees the expected provenance sidecar name."""
        dpd = DatedParquetDataset(
            cache_dir=source_with_dpd,
            name="test_dataset",
            date_col="Date",
            date_partition="month",
        )
        metadata = dpd._load_metadata()
        custom_sidecar = (
            dpd.dataset_dir / "_provenance" / "custom_provenance.pkl.gz"
        )
        custom_sidecar.parent.mkdir(parents=True, exist_ok=True)
        custom_sidecar.write_bytes(b"custom")
        metadata.provenance = SnapshotProvenanceRef(
            path="_provenance/custom_provenance.pkl.gz",
            checksum="not_checked_by_sync",
            size_bytes=custom_sidecar.stat().st_size,
        )
        meta_path = dpd.meta_dir / f"test_dataset_{metadata.suffix}.pkl.gz"
        metadata.to_pickle(meta_path)

        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
            ]
        )

        assert result == 0
        assert not (
            dest_cache
            / "test_dataset"
            / "_provenance"
            / "custom_provenance.pkl.gz"
        ).exists()

    def test_push_dry_run_no_copy(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push --dry-run should not copy files."""
        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--dry-run",
            ]
        )
        assert result == 0

        # Nothing should be copied
        assert not (dest_cache / "test_dataset").exists()

    def test_push_skips_unchanged_files(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push should skip files that haven't changed."""
        # First sync
        result1 = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
            ]
        )
        assert result1 == 0

        # Second sync should skip all files (unchanged)
        result2 = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--verbose",
            ]
        )
        assert result2 == 0

    def test_push_dataset_filter(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push --dataset should only sync matching datasets."""
        # Try to sync non-existent dataset
        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--dataset",
                "nonexistent",
            ]
        )
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

        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--dpd-only",
            ]
        )
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

        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--npd-only",
            ]
        )
        assert result == 0

        # DPD should NOT be synced
        assert not (dest_cache / "test_dataset").exists()
        # NPD should be synced
        assert (dest_cache / "non-dated" / "test_npd").exists()

    def test_push_copies_npd_sidecars(
        self,
        source_with_dpd: Path,
        dest_cache: Path,
        tmp_path: Path,
    ) -> None:
        """Push should copy convention-named NPD info/provenance sidecars."""
        source = tmp_path / "ref.parquet"
        pd.DataFrame({"symbol": ["AAPL"]}).to_parquet(source, index=False)
        npd = NonDatedParquetDataset(
            cache_dir=source_with_dpd,
            name="test_npd",
        )
        suffix = npd.import_snapshot(
            source,
            info={"notes": "NPD notes."},
            provenance={"source": "unit-test"},
        )

        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--npd-only",
            ]
        )

        assert result == 0
        dest_npd_dir = dest_cache / "non-dated" / "test_npd"
        assert (dest_npd_dir / f"test_npd_{suffix}.parquet").exists()
        assert (
            dest_npd_dir / "_meta_data" / f"test_npd_{suffix}.pkl.gz"
        ).exists()
        assert (
            dest_npd_dir
            / "_provenance"
            / f"test_npd_{suffix}.provenance.pkl.gz"
        ).exists()

    def test_push_delete_removes_extra_files(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push --delete should remove files at dest not in source."""
        # First sync
        result1 = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
            ]
        )
        assert result1 == 0

        # Create extra file at destination
        extra_file = dest_cache / "test_dataset" / "extra.parquet"
        extra_file.write_bytes(b"extra data")
        assert extra_file.exists()

        # Sync with --delete
        result2 = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--delete",
            ]
        )
        assert result2 == 0

        # Extra file should be deleted
        assert not extra_file.exists()

    def test_push_delete_dry_run_no_delete(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push --delete --dry-run should not delete files."""
        # First sync
        sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
            ]
        )

        # Create extra file at destination
        extra_file = dest_cache / "test_dataset" / "extra.parquet"
        extra_file.write_bytes(b"extra data")

        # Dry run with --delete
        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--delete",
                "--dry-run",
            ]
        )
        assert result == 0

        # Extra file should NOT be deleted
        assert extra_file.exists()

    def test_push_delete_preserves_yaml_code(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push --delete should preserve yaml/ and code/ directories."""
        # First sync
        sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
            ]
        )

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
        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--delete",
            ]
        )
        assert result == 0

        # yaml/ and code/ should be preserved
        assert yaml_file.exists()
        assert code_file.exists()

    def test_push_delete_preserves_ignored_datasets(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Push --delete should not remove ignored destination datasets."""
        ignored_file = dest_cache / "ignored_dataset" / "extra.parquet"
        ignored_file.parent.mkdir(parents=True)
        ignored_file.write_bytes(b"ignored data")

        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--ignore-datasets",
                "ignored_dataset",
                "--delete",
            ]
        )

        assert result == 0
        assert ignored_file.exists()


class TestSyncPull:
    """Tests for pull sync operation."""

    def test_pull_basic(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """Pull should copy files from source to destination."""
        result = sync_cache_main(
            [
                "pull",
                str(source_with_dpd),
                str(dest_cache),
            ]
        )
        assert result == 0

        # Check data was copied
        assert (dest_cache / "test_dataset").exists()


class TestGcsBlobSelection:
    """Tests for GCS-source snapshot selection."""

    def test_defaults_to_current_snapshots(self) -> None:
        """GCS source sync should select only current DPD and NPD snapshots."""
        rels = [
            "dpd/_meta_data/dpd_1AAAAAA.pkl.gz",
            "dpd/month=M2024-01/data_1AAAAAA.parquet",
            "dpd/_meta_data/dpd_1BBBBB0.pkl.gz",
            "dpd/month=M2024-01/data_1BBBBB0.parquet",
            "dpd/_provenance/dpd_1BBBBB0.provenance.pkl.gz",
            "dpd/_provenance/custom_1BBBBB0.provenance.pkl.gz",
            "non-dated/ref/ref_1AAAAAA/data.parquet",
            "non-dated/ref/ref_1BBBBB0/data.parquet",
            "non-dated/ref/_meta_data/ref_1BBBBB0.pkl.gz",
            "non-dated/ref/_provenance/ref_1BBBBB0.provenance.pkl.gz",
            "non-dated/ref/_provenance/custom_1BBBBB0.provenance.pkl.gz",
        ]

        selected = _collect_gcs_sync_blobs(
            _gcs_urls(*rels),
            GCS_CACHE,
            None,
            None,
            False,
            False,
            False,
            None,
        )

        assert {rel for _, rel in selected} == {
            "dpd/_meta_data/dpd_1BBBBB0.pkl.gz",
            "dpd/month=M2024-01/data_1BBBBB0.parquet",
            "dpd/_provenance/dpd_1BBBBB0.provenance.pkl.gz",
            "non-dated/ref/ref_1BBBBB0/data.parquet",
            "non-dated/ref/_meta_data/ref_1BBBBB0.pkl.gz",
            "non-dated/ref/_provenance/ref_1BBBBB0.provenance.pkl.gz",
        }

    def test_dataset_filter_matches_npd_dataset_name(self) -> None:
        """Dataset filters should match NPD names under non-dated/."""
        rels = [
            "dpd/_meta_data/dpd_1BBBBB0.pkl.gz",
            "dpd/month=M2024-01/data_1BBBBB0.parquet",
            "non-dated/ref/ref_1BBBBB0/data.parquet",
        ]

        selected = _collect_gcs_sync_blobs(
            _gcs_urls(*rels),
            GCS_CACHE,
            ["ref"],
            None,
            False,
            False,
            False,
            None,
        )

        assert {rel for _, rel in selected} == {
            "non-dated/ref/ref_1BBBBB0/data.parquet",
        }

    def test_honors_type_and_snapshot_filters(self) -> None:
        """GCS source sync should honor type and explicit snapshot filters."""
        rels = [
            "dpd/_meta_data/dpd_1AAAAAA.pkl.gz",
            "dpd/month=M2024-01/data_1AAAAAA.parquet",
            "dpd/_meta_data/dpd_1BBBBB0.pkl.gz",
            "dpd/month=M2024-01/data_1BBBBB0.parquet",
            "non-dated/ref/ref_1AAAAAA/data.parquet",
            "non-dated/ref/ref_1BBBBB0/data.parquet",
        ]
        urls = _gcs_urls(*rels)

        dpd_only = _collect_gcs_sync_blobs(
            urls, GCS_CACHE, None, None, True, False, False, None
        )
        assert {rel for _, rel in dpd_only} == {
            "dpd/_meta_data/dpd_1BBBBB0.pkl.gz",
            "dpd/month=M2024-01/data_1BBBBB0.parquet",
        }

        npd_only = _collect_gcs_sync_blobs(
            urls, GCS_CACHE, None, None, False, True, False, None
        )
        assert {rel for _, rel in npd_only} == {
            "non-dated/ref/ref_1BBBBB0/data.parquet",
        }

        explicit_snapshot = _collect_gcs_sync_blobs(
            urls, GCS_CACHE, None, None, False, False, False, ["1AAAAAA"]
        )
        assert {rel for _, rel in explicit_snapshot} == {
            "dpd/_meta_data/dpd_1AAAAAA.pkl.gz",
            "dpd/month=M2024-01/data_1AAAAAA.parquet",
            "non-dated/ref/ref_1AAAAAA/data.parquet",
        }

        all_snapshots = _collect_gcs_sync_blobs(
            urls, GCS_CACHE, None, None, False, False, True, None
        )
        assert {rel for _, rel in all_snapshots} == set(rels)


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
        df1 = pd.DataFrame(
            {
                "Date": pd.date_range("2024-01-01", "2024-01-15"),
                "value": range(15),
            }
        )
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
        df2 = pd.DataFrame(
            {
                "Date": pd.date_range("2024-01-01", "2024-01-31"),
                "value": range(31),
            }
        )
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
        result = sync_cache_main(
            [
                "push",
                str(source_cache),
                str(dest_cache),
                "--all-snapshots",
            ]
        )
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

        df1 = pd.DataFrame(
            {
                "Date": pd.date_range("2024-01-01", "2024-01-15"),
                "value": range(15),
            }
        )
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
        df2 = pd.DataFrame(
            {
                "Date": pd.date_range("2024-01-01", "2024-01-31"),
                "value": range(31),
            }
        )
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
        result = sync_cache_main(
            [
                "push",
                str(source_cache),
                str(dest_cache),
            ]
        )
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
        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--rename",
                "invalid_format",
            ]
        )
        assert result == 1

    def test_rename_copies_to_new_name(
        self, source_with_dpd: Path, dest_cache: Path
    ) -> None:
        """--rename should copy dataset with new name."""
        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--rename",
                "test_dataset:renamed_dataset",
            ]
        )
        assert result == 0

        # Source name should NOT exist at destination
        assert not (dest_cache / "test_dataset").exists()

        # Renamed dataset should exist
        assert (dest_cache / "renamed_dataset").exists()
        assert (dest_cache / "renamed_dataset" / "_meta_data").exists()

        # Metadata file should have new name
        meta_files = list(
            (dest_cache / "renamed_dataset" / "_meta_data").glob(
                "renamed_dataset_*.pkl.gz"
            )
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
        df = pd.DataFrame(
            {
                "Date": pd.date_range("2024-02-01", "2024-02-28"),
                "value": range(28),
            }
        )
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
        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--rename",
                "test_dataset:renamed_dataset",
            ]
        )
        assert result == 0

        # Only the renamed dataset should be at destination
        assert (dest_cache / "renamed_dataset").exists()
        # other_dataset should NOT be synced (filtered out)
        assert not (dest_cache / "other_dataset").exists()


class TestPostSyncFunctions:
    """Tests for sync-cache post-sync function execution."""

    def test_cli_sync_function_runs_after_local_push(
        self,
        source_with_dpd: Path,
        dest_cache: Path,
    ) -> None:
        """A command-line sync function should run after file copying."""
        hook_file = _write_sync_hooks(source_with_dpd)

        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--sync-function",
                f"{hook_file}:record_sync",
            ]
        )

        assert result == 0
        assert (dest_cache / "test_dataset").exists()
        calls = (dest_cache / "_sync_function_calls.txt").read_text()
        assert "dpd|test_dataset|test_dataset|" in calls
        assert str(source_with_dpd) in calls
        assert str(dest_cache) in calls

    def test_sync_targets_use_preselected_snapshots(
        self,
        source_with_dpd: Path,
        dest_cache: Path,
    ) -> None:
        """Post-sync targets should not rediscover newer source snapshots."""
        from ionbus_parquet_cache.snapshot import generate_snapshot_suffix

        selected = _collect_selected_local_snapshots(
            source_path=source_with_dpd,
            dataset_names=None,
            ignore_dataset_names=None,
            dpd_only=False,
            npd_only=False,
            all_snapshots=False,
            snapshot_suffixes=None,
        )
        selected_suffixes = [item.suffix for item in selected]

        future_suffix = generate_snapshot_suffix(time.time() + 86_400)
        assert future_suffix > selected_suffixes[0]

        dpd = DatedParquetDataset(
            cache_dir=source_with_dpd,
            name="test_dataset",
            date_col="Date",
            date_partition="month",
        )
        dpd._publish_snapshot(
            files=[],
            schema=pa.schema([]),
            cache_start_date=dt.date(2024, 2, 1),
            cache_end_date=dt.date(2024, 2, 1),
            partition_values={},
            suffix=future_suffix,
        )

        targets = _sync_function_targets_from_selected(
            selected,
            str(source_with_dpd),
            str(dest_cache),
            {},
        )

        assert [target.snapshot_id for target in targets] == selected_suffixes
        assert dpd._discover_current_suffix() == future_suffix

    def test_cli_class_sync_function_receives_init_args(
        self,
        source_with_dpd: Path,
        dest_cache: Path,
    ) -> None:
        """CLI init args should instantiate class-based sync functions."""
        hook_file = _write_sync_hooks(source_with_dpd)

        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--sync-function",
                f"{hook_file}:ClassRecorder",
                "--sync-function-init-args",
                '{"marker": "_class_sync_function_calls.txt"}',
            ]
        )

        assert result == 0
        calls = (dest_cache / "_class_sync_function_calls.txt").read_text()
        assert "dpd|test_dataset|" in calls

    def test_run_sync_only_runs_without_copying(
        self,
        source_with_dpd: Path,
        dest_cache: Path,
    ) -> None:
        """--run-sync-only should verify existing files then run hooks."""
        hook_file = _write_sync_hooks(source_with_dpd)
        assert (
            sync_cache_main(["push", str(source_with_dpd), str(dest_cache)])
            == 0
        )

        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--run-sync-only",
                "--sync-function",
                f"{hook_file}:record_sync",
            ]
        )

        assert result == 0
        calls = (dest_cache / "_sync_function_calls.txt").read_text()
        assert "dpd|test_dataset|test_dataset|" in calls

    def test_run_sync_only_fails_when_destination_snapshot_missing(
        self,
        source_with_dpd: Path,
        dest_cache: Path,
    ) -> None:
        """--run-sync-only should fail before hooks if files are missing."""
        hook_file = _write_sync_hooks(source_with_dpd)

        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--run-sync-only",
                "--sync-function",
                f"{hook_file}:record_sync",
            ]
        )

        assert result == 1
        assert not (dest_cache / "_sync_function_calls.txt").exists()

    def test_yaml_configured_sync_function_runs(
        self,
        source_with_dpd: Path,
        dest_cache: Path,
    ) -> None:
        """--run-sync-functions should use per-dataset YAML config."""
        _write_sync_hooks(source_with_dpd)
        _write_yaml_sync_config(source_with_dpd, "test_dataset")

        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--run-sync-functions",
            ]
        )

        assert result == 0
        calls = (dest_cache / "_sync_function_calls.txt").read_text()
        assert "dpd|test_dataset|test_dataset|" in calls

    def test_yaml_sync_function_missing_config_logs_clear_warning(
        self,
        source_with_dpd: Path,
        dest_cache: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Missing YAML hooks should produce a clear zero-run warning."""
        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--run-sync-functions",
            ]
        )

        assert result == 0
        assert (
            "No datasets with configured sync functions found" in caplog.text
        )
        assert "sync_function_name is set in YAML" in caplog.text

    def test_cli_sync_function_overrides_yaml(
        self,
        source_with_dpd: Path,
        dest_cache: Path,
    ) -> None:
        """A CLI sync function should ignore per-dataset YAML hooks."""
        hook_file = _write_sync_hooks(source_with_dpd)
        _write_yaml_sync_config(
            source_with_dpd,
            "test_dataset",
            function_name="failing_sync",
        )

        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--sync-function",
                f"{hook_file}:record_sync",
            ]
        )

        assert result == 0
        calls = (dest_cache / "_sync_function_calls.txt").read_text()
        assert "dpd|test_dataset|test_dataset|" in calls

    @pytest.mark.parametrize(
        "extra_args",
        [
            ["--run-sync-functions"],
            ["--run-sync-only"],
            ["--sync-function", "module://hooks:record_sync"],
        ],
    )
    def test_dry_run_rejects_sync_functions(
        self,
        source_with_dpd: Path,
        dest_cache: Path,
        extra_args: list[str],
    ) -> None:
        """Dry runs cannot request external sync-function side effects."""
        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--dry-run",
                *extra_args,
            ]
        )

        assert result == 1

    def test_pull_rejects_sync_functions(
        self,
        source_with_dpd: Path,
        dest_cache: Path,
    ) -> None:
        """Post-sync functions are supported only for push."""
        _write_sync_hooks(source_with_dpd)

        result = sync_cache_main(
            [
                "pull",
                str(source_with_dpd),
                str(dest_cache),
                "--run-sync-functions",
            ]
        )

        assert result == 1

    def test_rename_passes_source_and_dest_names(
        self,
        source_with_dpd: Path,
        dest_cache: Path,
    ) -> None:
        """Sync functions should receive both source and renamed dest names."""
        hook_file = _write_sync_hooks(source_with_dpd)

        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--rename",
                "test_dataset:renamed_dataset",
                "--sync-function",
                f"{hook_file}:record_sync",
            ]
        )

        assert result == 0
        calls = (dest_cache / "_sync_function_calls.txt").read_text()
        assert "dpd|test_dataset|renamed_dataset|" in calls

    def test_cli_sync_function_runs_for_npd(
        self,
        source_cache: Path,
        dest_cache: Path,
    ) -> None:
        """Sync functions should run for selected NPD snapshots."""
        hook_file = _write_sync_hooks(source_cache)
        npd_dir = source_cache / "non-dated" / "ref"
        npd_dir.mkdir(parents=True)
        suffix = "1BBBBB0"
        pq.write_table(
            pa.Table.from_pandas(pd.DataFrame({"value": [1, 2, 3]})),
            npd_dir / f"ref_{suffix}.parquet",
        )

        result = sync_cache_main(
            [
                "push",
                str(source_cache),
                str(dest_cache),
                "--sync-function",
                f"{hook_file}:record_sync",
            ]
        )

        assert result == 0
        calls = (dest_cache / "_sync_function_calls.txt").read_text()
        assert f"npd|ref|ref|{suffix}" in calls

    def test_gcs_push_runs_cli_sync_function(
        self,
        source_with_dpd: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Local-to-GCS push should copy files then run sync functions."""
        hook_file = _write_sync_hooks(source_with_dpd)
        uploads: list[str] = []

        monkeypatch.setattr(
            "ionbus_parquet_cache.gcs_utils.should_upload",
            lambda _local_file, _dest_url: True,
        )
        monkeypatch.setattr(
            "ionbus_parquet_cache.gcs_utils.gcs_upload",
            lambda _local_file, dest_url: uploads.append(dest_url),
        )

        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                GCS_CACHE,
                "--sync-function",
                f"{hook_file}:record_to_source",
            ]
        )

        assert result == 0
        assert uploads
        calls = (source_with_dpd / "_gcs_sync_function_calls.txt").read_text()
        assert f"dpd|{GCS_CACHE}|" in calls

    def test_sync_function_failure_returns_nonzero_after_copy(
        self,
        source_with_dpd: Path,
        dest_cache: Path,
    ) -> None:
        """A hook failure should not roll back already copied files."""
        hook_file = _write_sync_hooks(source_with_dpd)

        result = sync_cache_main(
            [
                "push",
                str(source_with_dpd),
                str(dest_cache),
                "--sync-function",
                f"{hook_file}:failing_sync",
            ]
        )

        assert result == 1
        assert (dest_cache / "test_dataset").exists()
