"""Tests for DatedParquetDataset update lock file feature."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from ionbus_parquet_cache.dated_dataset import DatedParquetDataset
from ionbus_parquet_cache.exceptions import UpdateLockedError


@pytest.fixture
def dpd(tmp_path: Path) -> DatedParquetDataset:
    return DatedParquetDataset(
        cache_dir=tmp_path / "cache",
        name="test_ds",
        date_col="date",
        date_partition="year",
    )


class TestLockPath:
    def test_lock_path_inside_dataset_dir(
        self, dpd: DatedParquetDataset
    ) -> None:
        assert dpd._lock_path == dpd.dataset_dir / "test_ds_update.lock"

    def test_lock_path_uses_lock_dir_when_set(self, tmp_path: Path) -> None:
        lock_dir = tmp_path / "locks"
        dpd = DatedParquetDataset(
            cache_dir=tmp_path / "cache",
            name="test_ds",
            date_col="date",
            date_partition="year",
            lock_dir=lock_dir,
        )
        assert dpd._lock_path == lock_dir / "test_ds_update.lock"

    def test_use_update_lock_false_default_is_true(
        self, dpd: DatedParquetDataset
    ) -> None:
        assert dpd.use_update_lock is True

    def test_use_update_lock_false_can_be_set(self, tmp_path: Path) -> None:
        dpd = DatedParquetDataset(
            cache_dir=tmp_path / "cache",
            name="test_ds",
            date_col="date",
            date_partition="year",
            use_update_lock=False,
        )
        assert dpd.use_update_lock is False


class TestAcquireLock:
    def test_creates_lock_file(self, dpd: DatedParquetDataset) -> None:
        dpd.dataset_dir.mkdir(parents=True, exist_ok=True)
        dpd._acquire_lock()
        assert dpd._lock_path.exists()

    def test_lock_file_contains_expected_fields(
        self, dpd: DatedParquetDataset
    ) -> None:
        dpd.dataset_dir.mkdir(parents=True, exist_ok=True)
        dpd._acquire_lock()
        info = json.loads(dpd._lock_path.read_text())
        assert info["dataset"] == "test_ds"
        assert "hostname" in info
        assert info["pid"] == os.getpid()
        assert "started_at" in info

    def test_raises_if_lock_exists(self, dpd: DatedParquetDataset) -> None:
        dpd.dataset_dir.mkdir(parents=True, exist_ok=True)
        dpd._acquire_lock()
        with pytest.raises(UpdateLockedError) as exc_info:
            dpd._acquire_lock()
        err = exc_info.value
        assert "test_ds" in str(err)
        assert err.lock_path is not None
        assert err.locked_by_pid == os.getpid()

    def test_error_message_includes_clear_instructions(
        self, dpd: DatedParquetDataset
    ) -> None:
        dpd.dataset_dir.mkdir(parents=True, exist_ok=True)
        dpd._acquire_lock()
        with pytest.raises(UpdateLockedError) as exc_info:
            dpd._acquire_lock()
        msg = str(exc_info.value)
        assert "clear_update_lock" in msg
        assert "rm " in msg

    def test_creates_parent_dir_if_missing(
        self, dpd: DatedParquetDataset
    ) -> None:
        assert not dpd.dataset_dir.exists()
        dpd._acquire_lock()
        assert dpd._lock_path.exists()


class TestReleaseLock:
    def test_removes_lock_file(self, dpd: DatedParquetDataset) -> None:
        dpd.dataset_dir.mkdir(parents=True, exist_ok=True)
        dpd._acquire_lock()
        assert dpd._lock_path.exists()
        dpd._release_lock()
        assert not dpd._lock_path.exists()

    def test_no_error_if_lock_missing(self, dpd: DatedParquetDataset) -> None:
        dpd._release_lock()  # Should not raise

    def test_can_reacquire_after_release(
        self, dpd: DatedParquetDataset
    ) -> None:
        dpd.dataset_dir.mkdir(parents=True, exist_ok=True)
        dpd._acquire_lock()
        dpd._release_lock()
        dpd._acquire_lock()  # Should not raise
        assert dpd._lock_path.exists()


class TestClearUpdateLock:
    def test_removes_lock_file(self, dpd: DatedParquetDataset) -> None:
        dpd.dataset_dir.mkdir(parents=True, exist_ok=True)
        dpd._acquire_lock()
        dpd.clear_update_lock(force=True)
        assert not dpd._lock_path.exists()

    def test_raises_if_no_lock(self, dpd: DatedParquetDataset) -> None:
        with pytest.raises(FileNotFoundError):
            dpd.clear_update_lock(force=True)

    def test_force_false_clears_dead_pid(
        self, dpd: DatedParquetDataset
    ) -> None:
        dpd.dataset_dir.mkdir(parents=True, exist_ok=True)
        # Write a lock with a PID that definitely doesn't exist
        import socket

        payload = {
            "dataset": "test_ds",
            "hostname": socket.gethostname(),
            "pid": 99999999,
            "started_at": "2020-01-01T00:00:00",
        }
        dpd._lock_path.write_text(json.dumps(payload))
        # force=False should succeed since that PID is gone
        dpd.clear_update_lock(force=False)
        assert not dpd._lock_path.exists()

    def test_force_false_raises_for_live_pid(
        self, dpd: DatedParquetDataset
    ) -> None:
        dpd.dataset_dir.mkdir(parents=True, exist_ok=True)
        import socket

        payload = {
            "dataset": "test_ds",
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "started_at": "2020-01-01T00:00:00",
        }
        dpd._lock_path.write_text(json.dumps(payload))
        with pytest.raises(UpdateLockedError):
            dpd.clear_update_lock(force=False)
        assert dpd._lock_path.exists()

    def test_force_true_clears_regardless_of_pid(
        self, dpd: DatedParquetDataset
    ) -> None:
        dpd.dataset_dir.mkdir(parents=True, exist_ok=True)
        import socket

        payload = {
            "dataset": "test_ds",
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "started_at": "2020-01-01T00:00:00",
        }
        dpd._lock_path.write_text(json.dumps(payload))
        dpd.clear_update_lock(force=True)
        assert not dpd._lock_path.exists()

    def test_force_false_clears_foreign_host_lock(
        self, dpd: DatedParquetDataset
    ) -> None:
        dpd.dataset_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "dataset": "test_ds",
            "hostname": "some-other-host.example.com",
            "pid": os.getpid(),
            "started_at": "2020-01-01T00:00:00",
        }
        dpd._lock_path.write_text(json.dumps(payload))
        # Different host — can't check PID, so we allow clearing
        dpd.clear_update_lock(force=False)
        assert not dpd._lock_path.exists()


class TestUpdateLockedErrorAttributes:
    def test_error_has_lock_metadata(self, dpd: DatedParquetDataset) -> None:
        dpd.dataset_dir.mkdir(parents=True, exist_ok=True)
        dpd._acquire_lock()
        with pytest.raises(UpdateLockedError) as exc_info:
            dpd._acquire_lock()
        err = exc_info.value
        assert err.lock_path == str(dpd._lock_path)
        assert err.locked_by_pid == os.getpid()
        assert err.locked_by_host is not None
        assert err.lock_age_seconds is not None
        assert err.lock_age_seconds >= 0
