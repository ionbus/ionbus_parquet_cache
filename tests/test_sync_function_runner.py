"""Tests for sync-cache post-sync function loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from ionbus_parquet_cache.exceptions import ConfigurationError
from ionbus_parquet_cache.sync_function_runner import (
    SyncFunctionConfig,
    SyncFunctionTarget,
    load_sync_function,
    parse_sync_function_spec,
    run_sync_function,
)


def _target(tmp_path: Path) -> SyncFunctionTarget:
    """Return a representative sync function target."""
    return SyncFunctionTarget(
        source_dataset_name="source_dataset",
        dest_dataset_name="dest_dataset",
        dataset_type="dpd",
        snapshot_id="1AAAAAA",
        source_location=str(tmp_path / "source"),
        dest_location=str(tmp_path / "dest"),
    )


def test_parse_sync_function_spec_module_location() -> None:
    """The final colon separates module:// location from callable name."""
    location, name = parse_sync_function_spec(
        "module://my_library.sync_utils:SyncProvenance"
    )

    assert location == "module://my_library.sync_utils"
    assert name == "SyncProvenance"


def test_load_sync_function_from_file(tmp_path: Path) -> None:
    """Plain functions can be loaded from cache-local Python files."""
    code_dir = tmp_path / "code"
    code_dir.mkdir()
    hook_file = code_dir / "hooks.py"
    hook_file.write_text("""
from pathlib import Path


def record_sync(
    source_dataset_name,
    dest_dataset_name,
    dataset_type,
    snapshot_id,
    source_location,
    dest_location,
):
    Path(dest_location).mkdir(parents=True, exist_ok=True)
    Path(dest_location, "hook.txt").write_text(
        f"{dataset_type}:{source_dataset_name}:{dest_dataset_name}:{snapshot_id}"
    )
""")

    sync_function = load_sync_function(
        SyncFunctionConfig(
            location="code/hooks.py",
            name="record_sync",
        ),
        tmp_path,
    )
    run_sync_function(sync_function, _target(tmp_path))

    assert (tmp_path / "dest" / "hook.txt").read_text() == (
        "dpd:source_dataset:dest_dataset:1AAAAAA"
    )


def test_load_class_sync_function_with_init_args(tmp_path: Path) -> None:
    """Callable classes are instantiated with sync_function_init_args."""
    hook_file = tmp_path / "hooks.py"
    hook_file.write_text("""
from pathlib import Path


class Recorder:
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
        Path(dest_location).mkdir(parents=True, exist_ok=True)
        Path(dest_location, self.marker).write_text(snapshot_id)
""")

    sync_function = load_sync_function(
        SyncFunctionConfig(
            location=str(hook_file),
            name="Recorder",
            init_args={"marker": "class_hook.txt"},
        ),
        tmp_path,
    )
    run_sync_function(sync_function, _target(tmp_path))

    assert (tmp_path / "dest" / "class_hook.txt").read_text() == "1AAAAAA"


def test_plain_function_rejects_init_args(tmp_path: Path) -> None:
    """Init args are only valid for class-based sync functions."""
    hook_file = tmp_path / "hooks.py"
    hook_file.write_text("""
def record_sync(
    source_dataset_name,
    dest_dataset_name,
    dataset_type,
    snapshot_id,
    source_location,
    dest_location,
):
    pass
""")

    with pytest.raises(ConfigurationError, match="class-based"):
        load_sync_function(
            SyncFunctionConfig(
                location=str(hook_file),
                name="record_sync",
                init_args={"marker": "nope"},
            ),
            tmp_path,
        )


def test_module_sync_function_loads(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """module:// locations load from importable Python modules."""
    module_file = tmp_path / "module_hooks.py"
    module_file.write_text("""
def record_sync(
    source_dataset_name,
    dest_dataset_name,
    dataset_type,
    snapshot_id,
    source_location,
    dest_location,
):
    return None
""")
    monkeypatch.syspath_prepend(str(tmp_path))

    sync_function = load_sync_function(
        SyncFunctionConfig(
            location="module://module_hooks",
            name="record_sync",
        ),
        tmp_path,
    )

    run_sync_function(sync_function, _target(tmp_path))


def test_incompatible_signature_raises(tmp_path: Path) -> None:
    """Sync functions must accept the documented keyword contract."""
    hook_file = tmp_path / "hooks.py"
    hook_file.write_text("""
def bad_sync():
    pass
""")

    with pytest.raises(ConfigurationError, match="incompatible signature"):
        load_sync_function(
            SyncFunctionConfig(
                location=str(hook_file),
                name="bad_sync",
            ),
            tmp_path,
        )
