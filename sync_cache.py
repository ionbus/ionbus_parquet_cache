"""
Command-line tool for syncing DatedParquetDataset caches.

Entry point: sync-cache
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ionbus_utils.file_utils import format_size, get_file_hash
from ionbus_utils.logging_utils import logger

# Import discovery functions from cleanup module
from ionbus_parquet_cache.cleanup_cache import (
    _discover_dpd_snapshots,
    _discover_npd_snapshots,
)
from ionbus_parquet_cache.dated_dataset import SnapshotMetadata
from ionbus_parquet_cache.exceptions import ConfigurationError, SyncError
from ionbus_parquet_cache.snapshot import extract_suffix_from_filename
from ionbus_parquet_cache.sync_function_runner import (
    SyncFunctionConfig,
    SyncFunctionTarget,
    load_sync_function,
    parse_sync_function_spec,
    run_sync_function,
)
from ionbus_parquet_cache.yaml_config import load_all_configs


def sync_cache_main(args: list[str] | None = None) -> int:
    """
    Main entry point for sync-cache command.

    Syncs a cache between locations (push or pull).

    Returns:
        Exit code (0 for success, non-zero for errors).
    """
    parser = argparse.ArgumentParser(
        prog="sync-cache",
        description="Sync cache between locations.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Push subcommand
    push_parser = subparsers.add_parser(
        "push", help="Push local cache to destination"
    )
    push_parser.add_argument("source", help="Source cache directory (local)")
    push_parser.add_argument(
        "destination", help="Destination path (local or gs://)"
    )
    _add_sync_options(push_parser)

    # Pull subcommand
    pull_parser = subparsers.add_parser(
        "pull", help="Pull from source to local cache"
    )
    pull_parser.add_argument("source", help="Source path (local or gs://)")
    pull_parser.add_argument(
        "destination", help="Destination cache directory (local)"
    )
    _add_sync_options(pull_parser)
    pull_parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run continuously, checking for updates",
    )
    pull_parser.add_argument(
        "--update-interval",
        type=int,
        default=60,
        metavar="N",
        help="Seconds between sync checks (default: 60)",
    )

    parsed = parser.parse_args(args)

    # Validate mutually exclusive options
    if parsed.datasets and parsed.ignore_datasets:
        logger.error(
            "Error: --datasets and --ignore-datasets are mutually exclusive"
        )
        return 1

    if parsed.snapshot and parsed.all_snapshots:
        logger.error(
            "Error: --snapshot and --all-snapshots are mutually exclusive"
        )
        return 1

    mode_count = sum(
        [
            bool(parsed.datasets),
            parsed.dpd_only,
            parsed.npd_only,
        ]
    )
    if mode_count > 1:
        logger.error(
            "Error: --datasets, --dpd-only, and --npd-only are mutually exclusive"
        )
        return 1

    sync_function_config: SyncFunctionConfig | None = None
    try:
        sync_function_config = _parse_cli_sync_function_config(
            parsed.sync_function,
            parsed.sync_function_init_args,
        )
    except ConfigurationError as e:
        logger.error(f"Error: {e}")
        return 1

    # Parse rename option
    rename_map: dict[str, str] = {}
    if parsed.rename:
        if ":" not in parsed.rename:
            logger.error(
                "Error: --rename must be in format 'source_name:target_name'"
            )
            return 1
        old_name, new_name = parsed.rename.split(":", 1)
        rename_map[old_name] = new_name
        # If renaming, must also filter to that dataset
        if not parsed.datasets:
            parsed.datasets = [old_name]
        # Enforce exactly 1 dataset when renaming
        if len(parsed.datasets) != 1:
            logger.error(
                "Error: --rename works with exactly one dataset, "
                f"but got {len(parsed.datasets)}: {parsed.datasets}"
            )
            return 1

    # Check for S3 paths
    if parsed.source.startswith("s3://") or parsed.destination.startswith(
        "s3://"
    ):
        logger.error("Error: S3 sync is not yet implemented")
        return 1

    # Route GCS operations
    src_is_gcs = parsed.source.startswith("gs://")
    dst_is_gcs = parsed.destination.startswith("gs://")
    sync_functions_requested = (
        parsed.run_sync_functions
        or parsed.run_sync_only
        or sync_function_config is not None
    )

    if sync_functions_requested:
        if parsed.dry_run:
            logger.error(
                "Error: --dry-run cannot be used with sync functions"
            )
            return 1
        if parsed.command != "push":
            logger.error("Error: sync functions are supported only for push")
            return 1
        if src_is_gcs:
            logger.error("Error: sync functions require a local source cache")
            return 1

    if src_is_gcs or dst_is_gcs:
        try:
            if parsed.command == "push":
                return _run_sync_push_gcs(
                    source=parsed.source,
                    destination=parsed.destination,
                    dataset_names=parsed.datasets,
                    ignore_dataset_names=parsed.ignore_datasets,
                    dpd_only=parsed.dpd_only,
                    npd_only=parsed.npd_only,
                    all_snapshots=parsed.all_snapshots,
                    snapshot_suffixes=parsed.snapshot,
                    dry_run=parsed.dry_run,
                    verbose=parsed.verbose,
                    workers=parsed.workers,
                    rename_map=rename_map,
                    run_sync_functions=parsed.run_sync_functions,
                    run_sync_only=parsed.run_sync_only,
                    sync_function_config=sync_function_config,
                )
            else:  # pull
                return _run_sync_pull_gcs(
                    source=parsed.source,
                    destination=parsed.destination,
                    dataset_names=parsed.datasets,
                    ignore_dataset_names=parsed.ignore_datasets,
                    dpd_only=parsed.dpd_only,
                    npd_only=parsed.npd_only,
                    all_snapshots=parsed.all_snapshots,
                    snapshot_suffixes=parsed.snapshot,
                    dry_run=parsed.dry_run,
                    verbose=parsed.verbose,
                    workers=parsed.workers,
                    daemon=parsed.daemon,
                    update_interval=parsed.update_interval,
                    rename_map=rename_map,
                )
        except Exception as e:
            logger.error(f"Error: {e}")
            return 1

    # Run sync
    try:
        if parsed.command == "push":
            return _run_sync_push(
                source=parsed.source,
                destination=parsed.destination,
                dataset_names=parsed.datasets,
                ignore_dataset_names=parsed.ignore_datasets,
                dpd_only=parsed.dpd_only,
                npd_only=parsed.npd_only,
                all_snapshots=parsed.all_snapshots,
                snapshot_suffixes=parsed.snapshot,
                dry_run=parsed.dry_run,
                delete=parsed.delete,
                verbose=parsed.verbose,
                workers=parsed.workers,
                rename_map=rename_map,
                run_sync_functions=parsed.run_sync_functions,
                run_sync_only=parsed.run_sync_only,
                sync_function_config=sync_function_config,
            )
        else:  # pull
            return _run_sync_pull(
                source=parsed.source,
                destination=parsed.destination,
                dataset_names=parsed.datasets,
                ignore_dataset_names=parsed.ignore_datasets,
                dpd_only=parsed.dpd_only,
                npd_only=parsed.npd_only,
                all_snapshots=parsed.all_snapshots,
                snapshot_suffixes=parsed.snapshot,
                dry_run=parsed.dry_run,
                delete=parsed.delete,
                daemon=parsed.daemon,
                update_interval=parsed.update_interval,
                verbose=parsed.verbose,
                workers=parsed.workers,
                rename_map=rename_map,
            )
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


def _add_sync_options(parser: argparse.ArgumentParser) -> None:
    """Add common sync options to a parser."""
    parser.add_argument(
        "--datasets",
        "--dataset",
        nargs="+",
        metavar="NAME",
        help="Sync only these datasets (whitelist)",
    )
    parser.add_argument(
        "--ignore-datasets",
        nargs="+",
        metavar="NAME",
        help="Exclude these datasets from sync (blacklist)",
    )
    parser.add_argument(
        "--snapshot",
        nargs="+",
        metavar="SUFFIX",
        help="Sync specific snapshots by suffix",
    )
    parser.add_argument(
        "--rename",
        type=str,
        metavar="OLD:NEW",
        help="Rename dataset during copy (format: 'source_name:target_name')",
    )
    parser.add_argument(
        "--dpd-only",
        action="store_true",
        help="Sync DPDs only, exclude NPDs",
    )
    parser.add_argument(
        "--npd-only",
        action="store_true",
        help="Sync NPDs only, exclude DPDs",
    )
    parser.add_argument(
        "--all-snapshots",
        action="store_true",
        help="Include historical snapshots, not just current",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be synced without copying",
    )
    parser.add_argument(
        "--run-sync-functions",
        action="store_true",
        help="Run configured post-sync functions after copying",
    )
    parser.add_argument(
        "--run-sync-only",
        action="store_true",
        help="Run configured sync functions without copying",
    )
    parser.add_argument(
        "--sync-function",
        metavar="LOCATION:NAME",
        help="Run this sync function for selected snapshots",
    )
    parser.add_argument(
        "--sync-function-init-args",
        metavar="JSON",
        help="Optional JSON kwargs for CLI sync function class",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete files at destination not in source",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output",
    )
    parser.add_argument(
        "--workers",
        "-j",
        type=int,
        default=8,
        metavar="N",
        help="Parallel upload/download workers (default: 8)",
    )


def _parse_cli_sync_function_config(
    sync_function: str | None,
    init_args_json: str | None,
) -> SyncFunctionConfig | None:
    """Parse command-line sync function configuration."""
    if init_args_json and not sync_function:
        raise ConfigurationError(
            "--sync-function-init-args requires --sync-function"
        )
    if not sync_function:
        return None

    location, name = parse_sync_function_spec(sync_function)
    init_args: dict[str, Any] = {}
    if init_args_json:
        try:
            parsed = json.loads(init_args_json)
        except json.JSONDecodeError as e:
            raise ConfigurationError(
                f"--sync-function-init-args must be valid JSON: {e}"
            ) from e
        if not isinstance(parsed, dict):
            raise ConfigurationError(
                "--sync-function-init-args must decode to a JSON object"
            )
        init_args = parsed

    return SyncFunctionConfig(
        location=location,
        name=name,
        init_args=init_args,
        context="Command-line sync function",
    )


_ETA_MIN_FILES = 3  # start showing ETA after this many files copied


@dataclass(frozen=True)
class _SelectedLocalSnapshot:
    """Local source snapshot selected by sync-cache filters."""

    dataset_name: str
    dataset_type: str
    suffix: str
    snapshot: dict[str, Any]

    def dest_dataset_name(self, rename_map: dict[str, str]) -> str:
        """Return destination dataset name after any sync rename."""
        return rename_map.get(self.dataset_name, self.dataset_name)


def _format_eta(elapsed: float, done: int, total: int) -> str:
    """Return a human-readable ETA string, or empty string if not enough data."""
    if done < _ETA_MIN_FILES or elapsed == 0:
        return ""
    secs = (total - done) / (done / elapsed)
    if secs < 60:
        return f"  ETA: {secs:.0f}s"
    elif secs < 3600:
        m, s = divmod(secs, 60)
        return f"  ETA: {m:.0f}m {s:.0f}s"
    else:
        h, remainder = divmod(secs, 3600)
        return f"  ETA: {h:.0f}h {remainder/60:.0f}m"


def _should_copy_file(source: Path, dest: Path) -> bool:
    """
    Check if a file should be copied (doesn't exist or differs).

    Uses size comparison first (fast), then checksum if sizes match.
    """
    if not dest.exists():
        return True
    # Quick check: size mismatch means definitely different
    if source.stat().st_size != dest.stat().st_size:
        return True
    # Size matches - compare checksums
    return get_file_hash(source) != get_file_hash(dest)


def _apply_rename(rel_path: Path, rename_map: dict[str, str]) -> Path:
    """Apply rename mapping to a relative path."""
    if not rename_map:
        return rel_path

    parts = list(rel_path.parts)
    for i, part in enumerate(parts):
        for old_name, new_name in rename_map.items():
            # Replace dataset name in directory names and file names
            if part == old_name:
                parts[i] = new_name
            elif part.startswith(f"{old_name}_"):
                parts[i] = part.replace(f"{old_name}_", f"{new_name}_", 1)
    return Path(*parts)


def _rel_parts(rel_path: str | Path) -> list[str]:
    """Return normalized relative path parts for local and GCS paths."""
    return [p for p in str(rel_path).replace("\\", "/").split("/") if p]


def _dataset_name_from_rel(rel_path: str | Path) -> str | None:
    """Return the cache dataset name for a relative cache path."""
    parts = _rel_parts(rel_path)
    if not parts or parts[0] in ("yaml", "code"):
        return None
    if parts[0] == "non-dated":
        return parts[1] if len(parts) > 1 else None
    return parts[0]


def _gcs_rel_snapshot_info(rel_path: str) -> tuple[str, bool, str] | None:
    """
    Return (dataset_name, is_npd, suffix) for a GCS cache blob relpath.

    DPD suffixes are encoded in metadata/data filenames. NPD directory
    snapshots encode the suffix in the snapshot directory name, while nested
    files usually have ordinary names like ``data.parquet``.
    """
    parts = _rel_parts(rel_path)
    if not parts or parts[0] in ("yaml", "code"):
        return None

    if parts[0] == "non-dated":
        if len(parts) < 3:
            return None
        suffix = extract_suffix_from_filename(parts[2])
        if suffix is None:
            return None
        return parts[1], True, suffix

    suffix = extract_suffix_from_filename(parts[-1])
    if suffix is None:
        return None
    return parts[0], False, suffix


def _is_gcs_dpd_metadata_rel(rel_path: str) -> bool:
    """Return True when rel_path is a DPD metadata pickle."""
    parts = _rel_parts(rel_path)
    return (
        len(parts) >= 3
        and parts[1] == "_meta_data"
        and parts[-1].endswith(".pkl.gz")
    )


def _is_gcs_dpd_provenance_rel(rel_path: str) -> bool:
    """Return True when rel_path is under a DPD provenance directory."""
    parts = _rel_parts(rel_path)
    return len(parts) >= 3 and parts[1] == "_provenance"


def _is_expected_gcs_dpd_provenance_rel(
    rel_path: str,
    dataset_name: str,
    suffix: str,
) -> bool:
    """Return True for the convention-named DPD provenance sidecar."""
    parts = _rel_parts(rel_path)
    # Provenance sync is intentionally convention-based. Only the standard
    # per-snapshot sidecar is copied; arbitrary files in _provenance/ are not.
    return (
        len(parts) == 3
        and parts[0] == dataset_name
        and parts[1] == "_provenance"
        and parts[2] == f"{dataset_name}_{suffix}.provenance.pkl.gz"
    )


def _select_snapshot_suffixes(
    available: dict[tuple[bool, str], set[str]],
    all_snapshots: bool,
    snapshot_suffixes: list[str] | None,
) -> dict[tuple[bool, str], set[str]]:
    """Select snapshot suffixes using the same latest/all/explicit policy."""
    requested = set(snapshot_suffixes or [])
    selected: dict[tuple[bool, str], set[str]] = {}

    for key, suffixes in available.items():
        if all_snapshots:
            selected[key] = set(suffixes)
        elif requested:
            selected[key] = suffixes & requested
        elif suffixes:
            selected[key] = {max(suffixes)}

    return selected


def _collect_gcs_sync_blobs(
    blob_urls: list[str],
    source: str,
    dataset_names: list[str] | None,
    ignore_dataset_names: list[str] | None,
    dpd_only: bool,
    npd_only: bool,
    all_snapshots: bool,
    snapshot_suffixes: list[str] | None,
    rename_map: dict[str, str] | None = None,
) -> list[tuple[str, str]]:
    """Collect GCS blobs selected for sync as (blob_url, relative_path)."""
    source_prefix = source.rstrip("/")
    available: dict[tuple[bool, str], set[str]] = {}
    candidates: list[tuple[str, str, str, bool, str]] = []
    rename_map = rename_map or {}

    for blob_url in blob_urls:
        rel = blob_url[len(source_prefix) :].lstrip("/")
        info = _gcs_rel_snapshot_info(rel)
        if info is None:
            continue

        name, is_npd, suffix = info
        if dataset_names and name not in dataset_names:
            continue
        if ignore_dataset_names and name in ignore_dataset_names:
            continue
        if dpd_only and is_npd:
            continue
        if npd_only and not is_npd:
            continue
        if (
            not is_npd
            and _is_gcs_dpd_provenance_rel(rel)
            and not _is_expected_gcs_dpd_provenance_rel(rel, name, suffix)
        ):
            continue

        candidates.append((blob_url, rel, name, is_npd, suffix))

        # DPD snapshots are defined by metadata files. NPD snapshots are
        # self-describing, so any blob under the snapshot contributes suffix.
        if is_npd or _is_gcs_dpd_metadata_rel(rel):
            available.setdefault((is_npd, name), set()).add(suffix)

    selected = _select_snapshot_suffixes(
        available=available,
        all_snapshots=all_snapshots,
        snapshot_suffixes=snapshot_suffixes,
    )

    return [
        (blob_url, _apply_rename(Path(rel), rename_map).as_posix())
        for blob_url, rel, name, is_npd, suffix in candidates
        if suffix in selected.get((is_npd, name), set())
    ]


def _select_local_snapshots(
    snapshots: list[dict],
    all_snapshots: bool,
    snapshot_suffixes: list[str] | None,
) -> list[dict]:
    """Select local snapshot records using latest/all/explicit policy."""
    if all_snapshots:
        return snapshots
    if snapshot_suffixes:
        return [s for s in snapshots if s["suffix"] in snapshot_suffixes]
    return [max(snapshots, key=lambda s: s["suffix"])]


def _collect_selected_local_snapshots(
    source_path: Path,
    dataset_names: list[str] | None,
    ignore_dataset_names: list[str] | None,
    dpd_only: bool,
    npd_only: bool,
    all_snapshots: bool,
    snapshot_suffixes: list[str] | None,
) -> list[_SelectedLocalSnapshot]:
    """Return local snapshots selected by sync-cache filters."""
    selected: list[_SelectedLocalSnapshot] = []

    if not npd_only:
        dpd_snapshots = _discover_dpd_snapshots(source_path, dataset_names)
        if ignore_dataset_names:
            dpd_snapshots = {
                k: v
                for k, v in dpd_snapshots.items()
                if k not in ignore_dataset_names
            }
        for name, snapshots in dpd_snapshots.items():
            if not snapshots:
                continue
            for snap in _select_local_snapshots(
                snapshots,
                all_snapshots,
                snapshot_suffixes,
            ):
                selected.append(
                    _SelectedLocalSnapshot(
                        dataset_name=name,
                        dataset_type="dpd",
                        suffix=snap["suffix"],
                        snapshot=snap,
                    )
                )

    if not dpd_only:
        npd_snapshots = _discover_npd_snapshots(source_path, dataset_names)
        if ignore_dataset_names:
            npd_snapshots = {
                k: v
                for k, v in npd_snapshots.items()
                if k not in ignore_dataset_names
            }
        for name, snapshots in npd_snapshots.items():
            if not snapshots:
                continue
            for snap in _select_local_snapshots(
                snapshots,
                all_snapshots,
                snapshot_suffixes,
            ):
                selected.append(
                    _SelectedLocalSnapshot(
                        dataset_name=name,
                        dataset_type="npd",
                        suffix=snap["suffix"],
                        snapshot=snap,
                    )
                )

    type_order = {"dpd": 0, "npd": 1}
    return sorted(
        selected,
        key=lambda item: (
            type_order[item.dataset_type],
            item.dataset_name,
            item.suffix,
        ),
    )


def _local_dpd_snapshot_files(snap: dict) -> list[Path]:
    """Return local files that belong to a DPD snapshot."""
    files = [snap["meta_file"]] + list(snap.get("files", []))
    metadata = SnapshotMetadata.from_pickle(snap["meta_file"])
    if metadata.provenance is None:
        return files

    # Provenance sync is intentionally convention-based. Even if metadata
    # points elsewhere, sync-cache only copies the standard snapshot sidecar.
    dataset_dir = snap["meta_file"].parent.parent
    provenance_path = (
        dataset_dir
        / "_provenance"
        / f"{metadata.name}_{metadata.suffix}.provenance.pkl.gz"
    )
    if provenance_path.exists():
        files.append(provenance_path)
    return files


def _run_sync_push(
    source: str,
    destination: str,
    dataset_names: list[str] | None,
    ignore_dataset_names: list[str] | None,
    dpd_only: bool,
    npd_only: bool,
    all_snapshots: bool,
    snapshot_suffixes: list[str] | None,
    dry_run: bool,
    delete: bool,
    verbose: bool,
    workers: int = 8,
    rename_map: dict[str, str] | None = None,
    run_sync_functions: bool = False,
    run_sync_only: bool = False,
    sync_function_config: SyncFunctionConfig | None = None,
) -> int:
    """Execute push sync operation."""
    source_path = Path(source)
    dest_path = Path(destination)
    rename_map = rename_map or {}

    if not source_path.exists():
        logger.error(f"Error: Source not found: {source}")
        return 1

    selected_snapshots = _collect_selected_local_snapshots(
        source_path=source_path,
        dataset_names=dataset_names,
        ignore_dataset_names=ignore_dataset_names,
        dpd_only=dpd_only,
        npd_only=npd_only,
        all_snapshots=all_snapshots,
        snapshot_suffixes=snapshot_suffixes,
    )
    pairs = _local_sync_pairs_from_selected(
        source_path,
        selected_snapshots,
        rename_map,
    )
    sync_function_targets = _sync_function_targets_from_selected(
        selected_snapshots,
        source,
        destination,
        rename_map,
    )

    if run_sync_only:
        _verify_destination_for_sync_only(destination, pairs)
        return _run_post_sync_if_requested(
            source_path=source_path,
            targets=sync_function_targets,
            sync_function_config=sync_function_config,
        )

    # Create destination if needed
    if not dry_run:
        dest_path.mkdir(parents=True, exist_ok=True)

    # --- Phase 1: discover all files, separate into to_copy / to_skip ---
    # Each entry: (src_file, dest_file, display_label, size)
    to_copy: list[tuple[Path, Path, str, int]] = []
    files_skipped = 0
    synced_paths: set[Path] = set()

    def _collect(
        src: Path,
        dst: Path,
        rel: Path,
        dest_rel: Path,
    ) -> None:
        nonlocal files_skipped
        synced_paths.add(dest_rel)
        if _should_copy_file(src, dst):
            label = f"{rel} -> {dest_rel}" if rename_map else str(rel)
            to_copy.append((src, dst, label, src.stat().st_size))
        else:
            files_skipped += 1

    for src, dest_rel_str in pairs:
        rel = src.relative_to(source_path)
        dest_rel = Path(dest_rel_str)
        _collect(src, dest_path / dest_rel, rel, dest_rel)

    # --- Phase 2: copy with progress ---
    total = len(to_copy)
    bytes_synced = 0
    start_time = time.perf_counter()
    w = len(str(total))

    if dry_run or total == 0:
        bytes_synced = sum(size for _, _, _, size in to_copy)
    else:
        done = 0
        lock = threading.Lock()

        def _copy_one(item: tuple) -> int:
            src, dst, _label, size = item
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            return size

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_copy_one, item): item for item in to_copy}
            for fut in as_completed(futures):
                src, dst, label, size = futures[fut]
                fut.result()  # re-raise any exception
                with lock:
                    done += 1
                    bytes_synced += size
                    if verbose:
                        eta = _format_eta(
                            time.perf_counter() - start_time, done, total
                        )
                        logger.info(f"  [{done:{w}}/{total}] {label}{eta}")

    # Handle --delete: remove files at destination not in source
    files_deleted = 0
    if delete and dest_path.exists():
        for dest_file in dest_path.rglob("*"):
            if not dest_file.is_file():
                continue
            rel = dest_file.relative_to(dest_path)
            if rel.parts and rel.parts[0] in ("yaml", "code"):
                continue
            if (
                ignore_dataset_names
                and _dataset_name_from_rel(rel) in ignore_dataset_names
            ):
                continue
            if rel not in synced_paths:
                files_deleted += 1
                if verbose:
                    logger.info(f"  DELETE {rel}")
                if not dry_run:
                    dest_file.unlink()

    # Summary
    if dry_run:
        delete_msg = f", would delete {files_deleted}" if delete else ""
        logger.info(f"Would sync {total} files{delete_msg}")
    else:
        skip_msg = (
            f", skipped {files_skipped} unchanged" if files_skipped else ""
        )
        delete_msg = f", deleted {files_deleted}" if files_deleted else ""
        logger.info(
            f"Synced {total} files ({format_size(bytes_synced)})"
            f"{skip_msg}{delete_msg}"
        )

    if run_sync_functions or sync_function_config is not None:
        _run_post_sync_if_requested(
            source_path=source_path,
            targets=sync_function_targets,
            sync_function_config=sync_function_config,
        )

    return 0


def _run_sync_pull(
    source: str,
    destination: str,
    dataset_names: list[str] | None,
    ignore_dataset_names: list[str] | None,
    dpd_only: bool,
    npd_only: bool,
    all_snapshots: bool,
    snapshot_suffixes: list[str] | None,
    dry_run: bool,
    delete: bool,
    daemon: bool,
    update_interval: int,
    verbose: bool,
    workers: int = 8,
    rename_map: dict[str, str] | None = None,
    run_sync_functions: bool = False,
    run_sync_only: bool = False,
    sync_function_config: SyncFunctionConfig | None = None,
) -> int:
    """Execute pull sync operation."""
    while True:
        result = _run_sync_push(
            source=source,
            destination=destination,
            dataset_names=dataset_names,
            ignore_dataset_names=ignore_dataset_names,
            dpd_only=dpd_only,
            npd_only=npd_only,
            all_snapshots=all_snapshots,
            snapshot_suffixes=snapshot_suffixes,
            dry_run=dry_run,
            delete=delete,
            verbose=verbose,
            workers=workers,
            rename_map=rename_map,
        )

        if not daemon:
            return result

        if verbose:
            logger.info(f"Sleeping {update_interval} seconds...")
        time.sleep(update_interval)


def _collect_local_sync_files(
    source_path: Path,
    dataset_names: list[str] | None,
    ignore_dataset_names: list[str] | None,
    dpd_only: bool,
    npd_only: bool,
    all_snapshots: bool,
    snapshot_suffixes: list[str] | None,
    rename_map: dict[str, str],
) -> list[tuple[Path, str]]:
    """
    Collect (local_file, relative_path_str) pairs from a local source cache.
    The relative_path_str has rename_map applied and uses forward slashes.
    """
    selected_snapshots = _collect_selected_local_snapshots(
        source_path=source_path,
        dataset_names=dataset_names,
        ignore_dataset_names=ignore_dataset_names,
        dpd_only=dpd_only,
        npd_only=npd_only,
        all_snapshots=all_snapshots,
        snapshot_suffixes=snapshot_suffixes,
    )
    return _local_sync_pairs_from_selected(
        source_path,
        selected_snapshots,
        rename_map,
    )


def _local_sync_pairs_from_selected(
    source_path: Path,
    selected_snapshots: list[_SelectedLocalSnapshot],
    rename_map: dict[str, str],
) -> list[tuple[Path, str]]:
    """Return selected local sync files without rediscovering snapshots."""
    pairs: list[tuple[Path, str]] = []
    for selected in selected_snapshots:
        if selected.dataset_type == "dpd":
            for f in _local_dpd_snapshot_files(selected.snapshot):
                rel = _apply_rename(
                    f.relative_to(source_path),
                    rename_map,
                )
                pairs.append((f, str(rel).replace("\\", "/")))
        else:
            item = selected.snapshot["path"]
            if item.is_file():
                rel = _apply_rename(
                    item.relative_to(source_path),
                    rename_map,
                )
                pairs.append((item, str(rel).replace("\\", "/")))
            else:
                for f in item.rglob("*"):
                    if f.is_file():
                        rel = _apply_rename(
                            f.relative_to(source_path),
                            rename_map,
                        )
                        pairs.append((f, str(rel).replace("\\", "/")))

    return pairs


def _sync_function_targets_from_selected(
    selected_snapshots: list[_SelectedLocalSnapshot],
    source: str,
    destination: str,
    rename_map: dict[str, str],
) -> list[SyncFunctionTarget]:
    """Return sync-function targets without rediscovering snapshots."""
    return [
        SyncFunctionTarget(
            source_dataset_name=selected.dataset_name,
            dest_dataset_name=selected.dest_dataset_name(rename_map),
            dataset_type=selected.dataset_type,
            snapshot_id=selected.suffix,
            source_location=source,
            dest_location=destination,
        )
        for selected in selected_snapshots
    ]


def _run_selected_sync_functions(
    source_path: Path,
    targets: list[SyncFunctionTarget],
    sync_function_config: SyncFunctionConfig | None,
) -> int:
    """Load and run sync functions for selected targets."""
    if not targets:
        logger.info("0 sync functions run")
        return 0

    functions_run = 0
    if sync_function_config is not None:
        sync_function = load_sync_function(sync_function_config, source_path)
        for target in targets:
            logger.info(
                "Running sync function for "
                f"{target.dataset_type} {target.source_dataset_name} "
                f"{target.snapshot_id}"
            )
            # Exceptions propagate immediately: later sync functions do not
            # run, and files copied before this phase are not rolled back.
            run_sync_function(sync_function, target)
            functions_run += 1
        logger.info(f"{functions_run} sync functions run")
        return functions_run

    yaml_configs = load_all_configs(source_path)
    loaded_functions: dict[str, Any] = {}
    for target in targets:
        dataset_config = yaml_configs.get(target.source_dataset_name)
        if dataset_config is None or not dataset_config.sync_function_name:
            logger.warning(
                "No sync function configured for "
                f"{target.dataset_type} {target.source_dataset_name}; "
                "skipping"
            )
            continue

        if target.source_dataset_name not in loaded_functions:
            config = SyncFunctionConfig(
                location=dataset_config.sync_function_location,
                name=dataset_config.sync_function_name,
                init_args=dataset_config.sync_function_init_args,
                context=(
                    f"Dataset '{target.source_dataset_name}' sync function"
                ),
            )
            loaded_functions[target.source_dataset_name] = load_sync_function(
                config,
                source_path,
            )

        logger.info(
            "Running sync function for "
            f"{target.dataset_type} {target.source_dataset_name} "
            f"{target.snapshot_id}"
        )
        # Exceptions propagate immediately: later sync functions do not run,
        # and files copied before this phase are not rolled back.
        run_sync_function(
            loaded_functions[target.source_dataset_name],
            target,
        )
        functions_run += 1

    if functions_run == 0:
        logger.warning(
            "No datasets with configured sync functions found. Check that "
            "sync_function_name is set in YAML for the target datasets."
        )
    logger.info(f"{functions_run} sync functions run")
    return functions_run


def _verify_local_destination_files(
    destination: str,
    pairs: list[tuple[Path, str]],
) -> None:
    """Verify selected snapshot files exist at a local destination."""
    dest_path = Path(destination)
    missing = [
        rel for _source_file, rel in pairs if not (dest_path / rel).exists()
    ]
    if missing:
        raise SyncError(
            "Destination is missing selected snapshot files: "
            + ", ".join(missing[:5])
            + (" ..." if len(missing) > 5 else "")
        )


def _verify_gcs_destination_files(
    destination: str,
    pairs: list[tuple[Path, str]],
) -> None:
    """Verify selected snapshot files exist at a GCS destination."""
    from ionbus_parquet_cache.gcs_utils import gcs_exists, gcs_join

    missing = []
    for _source_file, rel in pairs:
        dest_url = gcs_join(destination, rel)
        if not gcs_exists(dest_url):
            missing.append(rel)
    if missing:
        raise SyncError(
            "Destination is missing selected snapshot files: "
            + ", ".join(missing[:5])
            + (" ..." if len(missing) > 5 else "")
        )


def _run_post_sync_if_requested(
    source_path: Path,
    targets: list[SyncFunctionTarget],
    sync_function_config: SyncFunctionConfig | None,
) -> int:
    """Run configured or command-line post-sync functions."""
    _run_selected_sync_functions(
        source_path,
        targets,
        sync_function_config,
    )
    return 0


def _selected_local_sync_pairs(
    source_path: Path,
    dataset_names: list[str] | None,
    ignore_dataset_names: list[str] | None,
    dpd_only: bool,
    npd_only: bool,
    all_snapshots: bool,
    snapshot_suffixes: list[str] | None,
    rename_map: dict[str, str],
) -> list[tuple[Path, str]]:
    """Collect selected local sync files for copy or verification."""
    return _collect_local_sync_files(
        source_path=source_path,
        dataset_names=dataset_names,
        ignore_dataset_names=ignore_dataset_names,
        dpd_only=dpd_only,
        npd_only=npd_only,
        all_snapshots=all_snapshots,
        snapshot_suffixes=snapshot_suffixes,
        rename_map=rename_map,
    )


def _verify_destination_for_sync_only(
    destination: str,
    pairs: list[tuple[Path, str]],
) -> None:
    """Verify destination snapshot files before --run-sync-only hooks."""
    if destination.startswith("gs://"):
        _verify_gcs_destination_files(destination, pairs)
    else:
        _verify_local_destination_files(destination, pairs)


def _run_sync_push_gcs(
    source: str,
    destination: str,
    dataset_names: list[str] | None,
    ignore_dataset_names: list[str] | None,
    dpd_only: bool,
    npd_only: bool,
    all_snapshots: bool,
    snapshot_suffixes: list[str] | None,
    dry_run: bool,
    verbose: bool,
    workers: int = 8,
    rename_map: dict[str, str] | None = None,
    run_sync_functions: bool = False,
    run_sync_only: bool = False,
    sync_function_config: SyncFunctionConfig | None = None,
) -> int:
    """Push a local cache to GCS, or sync between two GCS locations."""
    from ionbus_parquet_cache.gcs_utils import (
        gcs_find,
        gcs_join,
        gcs_download,
        gcs_upload,
        is_gcs_path,
        should_download,
        should_upload,
    )
    from ionbus_utils.file_utils import format_size

    rename_map = rename_map or {}
    src_is_gcs = is_gcs_path(source)
    dst_is_gcs = is_gcs_path(destination)
    sync_functions_requested = (
        run_sync_functions
        or run_sync_only
        or sync_function_config is not None
    )
    if sync_functions_requested and src_is_gcs:
        raise SyncError("sync functions require a local source cache")

    from ionbus_parquet_cache.gcs_utils import gcs_exists, gcs_size

    files_skipped = 0

    # --- Phase 1: collect files that need copying ---
    # Each entry: (src_blob_or_path, dest_url_or_path, label, size)
    to_copy: list[tuple[object, object, str, int]] = []

    if src_is_gcs:
        pairs = _collect_gcs_sync_blobs(
            gcs_find(source),
            source,
            dataset_names,
            ignore_dataset_names,
            dpd_only,
            npd_only,
            all_snapshots,
            snapshot_suffixes,
            rename_map,
        )
        if dst_is_gcs:
            for blob_url, rel in pairs:
                dest_url = gcs_join(destination, rel)
                src_size = gcs_size(blob_url)
                if gcs_exists(dest_url) and gcs_size(dest_url) == src_size:
                    files_skipped += 1
                    continue
                to_copy.append((blob_url, dest_url, rel, src_size))
        else:
            dest_path = Path(destination)
            for blob_url, rel in pairs:
                local_file = dest_path / rel
                if not should_download(blob_url, local_file):
                    files_skipped += 1
                    continue
                to_copy.append(
                    (blob_url, local_file, rel, gcs_size(blob_url))
                )

    elif dst_is_gcs:
        source_path = Path(source)
        if not source_path.exists():
            logger.error(f"Error: Source not found: {source}")
            return 1
        selected_snapshots = _collect_selected_local_snapshots(
            source_path=source_path,
            dataset_names=dataset_names,
            ignore_dataset_names=ignore_dataset_names,
            dpd_only=dpd_only,
            npd_only=npd_only,
            all_snapshots=all_snapshots,
            snapshot_suffixes=snapshot_suffixes,
        )
        pairs = _local_sync_pairs_from_selected(
            source_path,
            selected_snapshots,
            rename_map,
        )
        sync_function_targets = _sync_function_targets_from_selected(
            selected_snapshots,
            source,
            destination,
            rename_map,
        )
        if run_sync_only:
            _verify_destination_for_sync_only(destination, pairs)
            return _run_post_sync_if_requested(
                source_path=source_path,
                targets=sync_function_targets,
                sync_function_config=sync_function_config,
            )
        for local_file, rel_str in pairs:
            dest_url = gcs_join(destination, rel_str)
            if not should_upload(local_file, dest_url):
                files_skipped += 1
                continue
            to_copy.append(
                (local_file, dest_url, rel_str, local_file.stat().st_size)
            )

    # --- Phase 2: copy with progress ---
    total = len(to_copy)
    bytes_synced = 0
    start_time = time.perf_counter()
    w = len(str(total))

    if dry_run or total == 0:
        bytes_synced = sum(size for _, _, _, size in to_copy)
    else:
        done = 0
        lock = threading.Lock()

        def _copy_one_gcs(item: tuple) -> int:
            src, dst, _label, size = item
            if src_is_gcs and dst_is_gcs:
                with tempfile.NamedTemporaryFile(delete=False) as tmp:
                    tmp_path = Path(tmp.name)
                try:
                    gcs_download(src, tmp_path)
                    gcs_upload(tmp_path, dst)
                finally:
                    tmp_path.unlink(missing_ok=True)
            elif not src_is_gcs and dst_is_gcs:
                gcs_upload(src, dst)
            else:
                gcs_download(src, dst)
            return size

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_copy_one_gcs, item): item for item in to_copy
            }
            for fut in as_completed(futures):
                src, dst, label, size = futures[fut]
                fut.result()  # re-raise any exception
                with lock:
                    done += 1
                    bytes_synced += size
                    if verbose:
                        eta = _format_eta(
                            time.perf_counter() - start_time, done, total
                        )
                        logger.info(f"  [{done:{w}}/{total}] {label}{eta}")

    if dry_run:
        logger.info(f"Would sync {total} files")
    else:
        skip_msg = (
            f", skipped {files_skipped} unchanged" if files_skipped else ""
        )
        logger.info(
            f"Synced {total} files ({format_size(bytes_synced)}){skip_msg}"
        )

    if sync_functions_requested:
        source_path = Path(source)
        _run_post_sync_if_requested(
            source_path=source_path,
            targets=sync_function_targets,
            sync_function_config=sync_function_config,
        )

    return 0


def _run_sync_pull_gcs(
    source: str,
    destination: str,
    dataset_names: list[str] | None,
    ignore_dataset_names: list[str] | None,
    dpd_only: bool,
    npd_only: bool,
    all_snapshots: bool,
    snapshot_suffixes: list[str] | None,
    dry_run: bool,
    verbose: bool,
    workers: int = 8,
    daemon: bool = False,
    update_interval: int = 60,
    rename_map: dict[str, str] | None = None,
) -> int:
    """Pull from GCS to local (or GCS → GCS), with optional daemon mode."""
    while True:
        result = _run_sync_push_gcs(
            source=source,
            destination=destination,
            dataset_names=dataset_names,
            ignore_dataset_names=ignore_dataset_names,
            dpd_only=dpd_only,
            npd_only=npd_only,
            all_snapshots=all_snapshots,
            snapshot_suffixes=snapshot_suffixes,
            dry_run=dry_run,
            verbose=verbose,
            workers=workers,
            rename_map=rename_map,
        )
        if not daemon:
            return result
        if verbose:
            logger.info(f"Sleeping {update_interval} seconds...")
        time.sleep(update_interval)


def main() -> None:
    """Entry point for sync-cache command."""
    sys.exit(sync_cache_main())


if __name__ == "__main__":
    main()
