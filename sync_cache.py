"""
Command-line tool for syncing DatedParquetDataset caches.

Entry point: sync-cache
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

from ionbus_utils.file_utils import format_size, get_file_hash

# Import discovery functions from cleanup module
from ionbus_parquet_cache.cleanup_cache import (
    _discover_dpd_snapshots,
    _discover_npd_snapshots,
)


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
    push_parser.add_argument("destination", help="Destination path (local or s3://)")
    _add_sync_options(push_parser)

    # Pull subcommand
    pull_parser = subparsers.add_parser(
        "pull", help="Pull from source to local cache"
    )
    pull_parser.add_argument("source", help="Source path (local or s3://)")
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
    mode_count = sum([
        bool(parsed.dataset),
        parsed.dpd_only,
        parsed.npd_only,
    ])
    if mode_count > 1:
        print(
            "Error: --dataset, --dpd-only, and --npd-only are mutually exclusive",
            file=sys.stderr,
        )
        return 1

    # Parse rename option
    rename_map: dict[str, str] = {}
    if parsed.rename:
        if ":" not in parsed.rename:
            print(
                "Error: --rename must be in format 'source_name:target_name'",
                file=sys.stderr,
            )
            return 1
        old_name, new_name = parsed.rename.split(":", 1)
        rename_map[old_name] = new_name
        # If renaming, must also filter to that dataset
        if not parsed.dataset:
            parsed.dataset = [old_name]

    # Check for S3 paths
    if parsed.source.startswith("s3://") or parsed.destination.startswith("s3://"):
        print("Error: S3 sync is not yet implemented", file=sys.stderr)
        return 1

    # Run sync
    try:
        if parsed.command == "push":
            return _run_sync_push(
                source=parsed.source,
                destination=parsed.destination,
                dataset_names=parsed.dataset,
                dpd_only=parsed.dpd_only,
                npd_only=parsed.npd_only,
                all_snapshots=parsed.all_snapshots,
                dry_run=parsed.dry_run,
                delete=parsed.delete,
                verbose=parsed.verbose,
                rename_map=rename_map,
            )
        else:  # pull
            return _run_sync_pull(
                source=parsed.source,
                destination=parsed.destination,
                dataset_names=parsed.dataset,
                dpd_only=parsed.dpd_only,
                npd_only=parsed.npd_only,
                all_snapshots=parsed.all_snapshots,
                dry_run=parsed.dry_run,
                delete=parsed.delete,
                daemon=parsed.daemon,
                update_interval=parsed.update_interval,
                verbose=parsed.verbose,
                rename_map=rename_map,
            )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def _add_sync_options(parser: argparse.ArgumentParser) -> None:
    """Add common sync options to a parser."""
    parser.add_argument(
        "--dataset",
        nargs="+",
        metavar="NAME",
        help="Sync only these datasets",
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
        "--delete",
        action="store_true",
        help="Delete files at destination not in source",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output",
    )


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


def _run_sync_push(
    source: str,
    destination: str,
    dataset_names: list[str] | None,
    dpd_only: bool,
    npd_only: bool,
    all_snapshots: bool,
    dry_run: bool,
    delete: bool,
    verbose: bool,
    rename_map: dict[str, str] | None = None,
) -> int:
    """Execute push sync operation."""
    source_path = Path(source)
    dest_path = Path(destination)
    rename_map = rename_map or {}

    if not source_path.exists():
        print(f"Error: Source not found: {source}", file=sys.stderr)
        return 1

    # Create destination if needed
    if not dry_run:
        dest_path.mkdir(parents=True, exist_ok=True)

    # Discover and sync DPDs
    files_synced = 0
    files_skipped = 0
    bytes_synced = 0
    synced_paths: set[Path] = set()  # Track synced files for --delete

    if not npd_only:
        dpd_snapshots = _discover_dpd_snapshots(source_path, dataset_names)
        for name, snapshots in dpd_snapshots.items():
            if not snapshots:
                continue

            # Get current snapshot (or all if --all-snapshots)
            if all_snapshots:
                target_snapshots = snapshots
            else:
                target_snapshots = [max(snapshots, key=lambda s: s["suffix"])]

            for snap in target_snapshots:
                # Sync metadata file
                meta_file = snap["meta_file"]
                rel_path = meta_file.relative_to(source_path)
                dest_rel_path = _apply_rename(rel_path, rename_map)
                dest_file = dest_path / dest_rel_path

                synced_paths.add(dest_rel_path)
                # Check if file needs copying
                if _should_copy_file(meta_file, dest_file):
                    files_synced += 1
                    bytes_synced += meta_file.stat().st_size
                    if verbose:
                        if rename_map:
                            print(f"  {rel_path} -> {dest_rel_path}")
                        else:
                            print(f"  {rel_path}")
                    if not dry_run:
                        dest_file.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(meta_file, dest_file)
                else:
                    files_skipped += 1

                # Sync data files
                for data_file in snap.get("files", []):
                    rel_path = data_file.relative_to(source_path)
                    dest_rel_path = _apply_rename(rel_path, rename_map)
                    dest_file = dest_path / dest_rel_path
                    synced_paths.add(dest_rel_path)

                    if _should_copy_file(data_file, dest_file):
                        files_synced += 1
                        bytes_synced += data_file.stat().st_size
                        if verbose:
                            if rename_map:
                                print(f"  {rel_path} -> {dest_rel_path}")
                            else:
                                print(f"  {rel_path}")
                        if not dry_run:
                            dest_file.parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy2(data_file, dest_file)
                    else:
                        files_skipped += 1

    # Sync NPDs
    if not dpd_only:
        npd_snapshots = _discover_npd_snapshots(source_path, dataset_names)
        for name, snapshots in npd_snapshots.items():
            if not snapshots:
                continue

            if all_snapshots:
                target_snapshots = snapshots
            else:
                target_snapshots = [max(snapshots, key=lambda s: s["suffix"])]

            for snap in target_snapshots:
                item = snap["path"]
                rel_path = item.relative_to(source_path)
                dest_rel_path = _apply_rename(rel_path, rename_map)
                dest_item = dest_path / dest_rel_path

                if item.is_file():
                    synced_paths.add(dest_rel_path)
                else:
                    # For directories, track all files inside
                    for f in item.rglob("*"):
                        if f.is_file():
                            f_rel = f.relative_to(source_path)
                            synced_paths.add(_apply_rename(f_rel, rename_map))

                if item.is_file():
                    if _should_copy_file(item, dest_item):
                        files_synced += 1
                        bytes_synced += item.stat().st_size
                        if verbose:
                            if rename_map:
                                print(f"  {rel_path} -> {dest_rel_path}")
                            else:
                                print(f"  {rel_path}")
                        if not dry_run:
                            dest_item.parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy2(item, dest_item)
                    else:
                        files_skipped += 1
                else:
                    # For directories, sync file by file with checksum check
                    for src_file in item.rglob("*"):
                        if not src_file.is_file():
                            continue
                        rel_file = src_file.relative_to(item)
                        dest_file = dest_item / rel_file
                        if _should_copy_file(src_file, dest_file):
                            files_synced += 1
                            bytes_synced += src_file.stat().st_size
                            if verbose:
                                src_full = rel_path / rel_file
                                dest_full = dest_rel_path / rel_file
                                if rename_map:
                                    print(f"  {src_full} -> {dest_full}")
                                else:
                                    print(f"  {src_full}")
                            if not dry_run:
                                dest_file.parent.mkdir(parents=True, exist_ok=True)
                                shutil.copy2(src_file, dest_file)
                        else:
                            files_skipped += 1

    # Handle --delete: remove files at destination not in source
    files_deleted = 0
    if delete and dest_path.exists():
        # Find all data files at destination (parquet and pkl.gz)
        for dest_file in dest_path.rglob("*"):
            if not dest_file.is_file():
                continue
            # Skip yaml/ and code/ directories
            rel = dest_file.relative_to(dest_path)
            if rel.parts and rel.parts[0] in ("yaml", "code"):
                continue
            # Check if this file was synced
            if rel not in synced_paths:
                files_deleted += 1
                if verbose:
                    print(f"  DELETE {rel}")
                if not dry_run:
                    dest_file.unlink()

    # Summary
    if dry_run:
        delete_msg = f", would delete {files_deleted}" if delete else ""
        print(f"Would sync {files_synced} files{delete_msg}")
    else:
        skip_msg = f", skipped {files_skipped} unchanged" if files_skipped else ""
        delete_msg = f", deleted {files_deleted}" if files_deleted else ""
        print(
            f"Synced {files_synced} files ({format_size(bytes_synced)})"
            f"{skip_msg}{delete_msg}"
        )

    return 0


def _run_sync_pull(
    source: str,
    destination: str,
    dataset_names: list[str] | None,
    dpd_only: bool,
    npd_only: bool,
    all_snapshots: bool,
    dry_run: bool,
    delete: bool,
    daemon: bool,
    update_interval: int,
    verbose: bool,
    rename_map: dict[str, str] | None = None,
) -> int:
    """Execute pull sync operation."""
    while True:
        result = _run_sync_push(
            source=source,
            destination=destination,
            dataset_names=dataset_names,
            dpd_only=dpd_only,
            npd_only=npd_only,
            all_snapshots=all_snapshots,
            dry_run=dry_run,
            delete=delete,
            verbose=verbose,
            rename_map=rename_map,
        )

        if not daemon:
            return result

        if verbose:
            print(f"Sleeping {update_interval} seconds...")
        time.sleep(update_interval)


def main() -> None:
    """Entry point for sync-cache command."""
    sys.exit(sync_cache_main())


if __name__ == "__main__":
    main()
