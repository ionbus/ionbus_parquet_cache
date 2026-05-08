"""
Command-line tool for cleaning up DatedParquetDataset caches.

Entry point: cleanup-cache
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from ionbus_utils.date_utils import date_partition_range
from ionbus_utils.file_utils import format_size
from ionbus_utils.logging_utils import logger

from ionbus_parquet_cache.dated_dataset import (
    DatedParquetDataset,
    FileMetadata,
    SnapshotMetadata,
)
from ionbus_parquet_cache.snapshot import (
    extract_suffix_from_filename,
    generate_snapshot_suffix,
    parse_snapshot_suffix,
)


def cleanup_cache_main(args: list[str] | None = None) -> int:
    """
    Main entry point for cleanup-cache command.

    Manages disk usage by analyzing old snapshots and generating
    cleanup scripts.

    Returns:
        Exit code (0 for success, non-zero for errors).
    """
    parser = argparse.ArgumentParser(
        prog="cleanup-cache",
        description="Analyze and cleanup old snapshots.",
    )
    parser.add_argument(
        "cache_dir",
        type=str,
        help="Path to the cache directory",
    )
    parser.add_argument(
        "--dataset",
        nargs="+",
        metavar="NAME",
        help="Analyze only these datasets",
    )
    parser.add_argument(
        "--dpd-only",
        action="store_true",
        help="Analyze DPDs only, exclude NPDs",
    )
    parser.add_argument(
        "--npd-only",
        action="store_true",
        help="Analyze NPDs only, exclude DPDs",
    )
    parser.add_argument(
        "--older-than",
        type=int,
        metavar="DAYS",
        help="Target snapshots older than N days",
    )
    parser.add_argument(
        "--keep-last",
        type=int,
        metavar="N",
        help="Keep only the N most recent snapshots per dataset",
    )
    parser.add_argument(
        "--snapshot",
        type=str,
        metavar="SUFFIX",
        help="Target a specific snapshot suffix",
    )
    parser.add_argument(
        "--find-orphans",
        action="store_true",
        help="Find orphaned files not in any snapshot",
    )
    parser.add_argument(
        "--keep-days",
        type=int,
        metavar="N",
        help="TRIM: Keep only N business days of data (dangerous)",
    )
    parser.add_argument(
        "--before-date",
        type=str,
        metavar="DATE",
        help="TRIM: Remove data before this date (dangerous)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output",
    )

    parsed = parser.parse_args(args)

    # Validate mutually exclusive options
    mode_count = sum(
        [
            bool(parsed.dataset),
            parsed.dpd_only,
            parsed.npd_only,
        ]
    )
    if mode_count > 1:
        logger.error(
            "Error: --dataset, --dpd-only, and --npd-only are mutually exclusive"
        )
        return 1

    # Run cleanup analysis
    try:
        return _run_cleanup(
            cache_dir=parsed.cache_dir,
            dataset_names=parsed.dataset,
            dpd_only=parsed.dpd_only,
            npd_only=parsed.npd_only,
            older_than_days=parsed.older_than,
            keep_last=parsed.keep_last,
            snapshot_suffix=parsed.snapshot,
            find_orphans=parsed.find_orphans,
            keep_days=parsed.keep_days,
            before_date=parsed.before_date,
            verbose=parsed.verbose,
        )
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


def _run_cleanup(
    cache_dir: str,
    dataset_names: list[str] | None,
    dpd_only: bool,
    npd_only: bool,
    older_than_days: int | None,
    keep_last: int | None,
    snapshot_suffix: str | None,
    find_orphans: bool,
    keep_days: int | None,
    before_date: str | None,
    verbose: bool,
) -> int:
    """Execute the cleanup analysis."""
    cache_path = Path(cache_dir)
    if not cache_path.exists():
        logger.error(f"Error: Cache directory not found: {cache_dir}")
        return 1

    # Trim operations are dangerous - run separately
    if keep_days is not None or before_date is not None:
        return _run_trim(
            cache_path=cache_path,
            dataset_names=dataset_names,
            keep_days=keep_days,
            before_date=before_date,
            verbose=verbose,
        )

    # Discover datasets
    dpd_snapshots: dict[str, list[dict[str, Any]]] = {}
    npd_snapshots: dict[str, list[dict[str, Any]]] = {}

    if not npd_only:
        dpd_snapshots = _discover_dpd_snapshots(cache_path, dataset_names)

    if not dpd_only:
        npd_snapshots = _discover_npd_snapshots(cache_path, dataset_names)

    # Calculate cutoff date if --older-than specified
    cutoff_date = None
    if older_than_days is not None:
        cutoff_date = dt.datetime.now() - dt.timedelta(days=older_than_days)

    # Analyze and collect files to delete
    files_to_delete: list[Path] = []
    total_size = 0

    # Process DPDs
    for name, snapshots in dpd_snapshots.items():
        if len(snapshots) <= 1:
            logger.info(f"{name}: only current snapshot, nothing to clean")
            continue

        # Sort by suffix (oldest first)
        snapshots = sorted(snapshots, key=lambda s: s["suffix"])
        current = snapshots[-1]

        logger.info(f"{name}:")
        for snap in snapshots:
            is_current = snap == current
            suffix = snap["suffix"]

            # Check if this snapshot should be deleted
            should_delete = False

            if snapshot_suffix and suffix == snapshot_suffix:
                should_delete = True
            elif (
                keep_last
                and snapshots.index(snap) < len(snapshots) - keep_last
            ):
                should_delete = True
            elif cutoff_date and snap.get("created_at"):
                if snap["created_at"] < cutoff_date:
                    should_delete = True

            if is_current:
                logger.info(f"  {suffix} <- current snapshot")
            elif should_delete:
                size = snap.get("size", 0)
                total_size += size
                size_str = format_size(size)
                logger.info(
                    f"  {suffix}: {len(snap.get('files', []))} files, "
                    f"{size_str} reclaimable"
                )
                files_to_delete.extend(snap.get("files", []))
                files_to_delete.extend(snap.get("sidecars", []))
                # Also delete metadata file
                meta_file = snap.get("meta_file")
                if meta_file:
                    files_to_delete.append(meta_file)
            else:
                logger.info(f"  {suffix}: {len(snap.get('files', []))} files")

    # Process NPDs
    for name, snapshots in npd_snapshots.items():
        if len(snapshots) <= 1:
            logger.info(
                f"{name} (NPD): only current snapshot, nothing to clean"
            )
            continue

        snapshots = sorted(snapshots, key=lambda s: s["suffix"])
        current = snapshots[-1]

        logger.info(f"{name} (NPD):")
        for snap in snapshots:
            is_current = snap == current
            suffix = snap["suffix"]

            should_delete = False
            if snapshot_suffix and suffix == snapshot_suffix:
                should_delete = True
            elif (
                keep_last
                and snapshots.index(snap) < len(snapshots) - keep_last
            ):
                should_delete = True
            elif cutoff_date and snap.get("created_at"):
                if snap["created_at"] < cutoff_date:
                    should_delete = True

            if is_current:
                logger.info(f"  {suffix} <- current snapshot")
            elif should_delete:
                size = snap.get("size", 0)
                total_size += size
                size_str = format_size(size)
                logger.info(f"  {suffix}: {size_str} reclaimable")
                if snap.get("path"):
                    files_to_delete.append(snap["path"])
                files_to_delete.extend(snap.get("sidecars", []))
            else:
                size_str = format_size(snap.get("size", 0))
                logger.info(f"  {suffix}: {size_str}")

    # Summary
    if files_to_delete:
        logger.info(f"\nTotal reclaimable: {format_size(total_size)}")

        # Generate cleanup script
        if older_than_days or keep_last or snapshot_suffix:
            script_path = _generate_cleanup_script(
                cache_path, files_to_delete
            )
            logger.info(
                f"\nWrote cleanup script: {script_path}\n"
                "Review the script, then run it to delete files."
            )
    else:
        logger.info("\nNo files to clean up.")

    # Find orphans if requested
    if find_orphans:
        orphans = _find_orphaned_files(
            cache_path, dpd_snapshots, npd_snapshots
        )
        if orphans:
            total_size = sum(f.stat().st_size for f in orphans if f.exists())
            logger.info(
                f"\nOrphaned files ({len(orphans)}, {format_size(total_size)}):"
            )
            for f in orphans[:10]:
                logger.info(f"  {f}")
            if len(orphans) > 10:
                logger.info(f"  ... and {len(orphans) - 10} more")

            # Generate cleanup script
            script_path = _generate_cleanup_script(cache_path, orphans)
            logger.info(
                f"\nWrote orphan cleanup script: {script_path}\n"
                "Review the script, then run it to delete orphaned files."
            )
        else:
            logger.info("\nNo orphaned files found.")

    return 0


def _run_trim(
    cache_path: Path,
    dataset_names: list[str] | None,
    keep_days: int | None,
    before_date: str | None,
    verbose: bool,
) -> int:
    """
    Execute trim operation on DPDs.

    Trims data older than the specified threshold and generates
    delete/undo scripts.
    """
    logger.error(
        "WARNING: Trim operations immediately modify data.\n"
        "Ensure all readers are stopped before proceeding."
    )

    # Calculate cutoff date
    if keep_days is not None:
        # Use pandas to get N business days ago
        today = pd.Timestamp.today().normalize()
        cutoff_ts = today - pd.offsets.BDay(keep_days)
        cutoff_date = cutoff_ts.date()
        logger.info(
            f"Trimming data before {cutoff_date} ({keep_days} business days)"
        )
    elif before_date is not None:
        cutoff_date = dt.date.fromisoformat(before_date)
        logger.info(f"Trimming data before {cutoff_date}")
    else:
        logger.error("Error: Must specify --keep-days or --before-date")
        return 1

    # Find DPDs to trim
    trimmed_files: list[Path] = []
    trimmed_meta_files: list[Path] = []
    new_meta_files: list[Path] = []  # Track new metadata files for undo
    datasets_trimmed = 0

    for item in cache_path.iterdir():
        if not item.is_dir():
            continue
        if item.name in ("yaml", "code", "non-dated"):
            continue

        meta_dir = item / "_meta_data"
        if not meta_dir.exists():
            continue

        name = item.name
        if dataset_names and name not in dataset_names:
            continue

        # Load the DPD
        try:
            dpd = DatedParquetDataset(cache_dir=cache_path, name=name)
            if dpd.current_suffix is None:
                dpd.current_suffix = dpd._discover_current_suffix()
            if dpd.current_suffix is None:
                continue

            metadata = dpd._load_metadata()
        except Exception as e:
            logger.error(f"  {name}: ERROR loading metadata - {e}")
            continue

        # Find files to trim
        files_to_keep: list[FileMetadata] = []
        files_to_trim: list[FileMetadata] = []

        for fm in metadata.files:
            # Check if file has dates before cutoff
            # We need to check the actual data, but for efficiency we use
            # the partition values if they contain date info
            should_trim = False

            # Check date_col in partition_values if it's a day partition
            if (
                dpd.date_partition == "day"
                and dpd.date_col in fm.partition_values
            ):
                file_date = fm.partition_values[dpd.date_col]
                if isinstance(file_date, str):
                    file_date = dt.date.fromisoformat(file_date)
                should_trim = file_date < cutoff_date
            elif dpd.date_partition in ("year", "month", "quarter", "week"):
                # Use date_partition_range to get the actual date range
                part_col = dpd.date_partition
                if part_col in fm.partition_values:
                    part_val = fm.partition_values[part_col]
                    if isinstance(part_val, str):
                        try:
                            _, part_end = date_partition_range(
                                part_val, dpd.date_partition
                            )
                            should_trim = part_end < cutoff_date
                        except (ValueError, KeyError):
                            # Invalid partition value format, keep the file
                            pass

            if should_trim:
                files_to_trim.append(fm)
            else:
                files_to_keep.append(fm)

        if not files_to_trim:
            if verbose:
                logger.info(f"{name}: no files to trim")
            continue

        logger.info(
            f"{name}: trimming {len(files_to_trim)} files, "
            f"keeping {len(files_to_keep)}"
        )

        # Create new snapshot metadata with only kept files
        new_suffix = generate_snapshot_suffix()

        # Recompute actual date range and partition values from kept files
        new_start = cutoff_date  # At minimum, the cutoff
        new_end = metadata.cache_end_date
        new_partition_values: dict[str, list[Any]] = {}

        if files_to_keep:
            # Scan kept files for actual partition values
            for fm in files_to_keep:
                for col, val in fm.partition_values.items():
                    if col not in new_partition_values:
                        new_partition_values[col] = []
                    if val not in new_partition_values[col]:
                        new_partition_values[col].append(val)

            # Sort partition values for consistency
            for col in new_partition_values:
                new_partition_values[col] = sorted(new_partition_values[col])

        new_metadata = SnapshotMetadata(
            name=metadata.name,
            suffix=new_suffix,
            schema=metadata.schema,
            files=files_to_keep,
            yaml_config=metadata.yaml_config,
            cache_start_date=new_start,
            cache_end_date=new_end,
            partition_values=new_partition_values,
        )

        # Write new metadata
        new_meta_path = meta_dir / f"{name}_{new_suffix}.pkl.gz"
        new_metadata.to_pickle(new_meta_path)
        new_meta_files.append(new_meta_path)  # Track for undo script

        # Rename old metadata files with _trimmed suffix
        for meta_file in meta_dir.glob("*.pkl.gz"):
            if "_trimmed" in meta_file.name:
                continue
            if meta_file.name == new_meta_path.name:
                continue
            trimmed_name = (
                meta_file.stem.replace(".pkl", "") + "_trimmed.pkl.gz"
            )
            trimmed_path = meta_file.parent / trimmed_name
            meta_file.rename(trimmed_path)
            trimmed_meta_files.append(trimmed_path)

        # Rename trimmed data files
        for fm in files_to_trim:
            file_path = dpd.dataset_dir / fm.path
            if file_path.exists():
                trimmed_name = file_path.stem + "_trimmed" + file_path.suffix
                trimmed_path = file_path.parent / trimmed_name
                file_path.rename(trimmed_path)
                trimmed_files.append(trimmed_path)

        datasets_trimmed += 1

    if not trimmed_files and not trimmed_meta_files:
        logger.info("\nNo files to trim.")
        return 0

    # Generate scripts
    all_trimmed = trimmed_files + trimmed_meta_files
    total_size = sum(f.stat().st_size for f in all_trimmed if f.exists())
    suffix = generate_snapshot_suffix()

    # Delete script
    delete_script = _generate_trim_delete_script(
        cache_path, all_trimmed, suffix
    )
    # Undo script (needs new_meta_files to delete them on undo)
    undo_script = _generate_trim_undo_script(
        cache_path, all_trimmed, new_meta_files, suffix
    )

    logger.info(
        f"\nTrimmed {datasets_trimmed} dataset(s)\n"
        f"Marked {len(all_trimmed)} files for deletion "
        f"({format_size(total_size)})\n"
        f"\nGenerated scripts:\n"
        f"  Delete: {delete_script}\n"
        f"  Undo:   {undo_script}\n"
        "\nYou MUST run one of these scripts to complete the trim."
    )

    return 0


def _generate_trim_delete_script(
    cache_path: Path,
    files: list[Path],
    suffix: str,
) -> Path:
    """Generate script to delete trimmed files."""
    if sys.platform == "win32":
        script_name = f"_cleanup_{suffix}_delete.bat"
        lines = [
            "@echo off",
            "REM Delete trimmed files - run to permanently remove trimmed data",
            "",
        ]
        for f in files:
            lines.append(f'del "{f}"')
    else:
        script_name = f"_cleanup_{suffix}_delete.sh"
        lines = [
            "#!/bin/bash",
            "# Delete trimmed files - run to permanently remove trimmed data",
            "",
        ]
        for f in files:
            lines.append(f'rm "{f}"')

    script_path = cache_path / script_name
    script_path.write_text("\n".join(lines))

    if sys.platform != "win32":
        script_path.chmod(0o755)

    return script_path


def _generate_trim_undo_script(
    cache_path: Path,
    files: list[Path],
    new_meta_files: list[Path],
    suffix: str,
) -> Path:
    """Generate script to undo trim (restore original names, delete new meta)."""
    if sys.platform == "win32":
        script_name = f"_cleanup_{suffix}_undo.bat"
        lines = [
            "@echo off",
            "REM Undo trim - restores files and deletes new metadata",
            "",
            "REM Step 1: Delete new metadata files created by trim",
        ]
        for meta_file in new_meta_files:
            lines.append(f'del "{meta_file}"')
        lines.append("")
        lines.append("REM Step 2: Restore trimmed files to original names")
        for f in files:
            original = str(f).replace("_trimmed", "")
            lines.append(f'move "{f}" "{original}"')
    else:
        script_name = f"_cleanup_{suffix}_undo.sh"
        lines = [
            "#!/bin/bash",
            "# Undo trim - restores files and deletes new metadata",
            "",
            "# Step 1: Delete new metadata files created by trim",
        ]
        for meta_file in new_meta_files:
            lines.append(f'rm "{meta_file}"')
        lines.append("")
        lines.append("# Step 2: Restore trimmed files to original names")
        for f in files:
            original = str(f).replace("_trimmed", "")
            lines.append(f'mv "{f}" "{original}"')

    script_path = cache_path / script_name
    script_path.write_text("\n".join(lines))

    if sys.platform != "win32":
        script_path.chmod(0o755)

    return script_path


def _discover_dpd_snapshots(
    cache_path: Path,
    dataset_names: list[str] | None,
) -> dict[str, list[dict[str, Any]]]:
    """Discover DPD snapshots by scanning _meta_data directories."""
    result: dict[str, list[dict[str, Any]]] = {}

    for item in cache_path.iterdir():
        if not item.is_dir():
            continue
        if item.name in ("yaml", "code", "non-dated"):
            continue

        meta_dir = item / "_meta_data"
        if not meta_dir.exists():
            continue

        name = item.name
        if dataset_names and name not in dataset_names:
            continue

        snapshots = []
        for meta_file in meta_dir.glob("*.pkl.gz"):
            suffix = extract_suffix_from_filename(meta_file.name)
            if not suffix:
                continue

            # Get creation time
            try:
                ts = parse_snapshot_suffix(suffix)
                created_at = dt.datetime.fromtimestamp(ts)
            except ValueError:
                created_at = None

            # Find associated data files
            files = list(item.rglob(f"*_{suffix}.parquet"))
            sidecars: list[Path] = []
            try:
                metadata = SnapshotMetadata.from_pickle(meta_file)
                if metadata.provenance is not None:
                    sidecar = (
                        item
                        / "_provenance"
                        / f"{name}_{suffix}.provenance.pkl.gz"
                    )
                    if sidecar.exists():
                        sidecars.append(sidecar)
            except Exception as e:
                logger.warning(
                    f"Skipping provenance sidecar discovery for {meta_file}: {e}"
                )

            size = sum(
                f.stat().st_size for f in files + sidecars if f.exists()
            )

            snapshots.append(
                {
                    "suffix": suffix,
                    "meta_file": meta_file,
                    "files": files,
                    "sidecars": sidecars,
                    "size": size,
                    "created_at": created_at,
                }
            )

        if snapshots:
            result[name] = snapshots

    return result


def _expected_npd_sidecars(
    dataset_dir: Path,
    name: str,
    suffix: str,
) -> list[Path]:
    """Return convention-named NPD sidecars that exist for a snapshot."""
    candidates = [
        dataset_dir / "_meta_data" / f"{name}_{suffix}.pkl.gz",
        dataset_dir / "_provenance" / f"{name}_{suffix}.provenance.pkl.gz",
    ]
    return [path for path in candidates if path.exists()]


def _discover_npd_snapshots(
    cache_path: Path,
    dataset_names: list[str] | None,
) -> dict[str, list[dict[str, Any]]]:
    """Discover NPD snapshots in the non-dated directory.

    NPD structure is:
        non-dated/
            {dataset_name}/
                {dataset_name}_{suffix}.parquet   <- file snapshot
                {dataset_name}_{suffix}/          <- directory snapshot (hive)
    """
    result: dict[str, list[dict[str, Any]]] = {}

    non_dated = cache_path / "non-dated"
    if not non_dated.exists():
        return result

    # Each subdirectory of non-dated/ is a dataset
    for dataset_dir in non_dated.iterdir():
        if not dataset_dir.is_dir():
            continue

        name = dataset_dir.name
        if dataset_names and name not in dataset_names:
            continue

        # Find snapshots inside this dataset directory
        snapshots = []
        for item in dataset_dir.iterdir():
            suffix = extract_suffix_from_filename(item.name)
            if not suffix:
                continue

            try:
                ts = parse_snapshot_suffix(suffix)
                created_at = dt.datetime.fromtimestamp(ts)
            except ValueError:
                created_at = None

            if item.is_file():
                size = item.stat().st_size
            else:
                size = sum(
                    f.stat().st_size for f in item.rglob("*") if f.is_file()
                )
            sidecars = _expected_npd_sidecars(dataset_dir, name, suffix)
            size += sum(f.stat().st_size for f in sidecars if f.exists())

            snapshots.append(
                {
                    "suffix": suffix,
                    "path": item,
                    "sidecars": sidecars,
                    "size": size,
                    "created_at": created_at,
                }
            )

        if snapshots:
            result[name] = snapshots

    return result


def _find_orphaned_files(
    cache_path: Path,
    dpd_snapshots: dict[str, list[dict[str, Any]]],
    npd_snapshots: dict[str, list[dict[str, Any]]],
) -> list[Path]:
    """Find files not referenced by any snapshot."""
    # Collect all known files
    known_files: set[Path] = set()

    for snapshots in dpd_snapshots.values():
        for snap in snapshots:
            known_files.add(snap["meta_file"])
            known_files.update(snap.get("files", []))
            known_files.update(snap.get("sidecars", []))

    for snapshots in npd_snapshots.values():
        for snap in snapshots:
            if snap.get("path"):
                if snap["path"].is_file():
                    known_files.add(snap["path"])
                else:
                    known_files.update(snap["path"].rglob("*"))
            known_files.update(snap.get("sidecars", []))

    # Find parquet files not in known set
    orphans = []
    for f in cache_path.rglob("*.parquet"):
        if f not in known_files:
            # Check if it's in a _meta_data dir (shouldn't have parquet)
            if "_meta_data" not in f.parts:
                orphans.append(f)

    known_dpd_suffixes = {
        name: {snap["suffix"] for snap in snapshots}
        for name, snapshots in dpd_snapshots.items()
    }
    known_npd_suffixes = {
        name: {snap["suffix"] for snap in snapshots}
        for name, snapshots in npd_snapshots.items()
    }

    for f in cache_path.glob("*/_provenance/*.provenance.pkl.gz"):
        if f in known_files:
            continue
        suffix = extract_suffix_from_filename(f.name)
        dataset_name = f.parent.parent.name
        if suffix not in known_dpd_suffixes.get(dataset_name, set()):
            orphans.append(f)

    for f in cache_path.glob("non-dated/*/_meta_data/*.pkl.gz"):
        if f in known_files:
            continue
        suffix = extract_suffix_from_filename(f.name)
        dataset_name = f.parent.parent.name
        if suffix not in known_npd_suffixes.get(dataset_name, set()):
            orphans.append(f)

    for f in cache_path.glob("non-dated/*/_provenance/*.provenance.pkl.gz"):
        if f in known_files:
            continue
        suffix = extract_suffix_from_filename(f.name)
        dataset_name = f.parent.parent.name
        if suffix not in known_npd_suffixes.get(dataset_name, set()):
            orphans.append(f)

    return orphans


def _generate_cleanup_script(cache_path: Path, files: list[Path]) -> Path:
    """Generate a shell script to delete the specified files."""
    suffix = generate_snapshot_suffix()

    if sys.platform == "win32":
        script_name = f"_cleanup_{suffix}.bat"
        lines = ["@echo off", "REM Generated cleanup script", ""]
        for f in files:
            lines.append(f'del "{f}"')
    else:
        script_name = f"_cleanup_{suffix}.sh"
        lines = ["#!/bin/bash", "# Generated cleanup script", ""]
        for f in files:
            lines.append(f'rm "{f}"')

    script_path = cache_path / script_name
    script_path.write_text("\n".join(lines))

    if sys.platform != "win32":
        script_path.chmod(0o755)

    return script_path


def main() -> None:
    """Entry point for cleanup-cache command."""
    sys.exit(cleanup_cache_main())


if __name__ == "__main__":
    main()
