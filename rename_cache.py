"""
rename_cache — rename a DatedParquetDataset in place.

Usage:
    python -m ionbus_parquet_cache.rename_cache <cache_dir> <old_name> <new_name>
    python -m ionbus_parquet_cache.rename_cache <cache_dir> <old_name> <new_name> --dry-run

Steps (safe recovery ordering):
    1. Validate: old_dir exists with _meta_data/, new_dir does not exist.
    2. Write new metadata files (<new_name>_<suffix>.pkl.gz) alongside old ones.
       If interrupted here, delete the new files — old cache is untouched.
    3. Rename directory old_name/ → new_name/.
       If interrupted here (very unlikely on local fs), rename back.
    4. Delete old <old_name>_<suffix>.pkl.gz from <new_name>/_meta_data/.
       The rename is already committed; this is cleanup only.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ionbus_utils.logging_utils import logger


def rename_cache(
    cache_dir: Path,
    old_name: str,
    new_name: str,
    dry_run: bool = False,
) -> None:
    """
    Rename a DatedParquetDataset from old_name to new_name.

    Args:
        cache_dir: Root cache directory containing the dataset subdirectory.
        old_name: Current dataset name.
        new_name: New dataset name.
        dry_run: If True, print what would happen without making any changes.

    Raises:
        FileNotFoundError: If old_dir doesn't exist or has no _meta_data/.
        FileExistsError: If new_dir already exists.
        ValueError: If old_name == new_name or names are empty.
    """
    if not old_name or not new_name:
        raise ValueError("old_name and new_name must be non-empty")
    if old_name == new_name:
        raise ValueError("old_name and new_name are the same")

    old_dir = cache_dir / old_name
    new_dir = cache_dir / new_name
    meta_dir = old_dir / "_meta_data"

    # --- Step 1: Validate ---
    if not old_dir.exists():
        raise FileNotFoundError(f"Dataset directory not found: {old_dir}")
    if not meta_dir.exists():
        raise FileNotFoundError(
            f"No _meta_data/ directory found in {old_dir}. "
            "Is this a valid DatedParquetDataset?"
        )
    if new_dir.exists():
        raise FileExistsError(
            f"Target directory already exists: {new_dir}. "
            "Delete or rename it first."
        )

    # Collect metadata files to update (skip trimmed snapshots)
    pkl_files = [
        p
        for p in meta_dir.iterdir()
        if p.suffix == ".gz"
        and p.name.endswith(".pkl.gz")
        and "_trimmed" not in p.name
    ]
    if not pkl_files:
        raise FileNotFoundError(
            f"No snapshot metadata files found in {meta_dir}"
        )

    logger.info(
        f"{'[dry-run] ' if dry_run else ''}"
        f"Renaming '{old_name}' → '{new_name}' "
        f"({len(pkl_files)} metadata file(s))"
    )

    # --- Step 2: Write new metadata files ---
    new_pkl_paths: list[Path] = []
    try:
        for old_pkl in pkl_files:
            # Derive new filename: replace leading "<old_name>_" with "<new_name>_"
            suffix_part = old_pkl.name[
                len(old_name) + 1 :
            ]  # e.g. "1wDpgS.pkl.gz"
            new_pkl_name = f"{new_name}_{suffix_part}"
            new_pkl_path = meta_dir / new_pkl_name

            logger.info(
                f"  {'[dry-run] ' if dry_run else ''}"
                f"metadata: {old_pkl.name} → {new_pkl_name}"
            )

            if not dry_run:
                from ionbus_parquet_cache.dated_dataset import (
                    SnapshotMetadata,
                )

                metadata = SnapshotMetadata.from_pickle(old_pkl)
                metadata.name = new_name
                metadata.to_pickle(new_pkl_path)
                new_pkl_paths.append(new_pkl_path)

    except Exception:
        # Clean up any new files written before the failure
        for p in new_pkl_paths:
            p.unlink(missing_ok=True)
        raise

    # --- Step 3: Rename directory ---
    logger.info(
        f"  {'[dry-run] ' if dry_run else ''}"
        f"directory: {old_dir.name}/ → {new_dir.name}/"
    )
    if not dry_run:
        old_dir.rename(new_dir)

    # --- Step 4: Delete old metadata files (cleanup) ---
    for old_pkl in pkl_files:
        old_pkl_in_new_dir = new_dir / "_meta_data" / old_pkl.name
        logger.info(
            f"  {'[dry-run] ' if dry_run else ''}"
            f"delete: {old_pkl_in_new_dir.name}"
        )
        if not dry_run:
            old_pkl_in_new_dir.unlink(missing_ok=True)

    logger.info(
        f"{'[dry-run] ' if dry_run else ''}"
        f"Done. '{old_name}' → '{new_name}'"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rename a DatedParquetDataset in place."
    )
    parser.add_argument("cache_dir", type=Path, help="Root cache directory")
    parser.add_argument("old_name", help="Current dataset name")
    parser.add_argument("new_name", help="New dataset name")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would happen without making changes",
    )
    args = parser.parse_args()

    try:
        rename_cache(
            cache_dir=args.cache_dir,
            old_name=args.old_name,
            new_name=args.new_name,
            dry_run=args.dry_run,
        )
    except (FileNotFoundError, FileExistsError, ValueError) as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
