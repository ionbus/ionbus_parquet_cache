"""
Command-line tool for updating existing DatedParquetDatasets.

Entry point: update-cache

Use this for routine updates of existing datasets. The dataset configuration
is read from stored metadata - no YAML files required.

To create a new dataset or change its configuration, use yaml-create-datasets instead.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

from ionbus_utils.logging_utils import logger

from ionbus_parquet_cache.dated_dataset import (
    DatedParquetDataset,
    SnapshotMetadata,
    dpd_from_metadata_config,
)
from ionbus_parquet_cache.exceptions import (
    ConfigurationError,
    DataSourceError,
)


def update_cache_main(args: list[str] | None = None) -> int:
    """
    Main entry point for update-cache command.

    Updates existing DatedParquetDatasets using configuration stored in
    their metadata. Does not require YAML files.

    Returns:
        Exit code (0 for success, non-zero for errors).
    """
    parser = argparse.ArgumentParser(
        prog="update-cache",
        description="Update existing datasets using stored metadata configuration.",
    )
    parser.add_argument(
        "cache_dir",
        type=str,
        help="Path to the cache directory",
    )
    parser.add_argument(
        "dataset_name",
        nargs="?",
        type=str,
        help="Name of dataset to update (default: all with metadata)",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        metavar="DATE",
        help="Start of date range (ISO format: YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        metavar="DATE",
        help="End of date range (ISO format: YYYY-MM-DD)",
    )
    parser.add_argument(
        "--instruments",
        type=str,
        metavar="X,Y,Z",
        help="Comma-separated list of instruments to update",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Backfill mode: extend cache backwards without replacing existing data",
    )
    parser.add_argument(
        "--restate",
        action="store_true",
        help="Restate mode: replace data for specified date range",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be updated without writing",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output",
    )

    parsed = parser.parse_args(args)

    # Validate arguments
    if parsed.backfill and parsed.restate:
        logger.error("Error: --backfill and --restate are mutually exclusive")
        return 1

    if parsed.backfill and parsed.end_date:
        logger.error("Error: --end-date not allowed with --backfill")
        return 1

    if parsed.restate and not (parsed.start_date and parsed.end_date):
        logger.error(
            "Error: --restate requires both --start-date and --end-date"
        )
        return 1

    # Parse dates
    start_date = None
    end_date = None
    if parsed.start_date:
        try:
            start_date = dt.date.fromisoformat(parsed.start_date)
        except ValueError:
            logger.error(f"Error: Invalid start date: {parsed.start_date}")
            return 1

    if parsed.end_date:
        try:
            end_date = dt.date.fromisoformat(parsed.end_date)
        except ValueError:
            logger.error(f"Error: Invalid end date: {parsed.end_date}")
            return 1

    # Parse instruments
    instruments = None
    if parsed.instruments:
        instruments = [i.strip() for i in parsed.instruments.split(",")]

    # Run update
    try:
        return _run_update(
            cache_dir=parsed.cache_dir,
            dataset_name=parsed.dataset_name,
            start_date=start_date,
            end_date=end_date,
            instruments=instruments,
            backfill=parsed.backfill,
            restate=parsed.restate,
            dry_run=parsed.dry_run,
            verbose=parsed.verbose,
        )
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


def _run_update(
    cache_dir: str,
    dataset_name: str | None,
    start_date: dt.date | None,
    end_date: dt.date | None,
    instruments: list[str] | None,
    backfill: bool,
    restate: bool,
    dry_run: bool,
    verbose: bool,
) -> int:
    """Execute update using stored metadata configuration."""
    cache_path = Path(cache_dir)
    if not cache_path.exists():
        logger.error(f"Error: Cache directory not found: {cache_dir}")
        return 1

    # Discover datasets from disk
    datasets = _discover_datasets_from_disk(cache_path)
    if not datasets:
        logger.error(f"Error: No datasets with metadata found in {cache_dir}")
        logger.error(
            "Hint: Use yaml-create-datasets to create datasets from "
            "YAML configuration."
        )
        return 1

    # Filter to requested dataset
    if dataset_name:
        if dataset_name not in datasets:
            logger.error(f"Error: Dataset '{dataset_name}' not found")
            return 1
        datasets = {dataset_name: datasets[dataset_name]}

    # Update each dataset
    success_count = 0
    error_count = 0

    for name, dpd in datasets.items():
        if verbose:
            logger.info(f"Updating {name}...")

        try:
            # Create source from stored metadata
            source = dpd.create_source_from_metadata()
            cleaner = dpd.create_cleaner_from_metadata()
            transforms = dpd.get_transforms_from_metadata()

            # Get stored yaml_config to preserve it
            yaml_config = dpd._metadata.yaml_config

            suffix = dpd.update(
                source=source,
                start_date=start_date,
                end_date=end_date,
                instruments=instruments,
                dry_run=dry_run,
                cleaner=cleaner,
                backfill=backfill,
                restate=restate,
                transforms=transforms,
                yaml_config=yaml_config,
            )

            if suffix:
                if dry_run:
                    logger.info(f"  {name}: would create snapshot {suffix}")
                else:
                    logger.info(f"  {name}: created snapshot {suffix}")
                success_count += 1
            else:
                logger.info(f"  {name}: no update needed")

        except (ConfigurationError, DataSourceError) as e:
            logger.error(f"  {name}: ERROR - {e}")
            error_count += 1

    # Summary
    if verbose or error_count > 0:
        logger.info(
            f"\nCompleted: {success_count} updated, {error_count} errors"
        )

    return 1 if error_count > 0 else 0


def _discover_datasets_from_disk(
    cache_path: Path,
) -> dict[str, DatedParquetDataset]:
    """
    Discover DatedParquetDatasets from their metadata on disk.

    Args:
        cache_path: Path to the cache directory.

    Returns:
        Dict mapping dataset name to DatedParquetDataset with loaded metadata.
    """
    datasets = {}

    # Look for _meta_data directories
    for item in cache_path.iterdir():
        if not item.is_dir():
            continue
        if (
            item.name.startswith("_")
            or item.name == "yaml"
            or item.name == "non-dated"
        ):
            continue
        if item.name == "code":
            continue

        meta_dir = item / "_meta_data"
        if not meta_dir.exists():
            continue

        # Find latest metadata file (excluding trimmed files)
        meta_files = sorted(
            [
                f
                for f in meta_dir.glob("*.pkl.gz")
                if "_trimmed" not in f.name
            ],
            reverse=True,
        )
        if not meta_files:
            continue

        # Load metadata to get config
        try:
            metadata = SnapshotMetadata.from_pickle(meta_files[0])
            config = metadata.yaml_config

            # Create DPD from stored config
            dpd = dpd_from_metadata_config(cache_path, item.name, config)
            dpd.current_suffix = metadata.suffix
            dpd._metadata = metadata

            datasets[item.name] = dpd

        except Exception:
            # Skip datasets with invalid metadata
            continue

    return datasets


def main() -> None:
    """Entry point for update-cache command."""
    sys.exit(update_cache_main())


if __name__ == "__main__":
    main()
