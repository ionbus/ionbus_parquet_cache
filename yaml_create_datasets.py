"""
Command-line tool for creating and configuring DatedParquetDatasets from YAML.

Entry point: yaml-create-datasets

Use this to:
- Create new datasets from YAML configuration
- Update datasets when YAML configuration changes
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path
from typing import Any

import yaml

from ionbus_parquet_cache.exceptions import (
    ConfigurationError,
    DataSourceError,
    SnapshotNotFoundError,
)
from ionbus_parquet_cache.yaml_config import load_yaml_file
from ionbus_utils.logging_utils import logger

# Fields that must not change between YAML config and stored metadata
IMMUTABLE_FIELDS = [
    "date_col",
    "date_partition",
    "partition_columns",
    "sort_columns",
    "instrument_column",
]


def check_config_consistency(
    yaml_config: dict[str, Any],
    stored_config: dict[str, Any],
    dataset_name: str,
) -> list[str]:
    """
    Check that immutable fields match between YAML and stored metadata.

    Args:
        yaml_config: Configuration from YAML file.
        stored_config: Configuration from snapshot metadata.
        dataset_name: Name of the dataset for error messages.

    Returns:
        List of error messages for any mismatches.
    """
    errors = []

    for field in IMMUTABLE_FIELDS:
        yaml_value = yaml_config.get(field)
        stored_value = stored_config.get(field)

        # Normalize None vs empty list/dict
        if yaml_value is None:
            yaml_value = [] if field in ("partition_columns", "sort_columns") else None
        if stored_value is None:
            stored_value = [] if field in ("partition_columns", "sort_columns") else None

        # Normalize sort_columns default
        if field == "sort_columns":
            if not yaml_value:
                yaml_value = [yaml_config.get("date_col", "Date")]
            if not stored_value:
                stored_value = [stored_config.get("date_col", "Date")]

        if yaml_value != stored_value:
            errors.append(
                f"  {field}: YAML has {yaml_value!r}, metadata has {stored_value!r}"
            )

    return errors


def create_cache_main(args: list[str] | None = None) -> int:
    """
    Main entry point for yaml-create-datasets command.

    Creates or updates DatedParquetDatasets using a YAML configuration file.

    Returns:
        Exit code (0 for success, non-zero for errors).
    """
    parser = argparse.ArgumentParser(
        prog="yaml-create-datasets",
        description="Create or update datasets from YAML configuration.",
    )
    parser.add_argument(
        "yaml_file",
        type=str,
        help="Path to the YAML configuration file",
    )
    parser.add_argument(
        "dataset_name",
        nargs="?",
        type=str,
        help="Name of dataset to process (default: all in YAML)",
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
        help="Backfill mode: extend cache backwards without replacing data",
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
        "--preserve-config",
        action="store_true",
        help="Keep existing cache config, only use YAML for DataSource definition",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output",
    )

    parsed = parser.parse_args(args)

    # Validate arguments
    if parsed.backfill and parsed.restate:
        logger.error(
            "Error: --backfill and --restate are mutually exclusive"
        )
        return 1

    if parsed.backfill and parsed.end_date:
        logger.error("Error: --end-date not allowed with --backfill")
        return 1

    if parsed.restate and not (parsed.start_date and parsed.end_date):
        logger.error(
            "Error: --restate requires both --start-date and --end-date"
        )
        return 1

    # Validate YAML file exists
    yaml_path = Path(parsed.yaml_file)
    if not yaml_path.exists():
        logger.error(f"Error: YAML file not found: {yaml_path}")
        return 1

    # Parse dates
    start_date = None
    end_date = None
    if parsed.start_date:
        try:
            start_date = dt.date.fromisoformat(parsed.start_date)
        except ValueError:
            logger.error(
                f"Error: Invalid start date: {parsed.start_date}"
            )
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
        return _run_create_from_yaml(
            yaml_path=yaml_path,
            dataset_name=parsed.dataset_name,
            start_date=start_date,
            end_date=end_date,
            instruments=instruments,
            backfill=parsed.backfill,
            restate=parsed.restate,
            dry_run=parsed.dry_run,
            preserve_config=parsed.preserve_config,
            verbose=parsed.verbose,
        )
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


def _run_create_from_yaml(
    yaml_path: Path,
    dataset_name: str | None,
    start_date: dt.date | None,
    end_date: dt.date | None,
    instruments: list[str] | None,
    backfill: bool,
    restate: bool,
    dry_run: bool,
    preserve_config: bool,
    verbose: bool,
) -> int:
    """Execute update using a YAML configuration file."""
    # Read YAML to check for cache_dir
    with open(yaml_path) as f:
        raw_yaml = yaml.safe_load(f)

    # Determine cache_dir: from YAML or default to parent of YAML file's dir
    if raw_yaml and "cache_dir" in raw_yaml:
        cache_path = Path(raw_yaml["cache_dir"])
        if not cache_path.is_absolute():
            # Relative paths are relative to the YAML file's directory
            cache_path = yaml_path.parent / cache_path
    else:
        # Default: one directory up from YAML file
        cache_path = yaml_path.parent.parent

    cache_path = cache_path.resolve()

    if not cache_path.exists():
        logger.error(f"Error: Cache directory not found: {cache_path}")
        return 1

    if verbose:
        logger.info(f"Using cache directory: {cache_path}")

    # Load configs from the YAML file
    configs = load_yaml_file(yaml_path, cache_path)
    if not configs:
        logger.error(
            f"Error: No dataset configurations found in {yaml_path}"
        )
        return 1

    # Filter to requested dataset
    if dataset_name:
        if dataset_name not in configs:
            logger.error(
                f"Error: Dataset '{dataset_name}' not found in YAML"
            )
            return 1
        configs = {dataset_name: configs[dataset_name]}

    # Update each dataset
    success_count = 0
    error_count = 0

    for name, config in configs.items():
        if verbose:
            logger.info(f"Processing {name}...")

        try:
            dpd = config.to_dpd()
            yaml_config = config.to_yaml_config()
            stored_config = None  # Will be set if metadata exists

            # Check consistency with stored metadata if it exists
            try:
                dpd._metadata = dpd._load_metadata()
                stored_config = dpd._metadata.yaml_config

                if preserve_config:
                    # Use stored config for DPD structure, only use YAML for source
                    if verbose:
                        logger.info(f"  {name}: Preserving existing config")
                    # Update DPD with stored config values
                    dpd.date_col = stored_config.get("date_col", dpd.date_col)
                    dpd.date_partition = stored_config.get(
                        "date_partition", dpd.date_partition
                    )
                    dpd.partition_columns = stored_config.get(
                        "partition_columns", dpd.partition_columns
                    )
                    dpd.sort_columns = stored_config.get(
                        "sort_columns", dpd.sort_columns
                    )
                    dpd.instrument_column = stored_config.get(
                        "instrument_column", dpd.instrument_column
                    )
                else:
                    errors = check_config_consistency(
                        yaml_config, stored_config, name
                    )
                    if errors:
                        logger.error(
                            f"  {name}: ERROR - YAML config differs from "
                            "stored metadata:"
                        )
                        for err in errors:
                            logger.error(err)
                        logger.error(
                            "  These fields cannot be changed. Use a new "
                            "dataset name, fix the YAML config, or use "
                            "--preserve-config."
                        )
                        error_count += 1
                        continue

            except SnapshotNotFoundError:
                # No existing metadata - this is a new dataset
                if preserve_config:
                    logger.error(
                        f"  {name}: ERROR - --preserve-config requires "
                        "existing metadata"
                    )
                    error_count += 1
                    continue
                if verbose:
                    logger.info(f"  {name}: Creating new dataset")

            source = config.create_source(dpd)
            cleaner = config.create_cleaner(dpd)

            # Extract YAML transform settings
            transforms = None
            if config.has_yaml_transforms():
                transforms = {
                    "columns_to_rename": config.columns_to_rename,
                    "columns_to_drop": config.columns_to_drop,
                    "dropna_columns": config.dropna_columns,
                    "dedup_columns": config.dedup_columns,
                    "dedup_keep": config.dedup_keep,
                }

            # Use stored config if preserving, otherwise use YAML config
            config_for_metadata = (
                stored_config if preserve_config and stored_config else yaml_config
            )

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
                yaml_config=config_for_metadata,
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


def main() -> None:
    """Entry point for yaml-create-datasets command."""
    sys.exit(create_cache_main())


if __name__ == "__main__":
    main()
