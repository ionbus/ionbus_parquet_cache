"""
Command-line tool for importing NonDatedParquetDataset snapshots.

Entry point:
    python -m ionbus_parquet_cache.import_npd
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import pyarrow.dataset as pds
import yaml
from ionbus_utils.logging_utils import logger

from ionbus_parquet_cache.exceptions import (
    SnapshotPublishError,
    ValidationError,
)
from ionbus_parquet_cache.non_dated_dataset import NonDatedParquetDataset
from ionbus_parquet_cache.snapshot import generate_snapshot_suffix


def import_npd_main(args: list[str] | None = None) -> int:
    """
    Main entry point for importing an NPD snapshot.

    Returns:
        Exit code (0 for success, non-zero for errors).
    """
    parser = argparse.ArgumentParser(
        prog="python -m ionbus_parquet_cache.import_npd",
        description=(
            "Import a parquet file or directory as a "
            "NonDatedParquetDataset snapshot."
        ),
    )
    parser.add_argument(
        "cache_dir",
        type=str,
        help="Existing local cache root",
    )
    parser.add_argument(
        "name",
        type=str,
        help="NPD name to create or update",
    )
    parser.add_argument(
        "source_path",
        type=str,
        help="Parquet file or directory containing parquet files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate inputs and show planned import without writing",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip PyArrow dataset validation before copying",
    )
    parser.add_argument(
        "--info-file",
        type=str,
        metavar="PATH",
        help="Strict YAML file for notes/annotations/column_descriptions",
    )
    parser.add_argument(
        "--provenance-file",
        type=str,
        metavar="PATH",
        help="YAML mapping stored as explicit snapshot provenance",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print source, destination, and snapshot details",
    )

    parsed = parser.parse_args(args)

    try:
        return _run_import_npd(
            cache_dir=parsed.cache_dir,
            name=parsed.name,
            source_path=parsed.source_path,
            dry_run=parsed.dry_run,
            skip_validation=parsed.skip_validation,
            info_file=parsed.info_file,
            provenance_file=parsed.provenance_file,
            verbose=parsed.verbose,
        )
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


def _run_import_npd(
    cache_dir: str,
    name: str,
    source_path: str,
    dry_run: bool,
    skip_validation: bool,
    info_file: str | None,
    provenance_file: str | None,
    verbose: bool,
) -> int:
    """Validate and import a parquet source into an NPD snapshot."""
    _validate_dataset_name(name)

    if cache_dir.startswith("gs://"):
        logger.error(
            "Error: Direct NPD imports into GCS caches are not supported. "
            "Import locally and sync to GCS."
        )
        return 1

    cache_path = Path(cache_dir)
    if not cache_path.exists() or not cache_path.is_dir():
        logger.error(f"Error: Cache directory not found: {cache_path}")
        return 1

    if _dpd_exists(cache_path, name):
        logger.error(
            f"Error: DPD '{name}' already exists in this cache. "
            "Refusing to create an NPD with the same name."
        )
        return 1

    if source_path.startswith("gs://"):
        logger.error("Error: source_path must be a local file or directory")
        return 1

    source = Path(source_path)
    validation_error = _validate_source(
        source,
        skip_pyarrow_validation=skip_validation,
    )
    if validation_error is not None:
        logger.error(f"Error: {validation_error}")
        return 1

    npd = NonDatedParquetDataset(cache_dir=cache_path, name=name)
    current = npd._discover_current_suffix()

    info = None
    provenance = None
    try:
        if info_file is not None:
            info = _load_yaml_mapping(Path(info_file), "--info-file")
            npd._validate_snapshot_info_keys(
                info,
                f"--info-file {info_file}",
            )
        if provenance_file is not None:
            provenance = _load_yaml_mapping(
                Path(provenance_file),
                "--provenance-file",
            )
        # Validate carry-forward and append-only rules before any write. The
        # import path repeats this validation so the Python API has the same
        # guarantees as the CLI.
        npd._resolve_import_info(info)
        npd._normalize_import_provenance(provenance)
    except (OSError, ValidationError, ValueError) as e:
        logger.error(f"Error: {e}")
        return 1

    if dry_run:
        suffix = _generate_valid_import_suffix(current)
        destination = _snapshot_destination(npd, source, suffix)
        logger.info(f"{name}: would import snapshot {suffix}")
        logger.info(f"path: {destination}")
        if verbose:
            logger.info(f"source: {source}")
            if skip_validation:
                logger.info("validation: skipped PyArrow dataset validation")
            if info_file:
                logger.info(f"info-file: {info_file}")
            if provenance_file:
                logger.info(f"provenance-file: {provenance_file}")
        return 0

    try:
        suffix = npd.import_snapshot(
            source,
            info=info,
            provenance=provenance,
        )
    except SnapshotPublishError as e:
        logger.error(f"Error: {e}")
        return 1

    destination = _snapshot_destination(npd, source, suffix)
    logger.info(f"{name}: imported snapshot {suffix}")
    logger.info(f"path: {destination}")
    if verbose:
        logger.info(f"source: {source}")
        if skip_validation:
            logger.info("validation: skipped PyArrow dataset validation")
        if info_file:
            logger.info(f"info-file: {info_file}")
        if provenance_file:
            logger.info(f"provenance-file: {provenance_file}")

    return 0


def _validate_dataset_name(name: str) -> None:
    """Validate a dataset name before using it as a cache subdirectory."""
    if not name:
        raise ValueError("Dataset name must not be empty")

    path = Path(name)
    if (
        "/" in name
        or "\\" in name
        or path.is_absolute()
        or len(path.parts) != 1
        or name in {".", ".."}
    ):
        raise ValueError(
            "Dataset name must be a single path segment, not a path"
        )


def _dpd_exists(cache_path: Path, name: str) -> bool:
    """Return True if a DPD with this name exists in the cache."""
    return (cache_path / name / "_meta_data").is_dir()


def _validate_source(
    source: Path,
    skip_pyarrow_validation: bool,
) -> str | None:
    """Return an error string if source is not a readable parquet dataset."""
    if not source.exists():
        return f"Source path does not exist: {source}"

    if source.is_file():
        if source.suffix.lower() != ".parquet":
            return f"Source file is not a parquet file: {source}"
    elif source.is_dir():
        parquet_files = list(source.rglob("*.parquet"))
        if not parquet_files:
            return f"Source directory contains no parquet files: {source}"
    else:
        return f"Source path is not a file or directory: {source}"

    if skip_pyarrow_validation:
        return None

    try:
        if source.is_dir():
            dataset = pds.dataset(
                source,
                format="parquet",
                partitioning="hive",
            )
        else:
            dataset = pds.dataset(source, format="parquet")
        _ = dataset.schema
    except Exception as e:
        return f"Source is not a readable parquet dataset: {e}"

    return None


def _load_yaml_mapping(path: Path, context: str) -> dict[str, Any]:
    """Load a YAML file that must contain a mapping."""
    if not path.exists():
        raise FileNotFoundError(f"{context} not found: {path}")
    with path.open() as f:
        payload = yaml.safe_load(f)
    if not isinstance(payload, dict):
        raise ValidationError(
            f"{context} must be a YAML mapping, "
            f"got {type(payload).__name__}"
        )
    return payload


def _generate_valid_import_suffix(current: str | None) -> str:
    """Generate a suffix and validate it against the current NPD suffix."""
    suffix = generate_snapshot_suffix()
    if current is not None and suffix <= current:
        raise SnapshotPublishError(
            f"Snapshot suffix collision: new suffix '{suffix}' is not "
            f"greater than current suffix '{current}'. "
            "This can happen if imports are too fast (within same second)."
        )
    return suffix


def _snapshot_destination(
    npd: NonDatedParquetDataset,
    source: Path,
    suffix: str,
) -> Path:
    """Return the destination path for an imported NPD snapshot."""
    npd_dir = npd.npd_dir
    if isinstance(npd_dir, str):
        raise ValueError("NPD destination must be a local path")

    if source.is_dir():
        return npd_dir / f"{npd.name}_{suffix}"
    return npd_dir / f"{npd.name}_{suffix}.parquet"


def main() -> None:
    """Entry point for python -m ionbus_parquet_cache.import_npd."""
    sys.exit(import_npd_main())


if __name__ == "__main__":
    main()
