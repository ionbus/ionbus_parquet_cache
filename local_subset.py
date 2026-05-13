"""
Command-line tool and API for creating local DPD subsets.

Entry point:
    python -m ionbus_parquet_cache.local_subset
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import shutil
import sys
from copy import deepcopy
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from ionbus_utils.date_utils import date_partition_value, to_date
from ionbus_utils.logging_utils import logger

from ionbus_parquet_cache.bucketing import (
    INSTRUMENT_BUCKET_COL,
    instrument_bucket,
)
from ionbus_parquet_cache.cache_registry import CacheRegistry, DatasetType
from ionbus_parquet_cache.dated_dataset import (
    DatedParquetDataset,
    FileMetadata,
    SnapshotMetadata,
    dpd_config_from_dataset,
    dpd_from_metadata_config,
)
from ionbus_parquet_cache.exceptions import (
    SnapshotNotFoundError,
    SnapshotPublishError,
    ValidationError,
)
from ionbus_parquet_cache.gcs_utils import is_gcs_path
from ionbus_parquet_cache.partition import (
    PartitionSpec,
    date_partition_column_name,
)
from ionbus_parquet_cache.snapshot import generate_snapshot_suffix
from ionbus_parquet_cache.snapshot_history import DateRange, SnapshotLineage
from ionbus_parquet_cache.update_pipeline import (
    _arrow_date_range,
    _sort_table,
    build_update_plan,
)

LOCAL_SUBSET_KIND = "local_subset"
LINEAGE_INSTRUMENT_LIMIT = 1000
TOP_LEVEL_KEYS = frozenset(
    {"source_cache", "dest_cache", "defaults", "datasets"}
)
DATASET_KEYS = frozenset(
    {
        "dest_name",
        "source_snapshot",
        "start_date",
        "end_date",
        "instruments",
        "instruments_file",
        "filters",
        "columns",
        "notes",
        "annotations",
        "column_descriptions",
    }
)
DEFAULT_KEYS = DATASET_KEYS - {"dest_name"}
INFO_KEYS = frozenset({"notes", "annotations", "column_descriptions"})


@dataclass
class LocalSubsetResult:
    """One dataset result from local subset processing."""

    source_dataset: str
    dest_dataset: str | None
    dest_cache: str
    source_snapshot: str | None
    dest_snapshot: str | None
    rows: int | None
    status: str
    spec_hash: str | None
    latest_local_source_snapshot: str | None = None
    message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dictionary for callers that prefer mappings."""
        return asdict(self)


@dataclass
class DatasetWork:
    """Resolved per-dataset work item."""

    source_dataset: str
    dest_name: str
    settings: dict[str, Any]
    source_snapshot: str
    instruments: list[str] | None
    spec_hash: str
    instrument_list_hash: str | None
    effective_spec: dict[str, Any]


def local_subset(
    spec_path: str | Path,
    *,
    dataset: str | None = None,
    source_snapshot: str | None = None,
    force: bool = False,
    dry_run: bool = False,
    count: bool = False,
) -> list[LocalSubsetResult]:
    """Create local DPD subsets described by a subset spec."""
    spec_file = Path(spec_path).expanduser()
    spec = load_subset_spec(spec_file)
    base_dir = spec_file.parent
    source_cache = resolve_cache_path(spec["source_cache"], base_dir)
    dest_cache = resolve_cache_path(spec["dest_cache"], base_dir)

    if is_gcs_path(str(dest_cache)):
        raise ValidationError(
            "dest_cache must be local. Build locally and sync to GCS."
        )

    defaults = spec.get("defaults") or {}
    datasets = spec["datasets"]
    selected = select_datasets(datasets, dataset)

    dest_path = Path(dest_cache)
    results: list[LocalSubsetResult] = []
    for source_name, entry in selected.items():
        try:
            work = resolve_work_item(
                source_name=source_name,
                raw_entry=entry,
                defaults=defaults,
                source_snapshot_override=source_snapshot,
                source_cache=source_cache,
                dest_cache=dest_path,
                spec_file=spec_file,
            )
            result = _process_work_item(
                work=work,
                source_cache=source_cache,
                dest_cache=dest_path,
                spec_file=spec_file,
                force=force,
                dry_run=dry_run,
                count=count,
            )
        except Exception as exc:
            dest_name = (
                entry.get("dest_name") if isinstance(entry, dict) else None
            )
            result = LocalSubsetResult(
                source_dataset=source_name,
                dest_dataset=dest_name,
                dest_cache=str(dest_path),
                source_snapshot=None,
                dest_snapshot=None,
                rows=None,
                status="error",
                spec_hash=None,
                message=str(exc),
            )
        results.append(result)

    return results


def load_subset_spec(spec_file: Path) -> dict[str, Any]:
    """Load and strictly validate the subset YAML file."""
    if not spec_file.exists():
        raise FileNotFoundError(f"Subset spec not found: {spec_file}")
    with spec_file.open(encoding="utf-8") as f:
        payload = yaml.safe_load(f)
    if not isinstance(payload, dict):
        raise ValidationError(
            f"Subset spec must be a YAML mapping, "
            f"got {type(payload).__name__}"
        )

    _reject_unknown(payload, TOP_LEVEL_KEYS, "subset spec")
    for key in ("source_cache", "dest_cache", "datasets"):
        if key not in payload:
            raise ValidationError(f"subset spec missing required key: {key}")
    if not isinstance(payload["datasets"], dict) or not payload["datasets"]:
        raise ValidationError(
            "subset spec datasets must be a non-empty mapping"
        )

    defaults = payload.get("defaults") or {}
    if not isinstance(defaults, dict):
        raise ValidationError("subset spec defaults must be a mapping")
    _reject_unknown(defaults, DEFAULT_KEYS, "subset spec defaults")

    for name, entry in payload["datasets"].items():
        if not isinstance(name, str) or not name:
            raise ValidationError("dataset names must be non-empty strings")
        if not isinstance(entry, dict):
            raise ValidationError(
                f"dataset '{name}' settings must be a mapping"
            )
        _reject_unknown(entry, DATASET_KEYS, f"dataset '{name}'")
        if "dest_name" not in entry:
            raise ValidationError(f"dataset '{name}' missing dest_name")

    return payload


def reject_unknown(
    values: dict[str, Any],
    allowed: frozenset[str],
    context: str,
) -> None:
    """Reject unknown mapping keys."""
    _reject_unknown(values, allowed, context)


def resolve_cache_path(value: Any, base_dir: Path) -> str | Path:
    """Resolve local cache paths relative to the subset spec."""
    if not isinstance(value, str):
        raise ValidationError(
            f"cache path must be a string, got {type(value).__name__}"
        )
    if is_gcs_path(value):
        return value.rstrip("/")
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path


def select_datasets(
    datasets: dict[str, dict[str, Any]],
    selected: str | None,
) -> dict[str, dict[str, Any]]:
    """Return the selected dataset entries."""
    if selected is None:
        return datasets
    if selected not in datasets:
        raise ValidationError(
            f"dataset '{selected}' not found in subset spec"
        )
    return {selected: datasets[selected]}


def resolve_work_item(
    *,
    source_name: str,
    raw_entry: dict[str, Any],
    defaults: dict[str, Any],
    source_snapshot_override: str | None,
    source_cache: str | Path,
    dest_cache: Path,
    spec_file: Path,
) -> DatasetWork:
    """Resolve one dataset entry enough to hash and process."""
    settings = {**defaults, **raw_entry}
    dest_name = settings["dest_name"]
    if not isinstance(dest_name, str) or not dest_name:
        raise ValidationError(
            f"dataset '{source_name}' dest_name must be a string"
        )

    if (
        _same_local_cache(source_cache, dest_cache)
        and dest_name == source_name
    ):
        raise ValidationError(
            f"dataset '{source_name}' cannot write back to the same cache/name"
        )

    if "instruments" in settings and "instruments_file" in settings:
        raise ValidationError(
            f"dataset '{source_name}' cannot set both instruments and "
            "instruments_file"
        )

    instruments, instrument_hash = _resolve_instruments(settings, spec_file)
    snapshot_setting = source_snapshot_override or settings.get(
        "source_snapshot",
        "latest",
    )
    concrete_snapshot = resolve_source_snapshot(
        source_cache,
        source_name,
        snapshot_setting,
    )

    effective = _canonical_effective_spec(
        source_name=source_name,
        dest_name=dest_name,
        settings=settings,
        instruments=instruments,
        source_cache=source_cache,
        source_snapshot=concrete_snapshot,
    )
    spec_hash = hash_jsonable(effective)

    return DatasetWork(
        source_dataset=source_name,
        dest_name=dest_name,
        settings=settings,
        source_snapshot=concrete_snapshot,
        instruments=instruments,
        spec_hash=spec_hash,
        instrument_list_hash=instrument_hash,
        effective_spec=effective,
    )


def resolve_source_snapshot(
    source_cache: str | Path,
    source_name: str,
    snapshot_setting: Any,
) -> str:
    """Resolve latest or explicit source snapshot for a source DPD."""
    source_dpd = open_source_dpd(source_cache, source_name)
    if snapshot_setting is None or snapshot_setting == "latest":
        suffix = (
            source_dpd.current_suffix or source_dpd._discover_current_suffix()
        )
        if suffix is None:
            raise SnapshotNotFoundError(
                f"No snapshot found for source DPD '{source_name}'",
                dataset_name=source_name,
            )
        return suffix
    if not isinstance(snapshot_setting, str):
        raise ValidationError(
            "source_snapshot must be 'latest' or a suffix string"
        )
    source_dpd._load_metadata_for_snapshot(snapshot_setting)
    return snapshot_setting


def open_source_dpd(
    source_cache: str | Path,
    source_name: str,
) -> DatedParquetDataset:
    """Open a source DPD from a local or remote cache."""
    registry = CacheRegistry()
    registry.add_cache("source", source_cache)
    dataset = registry.get_dataset(source_name, "source", DatasetType.DATED)
    if not isinstance(dataset, DatedParquetDataset):
        raise SnapshotNotFoundError(
            f"DPD dataset '{source_name}' not found in source cache",
            dataset_name=source_name,
        )
    return dataset


def resolve_snapshot_info(
    dest: DatedParquetDataset,
    source: DatedParquetDataset,
    source_snapshot: str,
    work: DatasetWork,
) -> dict[str, Any]:
    """Resolve snapshot info from dest, source, then spec overrides."""
    base = dest._load_current_snapshot_info_if_available()
    source_info = source._load_snapshot_info(source_snapshot)
    for key in INFO_KEYS:
        if key not in base and key in source_info:
            base[key] = deepcopy(source_info[key])

    supplied = {
        key: work.settings[key] for key in INFO_KEYS if key in work.settings
    }
    return _validate_info_against_base(dest, base, supplied)


def publish_subset_table(
    *,
    dest: DatedParquetDataset,
    table: pa.Table,
    source: DatedParquetDataset,
    source_metadata: SnapshotMetadata,
    work: DatasetWork,
    info: dict[str, Any],
    spec_file: Path,
) -> str:
    """Write filtered rows and publish a destination snapshot."""
    if table.num_rows == 0:
        raise ValidationError(
            f"Local subset for '{work.source_dataset}' produced no rows"
        )

    suffix = generate_snapshot_suffix()
    current = dest.current_suffix or dest._discover_current_suffix()
    if current is not None and suffix <= current:
        raise SnapshotPublishError(
            f"Snapshot suffix collision: new suffix '{suffix}' is not "
            f"greater than current suffix '{current}'. "
            "This can happen if local subsets run too fast (within same "
            "second)."
        )

    dataset_dir = Path(dest.dataset_dir)
    temp_dir = dataset_dir / f"_tmp_local_subset_{suffix}"
    moved_files: list[Path] = []
    provenance_ref = None
    try:
        temp_dir.mkdir(parents=True, exist_ok=True)
        min_date, max_date = _validated_date_range(table, dest)
        specs, row_groups = _partition_table(dest, table, min_date, max_date)
        plan = build_update_plan(dest, specs, temp_dir, suffix=suffix)

        file_metadata = []
        for spec in specs:
            key = spec.partition_key
            group = plan.groups[key]
            assert group.final_path is not None
            rows = row_groups[key]
            group_table = table.take(pa.array(rows, type=pa.int64()))
            if dest.sort_columns:
                group_table = _sort_table(group_table, dest.sort_columns)
            write_table = _drop_partition_columns(group_table, dest)
            temp_path = temp_dir / f"{group.final_path.name}.tmp"
            pq.write_table(
                write_table,
                temp_path,
                row_group_size=dest.row_group_size,
            )
            group.final_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path.replace(group.final_path)
            moved_files.append(group.final_path)
            file_metadata.append(
                FileMetadata.from_path(
                    group.final_path,
                    dataset_dir,
                    dict(spec.partition_values),
                )
            )

        provenance_payload = _provenance_payload(
            source=source,
            dest=dest,
            source_metadata=source_metadata,
            work=work,
            spec_file=spec_file,
        )
        provenance_ref = dest._write_provenance_sidecar(
            suffix,
            provenance_payload,
        )
        yaml_config = {**dest._default_yaml_config(), **info}
        lineage = _lineage_for_subset(
            dest=dest,
            work=work,
            min_date=min_date,
            max_date=max_date,
            suffix=suffix,
        )
        dest._publish_snapshot(
            files=file_metadata,
            schema=table.schema,
            cache_start_date=min_date,
            cache_end_date=max_date,
            partition_values=_partition_values_from_files(file_metadata),
            yaml_config=yaml_config,
            lineage=lineage,
            provenance=provenance_ref,
            suffix=suffix,
        )
        return suffix
    except Exception:
        dest._delete_provenance_sidecar(provenance_ref)
        for path in moved_files:
            path.unlink(missing_ok=True)
        raise
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def jsonable(value: Any) -> Any:
    """Convert dates and paths to stable JSON values."""
    if isinstance(value, dict):
        return {str(key): jsonable(val) for key, val in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [jsonable(item) for item in value]
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def hash_jsonable(value: Any) -> str:
    """Return a stable sha256 hash for a JSON-compatible value."""
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def local_subset_main(args: list[str] | None = None) -> int:
    """
    Main entry point for local subset creation.

    Returns:
        Exit code (0 for success, non-zero for errors).
    """
    parser = argparse.ArgumentParser(
        prog="python -m ionbus_parquet_cache.local_subset",
        description=(
            "Create normal local DPD snapshots from filtered source DPD "
            "snapshots."
        ),
    )
    parser.add_argument(
        "spec",
        type=str,
        help="Subset YAML spec",
    )
    parser.add_argument(
        "--dataset",
        type=str,
        metavar="NAME",
        help="Process only one source dataset entry from the spec",
    )
    parser.add_argument(
        "--source-snapshot",
        type=str,
        metavar="SUFFIX",
        help="Override all source_snapshot entries with one suffix",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Publish even when source snapshot and spec hash are current",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate, resolve snapshots, and report actions without writes",
    )
    parser.add_argument(
        "--count",
        action="store_true",
        help="Count matching rows during dry-run; this may scan remote data",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print resolved snapshots, hashes, row counts, and output paths",
    )

    parsed = parser.parse_args(args)
    try:
        results = local_subset(
            Path(parsed.spec),
            dataset=parsed.dataset,
            source_snapshot=parsed.source_snapshot,
            force=parsed.force,
            dry_run=parsed.dry_run,
            count=parsed.count,
        )
    except Exception as exc:
        logger.error(f"Error: {exc}")
        return 1

    _log_results(results, parsed.verbose or parsed.dry_run)
    return 1 if any(result.status == "error" for result in results) else 0


def main() -> None:
    """Entry point for python -m ionbus_parquet_cache.local_subset."""
    sys.exit(local_subset_main())


def _process_work_item(
    *,
    work: DatasetWork,
    source_cache: str | Path,
    dest_cache: Path,
    spec_file: Path,
    force: bool,
    dry_run: bool,
    count: bool,
) -> LocalSubsetResult:
    """Process one resolved dataset entry."""
    source = open_source_dpd(source_cache, work.source_dataset)
    source_metadata = source._load_metadata_for_snapshot(work.source_snapshot)
    dest = _resolve_destination_dpd(dest_cache, work.dest_name, source)

    _validate_existing_destination_layout(dest, source)
    filters = _build_filters(source, work)
    columns = _validate_projection(source, source_metadata, work.settings)

    latest_source = _latest_local_subset_source_snapshot(dest)
    latest_spec_hash = _latest_local_subset_spec_hash(dest)
    is_current = (
        latest_source == work.source_snapshot
        and latest_spec_hash == work.spec_hash
    )
    if is_current and not force:
        rows = _count_rows(source, work, filters, columns) if count else None
        return LocalSubsetResult(
            source_dataset=work.source_dataset,
            dest_dataset=work.dest_name,
            dest_cache=str(dest_cache),
            source_snapshot=work.source_snapshot,
            dest_snapshot=dest.current_suffix,
            rows=rows,
            status="skipped",
            spec_hash=work.spec_hash,
            latest_local_source_snapshot=latest_source,
        )

    if dry_run:
        rows = _count_rows(source, work, filters, columns) if count else None
        return LocalSubsetResult(
            source_dataset=work.source_dataset,
            dest_dataset=work.dest_name,
            dest_cache=str(dest_cache),
            source_snapshot=work.source_snapshot,
            dest_snapshot=None,
            rows=rows,
            status="publish",
            spec_hash=work.spec_hash,
            latest_local_source_snapshot=latest_source,
        )

    table = _read_source_table(source, work, filters, columns)
    info = resolve_snapshot_info(dest, source, work.source_snapshot, work)
    suffix = publish_subset_table(
        dest=dest,
        table=table,
        source=source,
        source_metadata=source_metadata,
        work=work,
        info=info,
        spec_file=spec_file,
    )
    return LocalSubsetResult(
        source_dataset=work.source_dataset,
        dest_dataset=work.dest_name,
        dest_cache=str(dest_cache),
        source_snapshot=work.source_snapshot,
        dest_snapshot=suffix,
        rows=table.num_rows,
        status="published",
        spec_hash=work.spec_hash,
        latest_local_source_snapshot=latest_source,
    )


def _reject_unknown(
    values: dict[str, Any],
    allowed: frozenset[str],
    context: str,
) -> None:
    """Reject unknown mapping keys."""
    unknown = sorted(set(values) - allowed)
    if unknown:
        allowed_text = ", ".join(sorted(allowed))
        raise ValidationError(
            f"{context} has unknown key(s): {', '.join(unknown)}. "
            f"Allowed keys are: {allowed_text}"
        )


def _same_local_cache(left: str | Path, right: Path) -> bool:
    """Return True when two local cache roots point to the same directory."""
    if is_gcs_path(str(left)):
        return False
    return Path(left).expanduser().resolve() == right.expanduser().resolve()


def _resolve_instruments(
    settings: dict[str, Any],
    spec_file: Path,
) -> tuple[list[str] | None, str | None]:
    """Resolve inline or file-based instruments to a normalized list."""
    if "instruments_file" in settings:
        raw_path = settings["instruments_file"]
        if not isinstance(raw_path, str):
            raise ValidationError("instruments_file must be a string")
        path = Path(raw_path).expanduser()
        if not path.is_absolute():
            path = spec_file.parent / path
        if not path.exists():
            raise FileNotFoundError(f"instruments_file not found: {path}")
        values = []
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            values.append(line)
        instruments = _normalize_instrument_list(values, "instruments_file")
    elif "instruments" in settings:
        instruments = _normalize_instrument_list(
            settings["instruments"],
            "instruments",
        )
    else:
        return (None, None)

    return (instruments, hash_jsonable(instruments))


def _normalize_instrument_list(values: Any, context: str) -> list[str]:
    """Validate, deduplicate, and sort instrument identifiers."""
    if not isinstance(values, list):
        raise ValidationError(f"{context} must be a list of strings")
    normalized = []
    for value in values:
        if not isinstance(value, str):
            raise ValidationError(f"{context} values must be strings")
        stripped = value.strip()
        if stripped:
            normalized.append(stripped)
    if not normalized:
        raise ValidationError(
            f"{context} must contain at least one instrument"
        )
    return sorted(set(normalized))


def _canonical_effective_spec(
    *,
    source_name: str,
    dest_name: str,
    settings: dict[str, Any],
    instruments: list[str] | None,
    source_cache: str | Path,
    source_snapshot: str,
) -> dict[str, Any]:
    """Return the canonical per-dataset spec that is hashed."""
    effective = {
        key: deepcopy(value)
        for key, value in settings.items()
        if key != "instruments_file"
    }
    if instruments is not None:
        effective["instruments"] = instruments
    effective["source_dataset"] = source_name
    effective["dest_name"] = dest_name
    effective["source_cache"] = str(source_cache)
    effective["source_snapshot"] = source_snapshot
    return jsonable(effective)


def _resolve_destination_dpd(
    dest_cache: Path,
    dest_name: str,
    source: DatedParquetDataset,
) -> DatedParquetDataset:
    """Return existing destination DPD or a new DPD with source layout."""
    registry = CacheRegistry()
    registry.add_cache("dest", dest_cache)
    existing = registry.get_dataset(dest_name, "dest", DatasetType.DATED)
    if isinstance(existing, DatedParquetDataset):
        return existing
    return _new_destination_dpd(dest_cache, dest_name, source)


def _new_destination_dpd(
    dest_cache: Path,
    dest_name: str,
    source: DatedParquetDataset,
) -> DatedParquetDataset:
    """Create a destination DPD with the source layout."""
    config = dpd_config_from_dataset(source)
    # A local subset destination is a derived materialized dataset. Preserve the
    # physical layout, but do not inherit source update policy.
    config["start_date_str"] = None
    config["end_date_str"] = None
    config["repull_n_days"] = 0
    config["instruments"] = None
    config["lock_dir"] = None
    config["use_update_lock"] = False
    return dpd_from_metadata_config(dest_cache, dest_name, config)


def _validate_existing_destination_layout(
    dest: DatedParquetDataset,
    source: DatedParquetDataset,
) -> None:
    """Ensure an existing destination still matches source DPD layout."""
    fields = (
        "date_col",
        "date_partition",
        "partition_columns",
        "instrument_column",
        "num_instrument_buckets",
        "sort_columns",
        "row_group_size",
    )
    for field in fields:
        if getattr(dest, field) != getattr(source, field):
            raise ValidationError(
                f"Destination DPD '{dest.name}' has incompatible {field}: "
                f"{getattr(dest, field)!r} != {getattr(source, field)!r}"
            )


def _build_filters(
    source: DatedParquetDataset,
    work: DatasetWork,
) -> list[tuple[str, str, Any]] | Any | None:
    """Build date/filter/instrument filters for the source read."""
    filters = work.settings.get("filters")
    if filters is not None and not isinstance(filters, list):
        raise ValidationError("filters must be a list of filter tuples")
    return source._build_instrument_filters(work.instruments, filters)


def _validate_projection(
    source: DatedParquetDataset,
    metadata: SnapshotMetadata,
    settings: dict[str, Any],
) -> list[str] | None:
    """Validate optional projection includes required layout columns."""
    columns = settings.get("columns")
    if columns is None:
        return None
    if not isinstance(columns, list) or not all(
        isinstance(col, str) for col in columns
    ):
        raise ValidationError("columns must be a list of strings")

    required = _required_projection_columns(source)
    missing = sorted(required - set(columns))
    if missing:
        raise ValidationError(
            "columns is missing required layout column(s): "
            + ", ".join(missing)
        )

    available = set(metadata.schema.names) | set(source.partition_columns)
    unknown = sorted(set(columns) - available)
    if unknown:
        raise ValidationError(
            "columns includes unknown source column(s): " + ", ".join(unknown)
        )
    return columns


def _required_projection_columns(source: DatedParquetDataset) -> set[str]:
    """Return columns that must survive projection."""
    required = {source.date_col}
    required.update(source.partition_columns)
    if source.instrument_column is not None:
        required.add(source.instrument_column)
    if source.sort_columns:
        required.update(source.sort_columns)
    return {col for col in required if col is not None}


def _latest_local_subset_source_snapshot(
    dest: DatedParquetDataset,
) -> str | None:
    """Return the latest local subset source snapshot, if available."""
    payload = _latest_local_subset_provenance(dest)
    if payload is None:
        return None
    value = payload.get("source_snapshot")
    return value if isinstance(value, str) else None


def _latest_local_subset_spec_hash(dest: DatedParquetDataset) -> str | None:
    """Return the latest local subset spec hash, if available."""
    payload = _latest_local_subset_provenance(dest)
    if payload is None:
        return None
    value = payload.get("subset_spec_hash")
    return value if isinstance(value, str) else None


def _latest_local_subset_provenance(
    dest: DatedParquetDataset,
) -> dict[str, Any] | None:
    """Read current provenance when it belongs to local_subset."""
    try:
        payload = dest.read_provenance()
    except SnapshotNotFoundError:
        return None
    if payload.get("kind") != LOCAL_SUBSET_KIND:
        return None
    return payload


def _count_rows(
    source: DatedParquetDataset,
    work: DatasetWork,
    filters: list[tuple[str, str, Any]] | Any | None,
    columns: list[str] | None,
) -> int:
    """Count rows by performing the requested source read."""
    return _read_source_table(source, work, filters, columns).num_rows


def _read_source_table(
    source: DatedParquetDataset,
    work: DatasetWork,
    filters: list[tuple[str, str, Any]] | Any | None,
    columns: list[str] | None,
) -> pa.Table:
    """Read the selected source rows as an Arrow table."""
    return source.to_table(
        start_date=work.settings.get("start_date"),
        end_date=work.settings.get("end_date"),
        filters=filters,
        columns=columns,
        snapshot=work.source_snapshot,
    )


def _validate_info_against_base(
    dest: DatedParquetDataset,
    base: dict[str, Any],
    supplied: dict[str, Any],
) -> dict[str, Any]:
    """Apply snapshot-info validation with an explicit base mapping."""
    dest._validate_snapshot_info_keys(
        supplied, f"snapshot info for '{dest.name}'"
    )
    resolved = deepcopy(base)

    if "annotations" in supplied:
        annotations = supplied["annotations"]
        if annotations is None or not isinstance(annotations, dict):
            raise ValidationError(
                f"annotations for '{dest.name}' must be a dict when supplied"
            )
        old_annotations = resolved.get("annotations", {})
        if old_annotations is None:
            old_annotations = {}
        if not isinstance(old_annotations, dict):
            raise ValidationError(
                f"Existing annotations for '{dest.name}' must be a dict"
            )
        dest._assert_annotations_append_only(old_annotations, annotations)
        resolved["annotations"] = deepcopy(annotations)

    if "notes" in supplied:
        dest._validate_notes(supplied["notes"], f"notes for '{dest.name}'")
        resolved["notes"] = supplied["notes"]

    if "column_descriptions" in supplied:
        descriptions = supplied["column_descriptions"]
        dest._validate_column_descriptions(
            descriptions,
            f"column_descriptions for '{dest.name}'",
        )
        old_descriptions = resolved.get("column_descriptions", {})
        if old_descriptions is None:
            old_descriptions = {}
        dest._validate_column_descriptions(
            old_descriptions,
            f"Existing column_descriptions for '{dest.name}'",
        )
        for column_name in old_descriptions:
            if column_name not in descriptions:
                raise ValidationError(
                    f"column_descriptions.{column_name} cannot be removed"
                )
        resolved["column_descriptions"] = deepcopy(descriptions)

    return {
        key: value
        for key, value in resolved.items()
        if key in INFO_KEYS and value is not None
    }


def _validated_date_range(
    table: pa.Table,
    dest: DatedParquetDataset,
) -> tuple[dt.date, dt.date]:
    """Return table date range or raise when it cannot be computed."""
    min_date, max_date = _arrow_date_range(table, dest.date_col)
    if min_date is None or max_date is None:
        raise ValidationError(
            f"Local subset for '{dest.name}' has no non-null {dest.date_col}"
        )
    return (min_date, max_date)


def _partition_table(
    dest: DatedParquetDataset,
    table: pa.Table,
    min_date: dt.date,
    max_date: dt.date,
) -> tuple[list[PartitionSpec], dict[tuple[tuple[str, Any], ...], list[int]]]:
    """Build partition specs and row indices for a table."""
    if not dest.partition_columns:
        spec = PartitionSpec({}, min_date, max_date)
        return ([spec], {spec.partition_key: list(range(table.num_rows))})

    values_by_col = _partition_values_by_column(dest, table)
    grouped: dict[tuple[tuple[str, Any], ...], list[int]] = {}
    partition_values_by_key: dict[
        tuple[tuple[str, Any], ...],
        dict[str, Any],
    ] = {}

    for row_idx in range(table.num_rows):
        values = {
            col: values_by_col[col][row_idx] for col in dest.partition_columns
        }
        key = tuple(sorted(values.items()))
        grouped.setdefault(key, []).append(row_idx)
        partition_values_by_key.setdefault(key, values)

    specs = [
        PartitionSpec(partition_values_by_key[key], min_date, max_date)
        for key in sorted(grouped)
    ]
    return (specs, grouped)


def _partition_values_by_column(
    dest: DatedParquetDataset,
    table: pa.Table,
) -> dict[str, list[Any]]:
    """Return partition values for every row and partition column."""
    values: dict[str, list[Any]] = {}
    date_part_col = date_partition_column_name(
        dest.date_partition, dest.date_col
    )

    if date_part_col in dest.partition_columns:
        date_values = table.column(dest.date_col).to_pylist()
        values[date_part_col] = [
            date_partition_value(
                _require_date(value, dest.date_col),
                dest.date_partition,
            )
            for value in date_values
        ]

    if INSTRUMENT_BUCKET_COL in dest.partition_columns:
        if (
            dest.instrument_column is None
            or dest.num_instrument_buckets is None
        ):
            raise ValidationError(
                "instrument bucketing requires instrument_column"
            )
        instruments = table.column(dest.instrument_column).to_pylist()
        values[INSTRUMENT_BUCKET_COL] = [
            instrument_bucket(value, dest.num_instrument_buckets)
            for value in instruments
        ]

    for col in dest.partition_columns:
        if col in values:
            continue
        if col not in table.column_names:
            raise ValidationError(
                f"Cannot write local subset; partition column '{col}' "
                "is missing from the table"
            )
        values[col] = table.column(col).to_pylist()

    return values


def _require_date(value: Any, column: str) -> dt.date:
    """Convert a value to date or raise a clear validation error."""
    converted = to_date(value)
    if converted is None:
        raise ValidationError(f"date column '{column}' contains null values")
    return converted


def _drop_partition_columns(
    table: pa.Table,
    dest: DatedParquetDataset,
) -> pa.Table:
    """Drop partition columns before writing parquet files."""
    drop_cols = [
        col for col in dest.partition_columns if col in table.column_names
    ]
    if drop_cols:
        return table.drop(drop_cols)
    return table


def _partition_values_from_files(
    files: list[FileMetadata],
) -> dict[str, list[Any]]:
    """Build metadata partition value lists from file metadata."""
    values: dict[str, set[Any]] = {}
    for file_meta in files:
        for col, value in file_meta.partition_values.items():
            values.setdefault(col, set()).add(value)
    return {col: sorted(col_values) for col, col_values in values.items()}


def _provenance_payload(
    *,
    source: DatedParquetDataset,
    dest: DatedParquetDataset,
    source_metadata: SnapshotMetadata,
    work: DatasetWork,
    spec_file: Path,
) -> dict[str, Any]:
    """Build explicit local subset provenance."""
    filters = {
        key: jsonable(work.settings[key])
        for key in ("start_date", "end_date", "filters", "columns")
        if key in work.settings
    }
    if work.instruments is not None:
        filters["instrument_count"] = len(work.instruments)
        filters["instrument_list_hash"] = work.instrument_list_hash
        if len(work.instruments) <= LINEAGE_INSTRUMENT_LIMIT:
            filters["instruments"] = list(work.instruments)

    return {
        "kind": LOCAL_SUBSET_KIND,
        "source_cache": str(source.cache_dir),
        "source_dataset": work.source_dataset,
        "source_snapshot": work.source_snapshot,
        "source_cache_start_date": jsonable(source_metadata.cache_start_date),
        "source_cache_end_date": jsonable(source_metadata.cache_end_date),
        "dest_cache": str(dest.cache_dir),
        "dest_dataset": work.dest_name,
        "subset_spec_hash": work.spec_hash,
        "subset_spec_path": str(spec_file.resolve()),
        "materialized_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "filters": filters,
    }


def _lineage_for_subset(
    *,
    dest: DatedParquetDataset,
    work: DatasetWork,
    min_date: dt.date,
    max_date: dt.date,
    suffix: str,
) -> SnapshotLineage:
    """Build DPD lineage for a local subset snapshot."""
    requested = _requested_date_range(work, min_date, max_date)
    instruments = work.instruments
    if dest.instrument_column is None:
        instrument_scope = "unknown"
        lineage_instruments = None
    elif instruments is None:
        instrument_scope = "all"
        lineage_instruments = None
    else:
        instrument_scope = "subset"
        lineage_instruments = (
            instruments
            if len(instruments) <= LINEAGE_INSTRUMENT_LIMIT
            else None
        )

    return SnapshotLineage(
        base_snapshot=work.source_snapshot,
        first_snapshot_id=dest.current_suffix or suffix,
        operation=LOCAL_SUBSET_KIND,
        requested_date_range=requested,
        added_date_ranges=[DateRange(min_date, max_date)],
        rewritten_date_ranges=[],
        instrument_column=dest.instrument_column,
        instrument_scope=instrument_scope,
        instruments=lineage_instruments,
    )


def _requested_date_range(
    work: DatasetWork,
    min_date: dt.date,
    max_date: dt.date,
) -> DateRange:
    """Return requested date range when supplied, else written date range."""
    start = work.settings.get("start_date")
    end = work.settings.get("end_date")
    start_date = to_date(start) if start is not None else min_date
    end_date = to_date(end) if end is not None else max_date
    if start_date is None or end_date is None:
        return DateRange(min_date, max_date)
    return DateRange(start_date, end_date)


def _log_results(
    results: list[LocalSubsetResult],
    verbose: bool,
) -> None:
    """Log per-dataset local subset results."""
    for result in results:
        dest = result.dest_dataset or "<unknown>"
        if result.status == "published":
            logger.info(
                f"{result.source_dataset} -> {dest}: "
                f"published {result.dest_snapshot}"
            )
        elif result.status == "skipped":
            logger.info(f"{result.source_dataset} -> {dest}: skipped")
        elif result.status == "publish":
            logger.info(f"{result.source_dataset} -> {dest}: would publish")
        else:
            logger.error(
                f"{result.source_dataset} -> {dest}: error: {result.message}"
            )

        if verbose:
            logger.info(f"  dest_cache: {result.dest_cache}")
            logger.info(f"  source_snapshot: {result.source_snapshot}")
            logger.info(
                "  latest_local_source_snapshot: "
                f"{result.latest_local_source_snapshot}"
            )
            logger.info(f"  spec_hash: {result.spec_hash}")
            if result.rows is not None:
                logger.info(f"  rows: {result.rows}")


if __name__ == "__main__":
    main()
