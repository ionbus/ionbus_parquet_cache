# Ionbus Parquet Dataset System

<!-- cSpell: ignore rels QYYYY MYYYY WYYYY -->

<!-- TOC start -->

- [Overview](#overview)
- [Implementation Invariants](#implementation-invariants)
- [Update State Machine](#update-state-machine)
- [Acceptance Criteria (MVP)](#acceptance-criteria-mvp)
- [Implementation Readiness Checklist](#implementation-readiness-checklist)
- [Cache Directory Layout](#cache-directory-layout)
- [Core Classes](#core-classes)
    * [`CacheRegistry`](#cacheregistry)
    * [`ParquetDataset` (base class)](#parquetdataset-base-class)
    * [`DatedParquetDataset`](#datedparquetdataset)
    * [`NonDatedParquetDataset`](#nondatedparquetdataset)
- [Snapshot Info and Provenance](#snapshot-info-and-provenance)
- [YAML Configuration](#yaml-configuration)
- [Credentials and Secrets](#credentials-and-secrets)
- [Data Source Interface](#data-source-interface)
    * [`DataSource` abstract class](#datasource-abstract-class)
    * [Implementing a `DataSource`](#implementing-a-datasource)
    * [Built-in Data Sources](#built-in-data-sources)
    * [Installed Module Data Sources](#installed-module-data-sources)
- [Data Cleaner Interface](#data-cleaner-interface)
    * [`DataCleaner` abstract class](#datacleaner-abstract-class)
    * [Implementing a `DataCleaner`](#implementing-a-datacleaner)
    * [Installed Module Data Cleaners](#installed-module-data-cleaners)
- [Schema Management](#schema-management)
- [Reading Data](#reading-data)
- [Updating Data](#updating-data)
    * [DuckDB-Based Update Processing](#duckdb-based-update-processing)
- [Partition Structure on Disk](#partition-structure-on-disk)
- [CLI Tools](#cli-tools)
    * [`update-cache`](#update-cache)
    * [`import-npd`](#import-npd)
    * [`local-subset`](#local-subset)
    * [`cleanup-cache`](#cleanup-cache)
        * [Trimming (Dangerous Operation)](#trimming-dangerous-operation)
        * [Finding orphaned files](#finding-orphaned-files)
    * [`sync-cache`](#sync-cache)
    * [`yaml-create-datasets`](#yaml-create-datasets)
- [Future Work: Auto-Partitioning for Large Datasets](#future-work-auto-partitioning-for-large-datasets)
- [Comparison with `core_utils/parquet_cache`](#comparison-with-core_utilsparquet_cache)

<!-- TOC end -->

---

## Overview

This system manages local parquet files for time-series data. It will
become the `ionbus_parquet_cache` pip/conda package. This system also
provides snapshot access to non-dated data (NPDs). For NPDs, this package
manages storage and snapshots; source data is produced externally and
imported via `python -m ionbus_parquet_cache.import_npd` or
`import_snapshot()`.

It is a recreation of `core_utils/parquet_cache` adapted to use
`ionbus_utils`. Any `core_utils` functions not yet in `ionbus_utils`
will be added there.

Every dated dataset must have a date column. The dataset is always read via a
PyArrow dataset object (never by re-scanning the directory on every
access).

**Key design goals:**

- Data is always read through a `pyarrow.dataset.Dataset` object,
  making reads fast and allowing DuckDB / Polars to use the dataset
  directly.
- The only data source is a `DataSource` subclass (built-in or user-supplied).
- The `DatedParquetDataset` will be described by a YAML file (and read using ionbus_utils' PDYaml)
- Schema changes are managed: new columns are additive and safe;
  incompatible type changes raise an error.

## Implementation Invariants

These are mandatory behaviors for the initial implementation:

- Reads are snapshot-based:
  - **DPDs:** Reads come from snapshot metadata in `_meta_data/`, not from scanning data directories.
  - **NPDs:** Reads come from the current snapshot file/directory in `non-dated/`, not from scanning data directories.
  - Discovery is by scanning snapshot locations (`_meta_data/` for DPDs,
    top level of `non-dated/` for NPDs), never data directories. Sidecars
    augment snapshots but do not define them; an NPD snapshot exists by its
    suffixed file or directory, not by optional info/provenance sidecars.
- Snapshot publish is atomic: readers either see the old snapshot or the new snapshot, never a partial state.
- Single-writer: a DPD must only be updated by one process at a time. Concurrent updates to the same DPD are not supported and may corrupt the cache. `DatedParquetDataset.update()` enforces this with a `<name>_update.lock` file written at the start of every update and removed on completion (success or failure). If a lock already exists, the update raises `UpdateLockedError` with the locking host, PID, age, and instructions for clearing it (`dpd.clear_update_lock(force=True)` or `rm <lock_path>`). The lock file is created atomically (exclusive open) to avoid race conditions. Stale locks from crashed processes can be cleared via `clear_update_lock(force=False)`, which checks whether the locking PID is still alive on the local host before deleting. Lock file location defaults to the dataset directory but can be overridden via `lock_dir` (useful when the dataset directory is read-only or GCS-backed). Locking can be disabled entirely with `use_update_lock=False` for use cases where single-writer is guaranteed by convention (e.g. a scheduled Cloud job).
- Snapshot ids are monotonic: later updates must produce lexicographically larger suffix ids. (With single-writer, second-granularity timestamps are sufficient. If a new snapshot would have the same timestamp as the current snapshot, the update fails with "snapshot will be duplicated." This is extremely unlikely in practice.)
- Partition values are virtual: partition columns are injected from snapshot metadata/path and are not required in parquet payload columns.
- A failed update must not change the current snapshot.
- `DatedParquetDataset.pyarrow_dataset()`
    - is created from information stored in its metadata
    - is cached per instance
    - is invalidated only when snapshot changes.
        - It is only invalidated in the instance that creates the new snapshot. Any other instances that were reading the old snapshot will not have that snapshot invalidated and replaced unless they explicitly ask it to be done.

**Exception - Trimming:** The trimming operation violates some of these
invariants by renaming files in place. Trimming must only be performed
when no readers are active. See [Trimming (Dangerous Operation)](#trimming-dangerous-operation).

## Update State Machine

The update flow should be implemented as a deterministic state machine:

1. `discover`: load config, source date bounds, current snapshot, and target update window.
2. `fetch`: request source data (optionally in batches) for target partitions/dates.
3. `transform`: apply YAML transformations and optional `DataCleaner` on DuckDB relations.
4. `write_temp`: write new parquet files to temporary paths and compute checksums/size.
5. `validate`: verify schema compatibility and file integrity for all new outputs.
6. `collect_provenance`: call the DataSource provenance hook and write an
   external sidecar only if it returns a non-empty dictionary.
7. `publish_snapshot`: write new snapshot metadata and atomically promote it to current.
8. `finalize`: best-effort cleanup of orphan temp files and orphan
   provenance sidecars from failed publishes.

Failure behavior:

- Previous snapshot is never touched/modified during an update.
    - Only separate cleaning (described below) cleans up old snapshots.
    - If updated snapshot fails before publish, it can be cleaned up (again by separate cleaning procedure)
    - if updated snapshot fails after publish, but before cleaning, this too can be cleaned up (again by separate cleaning procedure)

## Acceptance Criteria (MVP)

### Read path

- Reading with no filters returns exactly rows referenced by current snapshot files.
- Date-filtered reads prune partitions correctly for all date partition modes.
- `read_data()` and `read_data_pl()` return equivalent row sets and column values.

### Update path

- Normal update appends/replaces only targeted partitions for the computed window.
- `--dry-run` performs no writes and reports planned partition/file changes.
- `--restate` fully rewrites affected partitions and removes superseded files from new snapshot.
  With `--instruments`, this is allowed only when `instrument_column` is a partition column;
  then restate is limited to the specified instrument partitions.
- Instrument-limited update touches only requested instruments.

### Schema evolution

- Additive columns succeed without full dataset rebuild.
- Incompatible type changes fail with clear error showing column and old/new types.
- Snapshot is not advanced when schema validation fails.
- Optional `annotations`, `notes`, and `column_descriptions` are stored as
  snapshot info. For DPDs, they live in the captured YAML
  configuration. For NPDs, they live in the optional NPD info sidecar
  `non-dated/{name}/_meta_data/{name}_{suffix}.pkl.gz`.
  Missing fields carry forward from the previous snapshot; explicit values are
  validated according to each field's rules. They are readable through the
  common `ParquetDataset` accessors.

### Snapshot/cleanup/sync

- Current snapshot selection is deterministic (lexicographically last suffix).
- `cleanup-cache` in snapshot cleanup mode generates a script for human review; trim mode immediately mutates state (see [Trimming](#trimming-dangerous-operation)).
- `sync-cache` transfers data, DPD snapshot metadata, NPD info sidecars, and
  referenced snapshot provenance sidecars only (not `yaml/`, not `code/`).
  Supports DPDs only, NPDs only, or both.
- `python -m ionbus_parquet_cache.import_npd` imports a parquet file or
  parquet directory as a complete `NonDatedParquetDataset` snapshot. The
  imported snapshot becomes the current NPD snapshot and is discoverable by
  `CacheRegistry`.
- NPD imports may write optional snapshot info and optional provenance
  sidecars. Snapshot info carries forward field-by-field when omitted.
  Provenance is never carried forward.

## Implementation Readiness Checklist

- Define module layout and ownership (`cache_registry.py`, `dated_dataset.py`, `non_dated_dataset.py`, `snapshot_store.py`).
- Define exception taxonomy (`SchemaMismatchError`, `SnapshotPublishError`, `DataSourceError`, `SyncError`).
- Define atomic write strategy for local FS and S3-backed sync targets.
- Define logging and metrics contract (rows fetched/written, partitions touched, bytes written, duration, snapshot id).
- Define CLI exit codes for automation (success, validation failure, partial sync failure, usage error).
- Build integration fixtures with tiny datasets covering day/week/month/quarter/year partitions and one multi-partition dataset.
- Add failure-injection tests around crash points in `write_temp` and `publish_snapshot`.
- Add migration notes from `core_utils/parquet_cache` to this package, with feature mapping and known incompatibilities.

---

## Cache Directory Layout

The cache is a directory containing:
- A `yaml/` subdirectory with YAML files describing `DatedParquetDataset`s
  (one or more DPDs per file)
- A `code/` subdirectory with cache-local `DataSource`, `DataCleaner`, and
  sync-function implementations (custom Python files); built-in sources and
  installed package modules are referenced separately (see
  [Data Source Interface](#data-source-interface) and
  [Data Cleaner Interface](#data-cleaner-interface))
- A `non-dated/` subdirectory for `NonDatedParquetDataset` snapshots
- One data subdirectory per `DatedParquetDataset`

```
{cache_root}/
    yaml/
        futures.yaml               <- can define multiple DPDs
        equities.yaml
    code/
        futures_source.py          <- cache-local DataSource implementations
        equity_source.py
        cleaners.py                <- DataCleaner implementations
    non-dated/                     <- NonDatedParquetDataset snapshots
        instrument_master/         <- single-file NPD
            instrument_master_1H4DW00.parquet
            instrument_master_1H4DW01.parquet   <- current snapshot
            _meta_data/            <- optional NPD snapshot info/provenance refs
                instrument_master_1H4DW01.pkl.gz
            _provenance/           <- optional per-snapshot provenance
                instrument_master_1H4DW01.provenance.pkl.gz
        calendar_data/             <- hive-partitioned NPD
            calendar_data_1H4DW00/              <- old snapshot (directory)
                exchange=NYSE/
                    data.parquet
                exchange=CME/
                    data.parquet
            calendar_data_1H4DW01/              <- current snapshot (directory)
                exchange=NYSE/
                    data.parquet
                exchange=CME/
                    data.parquet
    md.futures_daily/              <- data for md.futures_daily (year-partitioned)
        _meta_data/
            md.futures_daily_1H4DW00.pkl.gz
            md.futures_daily_1H4DW01.pkl.gz   <- current snapshot (largest suffix)
        _provenance/               <- optional large per-snapshot provenance
            md.futures_daily_1H4DW01.provenance.pkl.gz
        FutureRoot=ES/
            year=Y2023/FutureRoot=ES_year=Y2023_1H4DW00.parquet
            year=Y2024/FutureRoot=ES_year=Y2024_1H4DW00.parquet
        FutureRoot=NQ/
            year=Y2023/FutureRoot=NQ_year=Y2023_1H4DW00.parquet
            year=Y2024/FutureRoot=NQ_year=Y2024_1H4DW01.parquet   <- updated in second run
    md.minute_bars/                <- data for md.minute_bars (day-partitioned)
        _meta_data/
            md.minute_bars_1H4DW01.pkl.gz
        ticker_letter=A/
            2024/                  <- navigation dir
                02/                <- navigation dir
                    Date=2024-02-27/ticker_letter=A_Date=2024-02-27_1H4DW01.parquet
        ...
```

The suffix (e.g., `1H4DW00`) is a base-36 encoded count of seconds since
epoch, using digits and uppercase letters (`0-9A-Z`). The suffix is exactly
7 characters (valid through April 5, 4453 — 36^7 seconds from Unix epoch),
ensuring lexicographic ordering matches chronological ordering. Because the
same suffix is applied to both the new data files and the metadata pickle
written in the same update run, the pickle filename alone identifies all
files produced in that run.

The cache is loaded by pointing to the cache root directory.

**Discovery vs. configuration:**

| Aspect | Based on | Required for |
|---|---|---|
| **Discovery** | Directory structure and `_meta_data/` | Reading data |
| **Configuration** | `yaml/` files | Updating data |

- **DPD discovery:** DPDs are discovered by scanning for `_meta_data/`
  directories containing snapshot metadata. The directory name is the
  dataset name. No yaml files are needed to read data.
- **NPD discovery:** NPDs are discovered by scanning the top level of
  `non-dated/` for snapshot files (`name_suffix.parquet`) or directories
  (`name_suffix/`). `_meta_data/` and `_provenance/` under an NPD directory
  are sidecar directories and must not be treated as snapshots. No yaml files
  are needed to read NPD data or snapshot info.
- **yaml/ files:** Only needed when updating data. They define the
  `DataSource` class and configuration for how to fetch new data.
- **code/ files:** Only needed when updating data with cache-local code. They
  contain `DataSource`, `DataCleaner`, and sync-function implementations
  referenced by yaml files. Equivalent shared implementations can live in
  installed/importable modules and be referenced with `module://`.

This means a synced cache (data + snapshot metadata, plus any referenced
provenance sidecars) is fully readable without the yaml/ or code/ directories.

**Separate git repos for `yaml/` and `code/`:**

The `yaml/` and `code/` directories can be managed in separate git
repositories. This is useful when:
- You want to share one `code/` repo across multiple caches
- Different teams own the YAML configuration vs. the DataSource code
- You want different versioning/release cycles for config vs. code
- Readers only need the synced data; updaters clone the config repos

Example setup with shared code:
```
/data/caches/
    futures_cache/
        yaml/                      <- git repo: futures-config
        code/                      <- git repo: shared-datasources (checked out here)
        md.futures_daily/
        ...
    equities_cache/
        yaml/                      <- git repo: equities-config
        code/                      <- git repo: shared-datasources (same repo, checked out here too)
        md.equities/
        ...
```

The `source_location` field in YAML supports three modes:
- **Built-in source (blank/empty):** Uses a built-in `DataSource` from
  `ionbus_parquet_cache` (e.g., `HiveParquetSource`, `DPDSource`)
- **Installed package source:** `module://my_library.data_sources` (loads from
  an installed Python package)
- **Cache-local source (file path):** Relative to cache root
  (e.g., `code/futures_source.py`, recommended) or absolute path

The `cleaning_class_location` field uses the same installed-module or file-path
resolution for `DataCleaner` classes, except there is no built-in cleaner
namespace: if `cleaning_class_name` is set, `cleaning_class_location` must be a
cache-local/absolute Python file or a `module://` import path.

See [YAML fields](#yaml-fields) for details.

---

## Core Classes

### `CacheRegistry`

Replaces `ParquetMetaCache`. A singleton that manages multiple cache
locations and searches them in priority order when reading data.

**Use case:** A user may have:
- A local cache on fast SSD for personal datasets
- A shared team cache on network storage
- A firm-wide official cache

The registry searches these in order and returns data from the first
cache that contains the requested dataset.

**Usage:**

```python
from ionbus_parquet_cache import CacheRegistry

# Register multiple cache locations (searched in order)
registry = CacheRegistry.instance(
    local="c:/Users/me/parquet_cache",
    team="w:/team/parquet_cache",
    official="w:/firm/parquet_cache",
)

# Read data -- searches local, then team, then official
df = registry.read_data("md.futures_daily", start_date="2024-01-01")

# Get the PyArrow dataset
dataset = registry.pyarrow_dataset("md.futures_daily")

# See what's available across all caches
registry.data_summary()

# Read from a specific cache only
df = registry.read_data("md.futures_daily", cache_name="official")
```

**Key methods:**

| Method | Description |
|---|---|
| `instance(**named_paths)` | Get or create the singleton, registering cache paths |
| `reset()` | Reset the singleton instance (mainly for testing) |
| `add_cache(name, path)` | Register an additional cache location |
| `get_dataset(name, cache_name=None, dataset_type=None)` | Get a specific dataset object (see below for `dataset_type`) |
| `read_data(name, filters=None, ...)` | Read data, returning a `pd.DataFrame` |
| `read_data_pl(name, filters=None, ...)` | Read data, returning a `pl.DataFrame` |
| `to_table(name, filters=None, ...)` | Read data, returning a PyArrow `Table` |
| `pyarrow_dataset(name, start_date=None, end_date=None, filters=None, cache_name=None, snapshot=None)` | Get PyArrow dataset from first matching cache |
| `get_latest_snapshot(name, cache_name=None)` | Return the latest snapshot suffix string for a dataset |
| `cache_history(name, cache_name=None, snapshot=None)` | Return snapshot lineage history, walking backward from the current or specified snapshot |
| `read_provenance(name, cache_name=None, snapshot=None)` | Explicitly load the optional external provenance sidecar for a dataset snapshot |
| `data_summary()` | DataFrame summarizing all datasets across all caches |
| `discover_dpds(cache_path)` | Discover DPDs in a cache by scanning for `_meta_data` directories |
| `discover_npds(cache_path)` | Discover NPDs by scanning the `non-dated` directory |
| `refresh_all()` | Refresh all cached datasets; returns True if any refreshed |

**`dataset_type` parameter for `get_dataset()`:**

The `dataset_type` parameter controls which type of dataset to look for:

| Value | Behavior |
|---|---|
| `None` (default) | Look for DPD first, then NPD |
| `DatasetType.DATED` | Only look for `DatedParquetDataset` |
| `DatasetType.NON_DATED` | Only look for `NonDatedParquetDataset` |

```python
from ionbus_parquet_cache import CacheRegistry, DatasetType

# Get any dataset (DPD preferred)
ds = registry.get_dataset("my_dataset")

# Get only a DPD
dpd = registry.get_dataset("my_dataset", dataset_type=DatasetType.DATED)

# Get only an NPD
npd = registry.get_dataset("my_dataset", dataset_type=DatasetType.NON_DATED)
```

**Behavior:**

- The registry is a singleton; calling `instance()` multiple times
  returns the same object (with any new paths added).
- Cache locations are searched in the order they were registered.
- The `cache_name` parameter on read methods allows targeting a
  specific cache, bypassing the search order.
- Each cache location is loaded lazily on first access.

**Lookup order within each cache:**

When searching for a dataset by name, the registry checks each cache
directory in registration order. Within each cache, it looks for:

1. A `DatedParquetDataset` (DPD) with that name
2. If not found, a `NonDatedParquetDataset` (NPD) with that name
3. If neither found, move to the next cache directory

```python
# Searches: local DPD -> local NPD -> team DPD -> team NPD -> official DPD -> official NPD
df = registry.read_data("instrument_master")
```

**Warning: Avoid name collisions between DPDs and NPDs.** If a name
exists as a DPD in one cache and an NPD in another, the lookup behavior
may be confusing (the DPD will shadow the NPD if it appears in an
earlier cache). Use naming conventions to prevent this:

- DPDs may use many namespaces (e.g., `md.*`, `analytics.*`, `signals.*`)
- Reserve specific namespaces for NPDs to clearly identify them
  (e.g., `ref.*` for reference data, `static.*` for static lookups)

---

### `ParquetDataset` (base class)

Abstract base class for both `DatedParquetDataset` and `NonDatedParquetDataset`.
Provides the common interface for reading parquet data with snapshot versioning.

**Key attributes (common to all subclasses):**

| Attribute | Type | Description |
|---|---|---|
| `name` | `str` | Unique name for this dataset |
| `cache_dir` | `Path` | Root directory of the cache |
| `current_suffix` | `str` | Suffix of the current snapshot (e.g., `"1H4DW01"`) |
| `schema` | `pa.Schema` | PyArrow schema (read from parquet files on demand; may be cached in metadata for DPDs) |

**Key methods (common to all subclasses):**

| Method | Description |
|---|---|
| `pyarrow_dataset(filters=None)` | Returns the `pyarrow.dataset.Dataset` for the current snapshot. Cached after first construction. |
| `read_data(filters=None, columns=None, ...)` | Read data into a `pd.DataFrame`. Subclasses may add parameters (e.g., `start_date`/`end_date` for DPDs). |
| `read_data_pl(filters=None, columns=None, ...)` | Read data into a `pl.DataFrame` (Polars). |
| `is_update_available()` | Returns `True` if a newer snapshot exists on disk than currently loaded. |
| `refresh()` | Invalidate cached dataset and load the latest snapshot. Returns `True` if refreshed, `False` if already current. |
| `get_annotations(snapshot=None)` | Return a copy of the snapshot's user annotations dictionary, or `{}` when absent. |
| `get_notes(snapshot=None)` | Return the snapshot's notes string, or `None` when absent. An explicit empty string is returned as `""`. |
| `get_column_descriptions(snapshot=None)` | Return a copy of the snapshot's column description dictionary, or `{}` when absent. |
| `read_provenance(snapshot=None)` | Explicitly load the optional external provenance sidecar for the current or requested snapshot. Returns `{}` when absent. |
| `summary()` | Returns a dict with dataset metadata (name, date range, schema info, etc.). |

**Dataset structure:**

Datasets can take different forms:
- **DPDs always have metadata:** Schema and file manifest are stored in a
  `_meta_data/` pickle under the DPD directory. This is required for DPDs.
- **NPD data snapshots are self-describing:** a snapshot is the suffixed
  parquet file or suffixed directory under `non-dated/{name}/`. Optional NPD
  snapshot info is stored in a sidecar file under
  `non-dated/{name}/_meta_data/`, and optional provenance is stored under
  `non-dated/{name}/_provenance/`. These sidecars augment snapshots but do not
  define them; they do not store schemas, file manifests, checksums, sizes, or
  lineage.

The `schema` property always returns the PyArrow schema regardless of how
it's stored or retrieved.

**Shared snapshot-info responsibilities:**

The base class owns the public API and validation semantics for snapshot info:

- `notes`
- `annotations`
- `column_descriptions`

It also owns the public `read_provenance()` API when a subclass exposes a
snapshot provenance reference. Subclasses own storage-specific hooks:

- DPDs load snapshot info from `SnapshotMetadata.yaml_config`.
- NPDs load snapshot info from their optional NPD info sidecar.
- DPDs and NPDs both expose provenance through a `SnapshotProvenanceRef`, but
  they write that reference through different publish/import paths.

Internal validation helpers should use consistent error message shapes:

- `<context> must be a dict, got <type>`
- `<context> must be a string, got <type>`
- `<context> keys and values must be strings`

**Snapshot versioning:**

Both DPDs and NPDs use suffix-based versioning. The suffix (e.g., `1H4DW00`)
is a base-36 encoded timestamp ensuring lexicographic ordering matches
chronological ordering. The `current_suffix` property returns the suffix
of the currently loaded snapshot; `is_update_available()` checks if a
newer suffix exists on disk.

**Usage pattern:**

```python
# Works with both DPDs and NPDs
def process_dataset(ds: ParquetDataset) -> pd.DataFrame:
    if ds.is_update_available():
        ds.refresh()
    return ds.read_data()
```

---

### `DatedParquetDataset`

Inherits from `ParquetDataset`. Manages a parquet dataset that is partitioned
by date (and optionally by additional columns).

**Responsibilities:**

- Knows where the data lives on disk (`cache_dir`).
- Knows the partition structure: date partition granularity
  (`day`, `week`, `month`, `quarter`, or `year`) and any additional partition
  columns (e.g., `ticker_letter`, `FutureRoot`).
- Knows the name of the date column (default `"Date"`).
- Knows the sort columns within a partition file.
- Validates the parquet schema (may cache in metadata, or read from files).
- Constructs and caches a `pyarrow.dataset.Dataset` covering all
  current partition files.
- Delegates data supply to a `DataSource` subclass when updates are
  requested.

**DPD-specific attributes** (in addition to base class attributes):

| Attribute | Type | Description |
|---|---|---|
| `date_col` | `str` | Name of the date column (default `"Date"`) |
| `date_partition` | `str` | Granularity: `"day"`, `"week"`, `"month"`, `"quarter"`, `"year"` |
| `partition_columns` | `list[str]` | All partition columns in order (see note below) |
| `sort_columns` | `list[str] \| None` | Sort order within a partition file. Defaults to `None`, which is set to `[date_col]` in `model_post_init`. |
| `description` | `str` | Human-readable description |
| `start_date_str` | `str \| None` | Earliest date to request from source (mostly for debugging) |
| `end_date_str` | `str \| None` | Latest date to request from source (mostly for debugging) |
| `repull_n_days` | `int` | How many trailing business days to re-fetch on each update (for corrections) |
| `instrument_column` | `str \| None` | Column containing instrument identifiers (e.g., `FutureRoot`, `ticker`). Required for `--instruments` CLI flag to work. Can be used for filtering at read time and does not need to be a partition column. |
| `instruments` | `list[str] \| None` | Instruments to include in updates. If `None`, fetches all instruments. |

**DPD-specific methods** (in addition to base class methods):

- `read_data(start_date, end_date, filters=None, columns=None)` -- extends
  base class with required `start_date` and `end_date` parameters. Filters
  are applied before reading for partition pruning.
- `read_data_pl(start_date, end_date, filters=None, columns=None)` -- Polars
  version, also requires date range.
- `update(source, start_date=None, end_date=None, instruments=None,
  dry_run=False, cleaner=None, backfill=False, restate=False,
  transforms=None, yaml_config=None)` --
  updates the cache using the given `DataSource`. If dates are not specified,
  computes the window from
  `source.available_dates()` and the cache's current state. Additional
  parameters: `dry_run` computes the plan but writes no files; `cleaner`
  applies a `DataCleaner` to the data; `transforms` is a dict of YAML
  transform settings (columns_to_rename, columns_to_drop, dropna_columns,
  dedup_columns, dedup_keep); `yaml_config` stores the full YAML configuration
  in snapshot metadata, including watched user metadata such as
  `annotations`, `notes`, and `column_descriptions`.
  External snapshot provenance comes from the DataSource provenance hook, not
  from an `update()` argument.
- `get_partition_values(column)` -- returns the list of distinct values
  for a partition column from the current snapshot metadata. Useful for
  DataSources to know what partition values exist in the cache.
- `create_source_from_metadata()` -- creates a `DataSource` from the
  stored `yaml_config` in snapshot metadata. Allows updating the dataset
  without the original YAML file, as long as the source class can be loaded.
  Raises `SnapshotNotFoundError` if no metadata exists, or `ConfigurationError`
  if the source class cannot be loaded.
- `create_cleaner_from_metadata()` -- creates a `DataCleaner` from the
  stored `yaml_config` in snapshot metadata. Returns `None` if no cleaner
  is configured. Raises `SnapshotNotFoundError` if no metadata exists, or
  `ConfigurationError` if the cleaner class cannot be loaded.
- `get_transforms_from_metadata()` -- retrieves YAML transform settings
  from the stored metadata. Returns a dict with keys `columns_to_rename`,
  `columns_to_drop`, `dropna_columns`, `dedup_columns`, `dedup_keep`, or
  `None` if no transforms are configured.
- `cache_history(snapshot=None)` -- returns snapshot lineage history,
  walking backward from the current snapshot or the specified snapshot.

Each `DatedParquetDataset` is defined by a YAML file in the `yaml` subdirectory of the cache root.
See [YAML Configuration](#yaml-configuration) for the full format.

---

### `NonDatedParquetDataset`

Inherits from `ParquetDataset`. Represents a parquet file or hive-partitioned
directory that does not have date-based partitioning. Unlike `DatedParquetDataset`,
NPDs are updated by importing complete snapshots rather than incremental updates.

All `NonDatedParquetDataset` snapshots live in the `non-dated/` subdirectory
of the cache root. Each NPD has its own subdirectory containing versioned
snapshots:

```
non-dated/
    instrument_master/                    <- single-file NPD
        instrument_master_1H4DW00.parquet  <- old snapshot
        instrument_master_1H4DW01.parquet  <- current snapshot (largest suffix)
        _meta_data/
            instrument_master_1H4DW01.pkl.gz  <- optional snapshot info/provenance ref
        _provenance/
            instrument_master_1H4DW01.provenance.pkl.gz  <- optional provenance
    calendar_data/                        <- hive-partitioned NPD
        calendar_data_1H4DW00/             <- old snapshot (directory)
            exchange=NYSE/data.parquet
            exchange=CME/data.parquet
        calendar_data_1H4DW01/             <- current snapshot (directory)
            exchange=NYSE/data.parquet
            exchange=CME/data.parquet
```

**Use cases:**

- Reference data (e.g., instrument master, exchange calendars).
- Static lookup tables.
- Any parquet data that is replaced wholesale rather than updated by date.

**NPD-specific attributes** (in addition to base class attributes):

| Attribute | Type | Description |
|---|---|---|
| `npd_dir` | `Path` | Path to the NPD directory under `non-dated/` |

**NPD-specific methods** (in addition to base class methods):

- `import_snapshot(source_path, info=None, provenance=None)` -- imports a new
  snapshot from a file or directory, with optional snapshot info and optional
  per-snapshot provenance. This is the required NPD import
  contract; implementations must not treat `info` or `provenance` as
  DPD-style operational metadata.
- `read_data(filters=None, columns=None)` -- same as base class, but raises
  `ValueError` if `start_date` or `end_date` are passed
- `read_data_pl(filters=None, columns=None)` -- Polars version, same restriction

**Importing snapshots:**

From the command line:

```bash
# Import from a single parquet file
python -m ionbus_parquet_cache.import_npd \
    /path/to/cache ref.instrument_master /source/path/instruments.parquet

# Import from a directory containing parquet files
python -m ionbus_parquet_cache.import_npd \
    /path/to/cache ref.exchange_calendar /source/path/calendar_data/
```

From Python:

```python
npd = NonDatedParquetDataset("/path/to/cache", "instrument_master")

# Import from a single parquet file
npd.import_snapshot("/source/path/instruments.parquet")

# Import from a hive-partitioned directory
npd.import_snapshot("/source/path/calendar_data/")
```

The `import_snapshot()` method:
- Copies the source file or directory into the NPD directory
- Assigns a new snapshot suffix (base-36 timestamp)
- For directories, performs a recursive copy preserving structure
- Resolves and writes optional snapshot info
- Writes optional provenance only when explicitly supplied
- The new snapshot becomes the current snapshot

Resolving snapshot info means loading the previous NPD info sidecar when one
exists, validating any newly supplied fields, carrying forward omitted
`notes`, `annotations`, and `column_descriptions`, and writing the normalized
info for the new suffix. Provenance is deliberately excluded from this
carry-forward behavior.

The `python -m ionbus_parquet_cache.import_npd` command is the CLI wrapper
around this same operation. It is intended for externally produced reference
data where the parquet cache should manage versioning but should not fetch or
transform the source.

**NPD discovery and structure:**

NPDs are discovered by scanning only the **top level** of the `non-dated/`
directory. The scan finds:
- **Single-file NPDs:** `name_suffix.parquet` files
- **Hive-partitioned NPDs:** `name_suffix/` directories (the scan does not
  recurse into hive partition subdirectories like `exchange=NYSE/`)

The current snapshot is simply the file or directory with the largest suffix.
The `_meta_data/` and `_provenance/` directories under an NPD directory are
sidecar directories and must not be treated as snapshot directories.

**NPD snapshot info sidecars:** NPDs may have optional per-snapshot info
sidecars at `non-dated/{name}/_meta_data/{name}_{suffix}.pkl.gz`. The file is
a gzip-compressed pickle containing only snapshot info and an optional
provenance reference. It is not JSON or YAML. It does not store schema, file
manifests, checksums, byte sizes, source labels, imported timestamps, or
lineage. The data snapshot itself remains the suffixed file or directory.

Legacy NPD snapshots without an info sidecar are valid. Their snapshot info is
treated as absent: `get_notes()` returns `None`, `get_annotations()` returns
`{}`, `get_column_descriptions()` returns `{}`, and `read_provenance()`
returns `{}`.

**Reading data:**

NPDs use the same read methods as DPDs, but **do not accept date parameters**.
Passing `start_date` or `end_date` raises a `ValueError`:

```python
# Returns pyarrow.dataset.Dataset
dataset = npd.pyarrow_dataset(filters=[("exchange", "=", "NYSE")])

# Read into pandas DataFrame
df = npd.read_data(filters=[("exchange", "=", "NYSE")])

# Read into Polars DataFrame
df_pl = npd.read_data_pl(columns=["symbol", "name", "exchange"])

# ERROR: NPDs do not support date filtering
df = npd.read_data(start_date="2024-01-01")  # raises ValueError
```

**PyArrow dataset caching:**

The PyArrow dataset is constructed on the fly from the current snapshot
file or directory (not from pre-cached metadata like DPDs), but once
constructed it is cached for subsequent reads. The cache is invalidated when `refresh()`
is called or when a new snapshot is detected.

**Detecting and loading new snapshots:**

Like DPDs, NPDs support `is_update_available()` and `refresh()`:

```python
# Check if a newer snapshot exists
if npd.is_update_available():
    npd.refresh()  # Returns True, loads latest snapshot

# Or simply refresh (returns False if already current)
if npd.refresh():
    print("Loaded new snapshot!")
```

After `refresh()`, the next call to `pyarrow_dataset()`, `read_data()`,
or `read_data_pl()` will construct a new dataset from the latest snapshot.

---

## Snapshot Info and Provenance

Snapshot info is small, reader-facing context that travels with a specific cache
snapshot and helps readers understand the dataset. It is separate from
operational metadata such as DPD schemas, DPD file manifests, update lineage,
and cache date ranges.

The supported snapshot-info fields are:

| Field | Type | Missing value returned by accessor | Update/import semantics |
|---|---|---|---|
| `notes` | `str` | `None` | Optional free-form text. Missing values carry forward from the previous snapshot. Explicit strings replace the previous value. `""` is allowed and means "intentionally cleared"; `null` is rejected. |
| `annotations` | `dict[str, Any]` | `{}` | Optional structured user metadata. Missing values carry forward. Explicit dictionaries may add keys, including nested keys, but may not remove existing keys or change existing values. |
| `column_descriptions` | `dict[str, str]` | `{}` | Optional per-column human-readable descriptions. Missing values carry forward. Explicit dictionaries may add keys or change text, but may not remove existing keys. |

The accessor return types intentionally differ:

```python
annotations = ds.get_annotations()          # dict[str, Any], {} if missing
notes = ds.get_notes()                      # str | None, None if missing
descriptions = ds.get_column_descriptions() # dict[str, str], {} if missing
```

For notes, `None` means the snapshot has no notes field. An empty string means
notes were explicitly set to `""`, which is the supported way to clear visible
notes without deleting the field from the lineage.

The common `ParquetDataset` implementation should own these public accessors,
the validation helpers, and the carry-forward/merge rules. Subclasses provide
storage hooks:

- DPDs store snapshot info in the captured YAML configuration inside DPD
  snapshot metadata.
- NPDs store snapshot info in an optional NPD info sidecar under
  `non-dated/{name}/_meta_data/{name}_{suffix}.pkl.gz`. That sidecar is a
  gzip-compressed pickle, not a JSON or YAML file.

### Validation

Validation should be strict and should fail before writing any data or sidecar
files:

- `notes` must be a string when supplied. `null` and all non-string values
  raise `ValidationError`.
- `annotations` must be a dictionary when supplied. Existing key paths are
  append-only. Changing or removing an existing value raises
  `ValidationError`.
- `column_descriptions` must be a dictionary whose keys and values are
  strings. Existing keys may not be removed.
- Error messages should use consistent forms:
  - `<context> must be a dict, got <type>`
  - `<context> must be a string, got <type>`
  - `<context> keys and values must be strings`

### Provenance

Provenance is larger, optional, per-snapshot information that callers load
explicitly. It is not snapshot info and it never carries forward.

If provenance is supplied for a snapshot, the cache writes a gzip-compressed
pickle sidecar:

```text
_provenance/{dataset_name}_{snapshot_id}.provenance.pkl.gz
```

The snapshot metadata or NPD info sidecar stores only a `SnapshotProvenanceRef`
with the relative path, checksum, and compressed size. `read_provenance()` loads
the sidecar explicitly and returns `{}` when no provenance is attached to that
snapshot.

For DPDs, provenance comes from `DataSource.get_provenance()`. For NPDs,
provenance comes only from the import command/API when explicitly supplied.

### NPD Snapshot Provenance

NPD snapshot provenance is supplied only through
`import_snapshot(..., provenance=...)` or the CLI `--provenance-file`. It is
never read from NPD snapshot info and it never carries forward from a previous
NPD snapshot.

When NPD provenance is supplied, the cache writes it as a gzip-compressed
pickle sidecar:

```text
non-dated/{name}/_provenance/{name}_{suffix}.provenance.pkl.gz
```

The NPD snapshot-info sidecar stores only the `SnapshotProvenanceRef` for that
sidecar. If NPD provenance is omitted, the new snapshot has
`provenance=None`, even if the previous NPD snapshot had a provenance sidecar.
Normal NPD data reads and snapshot-info accessors do not load provenance;
callers use `read_provenance()` when they explicitly need it.

### NPD info file

The NPD import CLI accepts a strict `--info-file` for snapshot info. The file is
a YAML mapping with exactly these allowed top-level keys:

```yaml
notes: |
  Long human notes are allowed.
  Multiple paragraphs are fine.

annotations:
  vendor: databento
  units:
    price: USD

column_descriptions:
  instrument_id: Quiet symbology_v2 listing-level UUID.
  vendor_symbol: Vendor ticker-like symbol.
```

Unknown top-level keys are a hard error. The command should say which key was
unknown and list the allowed keys. Missing keys carry forward from the previous
NPD snapshot according to the shared snapshot-info rules.

The CLI intentionally does not provide per-field flags such as `--notes`,
`--notes-str`, `--annotations`, or `--column-descriptions`. Notes may be long,
and all NPD snapshot info should travel through `--info-file`.

The NPD import CLI also accepts a separate `--provenance-file`. It must parse as
a mapping. Its keys are user-defined and are not restricted by the
snapshot-info schema. If `--provenance-file` is omitted, no provenance is
attached to the new NPD snapshot.

### NPD info sidecar

The NPD info sidecar is intentionally small. It is stored at
`non-dated/{name}/_meta_data/{name}_{suffix}.pkl.gz` as a gzip-compressed
pickle. The pickled payload is a normalized dictionary with this shape:

```python
{
    "name": str,
    "suffix": str,
    "info": {
        "notes": str,                    # optional
        "annotations": dict[str, Any],    # optional
        "column_descriptions": dict[str, str],  # optional
    },
    "provenance": SnapshotProvenanceRef | None,
}
```

It must not grow into a DPD-style operational metadata file. In particular, it
must not store schema, file count, byte size, checksums, source labels,
imported-at timestamps, or lineage. Those concerns either do not apply to NPD
full-copy imports or belong in the explicit provenance sidecar.

Legacy NPD snapshots without an info sidecar are treated as having absent
snapshot info and no provenance reference:

```python
ds.get_notes() is None
ds.get_annotations() == {}
ds.get_column_descriptions() == {}
ds.read_provenance() == {}
```

### Documentation Follow-Up

After implementation, [README.md](README.md) and [README_AI.md](README_AI.md)
need a full editorial pass so their NPD examples, API summaries, and CLI docs
match this reorganized spec.

---

## YAML Configuration

`DatedParquetDataset` definitions live in YAML files in the `yaml/`
subdirectory. Each file is read using `ionbus_utils`' `PDYaml` and may
define one or more datasets under a `datasets:` key.

**Example: multiple datasets in one file**

```yaml
# yaml/futures.yaml

datasets:
    md.futures_daily:
        description: Daily futures data
        date_col: Date
        date_partition: year
        partition_columns:
            - FutureRoot
            - year
        sort_columns:
            - FutureRoot
            - SYM
            - Date
        start_date_str: "1996-01-06"
        repull_n_days: 5
        source_location: code/my_futures_source.py
        source_class_name: MyFuturesSource
        source_init_args:
            api_endpoint: "https://api.example.com/futures"
            timeout_seconds: 30

    md.futures_intraday:
        description: Intraday futures bars
        date_col: Date
        date_partition: month
        partition_columns:
            - FutureRoot
            - month
        sort_columns:
            - FutureRoot
            - Date
            - timestamp
        start_date_str: "2020-01-01"
        repull_n_days: 3
        source_location: code/my_futures_source.py
        source_class_name: MyFuturesIntradaySource
```

**Example: single dataset file**

```yaml
# yaml/equities.yaml

datasets:
    ec.daily_prices:
        description: End-of-day equity prices
        date_col: Date
        date_partition: day
        sort_columns:
            - Date
            - USym
        start_date_str: "2010-01-01"   # Debugging only: don't go back before this date
        # end_date_str: "2024-12-31"   # Debugging only: stop at this date
        repull_n_days: 0
        source_location: code/price_source.py
        source_class_name: DailyPriceSource
```

**Example: with data transformations**

```yaml
# yaml/cleaned_data.yaml

datasets:
    md.equity_daily:
        description: Daily equity data with transformations
        date_col: PricingDate
        date_partition: month
        sort_columns:
            - USym
            - PricingDate
        start_date_str: "2015-01-01"
        source_location: code/equity_source.py
        source_class_name: EquitySource

        # Rename columns from source
        columns_to_rename:
            PriceOpen: Open
            PriceHigh: High
            PriceLow: Low
            PriceClose: Close

        # Drop unwanted columns
        columns_to_drop:
            - InternalId
            - LoadTimestamp

        # Drop rows where key price columns are null
        dropna_columns:
            - Close
            - Volume

        # Deduplicate by USym + Date, keeping the last occurrence
        dedup_columns:
            - USym
            - PricingDate
        dedup_keep: last

        # Optional: run a custom cleaning class
        cleaning_class_location: cleaning/equity_cleaning.py
        cleaning_class_name: EquityDataCleaner
        cleaning_init_args:
            min_price: 1.0
            add_returns: true
```

**Data transformation pipeline:**

All data transformations (YAML-specified and custom cleaning) are performed
using DuckDB for performance and memory efficiency. The pipeline is lazy --
data is not materialized until the final parquet write, which streams in
batches.

```
DataSource.get_data(partition_spec) -> pa.Table | pa.Dataset | pl.DataFrame | pd.DataFrame
                      |
                      v
              [register with DuckDB as lazy relation]
                      |
                      v
              YAML transforms via SQL:
                - columns_to_rename
                - columns_to_drop
                - dropna_columns
                - dedup_columns / dedup_keep
                      |
                      v
              DataCleaner(rel) -> rel  [still lazy]
                      |
                      v
              rel.fetch_arrow_reader()  [streams to parquet]
```

The cleaning class receives a DuckDB relation after all other transformations
and returns a modified relation. All cleaning classes must inherit from
`DataCleaner`:

```python
# ionbus_parquet_cache/data_cleaner.py

from __future__ import annotations
from abc import ABC, abstractmethod
import duckdb


class DataCleaner(ABC):
    """Base class for custom data cleaning logic."""

    def __init__(self, dataset: DatedParquetDataset | NonDatedParquetDataset):
        self.dataset = dataset

    @abstractmethod
    def __call__(
        self, rel: duckdb.DuckDBPyRelation
    ) -> duckdb.DuckDBPyRelation:
        """
        Apply custom cleaning logic to the data.

        Called after all YAML-specified transformations (rename, drop
        columns, dropna, dedup) have been applied. The relation is lazy
        -- no data is materialized until the final parquet write.

        Args:
            rel: A DuckDB relation representing the data.

        Returns:
            A DuckDB relation with cleaning applied.
        """
        pass
```

**Example implementation using DuckDB relation methods:**

```python
# cleaning/equity_cleaning.py

from __future__ import annotations
import duckdb
from ionbus_parquet_cache import DataCleaner


class EquityDataCleaner(DataCleaner):
    """Custom cleaning logic for equity data."""

    def __init__(
        self,
        dataset,
        min_price: float = 1.0,
    ):
        super().__init__(dataset)
        self.min_price = min_price

    def __call__(
        self, rel: duckdb.DuckDBPyRelation
    ) -> duckdb.DuckDBPyRelation:
        # Filter out penny stocks
        return rel.filter(f"Close >= {self.min_price}")
```

**Example using SQL for complex transformations:**

```python
# cleaning/futures_cleaning.py

from __future__ import annotations
import duckdb
from ionbus_parquet_cache import DataCleaner


class FuturesDataCleaner(DataCleaner):
    """Add roll indicators and filter expired contracts."""

    def __init__(
        self,
        dataset,
        expiry_buffer_days: int = 5,
    ):
        super().__init__(dataset)
        self.expiry_buffer_days = expiry_buffer_days

    def __call__(
        self, rel: duckdb.DuckDBPyRelation
    ) -> duckdb.DuckDBPyRelation:
        # Use SQL for complex window functions
        return duckdb.sql(f"""
            SELECT *,
                (DaysToExpiry = MIN(DaysToExpiry) OVER (
                    PARTITION BY RootSymbol, Date
                )) AS IsRollDay
            FROM rel
            WHERE DaysToExpiry > {self.expiry_buffer_days}
        """)
```

### YAML fields

| Field | Type | Default | Description |
|---|---|---|---|
| (key under `datasets:`) | `str` | required | The dataset name is the key in the `datasets:` dictionary (e.g., `md.futures_daily:`). This is how DPDs are always defined. |
| `description` | `str` | `""` | Human-readable description |
| `date_col` | `str` | `"Date"` | Name of the date column |
| `date_partition` | `str` | `"day"` | Partition granularity: `day`, `week`, `month`, `quarter`, `year` |
| `partition_columns` | `list[str]` | `[]` | All partition columns in directory order. We strongly encourage explicitly listing the date partition column (e.g., `year`, `month`) at the desired position in the list. If the date partition column is omitted, it is appended as the last partition column. |
| `sort_columns` | `list[str]` | `[date_col]` | Sort order within each partition file. Use `date_col`, not date partition columns (e.g., `year`, `month`) since those may not exist in the data. |
| `start_date_str` | `str` | `None` | **Debugging/testing only.** Optional override for earliest date (ISO format). If set, the DPD guarantees that `get_data()` will never be called with a start date earlier than this value. Rarely used in production YAML files. |
| `end_date_str` | `str` | `None` | **Debugging/testing only.** Optional override for latest date (ISO format). If set, the DPD guarantees that `get_data()` will never be called with an end date later than this value. Rarely used in production YAML files. |
| `repull_n_days` | `int` | `0` | Number of trailing business days to re-fetch each update |
| `row_group_size` | `int \| None` | `None` (PyArrow default: 1,048,576 rows) | Maximum rows per Parquet row group. Smaller values allow row-group-level predicate pushdown within a file at the cost of more metadata overhead. Only relevant when files are large enough to contain multiple row groups. |
| `annotations` | `dict \| None` | `None` | Small user-owned annotations stored as part of the captured YAML configuration in every snapshot metadata pickle. Intended for compact structured notes that make user code easier to understand or use, such as bitmask definitions, enum labels, units, or dataset-specific notes. Existing entries are append-only: later snapshots may add new keys, but may not remove keys or change values already present in the previous snapshot. Use `column_descriptions` for plain per-column blurbs. |
| `notes` | `str \| None` | `None` | Optional human-readable dataset notes stored as part of the captured YAML configuration in every snapshot metadata pickle. Omitted values carry forward. Explicit string updates are allowed at any time, including `""`; `null` is rejected because notes cannot be deleted once present. |
| `column_descriptions` | `dict[str, str] \| None` | `None` | Optional human-readable descriptions keyed by column name. Stored as part of the captured YAML configuration in every snapshot metadata pickle so users can inspect metadata to understand what each column means. |
| `instrument_column` | `str` | `None` | Column containing instrument identifiers (e.g., `FutureRoot`, `ticker`). Required for `--instruments` CLI flag to work. Can be used for filtering at read time and does not need to be a partition column. |
| `instruments` | `list[str]` | `None` | Instruments to include in updates. If `None`, fetches all instruments. Can be expanded over time using backfill (see below). |
| `source_location` | `str` | `""` | Location of the `DataSource` subclass. Resolution order: (1) if empty/blank, uses a built-in source from `ionbus_parquet_cache` (see [Built-in Data Sources](#built-in-data-sources)); (2) if starts with `module://`, loads from an installed Python package (e.g., `module://my_library.data_sources`) — must use the importable module path, not the distribution name (see [Installed Module Data Sources](#installed-module-data-sources)); (3) otherwise treated as a filesystem path relative to cache root or absolute. If the module cannot be imported, the class is missing, or the class does not inherit from `DataSource`, dataset creation/update fails with a configuration error. |
| `source_class_name` | `str` | required | Name of the `DataSource` subclass. When `source_location` is empty, this must be a built-in class name (e.g., `HiveParquetSource`, `DPDSource`). |
| `source_init_args` | `dict` | `{}` | Non-secret arguments passed to the `DataSource` constructor as `**kwargs` |
| `sync_function_location` | `str \| None` | `None` | Optional post-sync function location. Uses the same location rules as DataSources: empty for built-in, cache-local path such as `code/sync_functions.py`, or installed package via `module://pkg.mod`. |
| `sync_function_name` | `str \| None` | `None` | Optional function or callable class name to run when sync functions are explicitly requested by `sync-cache`. |
| `sync_function_init_args` | `dict` | `{}` | Non-secret keyword arguments used to instantiate class-based sync functions. Plain functions must not use init args. |
| `columns_to_drop` | `list[str]` | `[]` | Columns to remove after fetching data |
| `columns_to_rename` | `dict[str, str]` | `{}` | Columns to rename: `{old_name: new_name}` |
| `dropna_columns` | `list[str]` | `[]` | Drop rows where any of these columns are null |
| `dedup_columns` | `list[str]` | `[]` | Columns to use as keys for deduplication |
| `dedup_keep` | `str` | `"last"` | Which duplicate to keep: `"first"` or `"last"` |
| `cleaning_class_location` | `str` | `None` | Location of the optional `DataCleaner` subclass. If `cleaning_class_name` is unset, no cleaner is used. If `cleaning_class_name` is set, this is required and may be `module://pkg.mod` for an installed/importable module, or a cache-local/absolute Python file path such as `cleaning/equity_cleaning.py`. Empty/blank does not resolve to a built-in cleaner. |
| `cleaning_class_name` | `str` | `None` | Name of the `DataCleaner` subclass in `cleaning_class_location`. The class must inherit from `DataCleaner`. |
| `cleaning_init_args` | `dict` | `{}` | Non-secret arguments passed to the `DataCleaner` constructor as `**kwargs` |

**User annotations:**

The `annotations` YAML field is a small, user-owned namespace inside a
dataset's configuration. The parquet cache stores it, carries it forward, and
checks that existing entries are not removed or changed. The parquet cache will
not use annotations for anything. This information exists only to make the
user's life easier, and only user code should assign semantic meaning to its
contents.

Users may put whatever compact dictionary they want here, and it is copied into
the existing captured YAML stored with each snapshot metadata pickle.

Common examples include:

- bitmask definitions for compact integer flag columns,
- enum or categorical value descriptions,
- column units or display hints,
- dataset-specific notes that readers need frequently.

Example:

```yaml
datasets:
  md.quotes:
    annotations:
      bitmasks:
        quote_flags:
          dtype: uint8
          bits:
            0: regular_session
            1: crossed_market
            2: stale_quote
```

`annotations` must remain small. Large audit records, source manifests, process
graphs, or arbitrary payloads belong in the external provenance sidecar
described below.

`annotations` is watched across a cache lineage. The first snapshot establishes
the initial dictionary. Later snapshots may add fields, but they may not remove
existing fields or change the value of an existing field. This check is
recursive for nested dictionaries: new nested keys are allowed, but every
existing key path from the previous snapshot must still exist and compare
equal unless both old and new values are dictionaries being extended.

For an existing cache, omitting `annotations` carries forward the previous
snapshot's dictionary. Supplying `annotations: {}` is allowed only when the
previous dictionary was empty; otherwise it attempts to remove existing fields
and raises `ValidationError`. To remove or redefine entries, create a new
dataset or perform an explicit rebuild whose lineage starts a new cache.

Read stored annotations from snapshot metadata:

```python
annotations = ds.get_annotations()
old_annotations = ds.get_annotations(snapshot="1H4DW00")
```

The method returns a copy of the current or requested snapshot dictionary, or
`{}` when none are stored. See [Snapshot Info and Provenance](#snapshot-info-and-provenance)
for the shared DPD/NPD contract.

**Notes:**

The optional `notes` YAML field is a free-form string stored with the captured
YAML configuration in each snapshot metadata pickle. It is for short human
context that should travel with the cache but is not naturally per-column and
does not need append-only dictionary semantics.

Example:

```yaml
datasets:
  ref.vendor_instruments:
    notes: Vendor reference file normalized to Quiet symbology identifiers.
```

For an existing cache, omitting `notes` carries forward the previous snapshot's
string. Supplying `notes` replaces the previous value at any time. An empty
string is allowed and is the supported way to clear visible notes:

```yaml
notes: ""
```

`notes: null` is rejected because notes cannot be deleted once present. Legacy
snapshots without a `notes` YAML field are treated as absent, and
`get_notes()` returns `None`.

Read stored notes from snapshot metadata:

```python
notes = ds.get_notes()
old_notes = ds.get_notes(snapshot="1H4DW00")
```

The method returns `str | None`. `None` means no notes field is present;
`""` means notes were explicitly cleared.

**Column descriptions:**

The optional `column_descriptions` YAML field is a small dictionary of
`{column_name: description}` strings. It is copied into the captured YAML
configuration stored in each snapshot metadata pickle. This gives downstream
users a standard place to discover what columns mean without scanning source
code or external notes.

Example:

```yaml
datasets:
  ref.vendor_instruments:
    column_descriptions:
      instrument_id: Quiet symbology_v2 listing-level UUID.
      vendor_instrument_id: Vendor-native instrument identifier.
      vendor_symbol: Vendor ticker-like symbol.
```

Column descriptions should describe the public column names after YAML
renames, YAML transforms, and custom cleaners have run. The dictionary may be
partial; datasets are not required to document every column. Values must be
human-readable strings and should stay short enough to belong in snapshot
metadata. Text is only validated as a string; users may shorten, reword, or
correct existing descriptions. Do not store secrets or large documentation
payloads here.

For an existing cache, omitting `column_descriptions` carries forward the
previous snapshot's dictionary. Supplying `column_descriptions: {}` is allowed
only when the previous dictionary was empty; otherwise it attempts to remove
every existing key and raises `ValidationError`. The removal rule is per key:
later snapshots may add descriptions for new columns and may change text for
existing columns, but every existing column description key from the previous
snapshot must still be present. To remove descriptions, create a new dataset
or perform an explicit rebuild whose lineage starts a new cache.

Read stored column descriptions from snapshot metadata:

```python
descriptions = ds.get_column_descriptions()
old_descriptions = ds.get_column_descriptions(snapshot="1H4DW00")
```

The method returns a copy of the current or requested snapshot dictionary, or
`{}` when none are stored.

**Instrument filtering:**

The `instrument_column` and `instruments` fields work together to control
which instruments are included in the dataset:

```yaml
datasets:
  md.futures_daily:
    instrument_column: FutureRoot    # column containing instrument IDs
    instruments: [ES, NQ, YM, RTY]   # only fetch these instruments
    partition_columns: [FutureRoot, year]
```

- `instrument_column` identifies which column contains instrument identifiers.
  For read-time filtering, it can be any data column. For update-time filtering
  via the `instruments` YAML field, the update path must support it (typically
  requires a partition or bucket column for efficient updates).
- `instruments` limits normal updates to the listed instruments. If omitted
  or `None`, all instruments are fetched.
- The `--instruments` CLI flag overrides the YAML `instruments` list for
  a single run.

**Expanding the instrument universe with backfill:**

To add new instruments to an existing dataset:

1. Run backfill with the new instruments:
   ```bash
   update-cache /path/to/cache md.futures_daily \
       --backfill --start-date 2020-01-01 --instruments GC,SI
   ```

2. Update the YAML file to include the new instruments:
   ```yaml
   instruments: [ES, NQ, YM, RTY, GC, SI]  # added GC, SI
   ```

3. Future normal updates will now include all instruments.

This workflow lets you start with a subset of instruments and gradually
expand as needed, without re-fetching existing data.

---

## Credentials and Secrets

Do not store credentials, API keys, passwords, tokens, or private key material
in YAML files. YAML configuration is stored in snapshot metadata and may be
copied when caches are synced.

Treat `annotations`, provenance sidecar contents, `source_init_args`, `notes`,
`column_descriptions`, `cleaning_init_args`, and `sync_function_init_args` as
non-secret information: endpoint URLs, timeouts, project names, table names,
bitmask definitions, dataset notes, column blurbs, source manifests, process
graphs, and other values that are safe to sync with the cache. DataSources,
DataCleaners, and sync functions that need secrets should read them from
environment variables or an external credential provider and fail clearly if a
required value is missing.

```python
import os

from ionbus_parquet_cache import DataSource


class MyDataSource(DataSource):
    def __init__(self, dataset, endpoint: str):
        super().__init__(dataset)
        self.endpoint = endpoint
        self.api_key = os.environ.get("MY_API_KEY")
        if not self.api_key:
            raise ValueError("MY_API_KEY environment variable is not set")
```

---

## Data Source Interface

### `DataSource` abstract class

A `DataSource` is the only way to supply data to a `DatedParquetDataset`.

The class is initialized with:
- A reference to the owning `DatedParquetDataset` (to inspect partition
  structure, date column, etc.)
- Any additional kwargs from `source_init_args` in the YAML

**Class attributes (can be overridden in subclasses):**

| Attribute | Type | Default | Description |
|---|---|---|---|
| `chunk_days` | `int` | `None` | Business days to fetch per chunk. If `None`, fetches entire date range at once. Set this to limit memory usage (e.g., `15` for 15-business-day chunks). |
| `partition_order` | `list[str]` | `None` | Order to iterate partition columns when building `PartitionSpec` list. If `None`, uses: date partition first, then other partition columns in YAML order. |
| `partition_values` | `dict[str, list]` | `{}` | Known values for partition columns. Keys are column names, values are lists of possible values. Used by default `get_partitions()`. |
| `chunk_values` | `dict[str, list]` | `{}` | Dict mapping chunk dimension names to lists of values. Creates multiple `PartitionSpec` objects with the same `partition_key` but different `chunk_info`. Use for splitting work without creating separate files (e.g., fetching by exchange or batch number). |
| `chunk_expander` | `Callable[[PartitionSpec], list[PartitionSpec]]` | `None` | Optional function that takes a `PartitionSpec` and returns a list of `PartitionSpec` objects with different `chunk_info`. Use for dynamic chunk expansion that depends on the partition. Applied after `chunk_values` expansion. |

Example:
```python
class MyFuturesSource(DataSource):
    chunk_days = 30  # fetch 30 business days (~6 weeks)
    partition_order = ["year", "FutureRoot"]  # year first, then instrument
    partition_values = {
        "FutureRoot": ["ES", "NQ", "YM", "RTY"],  # known instruments
    }
    chunk_values = {
        "exchange": ["CME", "CBOT"],  # split work by exchange without separate files
    }
```

**`get_partition_column_values(column)` method:**

```python
def get_partition_column_values(self, column: str) -> list[Any]:
    ...
```

Returns the list of possible values for a partition column. The default
implementation:

1. If `column` is the date partition (e.g., `year`), computes values from
   the date range using `date_partitions()`
2. If `column` matches `instrument_column` and `instruments` was provided
   (via YAML or `prepare()`), returns those instruments
3. If `column` is in `self.partition_values` dict, returns those values
4. Falls back to `self.dataset.get_partition_values(column)` which extracts
   known values from the existing cache metadata (snapshot file list)
5. If still empty, raises an error

For normal updates (not initial load), step 4 means the system automatically
uses the same partition values already in the cache - no configuration needed.

Override this method only for special cases (e.g., adding new partition
values not yet in the cache):

```python
class MyFuturesSource(DataSource):
    def get_partition_column_values(self, column: str) -> list[Any]:
        if column == "FutureRoot":
            # Dynamic lookup from API (e.g., to discover new instruments)
            return self.api.get_available_futures()
        return super().get_partition_column_values(column)
```

**Default `get_partitions()` implementation:**

The base class provides a default `get_partitions()` that works for most cases:

1. Uses `date_partitions()` to split the date range by `date_partition` granularity
2. Further splits by `chunk_days` if set (subdividing each date partition)
3. Calls `get_partition_column_values()` for each non-date partition column
4. Iterates partition columns in `partition_order` (date first by default)
5. For each combination of partition values, creates a `PartitionSpec`
6. Expands with `chunk_values` if set (populates `chunk_info`)
7. Applies `chunk_expander` function if set

Most subclasses only need to override `available_dates()` and `get_data()`.
Override `get_partitions()` only for custom chunking logic (e.g., API limits,
chunking by specific tickers).

The `update()` method orchestrates the entire flow. The `prepare()` method
is internal - users call `update()` with explicit parameters rather than
calling `prepare()` directly.

**DPD/DataSource interaction flow:**

1. **DPD asks:** "What date range do you have?"
   - Calls `source.available_dates()` -> `(start_date, end_date)`

2. **DPD computes the update window:**
   - If `update()` was called with explicit `start_date`/`end_date`, use those
   - Otherwise, compute from cache state:
     - `default_start` = `cache_end + 1 day`
     - If `repull_n_days > 0`, moves start to `default_start - repull_n_days`
       standard business days; otherwise uses `default_start`
     - `start_date` = max(source_start, computed_start)
     - `end_date` = source_end (typically the previous business day)
   - If computed `start_date > end_date`, the update is a no-op
   - Determines `instruments` list from `--instruments` CLI flag or
     `update()` arguments

3. **DPD calls `source.prepare()` internally:**
   - Calls `source.prepare(start_date, end_date, instruments)`
   - This establishes the scope before requesting partitions
   - If `instruments` is provided but instrument is not a partition
     column, the call raises an error

4. **DPD asks:** "What partitions need updating?"
   - Calls `source.get_partitions()` -> `list[PartitionSpec]`
   - The DataSource uses the values from `prepare()` to determine
     partitions. It may choose to:
     - Fetch all data upfront and hold it in memory
     - Fetch data lazily, one partition at a time
     - Return chunked partitions for large datasets (see below)

5. **DPD groups specs by `partition_values`:**
   - Specs with identical `partition_values` belong to the same final
     partition file (these are "chunks")
   - DPD assigns a `chunk_id` to each spec for temp file naming

6. **DPD iterates over partitions in exact order returned:**
   - For each `PartitionSpec`, calls `source.get_data(partition_spec)`
   - The order is guaranteed: partitions are requested in the same
     order they were returned by `get_partitions()`
   - This allows DataSources to control memory usage and fetch order
   - YAML transforms and DataCleaner are applied per-chunk (each temp
     file is already cleaned)

7. **DPD calls `source.get_provenance(suffix, previous_suffix)` before publish:**
   - Called after all data files have been written and validated, but before
     snapshot metadata is published
   - Not called on dry runs, no-op updates, or failed updates
   - The base implementation returns `{}`
   - If the returned dictionary is non-empty, the DPD writes it to the
     external provenance sidecar and stores a `SnapshotProvenanceRef` in the
     snapshot metadata
   - If the returned dictionary is empty, no provenance sidecar is written and
     the snapshot metadata stores `provenance=None`
   - This hook is for data that should travel with the cache snapshot

8. **DPD calls `source.on_update_complete(suffix, previous_suffix)` once after publish:**
   - Called after all partitions have been written and the snapshot is
     published; not called on dry runs or if the update is a no-op
   - `suffix` is the newly published snapshot; `previous_suffix` is the
     snapshot that existed before this update, or `None` on first update
   - `self.start_date`, `self.end_date`, and `self.instruments` are still
     set from `prepare()` at this point
   - Default is a no-op; override for external side effects such as updating
     an asset registry, writing audit trails outside the cache, or sending
     notifications

**Data fetching strategies:**

The DataSource controls how data is fetched. Two common patterns:

- **Eager fetching:** Fetch all data in `get_partitions()`, store in
  memory, return pieces in `get_data()`. Good for small datasets or
  when the source is fast.

- **Lazy fetching:** Only fetch data when `get_data()` is called for
  each partition. Good for large datasets or expensive sources.

**Chunked partitions for large datasets:**

For datasets with coarse partitioning (e.g., year) but large data
volumes, the DataSource can return multiple `PartitionSpec` objects
with the same `partition_values` but different `start_date` / `end_date`
ranges. For example, a year partition might be returned as 15-day chunks:

```
PartitionSpec({"year": "Y2024"}, start_date=Jan 1, end_date=Jan 15)
PartitionSpec({"year": "Y2024"}, start_date=Jan 16, end_date=Jan 31)
...
PartitionSpec({"year": "Y2024"}, start_date=Dec 16, end_date=Dec 31)
```

The DPD groups these by `partition_values`, assigns a chunk ID to each
based on date range, writes each chunk to a temporary file, then
consolidates all chunks into a single partition file once all chunks for
that partition are processed. See
[Chunked Partition Processing](#chunked-partition-processing) for details.

**Two abstract methods** (subclasses must implement these):

- `available_dates()` - Return the date range the source can provide
- `get_data()` - Return data for a single partition/chunk

**Four optional methods** (base class provides default implementations):

- `prepare()` - Set up for fetching (default calls `set_date_instruments()`)
- `get_partitions()` - Return list of partitions to update (default uses class attributes)
- `get_provenance(suffix, previous_suffix)` - Return optional snapshot
  provenance to store with the cache (default returns `{}`).
- `on_update_complete(suffix, previous_suffix)` - Post-update side-effect hook
  (default is a no-op). `previous_suffix` is None on first update.

**Method details:**

#### `available_dates()`

```python
def available_dates(self) -> tuple[dt.date, dt.date]:
    ...
```

Returns the date range that this data source can provide.

- Returns a tuple of `(start_date, end_date)`.
- `start_date` is the earliest date the source has data for (e.g., when
  the data series began).
- `end_date` is the most recent date data is available - typically the
  previous business day, since today's data is usually not yet available.
- The implementation queries the actual data source (API, database, etc.)
  to determine what dates are available.
- The system uses this to compute the update window. For example, if the
  cache already has data through Jan 11 and `available_dates()` returns
  `(2020-01-01, 2024-01-15)`, the system will call `prepare()` with
  `start_date` = Jan 12 and `end_date` = Jan 15.

#### `prepare(...)`

```python
def prepare(
    self,
    start_date: dt.date,
    end_date: dt.date,
    instruments: list[str] | None = None,
) -> None:
    ...
```

Called internally by `update()` before `get_partitions()` to establish
the update scope. Users should not call this method directly - instead,
pass parameters to `update()`.

**Subclasses do NOT need to implement this method.** The base class provides
a default implementation that calls `set_date_instruments()` to store the
date range and instruments. Override only if you need custom setup logic
(e.g., establishing API connections, pre-fetching metadata). If you override,
call `self.set_date_instruments(start_date, end_date, instruments)` to set
the required state.

- `start_date` and `end_date` define the date range to update.
- `instruments` is an optional list to limit which instruments to update.
  If provided, instrument must be a partition column; otherwise, this
  method raises an error.

#### `get_partitions()`

```python
def get_partitions(self) -> list[PartitionSpec]:
    ...
```

Returns the list of partition files that need to be updated.

**The base class provides a default implementation** that handles most cases
using `chunk_days` and `partition_order` (see class attributes above). Only
override this method if you need custom chunking logic.

- Takes no arguments -- uses values set by `prepare()` (`start_date`,
  `end_date`, `instruments`).
- Returns a list of `PartitionSpec` objects describing each partition
  that should be written.
- The implementation has access to `self.dataset` (the owning
  `DatedParquetDataset`) to inspect `partition_columns`,
  `date_partition`, `date_col`, etc.
- For date ranges spanning partition boundaries (e.g., year boundary),
  return separate `PartitionSpec` objects for each affected partition.

`PartitionSpec` is a dataclass:

```python
@dataclass
class PartitionSpec:
    partition_values: dict[str, Any]  # all partition columns including date partition
    start_date: dt.date               # start of date range for this chunk/partition
    end_date: dt.date                 # end of date range for this chunk/partition
    chunk_info: dict[str, Any] | None = None  # optional: DataSource's chunking metadata
    temp_file_path: str | None = None         # assigned by DPD, not by DataSource
```

The optional `chunk_info` field lets the DataSource record what it's chunking on
(for its own tracking). This is useful when chunking on dimensions other than
dates - for example, chunking by specific tickers within a date range:

```python
PartitionSpec(
    partition_values={"FutureRoot": "ES", "year": "Y2024"},
    start_date=date(2024, 1, 1),
    end_date=date(2024, 1, 31),
    chunk_info={"tickers": ["ESH4", "ESH5"]},  # DataSource's own tracking
)
```

The DPD does not use `chunk_info` - it's purely for the DataSource's benefit
when `get_data()` is called.

The `partition_values` dict includes both non-date partition columns and
the date partition column. For example, with `date_partition: year` and
a `FutureRoot` partition column: `{"FutureRoot": "ES", "year": "Y2024"}`.
With `date_partition: month`: `{"FutureRoot": "ES", "month": "M2024-01"}`.

When `instrument_column` is set in the YAML and is also a partition column,
the instrument value is included in `partition_values` (e.g.,
`{"FutureRoot": "ES", ...}`). If `instrument_column` is not a partition column,
it is not included in `partition_values`. The `--instruments` CLI flag requires
`instrument_column` to be set. For non-bucketed datasets, the instrument column
must also be a partition column to support efficient updates. For bucketed
datasets, bucketed partial updates are not currently supported.

**`temp_file_path` field:** This field is **not** set by the DataSource.
After calling `get_partitions()`, the DPD assigns `temp_file_path` to each
`PartitionSpec` before processing. This tells the DataSource (and any
debugging/logging) exactly where the chunk will be written. The DPD
determines this upfront by analyzing all specs, grouping by
`partition_values`, and assigning chunk IDs with proper `_01`, `_02`
suffixes when multiple chunks share the same date range.

**Write plan mapping:** Before any data is fetched, the DPD builds an
internal mapping that tracks:
- Which `PartitionSpec` objects belong to each final partition file
- The `temp_file_path` for each spec
- The final partition file path for each group
- How many chunks remain before consolidation can occur

This mapping ensures the DPD knows the complete write strategy upfront,
enabling proper chunk numbering and efficient consolidation tracking.

**Note:** The DPD handles all file path logic (finding existing files,
determining output paths). The DataSource only specifies what data to
fetch via `partition_values` and date range.

**Chunked partitions:** Multiple `PartitionSpec` objects can share the
same `partition_values` but have different date ranges. The DPD groups
all specs by `partition_values`, assigns a `chunk_id` to each for temp
file naming, and consolidates them into a single partition file once all
chunks for that partition are processed.

#### `get_data(...)`

```python
def get_data(
    self,
    partition_spec: PartitionSpec,
) -> pa.Table | pa.dataset.Dataset | pl.DataFrame | pd.DataFrame:
    ...
```

Returns the new data for a single partition (or chunk). Called once per
`PartitionSpec` returned by `get_partitions()`, in the exact order
returned.

- `partition_spec` contains all the information needed to fetch the data:
  - `partition_spec.start_date` and `partition_spec.end_date` define the
    date range to fetch.
  - `partition_spec.partition_values` contains the partition context (e.g.,
    `{"FutureRoot": "ES", "year": "Y2024"}`).
- The returned data must contain a date column (as specified by
  `date_col` in the YAML configuration).

**Important:** The data returned by `get_data` does NOT need to include
partition columns like `year`, `month`, `quarter`, or `week`. The
parquet cache system derives these from the date column when writing
files. Just provide the raw data with a date column.

The return type can be:

- `pyarrow.Table`
- `pyarrow.dataset.Dataset`
- `polars.DataFrame`
- `pandas.DataFrame`

The system will convert to the appropriate format internally.

#### `get_provenance(...)`

```python
def get_provenance(
    self,
    suffix: str,
    previous_suffix: str | None,
) -> dict[str, Any]:
    return {}
```

Called after all data files for the update have been written and validated,
but before the snapshot metadata is published. The base class returns an empty
dictionary.

This hook is for provenance that should travel with the cache snapshot. The
DataSource may build the dictionary from information it gathered during
`prepare()`, `get_partitions()`, and `get_data()`, plus the final snapshot
context:

- `suffix` is the snapshot suffix about to be published.
- `previous_suffix` is the suffix of the snapshot that existed before this
  update, or `None` if this is the first update of the cache.
- `self.start_date`, `self.end_date`, and `self.instruments` are still set from
  `prepare()`.

The method must return a `dict[str, Any]`. Returning a non-dictionary raises
`ValidationError`.

If the returned dictionary is empty, no provenance sidecar is written and the
snapshot metadata stores `provenance=None`. If the returned dictionary is
non-empty, the DPD stores it as a gzip-compressed pickle sidecar and records a
`SnapshotProvenanceRef` in the snapshot metadata.

```python
class MySource(DataSource):
    def prepare(self, start_date, end_date, instruments=None):
        self.set_date_instruments(start_date, end_date, instruments)
        self.api_manifest = self.api.describe_run(start_date, end_date)

    def get_provenance(
        self,
        suffix: str,
        previous_suffix: str | None,
    ) -> dict[str, Any]:
        return {
            "snapshot": suffix,
            "previous_snapshot": previous_suffix,
            "source": "my_api",
            "manifest": self.api_manifest,
        }
```

#### `on_update_complete(...)`

```python
def on_update_complete(self, suffix: str, previous_suffix: str | None) -> None:
    ...
```

Called once after all partitions have been written and the snapshot is published.
The base class provides a no-op default. Override for side effects that should
happen after the cache snapshot is visible, such as updating an asset registry,
writing audit trails outside the cache, sending completion notifications, or
updating a separate tracking table.

- Called only when an actual snapshot is published — not on dry runs and not
  when the update is a no-op (no partitions to process).
- `suffix` is the snapshot suffix that was just published.
- `previous_suffix` is the suffix of the snapshot that existed before this
  update, or `None` if this is the first update of the cache.
- `self.start_date`, `self.end_date`, and `self.instruments` are still set from
  `prepare()` at this point, giving full context about the completed run.

```python
class MySource(DataSource):
    def on_update_complete(self, suffix: str, previous_suffix: str | None) -> None:
        write_audit_record(
            snapshot=suffix,
            previous_snapshot=previous_suffix,  # None on first update
            start=self.start_date,
            end=self.end_date,
            source="my_api",
        )
```

#### `date_partitions(...)` (static method)

```python
@staticmethod
def date_partitions(
    start_date: dt.date,
    end_date: dt.date,
    date_partition: str,  # "day", "week", "month", "quarter", "year"
    max_business_days: int | None = None,  # split into chunks if specified
) -> list[tuple[dt.date, dt.date, str]]:
    ...
```

Returns a list of `(partition_start, partition_end, partition_string)`
tuples covering the given date range. This is a utility method to help
DataSource implementations determine how to chunk their data fetching
based on the partition granularity.

**`max_business_days` parameter:** When specified, each date partition is
further split into chunks of at most `max_business_days` business days.
This is useful for large partitions (e.g., year) where fetching an entire
year at once would use too much memory. The partition string remains the
same for all chunks within a partition (e.g., all chunks return "Y2024"),
but the date ranges differ. The DPD handles chunk numbering (`_01`, `_02`)
when multiple chunks share the same partition string.

**Examples:**

```python
from ionbus_parquet_cache import DataSource

# Year partitions
DataSource.date_partitions(
    dt.date(2024, 3, 15),
    dt.date(2025, 6, 20),
    "year"
)
# Returns:
# [
#     (dt.date(2024, 3, 15), dt.date(2024, 12, 31), "Y2024"),
#     (dt.date(2025, 1, 1),  dt.date(2025, 6, 20),  "Y2025"),
# ]

# Quarter partitions
DataSource.date_partitions(
    dt.date(2024, 2, 15),
    dt.date(2024, 8, 10),
    "quarter"
)
# Returns:
# [
#     (dt.date(2024, 2, 15), dt.date(2024, 3, 31), "Q2024-1"),
#     (dt.date(2024, 4, 1),  dt.date(2024, 6, 30), "Q2024-2"),
#     (dt.date(2024, 7, 1),  dt.date(2024, 8, 10), "Q2024-3"),
# ]

# Month partitions
DataSource.date_partitions(
    dt.date(2024, 1, 15),
    dt.date(2024, 3, 10),
    "month"
)
# Returns:
# [
#     (dt.date(2024, 1, 15), dt.date(2024, 1, 31), "M2024-01"),
#     (dt.date(2024, 2, 1),  dt.date(2024, 2, 29), "M2024-02"),
#     (dt.date(2024, 3, 1),  dt.date(2024, 3, 10), "M2024-03"),
# ]

# Day partitions
DataSource.date_partitions(
    dt.date(2024, 1, 1),
    dt.date(2024, 1, 3),
    "day"
)
# Returns:
# [
#     (dt.date(2024, 1, 1), dt.date(2024, 1, 1), "D2024_01_01"),
#     (dt.date(2024, 1, 2), dt.date(2024, 1, 2), "D2024_01_02"),
#     (dt.date(2024, 1, 3), dt.date(2024, 1, 3), "D2024_01_03"),
# ]

# Year partitions with max_business_days chunking
DataSource.date_partitions(
    dt.date(2024, 1, 1),
    dt.date(2024, 12, 31),
    "year",
    max_business_days=20  # ~1 month of business days
)
# Returns multiple chunks, all with same partition string:
# [
#     (dt.date(2024, 1, 1),  dt.date(2024, 1, 31),  "Y2024"),  # chunk 1
#     (dt.date(2024, 2, 1),  dt.date(2024, 2, 29),  "Y2024"),  # chunk 2
#     (dt.date(2024, 3, 1),  dt.date(2024, 3, 29),  "Y2024"),  # chunk 3
#     ...  # more chunks, each ~20 business days
#     (dt.date(2024, 12, 2), dt.date(2024, 12, 31), "Y2024"),  # final chunk
# ]
```

**Use case:** A DataSource can use this method in `get_partitions()` to
generate one `PartitionSpec` per date partition:

```python
def get_partitions(self) -> list[PartitionSpec]:
    partitions = DataSource.date_partitions(
        self.start_date,
        self.end_date,
        self.dataset.date_partition,  # from YAML config
    )
    return [
        PartitionSpec(
            start_date=p_start,
            end_date=p_end,
            partition_values={"year": p_string},  # or month, quarter, etc.
        )
        for p_start, p_end, p_string in partitions
    ]
```

#### `get_partition_column_values(...)` (optional override)

```python
def get_partition_column_values(
    self,
    column: str,
) -> list[Any]:
    ...
```

Returns the unique values for a given partition column. DataSources can
override this method to provide the set of values for each non-date
partition column (e.g., all `FutureRoot` values like `["ES", "NQ", "CL"]`).

The default implementation raises `NotImplementedError`. When implemented,
this enables the `build_partitions()` helper to automatically generate
the cartesian product of all partition column values with date partitions.

**Example:**

```python
def get_partition_column_values(self, column: str) -> list[Any]:
    if column == "FutureRoot":
        return self.instruments or ["ES", "NQ", "CL", "GC"]
    raise ValueError(f"Unknown partition column: {column}")
```

#### `build_partitions(...)` helper

```python
def build_partitions(
    self,
    start_date: dt.date,
    end_date: dt.date,
    max_business_days: int | None = None,
) -> list[PartitionSpec]:
    ...
```

Convenience method that generates all `PartitionSpec` objects by combining:
1. Unique values for each non-date partition column (via `get_partition_column_values()`)
2. Date partitions for the given range (via `date_partitions()`)

This method reads `self.dataset.partition_columns` and `self.dataset.date_partition`
to determine the partition structure, then builds the cartesian product of
all partition column values with the date partitions.

**Example:**

```python
def get_partitions(self) -> list[PartitionSpec]:
    # Instead of manually building the cartesian product:
    return self.build_partitions(
        self.start_date,
        self.end_date,
        max_business_days=20,  # optional: chunk large partitions
    )
```

This replaces the common boilerplate pattern:

```python
# OLD: Manual cartesian product
def get_partitions(self) -> list[PartitionSpec]:
    partitions = []
    roots = self.instruments or self._get_all_roots()
    for root in roots:
        for p_start, p_end, p_string in DataSource.date_partitions(...):
            partitions.append(PartitionSpec(
                partition_values={"FutureRoot": root, "year": p_string},
                start_date=p_start,
                end_date=p_end,
            ))
    return partitions

# NEW: Using build_partitions()
def get_partition_column_values(self, column: str) -> list[Any]:
    if column == "FutureRoot":
        return self.instruments or self._get_all_roots()
    raise ValueError(f"Unknown partition column: {column}")

def get_partitions(self) -> list[PartitionSpec]:
    return self.build_partitions(self.start_date, self.end_date)
```

The `build_partitions()` helper:
- Reads partition structure from `self.dataset`
- Calls `get_partition_column_values()` for each non-date partition column
- Generates the cartesian product with date partitions
- Handles `max_business_days` chunking automatically
- Returns properly structured `PartitionSpec` objects

---

### Implementing a `DataSource`

```python
from __future__ import annotations

import datetime as dt
from typing import Any

import pyarrow as pa

from ionbus_parquet_cache import DataSource, PartitionSpec


class MyFuturesSource(DataSource):
    """Fetches daily futures data from a proprietary API."""

    def available_dates(self) -> tuple[dt.date, dt.date]:
        # Query the data source to find the available date range.
        # This could be an API call, database query, etc.
        return self._query_api_date_range()

    def prepare(
        self,
        start_date: dt.date,
        end_date: dt.date,
        instruments: list[str] | None = None,
    ) -> None:
        # Store the update scope for use in get_partitions() and get_data().
        # If instruments is provided, validate it's a partition column.
        self.start_date = start_date
        self.end_date = end_date
        self.instruments = instruments

    def get_partitions(self) -> list[PartitionSpec]:
        # Return all partitions affected by this date range.
        # Include both non-date partition columns and the date partition.
        # Uses self.start_date, self.end_date, self.instruments set by prepare().
        partitions = []
        roots = self.instruments or self._get_all_roots()

        # For year-partitioned data, determine which years are affected
        years = range(self.start_date.year, self.end_date.year + 1)

        for root in roots:
            for year in years:
                # Determine date range for this year partition
                year_start = max(self.start_date, dt.date(year, 1, 1))
                year_end = min(self.end_date, dt.date(year, 12, 31))
                partitions.append(
                    PartitionSpec(
                        partition_values={"FutureRoot": root, "year": f"Y{year}"},
                        start_date=year_start,
                        end_date=year_end,
                    )
                )
        return partitions

    def get_data(self, partition_spec: PartitionSpec) -> pa.Table:
        # Fetch data for this specific partition within the date range.
        # partition_spec contains e.g.:
        #   partition_values={"FutureRoot": "ES", "year": "Y2024"}
        #   start_date=2024-01-01, end_date=2024-12-31
        # Return data with a date column (e.g., "Date").
        # Do NOT add year/month/quarter/week columns -- the system
        # derives these automatically from the date column.
        root = partition_spec.partition_values["FutureRoot"]
        return self._fetch_from_api(
            root, partition_spec.start_date, partition_spec.end_date
        )

    # Helper methods -- implement these based on your data source
    def _query_api_date_range(self) -> tuple[dt.date, dt.date]:
        """Query API to find available date range."""
        ...

    def _get_all_roots(self) -> list[str]:
        """Return list of all instrument roots to process."""
        ...

    def _fetch_from_api(
        self, root: str, start_date: dt.date, end_date: dt.date
    ) -> pa.Table:
        """Fetch data from API for a specific root and date range."""
        ...
```

**Simplified version using `build_partitions()`:**

The same DataSource can be written more concisely using the helper methods:

```python
class MyFuturesSource(DataSource):
    """Fetches daily futures data from a proprietary API (simplified)."""

    def available_dates(self) -> tuple[dt.date, dt.date]:
        return self._query_api_date_range()

    def prepare(
        self,
        start_date: dt.date,
        end_date: dt.date,
        instruments: list[str] | None = None,
    ) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.instruments = instruments

    def get_partition_column_values(self, column: str) -> list[Any]:
        # Return unique values for each non-date partition column.
        # The DPD reads partition_columns from YAML and calls this
        # for each non-date column.
        if column == "FutureRoot":
            return self.instruments or self._get_all_roots()
        raise ValueError(f"Unknown partition column: {column}")

    def get_partitions(self) -> list[PartitionSpec]:
        # build_partitions() handles the cartesian product automatically:
        # - Reads partition_columns and date_partition from self.dataset
        # - Calls get_partition_column_values() for each non-date column
        # - Generates all PartitionSpec objects with correct partition_values
        return self.build_partitions(
            self.start_date,
            self.end_date,
            max_business_days=20,  # optional: chunk year partitions
        )

    def get_data(self, partition_spec: PartitionSpec) -> pa.Table:
        root = partition_spec.partition_values["FutureRoot"]
        return self._fetch_from_api(
            root, partition_spec.start_date, partition_spec.end_date
        )

    # Helper methods (same as before)
    def _query_api_date_range(self) -> tuple[dt.date, dt.date]: ...
    def _get_all_roots(self) -> list[str]: ...
    def _fetch_from_api(self, root: str, start_date: dt.date, end_date: dt.date) -> pa.Table: ...
```

The `build_partitions()` approach eliminates manual cartesian product logic
and ensures `partition_values` is correctly structured for any partition
configuration defined in the YAML.

**Important:**

1. **`partition_values` must include the date partition.** Based on the
   `date_partition` YAML setting, include the appropriate key with the
   formatted value (matching the path format):
   - `date_partition: year` -> `{"year": "Y2024"}`
   - `date_partition: quarter` -> `{"quarter": "Q2024-1"}`
   - `date_partition: month` -> `{"month": "M2024-01"}`
   - `date_partition: week` -> `{"week": "W2024-02"}` (ISO week, zero-padded)
   - `date_partition: day` -> `{"{date_col}": "2024-01-15"}` (e.g., `{"Date": "2024-01-15"}`)
2. **`prepare()` is called before `get_partitions()`.** The DPD computes
   the update window and calls `prepare(start_date, end_date, instruments)`
   to establish the scope. The implementation should store these values
   for use in `get_partitions()` and `get_data()`.
3. **`get_partitions()` is called once per update.** It takes no arguments
   -- uses values set by `prepare()`. For date ranges spanning partition
   boundaries (e.g., Dec 30 - Jan 3 for year-partitioned data), return
   `PartitionSpec` objects for each affected partition.
4. **`get_data(partition_spec)` is called once per `PartitionSpec`.** The
   DPD iterates over the partitions returned by `get_partitions()` **in
   the exact order returned** and calls `get_data()` for each one. The
   `PartitionSpec` contains `start_date`, `end_date`, and `partition_values`.
   This keeps memory usage bounded regardless of dataset size.
5. **Order is guaranteed.** Partitions are requested in the same order
   returned by `get_partitions()`. This allows DataSources to control
   fetch order and memory usage (e.g., process all chunks for one
   partition before moving to the next).
6. **DPD handles file paths.** The DataSource does not need to determine
   existing files or output paths -- that's the DPD's responsibility.
   The DataSource only specifies partition metadata and date ranges.
7. **`available_dates()` defines what the source can provide.** The system
   calls this first, then checks the cache to compute the actual update
   window. For example, if the cache has data through Jan 11 and
   `available_dates()` returns `(2020-01-01, 2024-01-15)`, the system calls
   `prepare()` with `start_date = Jan 12` and `end_date = Jan 15`.
8. The `DataSource` subclass need not define `__init__` unless it
   needs extra parameters. If it does define `__init__`, it must call
   `super().__init__(dataset)`. Constructor arguments from
   `source_init_args` are passed as `**kwargs`:

   ```python
   class MySource(DataSource):
       def __init__(
           self, dataset,
           *, api_endpoint: str, timeout_seconds: int = 10
       ):
           super().__init__(dataset)
           self.api_endpoint = api_endpoint
           self.timeout = timeout_seconds
   ```

### Built-in Data Sources

The `ionbus_parquet_cache` package includes standard data sources for
common use cases. To use a built-in source, leave `source_location` empty
(or omit it) and set `source_class_name` to the built-in class name.
Parameters are passed via `source_init_args`.

#### `HiveParquetSource`

Reads from Hive-partitioned Parquet directories or single Parquet files.

**YAML configuration:**

```yaml
datasets:
  md.futures_daily:
    description: "Futures data from Hive Parquet source"
    date_col: Date
    date_partition: year
    partition_columns: [FutureRoot, year]
    source_location: ""  # empty = built-in
    source_class_name: HiveParquetSource
    source_init_args:
      path: "/db/Systems/Operations/WritablePartitions"
      # Optional: glob pattern for files (default: "**/*.parquet")
      glob_pattern: "**/{table}/table.parquet"
```

**Constructor parameters (`source_init_args`):**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `path` | `str` | required | Base path to the Parquet source (directory or single file) |
| `glob_pattern` | `str` | `"**/*.parquet"` | Glob pattern for finding Parquet files within `path` (ignored if `path` is a single file) |

**Behavior:**

- If `path` points to a single `.parquet` file, reads that file directly
- If `path` is a directory, globs for Parquet files using `glob_pattern`
- Automatically discovers partition values from Hive directory structure
  (e.g., `future_root=ES/year=2024/` → discovers `future_root` values)
- Excludes loose parquet files at root level (only reads files inside
  Hive partition directories)
- Normalizes partition column types to `string` (handles `large_string`
  and dictionary-encoded columns automatically)
- Supports date columns stored as:
  - Native date types (`date32`, `date64`)
  - Timestamps
  - ISO date strings (`"2024-01-15"`) - compared lexicographically
- Returns all data for the requested date range from `prepare()`
- Suitable for reading from existing Hive-partitioned data lakes

#### `DPDSource`

Reads from another `DatedParquetDataset` in the same or different cache.
Useful for building derived datasets or chaining transformations.

**YAML configuration:**

```yaml
datasets:
  derived.futures_clean:
    description: "Cleaned futures data derived from raw"
    date_col: Date
    date_partition: year
    partition_columns: [FutureRoot, year]
    source_location: ""  # empty = built-in
    source_class_name: DPDSource
    source_init_args:
      dpd_name: "md.futures_raw"
      dpd_cache_path: "/path/to/source/cache"  # optional
      dpd_cache_name: "source_cache"  # optional, for CacheRegistry lookup
```

**Constructor parameters (`source_init_args`):**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dpd_name` | `str` | required | Name of the source `DatedParquetDataset` |
| `dpd_cache_path` | `str` | current cache | Path to the cache containing the source DPD. If omitted, uses the same cache as the target DPD. |
| `dpd_cache_name` | `str` | `None` | Name of cache in CacheRegistry to look up source DPD. If provided, takes precedence over `dpd_cache_path`. |

**Behavior:**

- Opens the source DPD and reads data using `pyarrow_dataset()`
- Applies partition pruning based on the requested date range
- Returns a `pyarrow.Dataset` from `get_data()` for lazy evaluation
- The source DPD's schema becomes the input schema for this DPD's
  transformations (rename, drop, cleaner, etc.)
- Useful for:
  - Creating filtered views of larger datasets
  - Applying different cleaning logic to existing data
  - Building aggregated datasets from detail data

### Installed Module Data Sources

Data sources can be packaged in external Python libraries and referenced
via the `module://` prefix in `source_location`. This allows teams to
distribute pre-built data source implementations as installable packages.

**YAML configuration with installed module:**

```yaml
datasets:
  md.external_data:
    description: "Data from an installed library"
    date_col: Date
    date_partition: year
    partition_columns: [Symbol, year]
    source_location: module://my_library.data_sources
    source_class_name: ExternalDataSource
    source_init_args:
      endpoint: "https://api.example.com"
```

**How it works:**

1. The library author packages a `DataSource` subclass in their package
   (e.g., `my_library.data_sources.ExternalDataSource`)
2. Users install the package: `pip install my_library`
3. In the YAML config, set `source_location: module://my_library.data_sources`
   and `source_class_name: ExternalDataSource`
4. At load time, the system uses `importlib.import_module()` to dynamically
   import the module and retrieve the class

For credentials and secret handling, see
[Credentials and Secrets](#credentials-and-secrets).

**Class requirements:**

The loaded class must:
- Be a top-level class in the module (not nested inside another class or function)
- Inherit from `DataSource` (or `BucketedDataSource` for bucketed datasets)
- Be instantiable with the arguments from `source_init_args` (passed as `**kwargs`)

If any of these conditions fail, dataset creation or update will raise a
configuration error with details about what is wrong (module not found, class
missing, wrong base class, etc.).

**Module path resolution:**

- The `module://` prefix signals that the following string is a Python
  module path (e.g., `my_library.data_sources`)
- The module can be in the installed site-packages, a local editable
  install, or any location on `sys.path`
- Always use the importable module name, not the distribution name
  (e.g., install `my-library` from PyPI but import `my_library.data_sources`)

**Example library structure:**

```
my_library/
  __init__.py
  data_sources.py    # contains ExternalDataSource class
  setup.py
```

```python
# my_library/data_sources.py
from ionbus_parquet_cache import DataSource

class ExternalDataSource(DataSource):
    def __init__(self, dataset, endpoint: str):
        super().__init__(dataset)
        self.endpoint = endpoint

    def available_dates(self) -> tuple:
        # ... implementation ...
        pass

    def get_partitions(self) -> list:
        # ... implementation ...
        pass

    def get_data(self, partition_spec):
        # ... implementation ...
        pass
```

**Shared module-location syntax:**

The `module://` prefix applies to `source_location` for `DataSource` classes
and to `cleaning_class_location` for `DataCleaner` classes. It is also used by
`sync_function_location` for post-sync functions.

---

### Chunked DataSource Example

For large datasets where fetching a full year at once is expensive, the
DataSource can return chunked partitions:

```python
from __future__ import annotations

import datetime as dt
from datetime import timedelta
from typing import Any

import pyarrow as pa

from ionbus_parquet_cache import DataSource, PartitionSpec


class ChunkedFuturesSource(DataSource):
    """Fetches data in 15-day chunks to avoid memory issues."""

    CHUNK_DAYS = 15

    def available_dates(self) -> tuple[dt.date, dt.date]:
        return self._query_api_date_range()

    def prepare(
        self,
        start_date: dt.date,
        end_date: dt.date,
        instruments: list[str] | None = None,
    ) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.instruments = instruments

    def get_partitions(self) -> list[PartitionSpec]:
        partitions = []
        roots = self.instruments or self._get_all_roots()
        years = range(self.start_date.year, self.end_date.year + 1)

        for root in roots:
            for year in years:
                # Determine the date range for this year partition
                year_start = max(self.start_date, dt.date(year, 1, 1))
                year_end = min(self.end_date, dt.date(year, 12, 31))

                # Break into chunks
                chunk_start = year_start
                while chunk_start <= year_end:
                    chunk_end = min(
                        chunk_start + timedelta(days=self.CHUNK_DAYS - 1),
                        year_end
                    )
                    partitions.append(
                        PartitionSpec(
                            partition_values={"FutureRoot": root, "year": f"Y{year}"},
                            start_date=chunk_start,
                            end_date=chunk_end,
                        )
                    )
                    chunk_start = chunk_end + timedelta(days=1)

        return partitions

    def get_data(self, partition_spec: PartitionSpec) -> pa.Table:
        root = partition_spec.partition_values["FutureRoot"]
        return self._fetch_from_api(
            root, partition_spec.start_date, partition_spec.end_date
        )

    # ... helper methods ...
```

With this pattern:
- Each 15-day chunk is fetched and written to a temp file
- DPD tracks chunks per partition
- After the last chunk for `{"FutureRoot": "ES", "year": "Y2024"}`, DPD
  consolidates all temp files into a single partition file

---

## Data Cleaner Interface

### `DataCleaner` abstract class

The `DataCleaner` base class allows custom cleaning logic to be applied
after all YAML-specified transformations (rename, drop columns, dropna,
dedup). Each cleaner receives a **DuckDB relation** (lazy, not materialized)
and returns a transformed relation. This keeps the entire pipeline lazy
and memory-efficient for large datasets.

```python
from __future__ import annotations
from abc import ABC, abstractmethod
import duckdb


class DataCleaner(ABC):
    """Base class for custom data cleaning logic."""

    def __init__(self, dataset: DatedParquetDataset | NonDatedParquetDataset):
        self.dataset = dataset

    @abstractmethod
    def __call__(
        self, rel: duckdb.DuckDBPyRelation
    ) -> duckdb.DuckDBPyRelation:
        """
        Apply custom cleaning logic to the data.

        Args:
            rel: A DuckDB relation (lazy, not yet materialized).

        Returns:
            A DuckDB relation with cleaning applied.
        """
        pass
```

### Implementing a `DataCleaner`

1. **Create a subclass.** Inherit from `DataCleaner` and implement
   `__call__()`.

2. **Accept constructor arguments.** Additional `__init__` parameters
   are populated from `cleaning_init_args` in the YAML.

3. **Use DuckDB relation methods or SQL.** The relation supports
   `.filter()`, `.select()`, `.project()`, and you can use `duckdb.sql()`
   for complex transformations.

4. **Keep it lazy.** Do not call `.fetchall()`, `.to_df()`, or similar
   methods that materialize the data. Return a relation.

**Example using relation methods:**

```python
# cleaning/equity_cleaning.py

from __future__ import annotations
import duckdb
from ionbus_parquet_cache import DataCleaner


class EquityDataCleaner(DataCleaner):
    """Filter penny stocks."""

    def __init__(self, dataset, min_price: float = 1.0):
        super().__init__(dataset)
        self.min_price = min_price

    def __call__(
        self, rel: duckdb.DuckDBPyRelation
    ) -> duckdb.DuckDBPyRelation:
        return rel.filter(f"Close >= {self.min_price}")
```

**Example using SQL for complex logic:**

```python
# cleaning/futures_cleaning.py

from __future__ import annotations
import duckdb
from ionbus_parquet_cache import DataCleaner


class FuturesDataCleaner(DataCleaner):
    """Remove expired contracts and add roll indicators."""

    def __init__(
        self,
        dataset,
        expiry_buffer_days: int = 5,
        add_roll_indicator: bool = True,
    ):
        super().__init__(dataset)
        self.expiry_buffer_days = expiry_buffer_days
        self.add_roll_indicator = add_roll_indicator

    def __call__(
        self, rel: duckdb.DuckDBPyRelation
    ) -> duckdb.DuckDBPyRelation:
        if self.add_roll_indicator:
            return duckdb.sql(f"""
                SELECT *,
                    (DaysToExpiry = MIN(DaysToExpiry) OVER (
                        PARTITION BY RootSymbol, Date
                    )) AS IsRollDay
                FROM rel
                WHERE DaysToExpiry > {self.expiry_buffer_days}
            """)
        else:
            return rel.filter(
                f"DaysToExpiry > {self.expiry_buffer_days}"
            )
```

**YAML configuration:**

```yaml
datasets:
    md.futures_daily:
        description: Daily futures data
        date_col: PricingDate
        source_location: code/futures_source.py
        source_class_name: FuturesSource

        cleaning_class_location: cleaning/futures_cleaning.py
        cleaning_class_name: FuturesDataCleaner
        cleaning_init_args:
            expiry_buffer_days: 3
            add_roll_indicator: true
```

### Installed Module Data Cleaners

`cleaning_class_location` may reference an installed/importable module:

```yaml
datasets:
    md.futures_daily:
        source_location: code/futures_source.py
        source_class_name: FuturesSource

        cleaning_class_location: module://my_library.cleaners
        cleaning_class_name: FuturesDataCleaner
        cleaning_init_args:
            expiry_buffer_days: 3
```

When `module://` is used, the value after the prefix must be an importable
Python module path, not the distribution name. The cleaner class must be
available from that module via `getattr(module, cleaning_class_name)` and must
inherit from `DataCleaner`.

---

## Schema Management

The schema for each `DatedParquetDataset` is stored inside each
snapshot pickle file in `_meta_data/` (see
[Partition Structure on Disk](#partition-structure-on-disk)). There is
no separate schema file.

**Schema rules:**

- **Schema is a union of all columns seen across all writes.** The
  stored schema is the union of columns from all data ever written.
  This ensures all parquet files (old and new) can be read with a
  single unified schema.
- **New columns are always allowed.** When data is written with
  additional columns not in the stored schema, those columns are added
  to the schema in the new snapshot. Older parquet files are left
  as-is; PyArrow returns `null` for those rows when the dataset is
  read with the unified schema from the latest snapshot.
- **Old columns are preserved when new data has fewer columns.** When
  new data is missing columns that exist in the stored schema, those
  columns are retained in the schema. New parquet files will have
  `null` values for those columns, while old files remain readable
  with their original data.
- **Type changes are validated.** Before writing new data, the new
  schema is compared against the schema in the current snapshot. If a
  column changes type in a way that is not backwards compatible, a
  `SchemaMismatchError` exception is raised and the update is
  aborted.
- **Backwards-compatible type widening is allowed.** For example,
  `int32` -> `int64` is permitted. `float64` -> `string` is not.
- **Validation occurs after all transformations.** Schema validation
  is performed after YAML transformations (`columns_to_rename`,
  `columns_to_drop`, `dropna_columns`, `dedup_columns`) and any
  custom `DataCleaner` have been applied.

The schema from the current snapshot is passed as the explicit schema
argument to `ds.FileSystemDataset.from_paths()`, ensuring that all
files are read consistently even when older files have fewer columns.

---

## Reading Data

Reading is **always** via a `pyarrow.dataset.Dataset` object. The dataset
is constructed using `ds.FileSystemDataset.from_paths()` with explicit
per-file partition expressions -- the same pattern as
`create_parquet_dataset.py` in this directory.

**Why `from_paths` instead of `ds.dataset(..., partitioning="hive")`:**

- Partition column values (e.g., `year`, `FutureRoot`) are authoritative
  in snapshot metadata (the file list + per-file `partition_values`), not
  inferred from directory names. Using `from_paths` with per-file
  expressions makes the partition columns available as virtual columns
  when reading.
- Snapshot versioning: because we write new files without deleting old
  ones, we need to explicitly list the files that belong to the current
  snapshot. `from_paths` lets us do this precisely.
- Avoids a full directory scan on every read; file metadata is cached
  in the manifest.

**How virtual partition columns work:**

The parquet files do NOT contain partition columns - they're derived from
the file path and injected at read time.

Example directory structure:
```
cache/
  md.futures_daily/
    FutureRoot=ES/
      year=Y2024/
        data_20240115_abc123.parquet
      year=Y2023/
        data_20240115_abc123.parquet
    FutureRoot=NQ/
      year=Y2024/
        data_20240115_abc123.parquet
```

What's IN the parquet file (note: no `FutureRoot` or `year` columns):
```
| Date       | Open    | High    | Low     | Close   | Volume |
|------------|---------|---------|---------|---------|--------|
| 2024-01-02 | 4750.25 | 4780.50 | 4745.00 | 4770.00 | 150000 |
| 2024-01-03 | 4770.00 | 4795.25 | 4768.00 | 4790.50 | 175000 |
```

Building the dataset with virtual columns:
```python
import pyarrow.dataset as ds
import pyarrow.fs as fs

# From snapshot metadata, we have file list and partition values:
files = [
    "cache/.../FutureRoot=ES/year=Y2024/data_20240115_abc123.parquet",
    "cache/.../FutureRoot=ES/year=Y2023/data_20240115_abc123.parquet",
    "cache/.../FutureRoot=NQ/year=Y2024/data_20240115_abc123.parquet",
]
partition_values = [
    {"FutureRoot": "ES", "year": "Y2024"},
    {"FutureRoot": "ES", "year": "Y2023"},
    {"FutureRoot": "NQ", "year": "Y2024"},
]

# Build partition expressions - these become virtual columns
partitions = []
for pv in partition_values:
    expr = (ds.field("FutureRoot") == pv["FutureRoot"]) & \
           (ds.field("year") == pv["year"])
    partitions.append(expr)

# Schema includes the virtual columns (appended to base schema)
schema = base_schema.append(pa.field("FutureRoot", pa.string())) \
                    .append(pa.field("year", pa.string()))

# Create dataset - partition columns are injected, not read from files
dataset = ds.FileSystemDataset.from_paths(
    files,
    schema=schema,
    format=ds.ParquetFileFormat(),
    filesystem=fs.LocalFileSystem(),
    partitions=partitions,  # virtual column values per file
)
```

When querying, virtual columns appear as real columns:
```python
# Filter by partition columns (no file I/O for non-matching partitions!)
table = dataset.filter(
    (ds.field("FutureRoot") == "ES") &
    (ds.field("year") == "Y2024")
).to_table()

# Result includes the virtual columns:
# | Date       | Open    | High    | ... | FutureRoot | year   |
# |------------|---------|---------|-----|------------|--------|
# | 2024-01-02 | 4750.25 | 4780.50 | ... | ES         | Y2024  |
# | 2024-01-03 | 4770.00 | 4795.25 | ... | ES         | Y2024  |
```

Benefits:
- **Partition pruning**: filtering by `FutureRoot == "ES"` skips NQ files
  entirely - no I/O for non-matching partitions
- **Storage efficiency**: partition columns aren't duplicated in every row
- **Snapshot isolation**: only files from current snapshot are included

**Reading a PyArrow dataset:**

```python
dataset = dpd.pyarrow_dataset()

# Filter and read
table = dataset.filter(
    (ds.field("FutureRoot") == "ES") &
    (ds.field("Date") >= "2024-01-01")
).to_table()
```

**Reading via Polars:**

```python
import polars as pl

lf = pl.scan_pyarrow_dataset(dpd.pyarrow_dataset())
df = (
    lf.filter(pl.col("FutureRoot") == "ES")
    .filter(pl.col("Date") >= pl.lit(dt.date(2024, 1, 1)))
    .collect()
)
```

**Reading via DuckDB:**

```python
import duckdb

dataset = dpd.pyarrow_dataset()
result = duckdb.query(
    "SELECT * FROM dataset WHERE FutureRoot = 'ES' AND Date >= '2024-01-01'"
).df()
```

**`read_data` and `read_data_pl` helpers:**

For simple cases, `DatedParquetDataset.read_data()` returns a
`pd.DataFrame`:

```python
df = dpd.read_data(
    start_date="2024-01-01",
    end_date="2024-12-31",
)
```

**Filtering with `filters=`:**

The `filters` parameter accepts a list of filter tuples. Each tuple has
the format `(column, operator, value)`:

| Operator | Description | Example |
|----------|-------------|---------|
| `"="` | Equals | `("FutureRoot", "=", "ES")` |
| `"!="` | Not equals | `("Status", "!=", "DELETED")` |
| `"<"` | Less than | `("Price", "<", 100.0)` |
| `"<="` | Less than or equal | `("Date", "<=", "2024-06-30")` |
| `">"` | Greater than | `("Volume", ">", 1000)` |
| `">="` | Greater than or equal | `("Date", ">=", "2024-01-01")` |
| `"in"` | In set | `("SYM", "in", {"ESH25", "ESZ24"})` |
| `"not in"` | Not in set | `("Exchange", "not in", {"TEST"})` |

Example with filters:
```python
# Filter by partition column (enables partition pruning - much faster!)
df = dpd.read_data(
    start_date="2024-01-01",
    end_date="2024-12-31",
    filters=[
        ("FutureRoot", "=", "ES"),
        ("SYM", "in", {"ESH25 Index", "ESZ24 Index"}),
    ],
)

# Same with Polars
df_pl = dpd.read_data_pl(
    start_date="2024-01-01",
    end_date="2024-12-31",
    filters=[
        ("FutureRoot", "=", "ES"),
    ],
    columns=["Date", "Open", "High", "Low", "Close", "Volume"],
)
```

**Performance note:** Filtering on partition columns (e.g., `FutureRoot`
when it's a partition column) enables partition pruning - files for
non-matching partitions are never read from disk. This can provide
10-100x speedups for large datasets.

**Automatic date partition filters:**

When `start_date` and `end_date` are passed to `read_data()`,
`read_data_pl()`, or `pyarrow_dataset()`, the system automatically
inserts partition filters based on the `date_partition` granularity
**before** applying the date column filter. This enables partition
pruning even when you only specify a date range.

For example, with `date_partition: year`:
```python
# User calls:
df = dpd.read_data(start_date="2024-03-01", end_date="2024-06-30")

# System automatically adds partition filter:
#   ("year", "in", {"Y2024"})
# Then applies date column filter:
#   Date >= "2024-03-01" AND Date <= "2024-06-30"
```

For a query spanning multiple partitions (e.g., `date_partition: quarter`):
```python
# User calls:
df = dpd.read_data(start_date="2024-03-01", end_date="2024-09-30")

# System automatically adds:
#   ("quarter", "in", {"Q2024-1", "Q2024-2", "Q2024-3"})
# Only Q1, Q2, Q3 partition files are read; Q4 is skipped entirely.
```

This happens transparently - you don't need to know the partition
structure to get partition pruning benefits.

**Filtering with `pyarrow_dataset()`:**

```python
# Get filtered dataset (still lazy - no I/O yet)
dataset = dpd.pyarrow_dataset(
    filters=[("FutureRoot", "=", "ES")]
)

# Now use with DuckDB, Polars, or PyArrow
result = duckdb.query("SELECT * FROM dataset WHERE Date >= '2024-01-01'").df()
```

**Cache refresh methods:**

When a `DatedParquetDataset` is opened, it loads the current snapshot and
caches the PyArrow dataset in memory. If the underlying data is updated,
the cached dataset becomes stale. Two methods support detecting and
reloading updated data.

**Note:** In almost all cases, the processes that **update** the data
(scheduled jobs, ETL pipelines) are different from the processes that
**read** the data (dashboards, analysis scripts, APIs). This separation
is by design - readers work with a consistent snapshot while writers
publish new snapshots independently. The refresh methods allow readers
to pick up new data when ready.

**Single dataset refresh:**

```python
dpd = registry.get_dataset("md.futures_daily")

# Check and refresh in one call
if dpd.refresh():
    print("Loaded new snapshot!")
# Or check first if you want to know before refreshing
if dpd.is_update_available():
    dpd.refresh()  # Returns True
```

**Refresh all datasets** (for long-running notebooks):

```python
# In a long-running notebook, periodically refresh all data
if registry.refresh_all():
    print("Some datasets were updated")
```

**`is_update_available() -> bool`:**

Returns `True` if a newer snapshot exists on disk than the one currently
loaded. This is a cheap operation - it only checks the `_meta_data/`
directory for a newer snapshot file, without loading any data.

Use cases:
- Long-running processes that want to periodically check for updates
- Dashboard applications that display "new data available" indicators
- Conditional refresh logic based on business rules

**`refresh() -> bool`:**

Invalidates the cached PyArrow dataset and reloads from the latest
snapshot. Returns `True` if refreshed, `False` if already current.
After calling `refresh()`:
- `pyarrow_dataset()` returns a dataset pointing to the new files
- `read_data()` and `read_data_pl()` read from the new snapshot
- `is_update_available()` returns `False` (until the next update)

If the DPD is already on the latest snapshot, `refresh()` returns `False`.

**`refresh_all()` (CacheRegistry method):**

Refreshes all DPDs and NPDs that have been accessed via the registry.
Returns `True` if any dataset was refreshed, `False` if all were current.
Useful for Jupyter notebooks kept open for days.

**Cache Invalidation:**

When `refresh()` detects a new snapshot, it invalidates the internal read
cache to ensure subsequent reads use the new data. The `invalidate_read_cache()`
method can be called directly to manually clear cached state:

```python
dpd = registry.get_dataset("my_dataset")
# Force cache invalidation without reloading metadata
dpd.invalidate_read_cache()
```

This clears:
- The cached PyArrow dataset (`_dataset`)
- The cached schema (`_schema`)
- For DPDs: the cached snapshot metadata (`_metadata`)

The next `read_data()` or `pyarrow_dataset()` call will reload from disk.

**`data_summary(refresh_and_possibly_change_loaded_caches=True)`:**

The `CacheRegistry.data_summary()` method can discover fresh snapshots and
reload metadata for display purposes:

```python
# Get dataset summary without discovering new snapshots (default, safe)
df = registry.data_summary()

# Discover new snapshots and reload metadata (mutates cached DPDs)
df = registry.data_summary(
    refresh_and_possibly_change_loaded_caches=True
)
```

When `refresh_and_possibly_change_loaded_caches=True`:
- Discovers the latest snapshot on disk for each dataset
- If a new snapshot exists, reloads the SnapshotMetadata
- **Clears the read cache** via `invalidate_read_cache()`
- Subsequent `read_data()` calls will return data from the new snapshot

**CRITICAL:** Only use `refresh_and_possibly_change_loaded_caches=True` if
you have no active references to DPD instances. Any code holding a reference
to a DPD will now read from a different snapshot than before.

---

## Updating Data

Updating a `DatedParquetDataset` requires a `DataSource` subclass.

**Update flow:**

1. Generate a suffix via `timestamped_id(use_seconds=True)`. This
   suffix is used for all new files written in this run.
2. Call `source.available_dates()` to get the date range the source
   can provide.
3. Determine the update window:
   - If `update()` was called with explicit `start_date`/`end_date`, use those
   - Otherwise, compute from cache state:
     - `default_start` = `cache_end + 1 day`
     - If `repull_n_days > 0`, set `computed_start` to `default_start`
       minus `repull_n_days` standard business days; else
       `computed_start = default_start`
     - `start_date` = max(source_start, computed_start)
     - `end_date` = source_end (typically the previous business day)
   - If `start_date_str` is set, clamp: `start_date` = max(start_date,
     parse(start_date_str))
   - If `end_date_str` is set, clamp: `end_date` = min(end_date,
     parse(end_date_str))
   - **Empty window:** If `start_date > end_date`, the update is a no-op
     (no error, no files written).
   - **Guarantee:** `get_data()` will never be called with dates outside
     these bounds.
4. Validate watched user metadata before any data fetches or file writes. This
   includes `annotations`, `notes`, and `column_descriptions`.
   If this is a new cache, store supplied values in the captured YAML.
   If the dataset already has a snapshot and a watched field is omitted, carry
   forward the previous snapshot's value in the new captured YAML. If
   `annotations` is supplied, it may only add keys to the previous dictionary.
   Removing existing annotation keys or changing existing annotation values
   raises `ValidationError`. If `notes` is supplied, it must be a string and may
   replace the previous value, including with `""`. If `column_descriptions` is
   supplied, it may add keys or change text, but may not remove existing keys.
5. Call `source.prepare(start_date, end_date, instruments)` internally
   to establish the update scope.
6. Call `source.get_partitions()` to get the list of `PartitionSpec`
   objects that need updating.
7. **Plan the write strategy (before any writes):**
   - Group specs by `partition_values` to determine which chunks combine
     into which final partition files.
   - For each group, determine the final partition file path.
   - Assign chunk IDs (with `_01`, `_02` suffixes when multiple chunks
     share the same date range—determined upfront).
   - Set `temp_file_path` on each `PartitionSpec` so the target is known
     before processing begins.
   - Result: a complete map of temp files → final files before any I/O.
8. For each `PartitionSpec` (in order returned):
   a. Call `source.get_data(partition_spec)` to fetch data for this chunk.
   b. Apply YAML transformations (rename, drop, dropna, dedup).
   c. Apply `DataCleaner` if configured.
   d. Sort by `sort_columns`.
   e. Write a temporary parquet file to a system temp directory
      (via Python's tempfile module, not the target partition directory;
      this prevents orphaned temp files from cluttering the cache on crashes;
      partition columns are stripped from the data before writing;
      temp filename encodes partition context, run suffix, and chunk_id).
9. After all chunks for a partition are written, consolidate:
   a. Read all temp files for the partition.
   b. If an existing file exists, merge: keep rows outside the update
      date range from the existing file, union with new data.
   c. Sort again by `sort_columns` (to interleave chunks correctly).
   d. Write the final consolidated partition file.
   e. Delete temp chunk files.
10. Validate schema compatibility and file integrity for all new files.
11. Call `source.get_provenance(plan.suffix, previous_suffix)`. The hook must
    return a dictionary. If it returns `{}`, do not write a provenance sidecar.
    If it returns a non-empty dictionary, write that dictionary as a
    gzip-compressed pickle sidecar under `_provenance/` and compute its
    checksum and size. This file is written before publishing snapshot metadata
    so the metadata can contain a `SnapshotProvenanceRef`.
12. Publish snapshot atomically:
    - Write `_meta_data/{name}_{suffix}.pkl.gz` containing the updated
      file list, partition values, and checksums for every file in the
      dataset (not just files written this run).
    - Include lineage metadata describing the base snapshot, operation,
      added date ranges, rewritten date ranges, and instrument scope for
      this update.
    - Include the captured YAML configuration, with watched user metadata
      carried forward, extended, or updated according to each field's rules, and
      when present a provenance sidecar reference.
13. Invalidate the cached PyArrow dataset so the next read constructs
    a fresh one from the new snapshot.
14. Best-effort cleanup of orphan temp files and orphan provenance sidecars
    from failed publishes.

**Correction window (`repull_n_days`):**

When `repull_n_days > 0`, the update fetches and rewrites the last N
business days on every run. This handles datasets where recent data
is subject to corrections (the new data completely replaces the old
data for those days).

**Standard business-day convention (MVP):**

- All business-day logic in this spec uses standard business days
  (Monday-Friday).
- This applies to `repull_n_days`, normal update cutoff, and
  `cleanup-cache --keep-days`.
- A future version may add configurable market/exchange calendars.

**Update API:**

The `update()` method is the single entry point for all updates:

```python
dpd.update(
    source,                    # DataSource instance (required)
    start_date=None,           # Override start date (optional)
    end_date=None,             # Override end date (optional)
    instruments=None,          # Limit to specific instruments (optional)
    backfill=False,            # Preserve existing data for dates
    restate=False,             # Replace data for date range
)
```

**Examples:**

```python
source = MyFuturesSource(dpd)

# Basic update -- system computes date range from available_dates() and cache
dpd.update(source)

# Update specific instruments only (instrument_column must be a partition column)
dpd.update(source, instruments=["ES", "NQ"])

# Backfill to 2020 (cache currently starts at 2024-01-01)
# end_date is automatically set to cache_start_date - 1 day
dpd.update(source, start_date=date(2020, 1, 1), backfill=True)

# Backfill a new instrument to 2020
dpd.update(source, start_date=date(2020, 1, 1), instruments=["MES"],
           backfill=True)

# Restate (replace) data for a specific date range
dpd.update(source, start_date=date(2024, 1, 15), end_date=date(2024, 1, 20),
           restate=True)
```

**Date requirements by mode:**

| Mode | `start_date` | `end_date` | Notes |
|------|--------------|------------|-------|
| Normal | optional | optional | Computed from `available_dates()` and cache state |
| Backfill | required | forbidden | Auto-set to `cache_start_date - 1 day` |
| Restate | required | required | Both must be specified |

**Backfill constraints:**
- Only `start_date` is allowed; passing `end_date` with `backfill=True`
  raises an error
- `start_date` must be before the cache's current `cache_start_date`
- The system automatically sets `end_date` to `cache_start_date - 1 day`
- This ensures the cache remains contiguous with no gaps

**Restate constraints:**
- Both `start_date` and `end_date` are required; omitting either raises
  an error
- `instruments` is optional (if omitted, restates all instruments)
- **Completeness assumption:** `get_data()` must return a complete
  replacement for the requested scope (date range + instruments). All
  existing rows in that scope are deleted and replaced with the returned
  data. If `get_data()` returns partial data, the missing rows will be lost.

**Empty window behavior:** If the computed or specified window results in
`start_date > end_date`, the update is a no-op (no error, no files written).

### DuckDB-Based Update Processing

All update processing uses DuckDB for lazy evaluation and memory efficiency.
The system always processes partition-by-partition, which handles both
extremes naturally:

1. **Small datasets / date partitions:** Each partition's data is small,
   so `get_data()` fetches quickly and fits in memory easily.

2. **Large datasets / year partitions:** Each partition is fetched
   independently via `get_data(...)`, streaming through DuckDB.

**Update processing pipeline:**

```
            +----------------------------------------------------+
            |  DataSource.available_dates()                      |
            |  Returns: (start_date, end_date) from source       |
            +---------------------+------------------------------+
                                  |
                                  v
            +----------------------------------------------------+
            |  Compute update window:                            |
            |  - Check existing cache for last date stored       |
            |  - Compute start_date, end_date, instruments       |
            +---------------------+------------------------------+
                                  |
                                  v
            +----------------------------------------------------+
            |  DataSource.prepare(start_date, end_date,          |
            |                     instruments)                   |
            +---------------------+------------------------------+
                                  |
                                  v
            +----------------------------------------------------+
            |  DataSource.get_partitions()                       |
            |  Returns: list[PartitionSpec]                      |
            +---------------------+------------------------------+
                                  |
                                  v
            +----------------------------------------------------+
            |  DPD plans write strategy (before any data fetch): |
            |  - Group specs by partition_values                 |
            |  - Map: which temp files → which final files       |
            |  - Assign chunk_id (_01, _02 when needed)          |
            |  - Set temp_file_path on each PartitionSpec        |
            +---------------------+------------------------------+
                                  |
                                  v
            +----------------------------------------------------+
            |  For each PartitionSpec (in order returned):       |
            |                                                    |
            |  1. Call get_data(partition_spec)                  |
            |     -> Returns pa.Table | pa.Dataset |             |
            |        pl.DataFrame | pd.DataFrame                 |
            |                                                    |
            |  2. Register as DuckDB relation (lazy)             |
            |                                                    |
            |  3. Apply YAML transforms (rename, drop, dropna,   |
            |     dedup) as SQL operations                       |
            |                                                    |
            |  4. Apply DataCleaner if configured                |
            |                                                    |
            |  5. Sort by sort_columns                           |
            |                                                    |
            |  6. Stream to temp parquet file                    |
            |     (batched write, never fully in memory)         |
            +---------------------+------------------------------+
                                  |
                                  v
            +----------------------------------------------------+
            |  After all chunks for a partition are written:     |
            |                                                    |
            |  1. Read all temp files + existing file (if any)   |
            |  2. Merge: keep existing rows OUTSIDE update window|
            |  3. Sort again by sort_columns (interleave chunks) |
            |  4. Write final consolidated partition file        |
            |  5. Delete temp chunk files                        |
            +---------------------+------------------------------+
                                  |
                                  v
            +----------------------------------------------------+
            |  Validate schema and file integrity                |
            +---------------------+------------------------------+
                                  |
                                  v
            +----------------------------------------------------+
            |  DataSource.get_provenance(suffix, previous)       |
            |  - {}: no provenance sidecar                       |
            |  - non-empty dict: write .provenance.pkl.gz        |
            +---------------------+------------------------------+
                                  |
                                  v
            +----------------------------------------------------+
            |  Publish snapshot metadata with optional           |
            |  SnapshotProvenanceRef                             |
            +----------------------------------------------------+

```

**Merge logic (during consolidation):**

When an existing file exists for the partition, the system merges old
and new data. The merge strategy depends on the update mode:

**Normal update / Restate mode:** Replace rows in the update window.
```python
# Pseudocode for normal/restate merge
merged = duckdb.sql(f"""
    -- Keep rows from existing file that are OUTSIDE the update window
    SELECT * FROM existing_rel
    WHERE {date_col} < '{update_start_date}' OR {date_col} > '{update_end_date}'

    UNION ALL

    -- Add all new data from chunks (replaces rows in update window)
    SELECT * FROM chunk_rel_1
    UNION ALL
    SELECT * FROM chunk_rel_2
    -- ... etc
""")
```

This handles the correction window correctly: if `repull_n_days = 5`, the
last 5 business days are completely replaced by new data, while older rows
are preserved from the existing file.

**Backfill mode:** Preserve existing rows, add new rows for missing dates.
```python
# Pseudocode for backfill merge
merged = duckdb.sql(f"""
    -- Keep ALL existing rows (backfill preserves existing data)
    SELECT * FROM existing_rel

    UNION ALL

    -- Add new data only for dates NOT already in existing file
    -- (Date-only check assumes partition+date is complete once present)
    SELECT * FROM new_data
    WHERE {date_col} NOT IN (SELECT DISTINCT {date_col} FROM existing_rel)
""")
```

With backfill, if the source returns data for a date that already exists
in the partition file, the existing rows are kept and the new rows are
discarded. This is safe only when a partition+date combination is
considered complete once any data is present for it.

Since `get_data(...)` is called once per chunk, memory usage stays bounded
regardless of total dataset size. The `DataSource` implementation fetches
only the data needed for each chunk.

### Chunked Partition Processing

For large datasets with coarse partitioning (e.g., year-partitioned data
with millions of rows per year), the DataSource can return chunked
partitions. This keeps memory bounded on both the source and processing
sides.

**How chunking works:**

1. **DataSource decides chunking strategy.** Based on its own constraints
   (API limits, memory, etc.), the DataSource returns multiple
   `PartitionSpec` objects with the same `partition_values` but different
   `start_date` / `end_date` ranges.

2. **DPD groups specs by `partition_values`.** All specs with identical
   `partition_values` belong to the same final partition file. DPD counts
   how many chunks exist for each partition.

3. **DPD assigns chunk IDs based on date range.** Each chunk gets an ID
   in compact `YYYYMMDD_YYYYMMDD` format based on its date range:

   ```
   FutureRoot=ES_year=Y2024_{suffix}_temp_20240101_20240115.parquet
   FutureRoot=ES_year=Y2024_{suffix}_temp_20240116_20240131.parquet
   FutureRoot=ES_year=Y2024_{suffix}_temp_20240201_20240215.parquet
   ...
   ```

   **When `_01`, `_02` suffixes are needed:** If the DataSource chunks on
   dimensions *other than dates* (e.g., fetching different tickers in
   separate API calls), multiple chunks may share the same date range.
   In this case, DPD adds numeric suffixes:

   ```
   # DataSource chunks by ticker within same date range
   FutureRoot=ES_year=Y2024_{suffix}_temp_20240116_20240131_01.parquet  <- chunk_info: {"tickers": ["ESH4", "ESH5"]}
   FutureRoot=ES_year=Y2024_{suffix}_temp_20240116_20240131_02.parquet  <- chunk_info: {"tickers": ["ESM4", "ESM5"]}
   ```

   The DataSource can use the `chunk_info` field in `PartitionSpec` to
   track what each chunk contains (for its own use in `get_data()`).

4. **DPD processes chunks in order, applying transforms per-chunk.**
   For each `PartitionSpec`, DPD:
   - Calls `get_data(partition_spec)`
   - Applies YAML transforms (rename, drop, dropna, dedup)
   - Applies DataCleaner if configured
   - Sorts by `sort_columns`
   - Writes the cleaned, sorted data to a temp file

5. **Consolidation after last chunk.** When all chunks for a partition
   are processed, DPD:
   - Reads all temp files for that partition via DuckDB
   - Merges with existing data (if any) outside the update window
   - Sorts again by `sort_columns` (to interleave chunks correctly)
   - Writes the final consolidated partition file
   - Deletes the temporary chunk files

**Example: year partition with 15-day chunks**

For a year partition with data from Jan 1 - Dec 31, the DataSource might
return 24 chunks (15 days each, plus a final shorter chunk). The
`get_partitions()` return might look like:

```python
[
    PartitionSpec({"FutureRoot": "ES", "year": "Y2024"},
                  start_date=date(2024, 1, 1),
                  end_date=date(2024, 1, 15)),
    PartitionSpec({"FutureRoot": "ES", "year": "Y2024"},
                  start_date=date(2024, 1, 16),
                  end_date=date(2024, 1, 31)),
    # ... more chunks ...
    PartitionSpec({"FutureRoot": "ES", "year": "Y2024"},
                  start_date=date(2024, 12, 16),
                  end_date=date(2024, 12, 31)),
]
```

DPD groups these by `partition_values`, assigns chunk IDs, and processes
each in order, writing temp files (e.g.,
`FutureRoot=ES_year=Y2024_{suffix}_temp_20240101_20240115.parquet`).
After the last chunk (Dec 16-31), DPD consolidates all temp files into
`FutureRoot=ES_year=Y2024_{suffix}_temp.parquet`, then renames to
`FutureRoot=ES_year=Y2024_{suffix}.parquet`.

**Order guarantee:**

Partitions are requested in the exact order returned by `get_partitions()`.
This allows DataSources to:
- Process all chunks for one partition before moving to the next
- Interleave chunks across partitions if desired
- Pre-fetch data for upcoming partitions while processing current ones

The DataSource controls the order; DPD simply iterates and consolidates
when a partition's chunks are complete.

**Year boundary handling:**

When the update date range spans a year boundary (e.g., Dec 28 - Jan 3),
multiple partition files are affected. The system handles this correctly:

- `get_partitions()` returns `PartitionSpec` objects for both years
- Each partition is updated with only the rows that belong to it
- DuckDB's lazy evaluation ensures only relevant data is read/written

Example: updating 5 days spanning Dec 30, 2024 - Jan 3, 2025:

```
PartitionSpec({"FutureRoot": "ES", "year": "Y2024"}, ...)  <- gets Dec 30-31
PartitionSpec({"FutureRoot": "ES", "year": "Y2025"}, ...)  <- gets Jan 1-3
```

---

## Partition Structure on Disk

Each `DatedParquetDataset` lives in its own subdirectory of the cache
root (named after the dataset). Inside, partition data files are
organized in a directory hierarchy, and a `_meta_data/` subdirectory
holds snapshot metadata.

```
{cache_root}/{dataset_name}/
    _meta_data/
        {dataset_name}_{suffix_1}.pkl.gz   <- earlier snapshot
        {dataset_name}_{suffix_2}.pkl.gz   <- current snapshot (largest suffix)
        ...
    {part_col}={part_val}/
        {nav_dir}/
            {col1}={val1}_{col2}={val2}_{suffix_1}.parquet
            {col1}={val1}_{col2}={val2}_{suffix_2}.parquet   <- later update
            ...
```

**Two types of directories:**

1. **Partition directories** use `part_col=part_value` format (Hive-style).
   These represent actual partitions that we filter and prune on. Examples:
   `FutureRoot=ES/`, `year=Y2024/`, `Date=2024-02-27/`.

2. **Navigation directories** use bare values (no `key=`). These exist
   solely to break up large directories for human browsing and filesystem
   efficiency. Examples: `2024/`, `02/`. Navigation directories are NOT
   partitions -- partition truth comes from the snapshot metadata, not
   directory names.

**Directory structure by date partition granularity:**

| `date_partition` | Directory structure | Example |
|---|---|---|
| `year` | `year=YNNNN/` | `year=Y2024/` |
| `quarter` | `YYYY/quarter=QYYYY-N/` | `2024/quarter=Q2024-1/` |
| `month` | `YYYY/month=MYYYY-MM/` | `2024/month=M2024-01/` |
| `week` | `YYYY/week=WYYYY-WW/` | `2024/week=W2024-02/` (ISO week, zero-padded) |
| `day` | `YYYY/MM/{date_col}=YYYY-MM-DD/` | `2024/02/Date=2024-02-27/` |

Additional partition columns (e.g., `FutureRoot`) are placed **before**
the date directories: `FutureRoot=ES/2024/quarter=Q2024-1/` not
`2024/quarter=Q2024-1/FutureRoot=ES/`.

Partition column order is **exactly** the effective `partition_columns`
order (i.e., YAML order after appending the date partition if omitted).
Example: if YAML lists `FutureRoot` before the date partition,
directories and filename context use `FutureRoot` first.

**Parquet filenames encode the full partition context** using
`col=val` pairs separated by `_`, plus the run suffix:
`{col1}={val1}_{col2}={val2}_{suffix}.parquet`. This makes each file
self-describing -- the partition it belongs to can be determined from
the filename alone, without inspecting the directory path.

**Temporary files for atomic writes:**

Files are written atomically using fsync and rename. Temporary files
use the same name with a `_temp` suffix before `.parquet`:

- **Single write:** `{name}_{suffix}_temp.parquet` -> rename to
  `{name}_{suffix}.parquet`

- **Chunked write:** When data is written in chunks, the DPD assigns a
  `chunk_id` to each chunk for temp file naming. The chunk_id is based
  on the date range (compact `YYYYMMDD` format). DPD determines upfront
  how many chunks share the same date range. When multiple chunks share
  dates, they are numbered `_01`, `_02`, `_03`, etc. from the start:
  ```
  {name}_{suffix}_temp_20240101_20240115.parquet      <- unique date range, no suffix
  {name}_{suffix}_temp_20240116_20240131_01.parquet   <- 2 chunks share this range
  {name}_{suffix}_temp_20240116_20240131_02.parquet   <- 2nd chunk, same dates
  ...
      -> consolidate to ->
  {name}_{suffix}_temp.parquet
      -> rename to ->
  {name}_{suffix}.parquet
  ```

All temp files live in a system temp directory (via `tempfile.mkdtemp`),
not beside the final file. This prevents orphaned temp files from
cluttering the cache on crashes. Readers never see the final filename
until the complete file is ready.

### DPD `_meta_data/` directory

Each file in a DPD `_meta_data/` directory is a gzip-compressed pickle file named
`{dataset_name}_{suffix}.pkl.gz`, where `suffix` is a base-36 encoded
timestamp at the start of each update run. The same suffix is used for all
new parquet data files and the metadata pickle written during that run.
Each pickle file is a complete **snapshot** of the dataset state at the
time it was written.

**Contents of each snapshot pickle:**

```python
{
    "schema": pa.Schema,          # PyArrow schema of the dataset
    "cache_start_date": dt.date,  # earliest date in the cache
    "cache_end_date": dt.date,    # latest date in the cache
    "partition_values": dict,     # {column: [distinct values]} for all partition columns
    "lineage": SnapshotLineage | None,  # how this snapshot was produced
    "provenance": SnapshotProvenanceRef | None,  # optional external sidecar
    "files": [
        {
            "path": str,          # relative path from dataset root
            "partition_values": dict,  # all partitions, e.g., {"FutureRoot": "ES", "year": "Y2024"}
            "checksum": str,      # sha256 of the parquet file
            "size_bytes": int,    # file size in bytes
        },
        ...
    ],
    "yaml": dict,                 # full YAML configuration (see below)
}
```

The `cache_start_date` and `cache_end_date` fields track the contiguous
date range covered by the cache. These are used to enforce backfill
constraints (see below).

The `size_bytes` field makes it easy to calculate disk savings when
cleaning up old snapshots: sum the sizes of files that only appear in
older snapshots and not in the current one.

**User annotations in the captured YAML:**

The user `annotations` dictionary is not a separate top-level snapshot field. It
lives under the dataset's captured YAML configuration, which is already stored
inside each snapshot metadata pickle. The library treats that YAML subtree as a
small, user-owned namespace: it stores it, compares it across snapshots, and
returns it to callers as part of the normal metadata/YAML path. The parquet
cache will not use annotations for anything. This information exists only to
make the user's life easier, and only user code should assign semantic meaning
to the dictionary's schema.

`annotations` must be pickle-serializable and equality-comparable. It should stay
small. If a value is large enough that loading it with every snapshot metadata
read would hurt cache performance, store it as external provenance instead.

For a new cache, the first snapshot establishes the initial dictionary. For
later snapshots, omitting `annotations` carries forward the previous snapshot's
dictionary into the new captured YAML. Supplying a dictionary compares it to
the previous snapshot's dictionary before data is fetched or written. The new
dictionary may add keys, including nested keys inside existing dictionaries,
but every previously existing key path must still exist and every previously
existing non-dictionary value must compare equal.

Removing a key, changing a scalar/list/object value, or changing a value's type
raises `ValidationError`. Replacing a dictionary with a non-dictionary also
raises `ValidationError`. To remove or redefine entries, use a new dataset or
an explicit rebuild whose lineage starts a new cache.

The YAML loader must preserve whether `annotations` was omitted. On an existing
cache, omitting annotations inherits the current snapshot annotations, while an
explicit `annotations: {}` is treated as a supplied dictionary and compared
normally.

Legacy snapshots without an `annotations` YAML field are treated as `{}`.
Adding annotations to a legacy empty dictionary is allowed because it is an
append-only extension.

**Notes in the captured YAML:**

The user `notes` string is not a separate top-level snapshot field. It lives
under the dataset's captured YAML configuration, alongside annotations and
column descriptions. The library stores it and carries it forward when omitted,
but users may replace it with any string on a later snapshot.

For a new cache, omitting `notes` means the field is absent. For later
snapshots, omitting `notes` carries forward the previous snapshot's string into
the new captured YAML. Supplying a string replaces the previous value. An empty
string is valid and is the supported way to clear visible notes. Supplying
`notes: null` or any non-string value raises `ValidationError`.

Legacy snapshots without a `notes` YAML field are treated as absent.

**Snapshot lineage metadata:**

Each new DPD snapshot should record how it was produced. This is stored on the
snapshot itself, not in a separate mutable history log.

The lineage deliberately does not include `created_at`. The snapshot suffix is
the chronological identifier.

```python
@dataclass
class DateRange:
    start_date: dt.date
    end_date: dt.date


@dataclass
class SnapshotLineage:
    # None means this snapshot did not use another snapshot as its base.
    base_snapshot: str | None

    # Snapshot id of the first snapshot in this cache lineage.
    first_snapshot_id: str | None

    # "initial", "update", "backfill", "restate", "rebuild", or "unknown".
    operation: str

    # What the caller asked for after automatic date-window resolution.
    # This is optional audit context; actual ranges below are authoritative.
    requested_date_range: DateRange | None

    # Dates newly added to the cache by this snapshot.
    added_date_ranges: list[DateRange]

    # Dates whose previous rows were intentionally replaced by this snapshot.
    rewritten_date_ranges: list[DateRange]

    # Instrument scope of the write, if any.
    instrument_column: str | None
    instrument_scope: str  # "all", "subset", or "unknown"
    instruments: list[str] | None
```

`base_snapshot` is the previous snapshot suffix when the new snapshot is
derived from an existing cache. It is `None` for a new cache, a full rebuild
that intentionally ignores previous metadata, or any other snapshot that is
not based on earlier cache state.

`first_snapshot_id` records the first snapshot id for this cache lineage. It is
the current snapshot suffix for an initial snapshot, and later snapshots carry
it forward from their base snapshot's lineage. This is an id, not a timestamp
field; any display timestamp should be derived from the snapshot id.

If a new lineage-aware snapshot is derived from a legacy snapshot with no
lineage, `base_snapshot` is the legacy snapshot suffix and
`first_snapshot_id=None`. History walking can then report that the chain
continues into legacy metadata whose origin is unknown.

`added_date_ranges` and `rewritten_date_ranges` must remain separate:

- First snapshot for a new cache:
  - `base_snapshot=None`
  - `first_snapshot_id=<new snapshot suffix>`
  - `operation="initial"`
  - `added_date_ranges` contains the written date range
  - `rewritten_date_ranges=[]`
- Normal daily update:
  - `base_snapshot=<previous suffix>`
  - `first_snapshot_id=<previous lineage first_snapshot_id>`
  - `operation="update"`
  - `added_date_ranges` contains the new date range
  - `rewritten_date_ranges=[]`
- Normal update with `repull_n_days`:
  - `added_date_ranges` contains any dates after the previous cache end
  - `rewritten_date_ranges` contains the trailing correction window that was
    re-fetched and replaced
- Backfill:
  - `operation="backfill"`
  - `added_date_ranges` contains the backfilled range
  - `rewritten_date_ranges=[]`
- Restate:
  - `operation="restate"`
  - `rewritten_date_ranges` contains the restated date range
  - `added_date_ranges` is normally empty, unless the restate also expands
    coverage beyond the previous snapshot

The date ranges should represent the best-known actual written scope. In
practice this usually matches the resolved requested date window. If a source
returns no data for part of the requested window, the actual ranges may be
narrower than `requested_date_range`.

Instrument scope is independent of operation and records how the write was
requested, not whether the requested set happens to equal the full universe. A
normal update, backfill, or restate may write all instruments or a requested
subset:

- `instrument_scope="all"` when no instrument filter limited the write.
- `instrument_scope="subset"` when `instruments` was explicitly provided.
  Store the sorted instrument list in `instruments`. This remains `"subset"`
  even if the caller explicitly listed every instrument in the dataset.
- `instrument_scope="unknown"` when no `instrument_column` is defined, for
  legacy snapshots without lineage, or for other cases where the scope cannot
  be determined.

**DPD external snapshot provenance:**

`provenance` is for larger, optional, per-snapshot information that should
travel with the cache but should not be loaded as part of the normal snapshot
metadata read path. The library treats the provenance blob as opaque
user-provided data.

For DPDs, the DataSource supplies this data through `get_provenance()`. The base
implementation returns `{}`. If the returned dictionary is empty, the snapshot
has no external provenance sidecar. If the returned dictionary is non-empty,
the library stores it as a gzip-compressed pickle file:

```text
{dataset_name}/_provenance/{dataset_name}_{snapshot_id}.provenance.pkl.gz
```

The snapshot metadata stores only a small reference:

```python
@dataclass
class SnapshotProvenanceRef:
    # Relative path from dataset root.
    path: str

    # Checksum of the compressed pickle bytes.
    checksum: str

    # Size of the compressed pickle file.
    size_bytes: int
```

The provenance hook must return a `dict[str, Any]`; returning any other type
raises `ValidationError`. The dictionary contents are otherwise unconstrained.
It may contain Python objects such as `datetime.date` or `datetime.datetime`
values. Because it is stored as pickle, a synced cache with provenance should
be treated as a trusted Python artifact, the same way DPD snapshot metadata
pickles are trusted.

Normal metadata loads, dataset discovery, summaries, and reads must not load
provenance sidecars. They only read `SnapshotProvenanceRef`. Callers load the
blob explicitly via `read_provenance(snapshot=None)`.

If `get_provenance()` returns `{}`, no sidecar is written and `provenance` is
`None` in the snapshot metadata.

**YAML configuration in metadata:**

The `yaml` field stores the complete YAML configuration at the time
the snapshot was created. Each update re-reads the current YAML file
and stores a fresh copy in the new snapshot. This provides full
provenance - you can see exactly how the dataset was configured at
any point in its history, including:

- `date_partition`, `partition_columns`, `sort_columns`
- `columns_to_rename`, `columns_to_drop`, `dropna_columns`, `dedup_columns`
- `source_location`, `source_class_name`, `source_init_args`
- `cleaning_class_location`, `cleaning_class_name`, `cleaning_init_args`
- Any other configuration options

If the YAML configuration changes between updates (e.g., adding a new
transform or changing sort order), each snapshot reflects the
configuration that was in effect when it was created.

The captured YAML is authoritative for watched user metadata. On update, the
implementation compares the new YAML `annotations` subtree to the previous
snapshot's captured YAML `annotations` subtree and carries the previous
dictionary forward when the field is omitted. It carries `notes` and
`column_descriptions` forward when omitted, while allowing explicit note edits
and explicit column-description additions or text edits. The top-level snapshot
`provenance` field remains authoritative for external provenance loading and
sync.

**DPD configuration round-trip contract:**

`DatedParquetDataset` is itself a `PDYaml` model, but a cache YAML dataset
entry is larger than the DPD model. It also includes orchestration fields such
as `source_location`, `source_class_name`, YAML transforms, cleaner settings,
and sync-function settings. The implementation therefore needs an adapter
between YAML entries, DPD objects, and snapshot metadata.

That adapter must not be maintained as independent hand-copied field lists in
each caller. DPD-owned configuration fields must be classified once and reused
anywhere the library converts between:

- a YAML dataset entry and `DatedParquetDataset`
- `DatedParquetDataset` and captured snapshot `yaml_config`
- captured snapshot `yaml_config` and a reconstructed `DatedParquetDataset`
  for `update-cache`
- captured snapshot `yaml_config` and a reconstructed `DatedParquetDataset`
  for `CacheRegistry` reads
- captured snapshot `yaml_config` and source-DPD reconstruction for
  `DPDSource` fallback behavior

This rule exists because the cache is increasingly used as a self-describing
artifact. Today, updates are normally built locally and synced to GCS. Future
write paths may update cloud-backed caches directly, so operational fields such
as `use_update_lock`, `lock_dir`, and any future writer/locking settings must
round-trip through the same configuration contract as layout fields such as
`date_col`, `partition_columns`, `instrument_column`,
`num_instrument_buckets`, and `row_group_size`.

Implementation requirements:

- There must be one canonical source for DPD-owned config field names, derived
  from or checked against `DatedParquetDataset.model_fields`.
- Any DPD model field must be explicitly classified as a persisted config
  field, orchestration-only field, runtime-only/internal field, or intentionally
  excluded field.
- Unknown YAML keys must not be silently ignored. If a key is not recognized as
  DPD-owned config, orchestration config, or watched snapshot info, config
  loading should fail clearly.
- Tests must fail when a new DPD config field is added without updating the
  shared config-field classification and metadata reconstruction behavior.
- Tests must cover round-tripping non-default DPD config values through YAML
  loading, snapshot metadata capture, `update-cache` reconstruction, registry
  reconstruction, and source-DPD fallback reconstruction when that fallback is
  supported.

NPDs are intentionally separate from this DPD config contract. NPDs are not
created from cache YAML and do not have DPD layout, transform, source, cleaner,
or lock configuration. NPD snapshot info comes only from the strict
`--info-file` contract (`notes`, `annotations`, and `column_descriptions`) and
explicit provenance comes from `--provenance-file`.

**Note:** The metadata contains the configuration for reference, but may
not be sufficient to re-run the update, or to run configured post-sync hooks,
if the code referenced by `source_location`, `cleaning_class_location`, or
`sync_function_location` is not available, whether that code lives in
cache-local Python files or external `module://` modules. It serves as an
audit trail showing exactly how the data was processed.

**Snapshot selection:**

- The **current snapshot** is the lexicographically last
  `*.pkl.gz` file in `_meta_data/` that does NOT end in `_trimmed.pkl.gz`.
- Files ending in `_trimmed.pkl.gz` are staged for deletion and excluded
  from snapshot selection.
- `pyarrow_dataset()` loads the current snapshot and constructs the
  dataset via `ds.FileSystemDataset.from_paths()` using the file list
  and partition values from the snapshot.
- Older snapshots are kept on disk (never deleted automatically),
  allowing readers to pin to a specific snapshot by loading an
  earlier pickle file explicitly.

**Cache history API:**

The cache should expose a history function for DPDs:

```python
dpd.cache_history(snapshot: str | None = None) -> CacheHistory

registry.cache_history(
    name: str,
    cache_name: str | None = None,
    snapshot: str | None = None,
) -> CacheHistory
```

If `snapshot` is omitted, history starts at the current snapshot. If provided,
history starts at that specific snapshot. The function then walks backward by
following each snapshot's `lineage.base_snapshot`.

The returned history should include, in newest-to-oldest order:

- snapshot suffix,
- operation,
- base snapshot,
- first snapshot id,
- requested date range,
- added date ranges,
- rewritten date ranges,
- instrument scope,
- instrument column and instrument list when applicable,
- a status for each entry.

If a snapshot says it was based on another snapshot that cannot be found, the
history result should include a broken-link entry instead of failing silently.
For example:

```text
Snapshot 1XYZ999 says it was based on 1ABC123, but 1ABC123 was not found.
```

If a snapshot has no lineage metadata, the history result should include a
status entry instead of failing silently. For example:

```text
Snapshot 1XYZ999 has no lineage metadata.
```

**Partition column values are NOT stored in the parquet files.** They
are stored in the snapshot pickle metadata and injected as virtual
columns at read time via `from_paths()` partition expressions.
Directory names are not authoritative for partition resolution.

### Example layout

Year-partitioned with a `FutureRoot` partition column (two update runs):

```
/data/parquet_cache/md.futures_daily/
    _meta_data/
        md.futures_daily_1H4DW00.pkl.gz
        md.futures_daily_1H4DW01.pkl.gz   <- current snapshot
    FutureRoot=ES/
        year=Y2023/FutureRoot=ES_year=Y2023_1H4DW00.parquet
        year=Y2024/FutureRoot=ES_year=Y2024_1H4DW00.parquet
    FutureRoot=NQ/
        year=Y2023/FutureRoot=NQ_year=Y2023_1H4DW00.parquet
        year=Y2024/FutureRoot=NQ_year=Y2024_1H4DW00.parquet
        year=Y2024/FutureRoot=NQ_year=Y2024_1H4DW01.parquet   <- NQ/Y2024 updated in run 2
```

Day-partitioned with `date_col: Date` (date is the only partition):

```
/data/parquet_cache/ec.daily_prices/
    _meta_data/
        ec.daily_prices_1H4DW00.pkl.gz
        ec.daily_prices_1H4DW01.pkl.gz
    2024/                                <- navigation dir (year)
        01/                              <- navigation dir (month)
            Date=2024-01-02/Date=2024-01-02_1H4DW00.parquet
            Date=2024-01-03/Date=2024-01-03_1H4DW00.parquet
        02/
            Date=2024-02-03/Date=2024-02-03_1H4DW01.parquet   <- new date in run 2
    ...
```

Month-partitioned with an instrument column:

```
/data/parquet_cache/md.monthly_stats/
    _meta_data/
        md.monthly_stats_1H4DW01.pkl.gz
    instrument=ES/
        2024/                            <- navigation dir (year)
            month=M2024-01/instrument=ES_month=M2024-01_1H4DW01.parquet
            month=M2024-02/instrument=ES_month=M2024-02_1H4DW01.parquet
    instrument=NQ/
        2024/
            month=M2024-01/instrument=NQ_month=M2024-01_1H4DW01.parquet
    ...
```

Quarter-partitioned with a `FutureRoot` partition column:

```
/data/parquet_cache/md.futures_quarterly/
    _meta_data/
        md.futures_quarterly_1H4DW01.pkl.gz
    FutureRoot=ES/
        2024/                            <- navigation dir (year)
            quarter=Q2024-1/FutureRoot=ES_quarter=Q2024-1_1H4DW01.parquet
            quarter=Q2024-2/FutureRoot=ES_quarter=Q2024-2_1H4DW01.parquet
    ...
```

---

## CLI Tools

Five command-line tools are provided for managing the cache:

### `update-cache`

Updates one or more `DatedParquetDataset`s with new data from their
configured `DataSource`. Supports several modes of operation:

**Normal update (default):** Fetch new data from the day after the
current cache end date through the previous standard business day. This is the typical daily
update mode -- "there's more data available, just get it."

```bash
# Update all datasets with new data
update-cache /path/to/cache

# Update a specific dataset (positional argument after cache_dir)
update-cache /path/to/cache md.futures_daily
```

**Backfill:** Extend the cache backwards in time by fetching historical
data before the current cache start date.

**Backfill constraints:**
- Only `--start-date` is allowed; `--end-date` raises an error
- The `--start-date` must be before the cache's current `cache_start_date`
- The system automatically sets `end_date` to `cache_start_date - 1 day`
- This ensures the cache remains contiguous with no gaps

"Without touching existing data" means that for a given date, existing
rows are preserved. For coarse date partitions (e.g., year), the partition
file may be rewritten with merged data (existing rows plus new rows).

```bash
# Backfill to 2020 (cache currently starts at 2024-01-01)
update-cache /path/to/cache md.futures_daily \
    --backfill --start-date 2020-01-01

# ERROR: --end-date not allowed with --backfill
update-cache /path/to/cache md.futures_daily \
    --backfill --start-date 2020-01-01 --end-date 2020-12-31  # raises error
```

**Instrument-specific updates:** The `--instruments` option supports
two use cases:

1. **Adding new instruments** that weren't previously in the cache.
   Existing instruments are untouched.
2. **Rewriting data for specific instruments** that was incorrect.
   Only the specified instruments are re-fetched and rewritten.

```bash
# Add new instruments (fetches full history for MES, MNQ only)
update-cache /path/to/cache md.futures_daily \
    --instruments MES,MNQ --start-date 2020-01-01

# Rewrite data for specific instruments (fixes bad data)
update-cache /path/to/cache md.futures_daily \
    --instruments ES,NQ --start-date 2024-01-15 --end-date 2024-01-20
```

**Restate (rewrite):** Fix incorrect data by replacing data for the
specified dates and partition values. Both `--start-date` and `--end-date`
are required; omitting either raises an error. For the given date range
(and `--instruments` if specified), all existing rows matching those dates
and partition values are removed and replaced with freshly fetched data.
Rows outside the specified date range or partition values are preserved.

**Completeness assumption:** The DataSource must return a complete
replacement for the requested scope. All existing rows in that scope are
deleted and replaced with the returned data. If the source returns partial
data, the missing rows will be lost.

`--restate` and `--instruments` may be used together **only** if
`instruments` maps to a declared partition column. In that case, restate
rewrites only the specified instrument partitions.

```bash
# Restate a date range (rewrites all instruments in affected partitions)
update-cache /path/to/cache md.futures_daily \
    --restate --start-date 2024-01-15 --end-date 2024-01-20

# Restate only specific instrument partitions (instrument must be a partition column)
update-cache /path/to/cache md.futures_daily \
    --restate --instruments ES,NQ --start-date 2024-01-15 --end-date 2024-01-20
```

**Common options:**

```bash
[dataset_name]        # Optional positional argument after cache_dir
--start-date DATE     # Start of date range (ISO format)
--end-date DATE       # End of date range (ISO format)
--instruments X,Y,Z   # Limit to specific instruments
--backfill            # Fill missing dates without replacing existing data
--restate             # Replace data for specified date range
                      # (--backfill and --restate are mutually exclusive)
--dry-run             # Show what would be updated without writing
```

### `import-npd`

Imports a parquet file or parquet directory as a complete
`NonDatedParquetDataset` snapshot.

The canonical invocation is:

```bash
python -m ionbus_parquet_cache.import_npd CACHE_DIR NAME SOURCE_PATH
```

Use this when reference/static data has already been produced outside the
parquet cache and should be stored under a cache-managed NPD name. Unlike
`update-cache`, this command does not use a `DataSource`, date range,
instrument filter, YAML transforms, or DPD operational metadata. Each
successful import is a full replacement snapshot for the NPD.

**Basic usage:**

```bash
# Import a single parquet file as an NPD snapshot
python -m ionbus_parquet_cache.import_npd \
    /path/to/cache ref.instrument_master /source/instruments.parquet

# Import a directory of parquet files as an NPD snapshot
python -m ionbus_parquet_cache.import_npd \
    /path/to/cache ref.exchange_calendar /source/exchange_calendar/

# Import a snapshot with snapshot info and explicit provenance
python -m ionbus_parquet_cache.import_npd \
    /path/to/cache ref.instrument_master /source/instruments.parquet \
    --info-file /source/instruments.info.yaml \
    --provenance-file /source/instruments.provenance.yaml

# Preview the destination path and suffix without copying
python -m ionbus_parquet_cache.import_npd \
    /path/to/cache ref.instrument_master /source/instruments.parquet \
    --dry-run
```

**Arguments:**

```bash
cache_dir      # Existing local cache root
name           # NPD name to create or update
source_path    # Parquet file or directory containing parquet files
```

**Options:**

```bash
--dry-run          # Validate inputs and show planned import without writing
--skip-validation  # Skip PyArrow dataset validation before copying
--info-file PATH   # Strict YAML file for notes/annotations/column_descriptions
--provenance-file PATH  # YAML mapping stored as explicit snapshot provenance
--verbose          # Print source, destination, and snapshot details
```

**Source rules:**

- `source_path` must exist.
- A file source must be a parquet file and is copied to
  `non-dated/{name}/{name}_{suffix}.parquet`.
- A directory source must contain at least one `.parquet` file, recursively.
  The directory is copied to `non-dated/{name}/{name}_{suffix}/`, preserving
  its internal structure. Hive partition directories are preserved, but a flat
  directory of parquet files is also valid.
- By default, before publishing, the command validates that the source can be
  opened as a PyArrow parquet dataset and that PyArrow can discover its schema.
  For directory imports, this is the PyArrow dataset schema compatibility check;
  the command does not perform additional semantic validation. If validation
  fails, no cache files are written.
- With `--skip-validation`, the command skips the PyArrow dataset-open check.
  The source must still exist, single-file sources must still end in
  `.parquet`, and directory sources must still contain at least one `.parquet`
  file. Use this only for trusted or already validated parquet, such as
  multi-hop cache sync workflows where validation has already happened
  upstream.
- PyArrow validation can take noticeable time for large directory trees because
  it performs dataset discovery before copying.

**Info and provenance rules:**

- `--info-file` is optional. It must be a YAML mapping with only these
  top-level keys: `notes`, `annotations`, `column_descriptions`.
- The command intentionally has no per-field info flags such as `--notes`,
  `--notes-str`, `--annotations`, or `--column-descriptions`. All NPD snapshot
  info comes from `--info-file`.
- Unknown `--info-file` keys are a hard error. The command must not silently
  ignore extra fields.
- Before publishing, the command resolves snapshot info by validating supplied
  fields, loading the previous NPD info sidecar when present, carrying forward
  omitted `notes`, `annotations`, and `column_descriptions`, and writing the
  normalized result for the new suffix. If there is no previous info, omitted
  fields remain absent.
- Explicit `notes` must be a string. `notes: ""` is allowed and means the
  visible notes were intentionally cleared. `notes: null` is rejected.
- Explicit `annotations` must be a dictionary and follows the shared
  append-only annotation rules.
- Explicit `column_descriptions` must be a `dict[str, str]`; existing keys may
  not be removed.
- `--provenance-file` is optional and separate from `--info-file`. It must be a
  YAML mapping, but its keys are user-defined.
- Provenance never carries forward. If `--provenance-file` is omitted, the new
  NPD snapshot has no provenance sidecar even if the previous NPD snapshot had
  one.
- An empty `--provenance-file` mapping is treated the same as no supplied
  provenance and does not write a provenance sidecar.
- An empty `--info-file` mapping supplies no fields. It still resolves snapshot
  info normally, so any previous `notes`, `annotations`, and
  `column_descriptions` carry forward.
- On dry run, the command validates `--info-file` and `--provenance-file` but
  writes no data, info sidecar, or provenance sidecar.
- If validation, data copy, info sidecar writing, or provenance sidecar writing
  fails, the import must not publish a new current NPD snapshot. The command
  should perform best-effort cleanup of any files written for the failed suffix.

**Name and destination rules:**

- `cache_dir` must be an existing local cache directory. Direct imports into
  `gs://` caches are not supported; import locally and use `sync-cache push`.
- If `name` does not already exist as an NPD, the command creates
  `non-dated/{name}/`.
- If `name` already exists as an NPD, the command creates a new snapshot under
  the existing NPD directory. Older snapshots remain available.
- If snapshot info is supplied, snapshot info is carried forward, or provenance
  is supplied, the command writes
  `non-dated/{name}/_meta_data/{name}_{suffix}.pkl.gz` as a gzip-compressed
  pickle containing normalized snapshot info and, when present, the provenance
  reference.
- If provenance is supplied, the command writes
  `non-dated/{name}/_provenance/{name}_{suffix}.provenance.pkl.gz` and stores
  a reference to it in the NPD info sidecar.
- If a DPD with the same name already exists in the same cache, the command
  fails by default. DPD/NPD name collisions are confusing because registry
  lookup prefers DPDs.
- The new snapshot suffix must be lexicographically greater than the current
  NPD suffix. If the generated suffix would collide or move backward, the
  command fails without changing the current snapshot. Timestamp suffixes have
  one-second granularity, so two imports of the same NPD in quick succession
  can collide. If this happens, wait at least one second and retry.

**Output:**

On success, the command prints the dataset name, new suffix, and destination
snapshot path:

```text
ref.instrument_master: imported snapshot 1H4DW01
path: /path/to/cache/non-dated/ref.instrument_master/ref.instrument_master_1H4DW01.parquet
```

On dry run, the command prints the same planned destination but performs no
copy and does not create directories.

### `local-subset`

Creates normal local `DatedParquetDataset` snapshots from filtered source DPD
snapshots in another cache. The source cache may be local or remote, including
GCS. The destination cache must be local. NPDs are intentionally out of scope
for this command.

The expected operating model is a local working copy of a larger source cache:
materialize the subset once, run repeated local work against it, then refresh it
by re-running the subset spec. The destination is still a normal local DPD, so
the base cache APIs do not forbid later ordinary updates, but those updates are
outside the normal local-subset maintenance path.

The subset YAML file remains the durable source of truth for normal refreshes.
The local subset provenance records the resolved source snapshot, effective
spec hash, spec path, and filter summary for auditability and idempotence, but
it is not a complete embedded copy of the subset spec and is not sufficient to
reconstruct every refresh input. For local/manual use, the recommended layout is
to keep subset specs under the destination cache's `yaml/` directory; the
destination cache's `code/` directory may be empty. Worker or deployment-managed
flows may instead keep those YAML files with the job configuration and pass
explicit spec paths to the command.

The detailed design and acceptance rules live in
[Local_Subsetting_Spec.md](Local_Subsetting_Spec.md). This section links the
feature into the main cache spec and records its relationship to the base DPD
contract.

The canonical invocation is:

```bash
python -m ionbus_parquet_cache.local_subset SPEC.yaml [options]
```

The destination is a normal local DPD snapshot. It follows the same snapshot
metadata, partition-value, provenance-sidecar, and read semantics described in
this spec. In particular, partition columns are structural: they must be
available while writing the subset, but normal DPD layout rules may store those
values in directory names rather than physical parquet columns.

The destination dataset name is chosen by the subset spec. The recommended
standard pattern is to use a subset-specific destination name so the complete
remote dataset keeps its real name and the local subset is explicit. Reusing
the source dataset name is allowed only when the source and destination caches
are different, and is mainly useful when the local cache should intentionally
shadow the remote cache in registry lookup order.

Re-running the command is idempotent. If the latest local subset provenance
records the same resolved source snapshot and effective subset spec hash, the
command exits successfully without publishing a new snapshot unless `--force`
is supplied.

If a dataset entry matches zero rows, no destination snapshot is published for
that entry. Multi-dataset specs are atomic per dataset, not across the whole
spec: one dataset can publish while another fails, and the command exits
nonzero if any selected dataset fails.

### `cleanup-cache`

Manages disk usage by removing old snapshots and trimming data.

**Two distinct operation types:**

| Operation | Flags | Behavior |
|---|---|---|
| **Snapshot cleanup** | `--older-than`, `--keep-last`, `--snapshot`, `--find-orphans` | Analysis only - generates delete script, no immediate changes |
| **Trim** | `--keep-days`, `--before-date` | **Write operation** - immediately mutates cache, generates delete/undo scripts |

**Snapshot cleanup** is safe to run anytime. It analyzes the cache and
writes a script (`.sh` on Linux/Mac, `.bat` on Windows) for you to review
and run manually. No changes are made until you run the script.

**Trim is a write operation with rollback semantics.** It immediately
creates a new snapshot and renames files with a `_trimmed` suffix. This
requires stopping all readers before running. See
[Trimming (Dangerous Operation)](#trimming-dangerous-operation) for details.

**What gets cleaned up automatically vs. via script:**

| Automatic (no script) | Via cleanup script |
|---|---|
| Temp files during normal `update` operations | Old snapshot files |
| Consolidation temp files after successful write | Orphaned files from crashes |
| | Trimmed data files |

During normal update operations, the system automatically cleans up its
own temporary files (chunk files, consolidation temps) after successful
completion. These are internal working files, not user data.

The `cleanup-cache` command targets *real data files* (old snapshots,
orphaned files, trimmed data).

**Cleanup modes (mutually exclusive):**

| Mode | Description |
|---|---|
| Default | Analyze both DPDs and NPDs |
| `--dpd-only` | Analyze DPDs only, exclude all NPDs |
| `--npd-only` | Analyze NPDs only, exclude all DPDs |
| `--dataset NAME [NAME ...]` | Analyze only the named datasets (DPDs or NPDs) |

Only one mode can be specified. Combining `--dpd-only`, `--npd-only`, or
`--dataset` raises an error.

**Analyzing old snapshots:**

```bash
# List all old snapshots and their reclaimable disk space
cleanup-cache /path/to/cache

# Example output:
#   md.futures_daily:
#     1H4DW00 (2024-01-01)  3 files, 1.2 GB reclaimable
#     1H4DW01 (2024-01-10)  <- current snapshot
#   md.minute_bars:
#     1H4DW01 (2024-01-10)  <- current snapshot (no old snapshots)
#   instrument_master (NPD):
#     1H4DW00 (2024-01-01)  50 MB reclaimable
#     1H4DW01 (2024-01-10)  <- current snapshot
#
#   Total reclaimable: 1.25 GB

# Generate cleanup script for snapshots older than 30 days
cleanup-cache /path/to/cache --older-than 30

# Example output:
#   Wrote cleanup script: /path/to/cache/_cleanup_1H4DW00.sh
#   Review the script, then run it to delete files.

# Generate script for specific datasets only
cleanup-cache /path/to/cache --dataset md.futures_daily md.minute_bars --older-than 30

# Keep only the N most recent snapshots per dataset
cleanup-cache /path/to/cache --keep-last 3

# Target a specific snapshot
cleanup-cache /path/to/cache --dataset md.futures_daily --snapshot 1H4DW00
```

#### Trimming (Dangerous Operation)

**WARNING: Trimming is unlike other cache operations.** Normal operations
(updates, syncs, snapshot cleanup) are safe to run while readers are
accessing the cache. Trimming is NOT safe - it renames files in place and
can invalidate data currently being read.

**Key differences from normal operations:**

| Normal Operations | Trimming |
|---|---|
| Snapshot contents are immutable | Renames files in place |
| Safe while readers are active | Must run when cache is NOT in use |
| Single script (delete) | Two scripts (delete OR undo) |
| Optional to run script | MUST run one of the two scripts |

**When to use trimming:**

Trimming is useful for keeping only recent data in a local cache while
the full history lives on a shared server. For example, keeping only
the last 30 business days of minute bars locally.

**Requirements:**

1. **Stop all readers** before running a trim command
2. **Run one of the two scripts** after trimming completes
3. **Do not leave cache in intermediate state** - if neither script is
   run, cache is in an unsupported state

**Usage:**

```bash
# Trim data older than 30 standard business days
cleanup-cache /path/to/cache --dataset md.minute_bars --keep-days 30

# Trim data before a specific date
cleanup-cache /path/to/cache --dataset md.minute_bars --before-date 2024-01-01
```

**Trimming lifecycle:**

When you run a trim command, the following happens immediately:

1. **New metadata created:** A new snapshot is created that excludes the
   trimmed files (this becomes the current snapshot). The new metadata
   has accurate values computed from the kept files only:
   - `cache_start_date` is set to the cutoff date
   - `partition_values` is recomputed from the kept files only
2. **Old metadata renamed:** Previous snapshot metadata files are renamed
   with a `_trimmed` suffix (e.g., `md.minute_bars_1H4DW01_trimmed.pkl.gz`)
3. **Data files renamed:** Data files to be deleted are renamed with a
   `_trimmed` suffix (e.g., `data_1H4DW00_trimmed.parquet`)
4. **Two scripts generated:**
   - `_cleanup_{suffix}_delete.sh` (or `.bat`) - removes all `*_trimmed*` files
   - `_cleanup_{suffix}_undo.sh` (or `.bat`) - reverts everything

**After trim command completes:**

```
_meta_data/
    md.minute_bars_1H4DW02.pkl.gz            <- NEW (current snapshot)
    md.minute_bars_1H4DW01_trimmed.pkl.gz    <- old snapshot, marked for cleanup
    md.minute_bars_1H4DW00_trimmed.pkl.gz    <- older snapshot, marked for cleanup

FutureRoot=ES/
    year=Y2023/data_1H4DW01_trimmed.parquet  <- data to be deleted
    year=Y2024/data_1H4DW02.parquet          <- kept (in new snapshot)

_cleanup_1H4DW00_delete.sh   <- run to permanently delete trimmed files
_cleanup_1H4DW00_undo.sh     <- run to restore pre-trim state
```

**You MUST run one of the two scripts:**

- **Run delete script:** Removes all `*_trimmed*` files. Cache is now
  permanently trimmed. Readers can resume.
- **Run undo script:** Deletes the new metadata, renames all `_trimmed`
  files back to original names. Cache is restored to pre-trim state.
  Readers can resume.

**If neither script is run:** The cache is in an unsupported intermediate
state. Files with `_trimmed` suffix exist alongside the new snapshot.
While snapshot selection will work (it excludes `_trimmed` files), this
state is unsafe - disk space is wasted, and future operations may behave
unexpectedly. Always run one of the two scripts to complete the trim.

**Options:**

```bash
--older-than N             # Target snapshots older than N days
--keep-last N              # Keep only the N most recent snapshots per dataset
--keep-days N              # Trim data older than N standard business days (DPDs only)
--before-date DATE         # Trim data before this date (DPDs only)
--dataset NAME [NAME ...]  # Limit to specific datasets (can be DPDs or NPDs)
--dpd-only                 # Analyze DPDs only, exclude all NPDs
--npd-only                 # Analyze NPDs only, exclude all DPDs
--snapshot SUFFIX          # Target a specific snapshot by suffix
--find-orphans             # Search for orphaned files (see below)
```

Note: `--keep-days` and `--before-date` options do not apply to NPDs
(since they have no date partitioning).

#### Finding orphaned files

Over time, crashes, interrupted updates, or bugs may leave orphaned files
in the cache - temp files that were never cleaned up, or data files that
are no longer referenced by any snapshot. The `--find-orphans` mode
scans for these files:

```bash
# Scan for orphaned files
cleanup-cache /path/to/cache --find-orphans

# Example output:
#   Scanning cache for orphaned files...
#   Loaded metadata for 5 datasets
#   Found 1,247 parquet files on disk
#   Found 1,241 files referenced by snapshots
#
#   Orphaned files (6 files, 245 MB):
#     md.futures_daily/_tmp/chunk_abc123_0.parquet  (temp file)
#     md.futures_daily/_tmp/chunk_abc123_1.parquet  (temp file)
#     md.futures_daily/FutureRoot=ES/year=Y2023/data_20231015_xyz789.parquet
#     md.minute_bars/_tmp/chunk_def456_0.parquet  (temp file)
#     ...
#
#   Wrote cleanup script: /path/to/cache/_cleanup_1H4DW00.sh
#   Review the script, then run it to delete files.
```

**How it works:**

1. Discovers all DPD and NPD snapshots, and loads DPD snapshot metadata plus
   optional NPD info sidecars when present
2. Scans for all `.parquet` files in the cache directory tree
3. Identifies files not referenced by any snapshot:
   - Temp files in `_tmp/` directories (from interrupted updates)
   - Data files orphaned by bugs or manual edits
   - Info/provenance sidecars whose suffix does not correspond to a discovered
     snapshot
4. Writes a cleanup script with delete commands for each orphan

**Why a script instead of direct deletion?**

Cleanup operations are inherently risky - a bug in the detection logic
could incorrectly identify valid files for deletion. Writing a script
instead of auto-deleting provides a safety check:

- You can review the script before running it
- The script serves as a record of what was deleted
- No risk of data loss from a bug that corrupts state

**Platform-specific scripts:**

The cleanup script is written to the cache root with a base-36 timestamp suffix:
- **Linux/Mac:** `_cleanup_{suffix}.sh` (uses `rm` commands)
- **Windows:** `_cleanup_{suffix}.bat` (uses `del` commands)

The `{suffix}` is a 7-character base-36 encoded timestamp (e.g., `_cleanup_1H4DW00.sh`).
The tool detects the current platform and generates the appropriate script format.

### `sync-cache`

Syncs a cache between locations. Supports both **push** (local -> remote)
and **pull** (remote -> local). Works with local paths and GCS (`gs://...`).
S3 support will be implemented in a future release.
By default, only the current snapshot is synced (not historical
snapshots), making it efficient for replication.

**What gets synced:**

- Data files (parquet)
- DPD snapshot metadata (`_meta_data/*.pkl.gz`)
- NPD info sidecars (`non-dated/{name}/_meta_data/{name}_{suffix}.pkl.gz`)
  for selected NPD snapshots, when present
- Referenced DPD and NPD provenance sidecars
  (`_provenance/*.provenance.pkl.gz`)

**What does NOT get synced:**

- `yaml/` directory (managed via git, only needed for updates)
- `code/` directory (managed via git, only needed for updates)

**Note:** A synced cache is fully readable without yaml/ or code/.
Dataset discovery is based on directory structure and metadata, not yaml
configuration. If you need to run updates on the destination, clone the
yaml/ and code/ git repos separately.

**Sync modes (mutually exclusive):**

| Mode | Description |
|---|---|
| Default | Sync both DPDs and NPDs |
| `--dpd-only` | Sync DPDs only, exclude all NPDs |
| `--npd-only` | Sync NPDs only, exclude all DPDs |
| `--datasets NAME [NAME ...]` | Sync only the named datasets (whitelist) |
| `--ignore-datasets NAME [NAME ...]` | Exclude named datasets from sync (blacklist) |

Only one filtering mode can be specified. Combining `--dpd-only`, `--npd-only`,
or `--datasets` raises an error. `--datasets` and `--ignore-datasets` are mutually exclusive.

**Push (local to remote):**

```bash
# Push all datasets
sync-cache push /path/to/cache /path/to/destination

# Push specific dataset(s) only
sync-cache push /path/to/cache /path/to/destination --datasets md.futures_daily md.futures_intraday

# Exclude specific datasets
sync-cache push /path/to/cache /path/to/destination --ignore-datasets eod_prices_bucketed

# Push DPDs only (no NPDs)
sync-cache push /path/to/cache /path/to/destination --dpd-only

# Push NPDs only (no DPDs)
sync-cache push /path/to/cache /path/to/destination --npd-only
```

**Pull (remote to local):**

```bash
# Pull all datasets
sync-cache pull /path/to/source /path/to/local/cache

# Pull specific dataset(s) only
sync-cache pull /path/to/source /path/to/local/cache --datasets md.futures_daily instrument_master

# Exclude specific datasets
sync-cache pull /path/to/source /path/to/local/cache --ignore-datasets eod_prices_bucketed
```

**Snapshot selection:**

By default, only the current (latest) snapshot is synced. Use `--snapshot`
to sync specific historical snapshots, or `--all-snapshots` for all snapshots:

```bash
# Sync specific snapshots only
sync-cache push /path/to/cache /path/to/destination \
    --datasets md.futures_daily --snapshot 1H4DW01 1H4DW02

# Include all historical snapshots
sync-cache push /path/to/cache /path/to/destination \
    --datasets md.futures_daily --all-snapshots
```

**Daemon mode (continuous sync):**

```bash
# Run continuously, checking for updates every 60 seconds
sync-cache pull /path/to/source /path/to/local --daemon

# Custom update interval (in seconds)
sync-cache pull /path/to/source /path/to/local \
    --daemon --update-interval 300
```

In daemon mode, the process runs indefinitely, periodically checking
the source for new snapshots and syncing them to the destination.
During normal operations (updates and syncs), snapshot contents are
immutable - readers can safely access data while new snapshots are
being written. There's no risk of reading partial or inconsistent state.

**Exception:** Trimming operations rename files in place and can
invalidate data being read. See [Trimming (Dangerous Operation)](#trimming-dangerous-operation)
for details.

**Options:**

```bash
--datasets NAME [NAME ...]      # Sync only these datasets (whitelist)
--ignore-datasets NAME [NAME ...] # Exclude these datasets (blacklist)
--snapshot SUFFIX [SUFFIX ...]  # Sync specific snapshots by suffix
--dpd-only                      # Sync DPDs only, exclude all NPDs
--npd-only                      # Sync NPDs only, exclude all DPDs
--all-snapshots                 # Include all historical snapshots
--dry-run                       # Show what would be synced without copying
--delete                        # Delete files at destination that aren't in source
--daemon                        # Run continuously instead of once
--update-interval N             # Seconds between sync checks (default: 60)
--rename OLD:NEW                # Copy dataset with a different name at destination
--run-sync-functions            # Run configured post-sync functions after copying
--run-sync-only                 # Run configured sync functions without copying
--sync-function LOCATION:NAME   # Run this sync function for selected snapshots
--sync-function-init-args JSON  # Optional JSON kwargs for CLI sync function class
--workers N                     # Parallel upload/download workers (default: 8)
```

**Renaming datasets during sync:**

The `--rename` option copies a dataset to the destination with a different
name. This is useful for creating a copy of a dataset under a new name
without modifying the source cache.

```bash
# Copy md.futures_daily as md.futures_daily_backup
sync-cache push /path/to/cache /path/to/destination \
    --datasets md.futures_daily --rename md.futures_daily:md.futures_daily_backup

# Pull and rename
sync-cache pull /path/to/source /path/to/local \
    --datasets instrument_master --rename instrument_master:instrument_master_v2
```

**Important:** The rename option works with only a single dataset. When you
use `--rename`, you must specify `--datasets` with exactly one dataset name.
If you do not specify `--datasets`, the old dataset name is automatically
used as the filter. Only one rename mapping can be specified per command.
The source dataset name must match the dataset being synced.

**Snapshot info and provenance sidecars:**

Snapshot sidecars are selected and synced with their owning snapshot. This
applies to both local-path syncs and GCS-backed syncs. Missing optional sidecars
are not errors; a legacy NPD snapshot without an info sidecar is still a valid
snapshot.

When `sync-cache` selects a DPD snapshot, it copies:

1. the snapshot metadata pickle,
2. every parquet file referenced by that metadata,
3. the provenance sidecar at the expected path, if present:
   `_provenance/{dataset_name}_{snapshot_id}.provenance.pkl.gz`.

When `sync-cache` selects an NPD snapshot, it copies:

1. the snapshot file or snapshot directory,
2. the NPD info sidecar at the expected path, if present:
   `_meta_data/{dataset_name}_{snapshot_id}.pkl.gz`,
3. the provenance sidecar at the expected path, if present:
   `_provenance/{dataset_name}_{snapshot_id}.provenance.pkl.gz`.

No separate flag is required to sync info or provenance sidecars. They are part
of the cache snapshot, unlike sync functions, which are optional external side
effects. A snapshot with no info sidecar has no snapshot info to copy. A
snapshot with `provenance=None` has no provenance sidecar to copy.

The sync contract is convention-based. If a user creates or renames some other
info/provenance-like file outside the expected snapshot naming convention,
`sync-cache` does not promise to discover or copy it. Sidecars augment
snapshots; they never cause a snapshot suffix to be selected on their own.

For `--snapshot` and `--all-snapshots`, info/provenance sidecars follow the
same snapshot-selection rules as metadata and parquet files. For `--rename`,
NPD info sidecar paths and provenance sidecar paths follow the same
dataset-name path rewrite as the rest of the selected snapshot files.

**Sync functions (optional post-sync operations):**

Sync functions allow a dataset to declare optional supplemental work that can
run after cache files have been synced (e.g., syncing provenance metadata,
updating external catalogs, or replicating auxiliary data).

Defining a sync function in YAML does **not** make it run automatically. The
normal `sync-cache push` and `sync-cache pull` commands copy cache files only.
Configured sync functions run only when the user explicitly requests them with
`--run-sync-functions`, or when retrying them with `--run-sync-only`.

**Expected use cases:**

- Fast file replication: copy cache files between local locations or between
  cache storage locations without updating any external systems. This is the
  default `sync-cache` behavior and should stay fast and side-effect free.
- Publishing a cache update: push files from a local cache to remote storage,
  then update an asset registry, provenance catalog, or other external system
  with metadata that differs from the cache's internal snapshot metadata. This
  requires `--run-sync-functions` or `--sync-function`.
- Repairing post-sync side effects: if file copying succeeded but an asset
  registry or catalog update failed, rerun only the external update with
  `--run-sync-only`.

**Execution modes:**

```bash
# Copy selected cache files, then run configured post-sync functions
sync-cache push /path/to/cache /path/to/destination --run-sync-functions

# Copy selected cache files, then run a command-line sync function
sync-cache push /path/to/cache /path/to/destination \
    --sync-function module://my_library.sync_utils:sync_provenance

# Retry configured post-sync functions without copying files
sync-cache push /path/to/cache /path/to/destination --run-sync-only

# Retry a command-line sync function without copying files
sync-cache push /path/to/cache /path/to/destination \
    --run-sync-only \
    --sync-function module://my_library.sync_utils:sync_provenance
```

For the first implementation, sync functions are supported only for
local-source push operations:

```bash
sync-cache push /local/cache /mounted/cache --run-sync-functions
sync-cache push /local/cache gs://bucket/cache --run-sync-functions
```

If `--run-sync-functions`, `--run-sync-only`, or `--sync-function` is used with
pull, a remote source cache, remote-to-remote sync, or any destination backend
that `sync-cache` cannot verify, the command should fail with a clear error.
Local filesystem destinations, including other disks or mounted filesystems,
are supported. This keeps config loading simple: YAML and cache-local code are
loaded from the local source cache, and `module://` hooks are loaded from the
local Python environment.

`--run-sync-functions` runs configured sync functions only after all selected
cache files have been copied successfully. It does not interleave copying and
post-sync hooks.

`--run-sync-only` performs no file copying and runs only the configured sync
functions for the selected datasets and snapshots. It is intended for retrying
external side effects after a previous sync copied files but a post-sync
function failed. Before calling a sync function, the command should verify that
the selected destination snapshot exists:

- For DPDs, the destination metadata pickle, data files referenced by that
  snapshot, and any convention-named provenance sidecar should exist.
- For NPDs, the destination snapshot file or snapshot directory should exist,
  along with any convention-named info/provenance sidecars that were selected
  for sync.

These checks apply to both local filesystem destinations and supported remote
destinations such as GCS.

`--dry-run` is mutually exclusive with both `--run-sync-functions` and
`--run-sync-only`. `--dry-run` is also mutually exclusive with
`--sync-function`, because passing a command-line sync function requests an
external side effect. Sync functions may perform external side effects, so
dry-run mode must fail fast instead of pretending to simulate them.

**Post-sync ordering and execution:**

Post-sync execution has two phases:

1. Copy all selected cache files.
2. If all file copies succeeded, run selected sync functions.

Sync functions run serially in deterministic dataset/snapshot order, not in
the file-copy worker pool. If any sync function fails, remaining sync functions
are not run and `sync-cache` exits nonzero. Files already copied are not rolled
back. Use `--run-sync-only` to retry the post-sync phase after fixing the
underlying problem.

**Command-line sync function override:**

Instead of configuring a sync function in YAML, a caller may provide one at
sync time:

```bash
sync-cache push /path/to/cache /path/to/destination \
    --datasets md.futures_daily \
    --sync-function module://my_library.sync_utils:SyncProvenance \
    --sync-function-init-args '{"catalog_url": "https://internal.catalog.io"}'
```

`--sync-function LOCATION:NAME` identifies the callable to run for every
selected dataset snapshot. The `LOCATION` part follows the same rules as
`sync_function_location` in YAML:

- empty/built-in location for built-in functions,
- cache-local paths such as `code/sync_functions.py`,
- installed packages such as `module://my_library.sync_utils`.

The callable name after the final `:` is the function name or class name.
If the callable is a class, `--sync-function-init-args` may provide JSON
keyword arguments for its constructor. If it is a plain function,
`--sync-function-init-args` should be omitted.

When `--sync-function` is provided, it is used for all selected datasets and
snapshots and per-dataset YAML `sync_function_*` settings are ignored for that
command. This avoids mixed ordering between YAML-configured hooks and an
operator-supplied one-off hook. To run the command-line hook for only one
dataset, combine it with `--datasets NAME`.

Providing `--sync-function` implies that sync functions should run after the
file sync. It does not require `--run-sync-functions`. `--run-sync-only` may be
combined with `--sync-function` to retry the one-off hook without copying
files.

**DPD and NPD support:**

Sync functions apply cleanly to both dataset types:

- For DPDs, `snapshot_id` is the DPD snapshot suffix from `_meta_data/`.
- For NPDs, `snapshot_id` is the NPD snapshot suffix from the snapshot file or
  directory name under `non-dated/`.

The sync command should call the function once per selected dataset snapshot.
That means the default current-snapshot sync calls at most one function per
dataset, while `--snapshot ...` or `--all-snapshots` may call a function
multiple times for the same dataset.

**Function signature:**

A sync function must accept the selected source and destination dataset names,
the dataset type, the snapshot ID, and the source/destination cache locations:

```python
def my_sync_function(
    source_dataset_name: str,  # Dataset name at the source cache
    dest_dataset_name: str,    # Dataset name at the destination cache
    dataset_type: str,         # "dpd" or "npd"
    snapshot_id: str,          # Selected DPD or NPD snapshot suffix
    source_location: str,      # Source cache root (/local/cache or gs://bucket/cache)
    dest_location: str,        # Destination cache root
) -> None:
    """Sync supplemental data for the selected dataset snapshot."""
    # Update provenance, replicate metadata, notify catalogs, etc.
    # Raise an exception to fail the sync operation.
```

When `--rename OLD:NEW` is used, `source_dataset_name` is `OLD` and
`dest_dataset_name` is `NEW`. Hooks that only care about the destination name
should use `dest_dataset_name`.

**Configuration in YAML:**

Add optional sync function settings to a dataset:

```yaml
datasets:
  md.futures_daily:
    source_location: module://my_library.sources
    source_class_name: FuturesDataSource
    sync_function_location: ""                          # or code/sync_functions.py or module://pkg.sync
    sync_function_name: sync_provenance                 # function name or class name
    sync_function_init_args:                            # optional kwargs passed to __init__ if class-based
      catalog_endpoint: "https://catalog.example.com"
```

Configured per-dataset sync functions currently come from DPD YAML entries.
NPD sync targets are supported by the sync function runner, but NPDs do not
yet have a YAML metadata/configuration contract because they are imported with
`python -m ionbus_parquet_cache.import_npd` rather than created from YAML. Use
the CLI `--sync-function` override for NPD sync hooks until an NPD configuration
contract exists.

When sync functions are requested, datasets without a configured sync function
are skipped and a warning is logged for each skipped dataset. This warning
applies whether one dataset or many datasets were selected. Missing configured
sync functions are not errors: if all selected datasets lack configured sync
functions, the command exits successfully with warnings and a summary such as
`0 sync functions run`.

For credentials and secret handling in `sync_function_init_args`, see
[Credentials and Secrets](#credentials-and-secrets).

**Sourcing sync functions:**

Sync functions can come from three locations (same as DataSource):

1. **Built-in** (empty `sync_function_location`) - use a built-in function if available
2. **Cache-local** (`code/sync_functions.py`) - Python file in the cache's `code/` directory
3. **Installed package** (`module://my_library.sync`) - function/class from an installed Python package

Sync-function sourcing follows the supported execution modes described above.
In those modes, YAML and cache-local hooks are loaded from the local source
cache, and `module://` hooks are loaded from the local Python environment.

The loader must validate the resolved object:

- If the location or name cannot be loaded, raise `ConfigurationError`.
- If a class cannot be instantiated with `sync_function_init_args` or
  `--sync-function-init-args`, raise `ConfigurationError`.
- If the final object is not callable, raise `ConfigurationError`.

**Examples:**

Local sync function (in `code/sync_functions.py`):

```python
def sync_provenance(
    source_dataset_name: str,
    dest_dataset_name: str,
    dataset_type: str,
    snapshot_id: str,
    source_location: str,
    dest_location: str,
) -> None:
    """Copy provenance metadata from source to destination."""
    import json
    from pathlib import Path

    source_prov = (
        Path(source_location)
        / "provenance"
        / source_dataset_name
        / f"{snapshot_id}.json"
    )
    dest_prov = Path(dest_location) / "provenance" / dest_dataset_name

    if source_prov.exists():
        dest_prov.mkdir(parents=True, exist_ok=True)
        with open(source_prov) as f:
            data = json.load(f)
        data["dataset_name"] = dest_dataset_name
        data["dataset_type"] = dataset_type
        data["snapshot_id"] = snapshot_id
        with open(dest_prov / f"{snapshot_id}.json", "w") as f:
            json.dump(data, f)
```

Installed package sync function:

```yaml
datasets:
  md.futures_daily:
    source_class_name: FuturesDataSource
    sync_function_location: module://my_library.sync_utils
    sync_function_name: SyncProvenance
    sync_function_init_args:
      catalog_url: "https://internal.catalog.io"
```

In `my_library/sync_utils.py`:

```python
class SyncProvenance:
    """Syncs provenance data to an external catalog."""

    def __init__(self, catalog_url: str):
        self.catalog_url = catalog_url

    def __call__(
        self,
        source_dataset_name: str,
        dest_dataset_name: str,
        dataset_type: str,
        snapshot_id: str,
        source_location: str,
        dest_location: str,
    ) -> None:
        # Fetch provenance from source, update location, POST to catalog
        import requests

        prov = self._fetch_provenance(
            source_location,
            source_dataset_name,
            snapshot_id,
        )
        prov["dest_dataset_name"] = dest_dataset_name
        prov["dest_location"] = dest_location
        prov["dataset_type"] = dataset_type
        resp = requests.post(f"{self.catalog_url}/snapshots", json=prov)
        resp.raise_for_status()

    def _fetch_provenance(
        self,
        location: str,
        dataset_name: str,
        snapshot_id: str,
    ) -> dict:
        # Implementation
        pass
```

**Common sync-function patterns:**

- **Record sync events:** append one row per selected dataset snapshot to an
  audit table or log, including `dataset_type`, `dest_dataset_name`,
  `snapshot_id`, and `dest_location`. This is useful for operational tracking
  without adding mutable state to snapshot metadata.
- **Update a metadata catalog:** publish destination cache locations,
  checksums, first/last dates, lineage summaries, or provenance references to
  an external catalog after the files are safely in place.
- **Sync auxiliary metadata:** copy or transform user-owned metadata that is
  intentionally outside the cache snapshot contract. Cache-local provenance
  sidecars follow their own convention and are copied automatically; arbitrary
  auxiliary files should be handled explicitly by a sync function.
- **Handle reruns idempotently:** sync functions should tolerate
  `--run-sync-only` retries. A function can check whether the external record
  already exists for `(dest_dataset_name, dataset_type, snapshot_id)` and skip,
  update, or verify it rather than blindly appending duplicates.
- **Skip when nothing external is needed:** a sync function may return without
  side effects for datasets, snapshots, or rerun conditions that do not need an
  external catalog update.

**When sync function errors occur:**

If a sync function raises after file copying, `sync-cache` exits nonzero. Files
already copied are not rolled back. Callers should treat the destination
snapshot as operationally incomplete for any external systems maintained by the
sync function.

Use `--run-sync-only` to retry the configured sync functions after the file
sync has already succeeded.

**S3 paths (future release):**

S3 support will be implemented in a future release. When available, S3
locations will use the format `s3://bucket-name/path/to/cache`. AWS
credentials will be read from the environment (`AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`) or from `~/.aws/credentials`.

**Sync behavior:**

- For each DPD, syncs only the files listed in the current snapshot's
  metadata pickle (unless `--all-snapshots`)
- For each NPD, syncs the current snapshot file or directory
- Uses checksums to skip files that already exist at the destination
  with matching content
- Creates the destination directory structure as needed
- With `--delete`, removes files at destination that aren't in the
  source snapshot (use with caution)

### `rename-cache`

Renames a `DatedParquetDataset` directory in place. The parquet data files
are untouched; only the `_meta_data/*.pkl.gz` files (which store the dataset
name internally) and the directory itself are modified.

**Usage:**

```bash
# Preview (no changes)
python -m ionbus_parquet_cache.rename_cache /path/to/cache old_name new_name --dry-run

# Rename
python -m ionbus_parquet_cache.rename_cache /path/to/cache old_name new_name
```

**Implementation — safe recovery ordering:**

1. **Validate:** `old_dir` exists with `_meta_data/`; `new_dir` does not exist.
2. **Write new metadata:** For each `<old_name>_<suffix>.pkl.gz`, read it,
   set `metadata.name = new_name`, write as `<new_name>_<suffix>.pkl.gz`
   alongside the old file. *Interrupted here → delete the new files; old cache
   is untouched.*
3. **Rename directory:** `old_name/` → `new_name/`. *Interrupted here → rename
   back.*
4. **Delete old metadata:** Remove `<old_name>_*.pkl.gz` from
   `new_name/_meta_data/`. *(Cleanup only — rename already committed.)*

Trimmed snapshots (`*_trimmed.pkl.gz`) are left in place without renaming.

**Errors raised:**
- `FileNotFoundError` — `old_dir` missing, no `_meta_data/`, or no `.pkl.gz`
  snapshot files found
- `FileExistsError` — `new_dir` already exists
- `ValueError` — names are empty or identical

### `yaml-create-datasets`

Creates or updates datasets based on YAML configuration files. This tool
reads YAML files from the `yaml/` subdirectory and creates or updates
`DatedParquetDataset` entries in the cache.

NPD snapshots are not created by YAML. Use
`python -m ionbus_parquet_cache.import_npd` to import a parquet file or
directory as a `NonDatedParquetDataset` snapshot.

**Basic usage:**

```bash
# Create/update all datasets defined in yaml/ files
yaml-create-datasets /path/to/cache

# Create/update a specific dataset
yaml-create-datasets /path/to/cache md.futures_daily
```

**Options:**

```bash
[dataset_name]       # Optional positional argument to target a specific dataset
--preserve-config    # Keep existing cache config, use YAML's DataSource only
--dry-run            # Show what would be created without making changes
```

**The `--preserve-config` flag:**

By default, `yaml-create-datasets` applies the full YAML configuration to
the dataset, including partition columns, date column, transforms, and
other settings. The `--preserve-config` flag changes this behavior:

- **Without `--preserve-config`:** Full YAML config is applied. Use this
  when creating a new dataset or when you want to update all settings.
- **With `--preserve-config`:** Keep the dataset's existing configuration
  (partition columns, date column, transforms, etc.) but use the YAML's
  `DataSource` definition for future updates.

**When to use `--preserve-config`:**

This flag is useful when you want to change the data source for an existing
dataset without modifying its structure. Common scenarios:

1. **Switching data providers:** The dataset structure stays the same, but
   data comes from a different source.
2. **Updating source credentials:** The source class or connection details
   changed, but the data format is identical.
3. **Fixing source bugs:** A bug in the original DataSource was fixed, and
   you want to use the corrected version for future updates.

```bash
# Update data source while preserving existing dataset configuration
yaml-create-datasets /path/to/cache md.futures_daily --preserve-config
```

**Note:** `--preserve-config` requires the dataset to already exist in the
cache. If the dataset doesn't exist, the flag is ignored and the full YAML
configuration is applied.

---

## GCS Support

Google Cloud Storage paths (`gs://`) are supported for **reading** and
**syncing**, but not yet for direct write operations. The intended workflow
is to maintain and update the cache locally, then push it to GCS using
`sync-cache push`.

**What works today:**

- **`CacheRegistry` reads:** A `CacheRegistry` can be opened with a
  `gs://bucket-name/path/to/cache` root. All `DatedParquetDataset` and
  `NonDatedParquetDataset` discovery, metadata loading, and data reads
  work against GCS paths.
- **`sync-cache push/pull`:** Both directions are supported. Use
  `sync-cache push /local/cache gs://bucket/cache` to replicate a local
  cache to GCS, and `sync-cache pull gs://bucket/cache /local/cache` to
  download a remote cache locally.
- **NPD snapshot path resolution:** A
  `NonDatedParquetDataset` stored on GCS correctly resolves its current
  snapshot path, allowing reads via the standard API.

**What does not work yet:**

- **`DatedParquetDataset.update()` / `publish()`:** Writing new snapshots
  or updating data directly to a GCS-backed DPD is not supported.
  Atomic metadata writes and locking rely on local filesystem semantics
  that are not yet adapted for GCS.
- **`python -m ionbus_parquet_cache.import_npd` /
  `NonDatedParquetDataset.import_snapshot()`:** Importing a new snapshot
  directly into a GCS-backed NPD is not supported. Write locally and sync to
  GCS instead.

**Recommended workflow:**

```python
# 1. Update or import into the cache locally
dpd = registry.get_dataset("md.futures_daily", cache_name="local")
dpd.update()

# For an NPD:
# python -m ionbus_parquet_cache.import_npd /local/cache ref.instrument_master /source/instruments.parquet

# 2. Push the updated cache to GCS (via CLI or Python)
# sync-cache push /local/cache gs://bucket-name/cache

# 3. Readers access GCS directly via CacheRegistry
from ionbus_parquet_cache import CacheRegistry

registry = CacheRegistry.instance(gcs="gs://bucket-name/cache")
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    cache_name="gcs",
)
```

Full GCS write support (direct DPD updates and NPD snapshot imports
against GCS-backed caches) is planned for a future release.

---

## Future Work: Auto-Partitioning for Large Datasets

**Not implemented in initial release**, but planned for future versions.

For very large datasets (e.g., tick data with billions of rows per day),
the current fixed partitioning scheme may not be sufficient. The
`core_utils/parquet_cache` system has an auto-partitioning feature
(`PCPartition`) that dynamically determines partition boundaries based
on row counts.

**Planned features:**

- `auto_id_column`: Column that uniquely identifies a symbol
- `auto_partition_columns`: Hierarchy of columns for partitioning
- `auto_min_size`: Minimum rows before splitting into separate partition
- Dynamic `part_col` generation stored in per-day metadata

**Design considerations for future compatibility:**

The current design should not preclude auto-partitioning:

1. **Snapshot metadata is extensible.** The pickle format can store
   additional per-file or per-day partition metadata without breaking
   existing snapshots.

2. **`DataSource` interface is flexible.** The `get_partitions()` method
   returns `PartitionSpec` objects that can describe any partition
   structure, including dynamically computed ones.

3. **`from_paths()` construction.** Because we build datasets from
   explicit file lists with partition expressions (not Hive directory
   scanning), we can handle arbitrary partition schemes.

4. **No assumptions about partition uniformity.** The system already
   handles partitions that vary across the dataset (e.g., some
   instruments have more partitions than others).

When implementing auto-partitioning, the main additions will be:
- New YAML fields (`auto_id_column`, etc.)
- Extended snapshot pickle format with per-day partition info
- Logic in update flow to compute partitions dynamically

---

## Comparison with `core_utils/parquet_cache`

### Class and architecture changes

| Feature | `core_utils/parquet_cache` | `ionbus_parquet_cache` |
|---|---|---|
| `PCQuery` class | [x] | -> `DatedParquetDataset` |
| `SimpleParquet` class | [x] | -> `NonDatedParquetDataset` (snapshot-based) |
| `ParquetCache` orchestrator | [x] YAML-driven | Not needed (datasets are self-contained) |
| `ParquetMetaCache` singleton | [x] | -> `CacheRegistry` (see below) |
| Configuration | YAML file(s) in cache root | YAML files in `yaml/` subdirectory |
| `ionbus_utils` | [ ] uses `core_utils` | [x] |

### `ParquetMetaCache` -> `CacheRegistry`

| Feature | `ParquetMetaCache` | `CacheRegistry` |
|---|---|---|
| Singleton pattern | [x] `instance()` | [x] `instance()` |
| Multiple cache locations | [x] named kwargs | [x] named kwargs |
| Search order | [x] registration order | [x] registration order |
| Target specific cache | [x] `cache_name=` param | [x] `cache_name=` param |
| `read_data()` | [x] | [x] (returns `pd.DataFrame`) |
| `read_data_pl()` | [ ] | [x] (returns `pl.DataFrame`) |
| `pyarrow_dataset()` | [x] | [x] |
| `data_summary()` | [x] | [x] |
| Add caches after init | [x] `add_named_cache_paths()` | [x] `add_cache()` |
| Lazy loading | [x] | [x] |
| Default local cache | [x] `CU_PARQUET_CACHE` env var | [x] `IONBUS_PARQUET_CACHE` env var |

### Data source and transformation

| Feature | `core_utils/parquet_cache` | `ionbus_parquet_cache` |
|---|---|---|
| `columns_to_drop` | [x] YAML option | [x] YAML option |
| `columns_to_rename` | [x] YAML option | [x] YAML option |
| `fillna` / `dropna` | [x] YAML options | [x] `dropna_columns` YAML option |
| Deduplication | [ ] | [x] `dedup_columns` / `dedup_keep` YAML options |
| Cleaning class | [x] `module_location` / `function_name` (pandas) | [x] `cleaning_class_*` (DuckDB relation, lazy) |

### Reading and partitioning

| Feature | `core_utils/parquet_cache` | `ionbus_parquet_cache` |
|---|---|---|
| Reading mechanism | pandas or PyArrow dataset (optional) | PyArrow dataset always |
| `PyarrowReadMode` enum | [x] `AUTO`, `AGGRESSIVE`, `NEVER` | [ ] always PyArrow |
| `create_pyarrow_dataset_from_file_info` | [x] optional | [x] always (via snapshot metadata) |
| Auto-partitioning (`PCPartition`) | [x] | [ ] not initially (planned future work) |
| `instrument_col` / `instrument_list` | [x] | Handled via `--instruments` CLI option |

### Schema and versioning

| Feature | `core_utils/parquet_cache` | `ionbus_parquet_cache` |
|---|---|---|
| Schema change handling | optional recreate or skip | error on incompatible change |
| `recreate_on_schema_change` | [x] YAML option | [ ] not needed (additive schema) |
| Snapshot versioning | [ ] (overwrites in place) | [x] DPDs: `_meta_data/*.pkl.gz`; NPDs: suffixed files in `non-dated/` |
| Rollback to prior snapshot | [ ] | [x] (load earlier snapshot) |

### CLI and sync

| Feature | `core_utils/parquet_cache` | `ionbus_parquet_cache` |
|---|---|---|
| Update cache CLI | [x] `refresh_cache_from_*` | [x] `update-cache` |
| Import NPD CLI | [ ] | [x] `python -m ionbus_parquet_cache.import_npd` |
| Disk sync | [x] `refresh_cache_from_disk` (pull only) | [x] `sync-cache` (push/pull) |
| S3 support | [ ] | [ ] (planned for future release) |
| Daemon sync mode | [x] `update_interval`, `shutdown_time` | [x] `--daemon`, `--update-interval` |
| Quiet hours | [x] | [ ] not needed (snapshots are safe) |
| Trim old data locally | [x] `number_of_days` in sync YAML | [x] `cleanup-cache --keep-days` |
| Calendar-aware updates | [x] `yesterday_calendar` | [x] standard business days (Mon-Fri); exchange calendars not initially |
| `end_date_str` for testing | [x] | [x] |

**Key simplifications:**

1. **Data transformations via DuckDB.** YAML supports `columns_to_drop`,
   `columns_to_rename`, `dropna_columns`, `dedup_columns`/`dedup_keep`,
   and a cleaning class (`cleaning_class_location`/`cleaning_class_name`/
   `cleaning_init_args`) for arbitrary transformations. All transformations
   are executed lazily via DuckDB for performance and memory efficiency.
   The `DataCleaner` receives a `duckdb.DuckDBPyRelation` (not a DataFrame),
   enabling streaming writes for large datasets.

2. **YAML files in `yaml/` subdirectory.** Each file may define
   multiple datasets under a `datasets:` key. Similar flexibility to
   `core_utils`, but with a dedicated subdirectory for organization.

3. **`NonDatedParquetDataset` uses snapshot imports.** Unlike DPDs which
   support incremental date-based updates, NPDs are updated by importing
   complete snapshots via `python -m ionbus_parquet_cache.import_npd` or
   `import_snapshot()`. Each import creates a new versioned snapshot (file or
   directory with base-36 suffix).

4. **Always PyArrow dataset.** There is no `PyarrowReadMode` enum or
   toggle. Reading always uses a `pyarrow.dataset.Dataset`. The
   `create_pyarrow_dataset_from_file_info` option is effectively always
   on -- file info comes from the snapshot metadata.

5. **`CacheRegistry` replaces `ParquetMetaCache`.** Same multi-cache
   search functionality with the same API pattern (singleton, named
   cache locations, search order, `cache_name` targeting).

6. **`ionbus_utils` instead of `core_utils`.** Specifically:
   - `ionbus_utils.logging_utils.logger` instead of
     `core_utils.logging_utils.logger`
   - `ionbus_utils.date_utils` for date helpers
   - `ionbus_utils.file_utils` for file operations
   - Any missing helpers will be added to `ionbus_utils`.

7. **Snapshot versioning.** DPDs use `_meta_data/` pickle files - each
   update writes a new `{name}_{suffix}.pkl.gz` containing the full file
   list, partition values, and checksums. NPDs use suffixed files/directories
   in `non-dated/`. The latest suffix is the current snapshot. Older
   snapshots remain on disk for rollback. This also eliminates the need
   for `recreate_on_schema_change` -- new columns are additive and safe.

8. **Bidirectional sync with S3.** Push and pull to/from S3 buckets,
   with daemon mode for continuous synchronization. Quiet hours are not
   needed because snapshot-based reads are always consistent.

9. **Trimming old data via `cleanup-cache`.** The `number_of_days`
   option from disk sync YAML is replaced by `cleanup-cache --keep-days`,
   which creates a new snapshot excluding old files.

---

## Instrument Hash Bucketing

### Overview

When a DataSource returns one PartitionSpec per instrument per date-partition
(e.g., one spec per ticker per year), the default layout creates one directory
per instrument. With tens of thousands of instruments this creates filesystem
metadata pressure. Instrument hash bucketing solves this by grouping instruments
into a fixed number of bucket directories based on a deterministic hash.

### Reserved constant

```python
INSTRUMENT_BUCKET_COL = "__instrument_bucket__"
```

Defined at module level in `dated_dataset.py`. No user field may use this name
in `partition_columns` or `sort_columns`; doing so raises `ValidationError`.

### New DatedParquetDataset fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `instrument_column` | `str \| None` | `None` | Column name holding the instrument identifier (e.g., `"ticker"`). When used **without** `num_instrument_buckets`, this is just metadata tagging which column contains instrument IDs (existing non-bucketed behavior). |
| `num_instrument_buckets` | `int \| None` | `None` | Number of hash buckets. Must be set together with `instrument_column` to activate bucketing. |

### Validation rules (enforced in `model_post_init`)

1. `num_instrument_buckets` set without `instrument_column` → `ValidationError`
2. `instrument_column` in `partition_columns` when `num_instrument_buckets` is set → `ValidationError` (the library manages the column via `__instrument_bucket__`)
3. `INSTRUMENT_BUCKET_COL` in `partition_columns` → `ValidationError` (reserved)
4. `INSTRUMENT_BUCKET_COL` in `sort_columns` → `ValidationError` (reserved)

### Bucket hash formula

```python
import zlib
from ionbus_utils.base_utils import int_to_base
bucket_int = zlib.crc32(instrument.encode()) % num_instrument_buckets
bucket_str = int_to_base(bucket_int, 36).zfill(3)  # 3-char base-36, e.g. "02K"
```

Base-36 (digits 0–9 and A–Z) is used because macOS and Windows both have
case-insensitive filesystems. A mixed-case alphabet (e.g. base-62) produces
pairs like `"1U"` and `"1u"` that resolve to the same directory, silently
merging buckets. Base-36 avoids this entirely.

The hash is deterministic and stable across Python versions (zlib.crc32 is
not subject to Python's hash randomization).

### Partition column injection

When `num_instrument_buckets` is set, `model_post_init` rewrites
`partition_columns` to:

```python
[INSTRUMENT_BUCKET_COL] + [other non-date cols] + [date_partition_col]
```

The `instrument_column` itself is NOT added to `partition_columns`; it remains
a payload column in the parquet files.

The default `sort_columns` changes from `[date_col]` to
`[instrument_column, date_col]`.

### Directory layout example

```
eod_prices_bucketed/
    __instrument_bucket__=02K/
        year=Y2024/
            eod_prices_bucketed_02K_Y2024_1WXY2A.parquet   # ~125 tickers
    __instrument_bucket__=073/
        year=Y2024/
            eod_prices_bucketed_073_Y2024_1wXyZa.parquet
    ...
```

### BucketedDataSource

`BucketedDataSource` (in `data_source.py`) is the abstract base class for any
DataSource targeting a bucketed dataset. Subclasses implement two methods;
all bucketing machinery lives in the base class.

**Interface:**

```python
class BucketedDataSource(DataSource, ABC):
    @abstractmethod
    def get_instruments_for_time_period(
        self, start_date: dt.date, end_date: dt.date
    ) -> list: ...

    @abstractmethod
    def get_data_for_bucket(
        self,
        instruments: list,
        start_date: dt.date,
        end_date: dt.date,
    ) -> pa.Table | None: ...
```

**`get_partitions()` behavior:**
Generates `num_buckets × num_date_partitions` `PartitionSpec`s with zero I/O.
Each spec has `INSTRUMENT_BUCKET_COL` and the date partition column in its
`partition_values`. No instrument enumeration happens here.

**`get_data(spec)` behavior:**
1. Extracts `bucket_str` and `date_part_val` from `spec.partition_values`
2. If `date_part_val` not in `_period_bucket_cache`: calls
   `get_instruments_for_time_period(spec.start_date, spec.end_date)`, hashes
   results into buckets via `bucket_instruments()`, stores in cache keyed by
   `date_part_val` (e.g. `"Y2024"`)
3. Looks up `bucket_instrument_list` from cache; returns `None` immediately if empty
4. Otherwise calls `get_data_for_bucket(bucket_instrument_list, ...)`

**Cache lifecycle:** `_period_bucket_cache` is cleared in `prepare()`. This
means instrument lists are re-fetched on each full update run. Within a run,
each date partition value triggers at most one `get_instruments_for_time_period`
call.

**Validation:** `DatedParquetDataset._do_update()` raises `ValidationError` if
`num_instrument_buckets` is set but the provided DataSource is not a
`BucketedDataSource`.

### DataSource.get_data() returning None

Spec: when `get_data()` returns `None`, the pipeline skips that partition. No
parquet file is written and no error is raised. This applies in both bucketed
and non-bucketed modes.

In bucketed mode: `BucketedDataSource.get_data()` returns `None` immediately
when a bucket has no instruments for the period. `get_data_for_bucket()` is
never called for empty buckets.

### Breaking change detection

`_publish_snapshot()` stores `num_instrument_buckets` in `yaml_config`. On
`_load_metadata()`, if the stored value and the current instance value differ
(including `None` vs an integer), a `ValidationError` is raised:

```
num_instrument_buckets mismatch: cache was built with 4
but current config has 8. Changing num_instrument_buckets requires a full rebuild.
```

### Partial-instrument update guard

`update(..., instruments=[...])` raises `ValidationError` when
`num_instrument_buckets` is set. Writing only a subset of instruments to an
existing bucket file would overwrite all other instruments in that file. Until
this combination is explicitly blocked. Full-universe updates (no `instruments`
argument) are unaffected.

### `instruments` parameter on `read_data()` / `read_data_pl()`

Both methods accept:

```python
instruments: str | list[str] | set[str] | None = None
```

When `instruments` is provided:

1. If `instrument_column` is not set → `ValueError`
2. Build a row-level filter:
   - `(instrument_column, "in", sorted_instruments)`
3. If `num_instrument_buckets` is set, also compute bucket strings and add:
   - `(INSTRUMENT_BUCKET_COL, "in", sorted_bucket_strs)`
4. Merge with any user-supplied `filters`
5. Delegate to `super().read_data()` (or `read_data_pl()`) with combined filters
