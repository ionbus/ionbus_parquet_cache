# ionbus_parquet_cache Architecture Guide

This document explains the class structure and provides a roadmap for understanding the codebase.

## Package Overview

`ionbus_parquet_cache` is a parquet dataset management system with snapshot versioning. It provides:

- **DatedParquetDataset (DPD)**: Date-partitioned datasets with incremental updates
- **NonDatedParquetDataset (NPD)**: Complete snapshot imports (no date partitioning)
- **CacheRegistry**: Multi-cache search across prioritized locations
- **Update Pipeline**: Fetch data from sources, apply transforms, write parquet files
- **CLI Tools**: Command-line interface for updates, cleanup, and sync

## File Structure

```
ionbus_parquet_cache/
    __init__.py              # Package exports
    exceptions.py            # Exception hierarchy
    snapshot.py              # Base-62 timestamp utilities
    partition.py             # PartitionSpec and date partition utilities
    filter_utils.py          # Filter utilities for PyArrow expressions
    parquet_dataset_base.py  # ParquetDataset abstract base class
    dated_dataset.py         # DatedParquetDataset + SnapshotMetadata
    non_dated_dataset.py     # NonDatedParquetDataset
    cache_registry.py        # CacheRegistry singleton
    data_source.py           # DataSource abstract class
    data_cleaner.py          # DataCleaner abstract class
    update_pipeline.py       # Update execution helpers
    yaml_config.py           # YAML configuration loader
    builtin_sources.py       # HiveParquetSource, DPDSource
    cli.py                   # CLI commands
    tests/                   # Test suite (244 tests)
```

## Suggested Reading Order

### Level 1: Foundation (Read First)

These modules have no internal dependencies and establish core concepts:

1. **`exceptions.py`** - Exception taxonomy
   - `ParquetCacheError` (base)
   - `SchemaMismatchError`, `SnapshotError`, `DataSourceError`, etc.
   - Understanding these helps interpret error conditions throughout the code

2. **`snapshot.py`** - Snapshot suffix utilities
   - `generate_snapshot_suffix()` - Creates 6-char base-62 timestamps
   - `parse_snapshot_suffix()` - Converts suffix back to Unix timestamp
   - `extract_suffix_from_filename()` - Extracts suffix from file names
   - Key insight: Suffixes are lexicographically ordered = chronologically ordered

3. **`partition.py`** - Partition specifications
   - `PartitionSpec` dataclass - Defines a partition's values and date range
   - `date_partition_value()` - Converts dates to partition strings (e.g., "M2024-01")
   - `date_partitions()` - Splits date ranges into partition chunks
   - Key insight: Date partitions (day/week/month/quarter/year) drive file organization

4. **`filter_utils.py`** - Filter utilities
   - `or_filter()`, `and_filter()` - Combine PyArrow expressions
   - `pandas_filter_list_to_expression()` - Convert pandas-style filters
   - `build_filter_expression()` - Build expression from tuples
   - `build_dataset_filter()` - Combined date + user filters with dataset type validation

### Level 2: Core Classes

5. **`parquet_dataset_base.py`** - Abstract base class
   - `ParquetDataset` - Common interface for DPD and NPD
   - Key methods: `pyarrow_dataset()`, `to_table()`, `read_data()`, `refresh()`
   - Lazy loading pattern: `_build_dataset()` called on first access

6. **`dated_dataset.py`** - Primary dataset class
   - `SnapshotMetadata` - Pickled metadata (files, schema, date range)
   - `DatedParquetDataset` - Date-partitioned with `_meta_data/` directory
   - Key methods: `update()`, `_publish_snapshot()`, `read_data(start, end)`
   - Dependency: parquet_dataset_base.py, snapshot.py, partition.py

7. **`non_dated_dataset.py`** - Simpler dataset class
   - `NonDatedParquetDataset` - Imports complete snapshots
   - Key method: `import_snapshot()` - Copies parquet file/directory
   - Stored in `non-dated/` directory with suffixed names

8. **`cache_registry.py`** - Multi-cache coordinator
   - `CacheRegistry` - Singleton pattern with `instance()`
   - Searches multiple cache locations in priority order
   - Auto-discovers DPDs (by `_meta_data/`) and NPDs (by `non-dated/`)

### Level 3: Data Pipeline

9. **`data_source.py`** - Data input interface
   - `DataSource` - Abstract class for supplying data
   - Must implement: `available_dates()`, `prepare()`, `get_data()`
   - Default `get_partitions()` handles most cases
   - Class attributes: `chunk_days`, `partition_values`

10. **`data_cleaner.py`** - Data transformation interface
   - `DataCleaner` - Abstract class for custom transforms
   - Operates on DuckDB relations (lazy evaluation)
   - `NoOpCleaner` - Pass-through implementation

11. **`update_pipeline.py`** - Update execution
    - `WriteGroup` - Groups specs with same partition values
    - `UpdatePlan` - Complete plan with temp file paths
    - `compute_update_window()` - Determines date range to fetch
    - `execute_update()` - Fetches data, applies cleaner, writes files
    - This is the "engine" that `DatedParquetDataset.update()` uses

### Level 4: Configuration & CLI

12. **`yaml_config.py`** - YAML-based configuration
    - `DatasetConfig` - All settings for a DPD
    - `load_all_configs()` - Loads from `yaml/` directory
    - `apply_yaml_transforms()` - Rename, drop, dropna, dedup via DuckDB
    - Dynamic class loading from `code/` directory

13. **`builtin_sources.py`** - Standard data sources
    - `HiveParquetSource` - Reads existing parquet files
    - `DPDSource` - Reads from another DatedParquetDataset
    - Used when `source_location` is empty in YAML

14. **`cli.py`** - Command-line interface
    - `update_cache_main()` - Updates datasets from sources
    - `cleanup_cache_main()` - Analyzes old snapshots
    - `sync_cache_main()` - Push/pull between locations

## Class Hierarchy

```
ParquetDataset (parquet_dataset_base.py)
    ├── DatedParquetDataset (dated_dataset.py)
    └── NonDatedParquetDataset (non_dated_dataset.py)

DataSource (data_source.py)
    ├── HiveParquetSource (builtin_sources.py)
    ├── DPDSource (builtin_sources.py)
    └── [User implementations]

DataCleaner (data_cleaner.py)
    ├── NoOpCleaner (data_cleaner.py)
    └── [User implementations]

ParquetCacheError (exceptions.py)
    ├── SchemaMismatchError
    ├── SnapshotError
    │   ├── SnapshotPublishError
    │   └── SnapshotNotFoundError
    ├── DataSourceError
    ├── SyncError
    ├── ValidationError
    └── ConfigurationError
```

## Key Data Flows

### Reading Data
```
CacheRegistry.read_data("dataset_name", start_date, end_date)
    └── DatedParquetDataset.read_data()
        └── ParquetDataset.pyarrow_dataset()
            └── _build_dataset()  [lazy, cached]
                └── pyarrow.dataset.dataset(files)
```

### Updating Data
```
DatedParquetDataset.update(source, cleaner)
    ├── compute_update_window()     # What dates to fetch
    ├── source._do_prepare()        # Set up source
    ├── source.get_partitions()     # List of PartitionSpecs
    ├── build_update_plan()         # Assign temp file paths
    └── execute_update()
        ├── source.get_data(spec)   # Fetch data
        ├── cleaner(rel)            # Transform (optional)
        ├── write temp files
        ├── consolidate chunks
        └── _publish_snapshot()     # Atomic metadata update
```

### CLI Update Flow
```
update-cache /path/to/cache --dataset my_dataset
    └── cli._run_update()
        ├── yaml_config.load_all_configs()
        ├── config.to_dpd()
        ├── config.create_source(dpd)
        ├── config.create_cleaner(dpd)
        └── dpd.update(source, cleaner)
```

## Key Concepts

### Snapshot Suffixes
- 6-character base-62 string (e.g., `1Gz5hK`)
- Encodes Unix timestamp in seconds
- Lexicographic order = chronological order
- Applied to both data files and metadata files

### Partition Structure
- DPDs use Hive-style partitioning: `column=value/`
- Date partition column derived from `date_partition` setting
- File names include all partition values + snapshot suffix

### Metadata Storage
- DPDs: `_meta_data/{name}_{suffix}.pkl.gz` (pickled SnapshotMetadata)
- NPDs: Files/directories suffixed directly in `non-dated/`

### Lazy Evaluation
- PyArrow datasets built on first access, cached
- DuckDB transforms remain lazy until final write
- Enables memory-efficient processing of large datasets

## Quick Reference

| Task | Method/Function |
|------|-----------------|
| Read DPD data | `dpd.read_data(start_date, end_date)` |
| Read NPD data | `npd.read_data()` |
| Update DPD | `dpd.update(source, cleaner)` |
| Import NPD | `npd.import_snapshot(path)` |
| Multi-cache read | `registry.read_data("name", ...)` |
| Load YAML config | `get_dataset_config(cache_dir, name)` |
| Create DPD from config | `config.to_dpd()` |
| Apply YAML transforms | `apply_yaml_transforms(rel, config)` |
