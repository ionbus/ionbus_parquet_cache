# ionbus_parquet_cache

Python tools for managing versioned Parquet datasets with date partitioning,
snapshot versioning, multi-cache lookup, YAML-driven dataset creation, NPD
snapshot imports, and CLI workflows for update, cleanup, and synchronization.

## Installation

```bash
pip install ionbus-parquet-cache
```

Or install from source:

```bash
pip install -e .
```

## Includes

- `CacheRegistry` for reading from one or more cache locations
- `DatedParquetDataset` for date-partitioned, incrementally updated datasets
- `NonDatedParquetDataset` for full-refresh reference datasets
- `DataSource` and `DataCleaner` extension points
- YAML configuration helpers for declarative dataset setup
- Snapshot lineage, cache history, YAML annotations, column descriptions, and
  optional external provenance sidecars
- CLI modules for dataset creation, NPD imports, updating, cleanup, cache sync,
  and post-sync hooks

Full [documentation on GitHub](https://github.com/ionbus/ionbus_parquet_cache).

## Requirements

- Python >= 3.9
- See `requirements.txt` for runtime dependencies

## License

MIT License
