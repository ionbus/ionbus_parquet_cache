# ionbus_parquet_cache

Python tools for managing versioned Parquet datasets with date partitioning,
snapshot versioning, multi-cache lookup, YAML-driven dataset creation, and
CLI workflows for update, cleanup, and synchronization.

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
- CLI modules for dataset creation, updating, cleanup, and cache sync

Full [documentation on GitHub](https://github.com/ionbus/ionbus_parquet_cache).

## Requirements

- Python >= 3.9
- See `requirements.txt` for runtime dependencies

## License

MIT License
