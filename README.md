# ionbus_parquet_cache

![IonBus Parquet Cache](ionbus_parquet_cache.png)

<!-- TOC start (generated with https://bitdowntoc.derlin.ch/) -->

- [Installation](#installation)
- [Table of Contents](#table-of-contents)
- [What is this library?](#what-is-this-library)
- [Quick Start](#quick-start)
   * [Reading data](#reading-data)
   * [Creating and updating datasets](#creating-and-updating-datasets)
- [Reading Data](#reading-data-1)
   * [Basic reading](#basic-reading)
   * [Filtering data](#filtering-data)
   * [Using with other tools](#using-with-other-tools)
   * [Checking for updates](#checking-for-updates)
   * [Reading from historical snapshots](#reading-from-historical-snapshots)
   * [Reading non-dated (reference) data](#reading-non-dated-reference-data)
- [Dataset Types](#dataset-types)
   * [DatedParquetDataset (DPD)](#datedparquetdataset-dpd)
   * [NonDatedParquetDataset (NPD)](#nondatedparquetdataset-npd)
   * [Search order and naming conventions](#search-order-and-naming-conventions)
   * [Specifying dataset type](#specifying-dataset-type)
- [Using Multiple Caches](#using-multiple-caches)
- [Updating Data](#updating-data)
   * [Creating a new dataset (from YAML)](#creating-a-new-dataset-from-yaml)
   * [Routine updates (from metadata)](#routine-updates-from-metadata)
   * [Backfill (add historical data)](#backfill-add-historical-data)
   * [Restate (fix bad data)](#restate-fix-bad-data)
- [Writing a DataSource](#writing-a-datasource)
   * [Minimal example](#minimal-example)
   * [With chunking for large datasets](#with-chunking-for-large-datasets)
   * [Post-update bookkeeping](#post-update-bookkeeping)
   * [Built-in sources](#built-in-sources)
- [YAML Configuration](#yaml-configuration)
   * [Basic example](#basic-example)
   * [With data transformations](#with-data-transformations)
   * [Configuration reference](#configuration-reference)
- [Data Cleaning](#data-cleaning)
- [Instrument Hash Bucketing](#instrument-hash-bucketing)
   * [What is it and when to use it?](#what-is-it-and-when-to-use-it)
   * [Configuration](#configuration)
   * [Reading with an instrument filter](#reading-with-an-instrument-filter)
   * [Incremental updates and bucketed datasets](#incremental-updates-and-bucketed-datasets)
   * [DataSource.get_data() returning None](#datasourceget_data-returning-none)
- [Update Operations](#update-operations)
   * [Temp directory](#temp-directory)
   * [Update lock](#update-lock)
- [CLI Tools](#cli-tools)
   * [yaml-create-datasets](#yaml-create-datasets)
   * [update-cache](#update-cache)
   * [cleanup-cache](#cleanup-cache)
   * [sync-cache](#sync-cache)
- [Common Patterns](#common-patterns)
   * [Detecting new data](#detecting-new-data)
      + [Refreshing a single dataset](#refreshing-a-single-dataset)
      + [Refreshing all datasets](#refreshing-all-datasets)
      + [Polling for updates in a loop](#polling-for-updates-in-a-loop)
   * [Setting up a daily update job](#setting-up-a-daily-update-job)
   * [Reading across multiple caches](#reading-across-multiple-caches)
   * [Discovering available datasets](#discovering-available-datasets)

<!-- TOC end -->


A Python library for managing versioned Parquet datasets with automatic date partitioning, snapshot versioning, and multi-cache support.

## Installation

PyPI project:

```bash
pip install ionbus-parquet-cache
```

Conda package:

```bash
conda install -c ionbus ionbus-parquet-cache
```

Import package:

```python
import ionbus_parquet_cache
```

Maintainers: release instructions live in [RELEASING.md](RELEASING.md).

## Table of Contents

- [What is this library?](#what-is-this-library)
- [Quick Start](#quick-start)
- [Reading Data](#reading-data)
- [Dataset Types](#dataset-types)
- [Using Multiple Caches](#using-multiple-caches)
- [Updating Data](#updating-data)
- [Writing a DataSource](#writing-a-datasource)
- [YAML Configuration](#yaml-configuration)
- [Data Cleaning](#data-cleaning)
- [CLI Tools](#cli-tools)
- [Common Patterns](#common-patterns)

## What is this library?

This library helps you manage large Parquet datasets that:

- **Grow over time** (daily market data, log files, sensor readings)
- **Need versioning** (track what data looked like at any point in time)
- **Live in multiple locations** (local SSD, team share, firm-wide storage)
- **Require incremental updates** (add new data without rewriting everything)

Key concepts:

| Concept | What it means |
|---------|---------------|
| **Snapshot** | A versioned view of your dataset at a point in time |
| **Date partition** | Data is organized by year, quarter, month, or day |
| **DataSource** | A class you write to fetch data from your source (API, database, files) |
| **Cache** | A directory containing datasets and their metadata |

## Quick Start

### Reading data

```python
from ionbus_parquet_cache import CacheRegistry

# Point to your cache directory
registry = CacheRegistry.instance(my_cache="/path/to/cache")

# Read data into a pandas DataFrame
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    end_date="2024-03-31",
)
```

### Creating and updating datasets

```bash
# 1. Create YAML config (e.g., /path/to/cache/yaml/my_dataset.yaml)
# 2. Create DataSource in cache/code/my_source.py
# 3. Create the dataset:

python -m ionbus_parquet_cache.yaml_create_datasets /path/to/cache/yaml/my_dataset.yaml my_dataset

# 4. For routine updates (uses stored config, no YAML needed):

python -m ionbus_parquet_cache.update_datasets /path/to/cache my_dataset
```

See [YAML Configuration](#yaml-configuration) and [Writing a DataSource](#writing-a-datasource) for details.

## Reading Data

### Basic reading

```python
from ionbus_parquet_cache import CacheRegistry

registry = CacheRegistry.instance(cache="/path/to/cache")

# Read all data from a dataset
df = registry.read_data("md.futures_daily")

# Read with date range
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    end_date="2024-03-31",
)

# Read specific columns only
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    columns=["Date", "Symbol", "Close", "Volume"],
)
```

### Two workflows with CacheRegistry

**Workflow 1: Read via registry (convenience)**

Use `registry.read_data()` for straightforward single reads:

```python
from ionbus_parquet_cache import CacheRegistry

registry = CacheRegistry.instance(cache="/path/to/cache")

# Read data, one call at a time
df1 = registry.read_data("md.futures_daily", start_date="2024-01-01")
df2 = registry.read_data("md.futures_daily", start_date="2024-02-01")
df3 = registry.read_data("different_dataset")

# Refresh all when needed
if registry.refresh_all():
    print("New data available")
```

**Workflow 2: Grab a dataset instance for repeated use**

Get a dataset instance from the registry when you plan to use it repeatedly
or compare different snapshots:

```python
from ionbus_parquet_cache import CacheRegistry

registry = CacheRegistry.instance(cache="/path/to/cache")

# Grab a dataset instance
dpd = registry.get_dataset("md.futures_daily")

# Use it repeatedly
df_jan = dpd.read_data(start_date="2024-01-01", end_date="2024-01-31")
df_feb = dpd.read_data(start_date="2024-02-01", end_date="2024-02-28")

# Control refresh behavior for this instance
if dpd.is_update_available():
    dpd.refresh()
```

**Comparing different snapshots or caches**

Register multiple caches and read from each:

```python
from ionbus_parquet_cache import CacheRegistry

# Register both caches in the singleton registry
registry = CacheRegistry.instance(
    prod="/prod/cache",
    archive="/archive/cache"
)

# Get current version from prod cache
current_dpd = registry.get_dataset("md.futures_daily", cache_name="prod")
df_current = current_dpd.read_data(start_date="2024-01-01")

# Get old version from archive cache (can specify snapshot)
archive_dpd = registry.get_dataset("md.futures_daily", cache_name="archive")
df_old = archive_dpd.read_data(
    snapshot="1H4DW00",  # specific old snapshot
    start_date="2024-01-01"
)

# Compare the two
print(f"Current rows: {len(df_current)}")
print(f"Old rows: {len(df_old)}")
diff = df_current.compare(df_old)
```

### Filtering data

Use the `filters` parameter to filter rows. Filters use tuple syntax: `(column, operator, value)`.

```python
# Filter on any column
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    filters=[("FutureRoot", "in", ["ES", "NQ"])],
)

# Multiple filters (ANDed together)
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    filters=[
        ("FutureRoot", "=", "ES"),
        ("Volume", ">", 1000000),
    ],
)
```

Supported operators: `=`, `==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not in`

For complex filtering with AND/OR combinations, see [filtering.md](filtering.md).

### Using with other tools

Filter at the PyArrow level before passing to DuckDB or Polars. This ensures
partition pruning works correctly (DuckDB may not push predicates down to
PyArrow datasets in all versions).

```python
dpd = registry.get_dataset("md.futures_daily")

# Filter when creating the dataset - this enables partition pruning
dataset = dpd.pyarrow_dataset(
    start_date="2024-01-01",
    end_date="2024-03-31",
    filters=[("FutureRoot", "in", ["ES", "NQ"])],
)

# Use with DuckDB
import duckdb
result = duckdb.query("""
    SELECT Date, AVG(Close) as avg_close
    FROM dataset
    GROUP BY Date
""").df()

# Use with Polars
import polars as pl
lf = pl.scan_pyarrow_dataset(dataset)
df = lf.collect()
```

### Checking for updates

```python
# Get a dataset reference
dpd = registry.get_dataset("md.futures_daily")

# Check for and load new data in one call
if dpd.refresh():
    print("New data loaded!")

# Or refresh all datasets at once (useful in long-running notebooks)
if registry.refresh_all():
    print("Some datasets were updated!")

# Get dataset info
summary = dpd.summary()
print(f"Date range: {summary['cache_start_date']} to {summary['cache_end_date']}")
print(f"Files: {summary['file_count']}")
```

### Cache invalidation

When `refresh()` detects new data, it automatically clears the internal read cache
to ensure subsequent reads use fresh data. You can also manually invalidate the cache:

```python
dpd = registry.get_dataset("my_dataset")

# Manual cache invalidation (next read will reload from disk)
dpd.invalidate_read_cache()

# View dataset summary with fresh snapshots discovered
# WARNING: This mutates cached DPD instances!
df = registry.data_summary(refresh_and_possibly_change_loaded_caches=True)
```

**Important:** The `refresh_and_possibly_change_loaded_caches` parameter in
`data_summary()` discovers new snapshots and reloads metadata, which changes
the behavior of cached DPD instances. Only use this if you have no active
references to DPD instances and understand that existing reads will now return
different data.

### Reading from historical snapshots

Every update creates a new snapshot with a unique suffix (7-character base-36
encoding of the Unix timestamp, e.g. `1H4DW00`). Suffixes sort lexicographically
in chronological order and are valid through April 5, 4453 (when 36^7 seconds
elapses since the Unix epoch). You can read from any snapshot.

**Registry methods supporting `snapshot`:**
- `read_data(name, ..., snapshot=...)` - returns pandas DataFrame
- `read_data_pl(name, ..., snapshot=...)` - returns Polars DataFrame
- `pyarrow_dataset(name, ..., snapshot=...)` - returns PyArrow Dataset

Use `get_latest_snapshot()` to inspect or record the exact snapshot being read
before passing it explicitly — useful when you need a reproducible read or want
to log provenance.

```python
# Read from current snapshot (default)
df = registry.read_data("md.futures_daily", start_date="2024-01-01")

# Inspect the latest available snapshot suffix
suffix = registry.get_latest_snapshot("md.futures_daily")
print(suffix)  # e.g. "1H4DW01"

# Pin to a specific snapshot for a reproducible read
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    snapshot=suffix,
)

# Read from a specific historical snapshot by known suffix
df = registry.read_data(
    "md.futures_daily",
    start_date="2024-01-01",
    snapshot="1H4DW00",
)

# Also works with read_data_pl and pyarrow_dataset
df_pl = registry.read_data_pl("md.futures_daily", snapshot="1H4DW00")
dataset = registry.pyarrow_dataset("md.futures_daily", snapshot="1H4DW00")
```

### Reading non-dated (reference) data

```python
# Get reference data (no date filtering)
npd = registry.get_dataset("ref.instrument_master")
df = npd.read_data()

# Or directly through registry
df = registry.read_data("ref.instrument_master")

# Read from a specific historical snapshot
df = registry.read_data("ref.instrument_master", snapshot="1H4DW00")
```

## Dataset Types

### DatedParquetDataset (DPD)

For time-series data that grows over time. Data is partitioned by date (year, quarter, month, or day) and optionally by additional columns.

Examples: daily prices, minute bars, trade logs, sensor readings.

```python
# Reading a DPD
dpd = registry.get_dataset("md.futures_daily")
df = dpd.read_data(start_date="2024-01-01", end_date="2024-03-31")
```

### NonDatedParquetDataset (NPD)

For reference/static data that is replaced entirely on each update. No date partitioning.

Examples: instrument master, exchange calendars, lookup tables.

```python
# Reading an NPD
npd = registry.get_dataset("ref.instrument_master")
df = npd.read_data()
```

### Search order and naming conventions

When you call `get_dataset("name")`, the registry searches:
1. Cache 1: DPD named "name", then NPD named "name"
2. Cache 2: DPD named "name", then NPD named "name"
3. ... and so on through all registered caches

This means if a DPD and NPD have the same name, the DPD will always be found first, which can be confusing. To avoid this, use namespace prefixes:

| Prefix | Description | Example |
|--------|-------------|---------|
| `md.` | Market data | `md.futures_daily`, `md.equity_prices` |
| `ref.` | Reference/static data | `ref.instrument_master`, `ref.exchange_calendar` |
| `der.` | Derived/computed data | `der.volatility_surface`, `der.risk_factors` |
| `alt.` | Alternative data | `alt.sentiment_scores`, `alt.weather` |

These are just conventions - use whatever prefixes make sense for your organization. The key is to avoid giving a DPD and NPD the same name.

### Specifying dataset type

Use `DatasetType` to explicitly specify which type of dataset to search for. This is useful when a DPD and NPD might have similar names.

**Registry methods supporting `dataset_type`:**
- `get_dataset(name, dataset_type=...)` - returns the dataset object

Other registry methods (`read_data`, `read_data_pl`, `pyarrow_dataset`) use the default search order (DPD first, then NPD). To control the type for these, first get the dataset explicitly:

```python
from ionbus_parquet_cache import CacheRegistry, DatasetType

registry = CacheRegistry.instance(cache="/path/to/cache")

# Only search for dated datasets
dpd = registry.get_dataset("md.prices", dataset_type=DatasetType.DATED)

# Only search for non-dated datasets
npd = registry.get_dataset("ref.instruments", dataset_type=DatasetType.NON_DATED)

# Then read from the specific dataset
df = dpd.read_data(start_date="2024-01-01")
```

## Using Multiple Caches

The `CacheRegistry` lets you search across multiple cache locations in priority order:

```python
from ionbus_parquet_cache import CacheRegistry

# Register caches in priority order (local paths or GCS URLs)
registry = CacheRegistry.instance(
    local="c:/Users/me/cache",          # Checked first (fast SSD)
    team="n:/team/cache",               # Checked second
    firm="gs://my-bucket/parquet-cache" # GCS — checked last (authoritative)
)

# Reads from the first cache containing the dataset
df = registry.read_data("md.futures_daily", start_date="2024-01-01")

# See what's available across all caches
print(registry.data_summary())

# Force reading from a specific cache
df = registry.read_data("md.futures_daily", cache_name="firm")
```

### Auto-loading caches from an environment variable

Set `IBU_PARQUET_CACHE` to pre-register caches without any code change. The
registry reads this variable on first instantiation.

Format: `name|location,name|location`

```bash
# Local paths
export IBU_PARQUET_CACHE="local|/data/cache,team|/mnt/team/cache"

# Mix of local and GCS
export IBU_PARQUET_CACHE="local|/data/cache,prod|gs://my-bucket/parquet-cache"
```

```python
# No arguments needed — caches loaded from IBU_PARQUET_CACHE automatically
registry = CacheRegistry.instance()
df = registry.read_data("md.futures_daily")
```

### GCS caches

GCS paths (`gs://bucket/prefix`) are supported for **reading** and **syncing**.
Direct GCS-backed writes and dataset updates are not yet supported — the intended
workflow is to update locally and sync to GCS with `sync-cache push`. Full
GCS write support is planned for a future release.

Install the GCS dependency (only required when a `gs://` path is actually used):

```bash
pip install gcsfs
```

Authentication uses [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials) — run `gcloud auth application-default login` or set `GOOGLE_APPLICATION_CREDENTIALS` to a service-account key file.

## Updating Data

Use the CLI tools to update datasets. This keeps updates separate from your reading code.

There are two CLI tools for updating:

| Tool | Purpose |
|------|---------|
| `yaml-create-datasets` | Create new datasets or update from YAML config |
| `update-cache` | Routine updates using stored metadata (no YAML needed) |

### Creating a new dataset (from YAML)

```bash
# Create a specific dataset defined in YAML
python -m ionbus_parquet_cache.yaml_create_datasets /path/to/config.yaml md.futures_daily

# Create all datasets defined in the YAML file
python -m ionbus_parquet_cache.yaml_create_datasets /path/to/config.yaml
```

### Routine updates (from metadata)

Once a dataset exists, update it without needing YAML files:

```bash
# Update all existing datasets
python -m ionbus_parquet_cache.update_datasets /path/to/cache

# Update specific dataset(s)
python -m ionbus_parquet_cache.update_datasets /path/to/cache md.futures_daily

# Preview without making changes
python -m ionbus_parquet_cache.update_datasets /path/to/cache --dry-run --verbose
```

### Backfill (add historical data)

```bash
# Fetch data back to 2020 (preserves existing data)
python -m ionbus_parquet_cache.update_datasets /path/to/cache md.futures_daily \
    --backfill --start-date 2020-01-01
```

### Restate (fix bad data)

```bash
# Replace data for a date range
python -m ionbus_parquet_cache.update_datasets /path/to/cache md.futures_daily \
    --restate --start-date 2024-01-15 --end-date 2024-01-20
```

## Writing a DataSource

A `DataSource` tells the library how to fetch data from your source (API, database, files). Save it in `cache/code/`.

### Minimal example

```python
# code/my_source.py
from ionbus_parquet_cache import DataSource, PartitionSpec
import datetime as dt
import pandas as pd

class MySource(DataSource):
    def available_dates(self) -> tuple[dt.date, dt.date]:
        """What date range can this source provide?"""
        return (dt.date(2020, 1, 1), dt.date.today() - dt.timedelta(days=1))

    # prepare() is optional - base class sets start_date, end_date, instruments

    def get_data(self, partition_spec: PartitionSpec) -> pd.DataFrame:
        """Fetch data for one partition."""
        # partition_spec contains:
        #   - start_date, end_date: the date range to fetch
        #   - partition_values: e.g., {"FutureRoot": "ES", "month": "M2024-01"}

        return fetch_from_my_api(
            start=partition_spec.start_date,
            end=partition_spec.end_date,
        )
```

### With chunking for large datasets

```python
class MySource(DataSource):
    # Fetch 30 business days (~6 weeks) at a time to limit memory usage
    chunk_days: int = 30

    def __init__(self, dataset, **kwargs):
        # Known values for partition columns (set in __init__, not class level)
        self.partition_values = {"Exchange": ["NYSE", "CME", "ICE"]}
        super().__init__(dataset, **kwargs)

    def available_dates(self):
        return (dt.date(2020, 1, 1), dt.date.today() - dt.timedelta(days=1))

    # prepare() is optional - base class sets start_date, end_date, instruments

    def get_data(self, partition_spec):
        # Called once per chunk (30 days max)
        return fetch_data(
            partition_spec.start_date,
            partition_spec.end_date,
            partition_spec.partition_values.get("Exchange"),
        )
```

### Post-update bookkeeping

Override `on_update_complete(suffix)` to run any bookkeeping after all partitions
have been written and the snapshot is published. `self.start_date`,
`self.end_date`, and `self.instruments` are still set from `prepare()` at this
point, so you have full context about what was just run.

```python
class MySource(DataSource):
    def available_dates(self):
        return (dt.date(2020, 1, 1), dt.date.today() - dt.timedelta(days=1))

    def get_data(self, partition_spec):
        return fetch_from_my_api(
            start=partition_spec.start_date,
            end=partition_spec.end_date,
        )

    def on_update_complete(self, suffix: str, previous_suffix: str | None) -> None:
        # previous_suffix is None on the first update of a cache
        write_audit_record(
            snapshot=suffix,
            previous_snapshot=previous_suffix,
            start=self.start_date,
            end=self.end_date,
            source="my_api",
        )
```

Common uses: writing audit trails, recording API call counts or checksums,
updating a separate provenance table, sending a completion notification.

### Built-in sources

For common cases, you don't need to write a DataSource:

**HiveParquetSource** - Read from existing Hive-partitioned Parquet files:

```yaml
# In your YAML config
source_class_name: HiveParquetSource
source_init_args:
  path: "/data/existing_parquet_files"
  glob_pattern: "**/*.parquet"  # Optional, default
```

Features:
- Auto-discovers partition values from directory structure (`col=value/`)
- Handles string dates, timestamps, and native date types
- Normalizes partition column types automatically

**DPDSource** - Read from another DatedParquetDataset:

```yaml
source_class_name: DPDSource
source_init_args:
  dpd_name: "md.raw_futures"
  dpd_cache_path: "/path/to/source/cache"  # Optional, defaults to target cache
```

Parameters:
- `dpd_name`: Name of the source dataset (required)
- `dpd_cache_path`: Path to the source cache directory. Defaults to the target dataset's cache directory. Only required when the source DPD is in a different cache.
- `dpd_cache_name`: Cache name as used in `CacheRegistry.instance(local=..., team=..., firm=...)`. Only works in programmatic use where the cache was already registered. Not available in CLI tools.

## YAML Configuration

Datasets are defined in YAML files under `cache/yaml/`. This separates configuration from code.

### Basic example

```yaml
# yaml/futures.yaml

cache_dir: ".."  # Parent of yaml directory (default)

datasets:
  md.futures_daily:
    description: Daily futures prices
    date_col: Date
    date_partition: month
    partition_columns:
      - FutureRoot
      - month
    sort_columns:
      - Date
      - Symbol

    # Where to get data
    source_location: code/futures_source.py
    source_class_name: FuturesAPISource
    source_init_args:
      api_url: "https://api.example.com/futures"
```

### With data transformations

```yaml
datasets:
  md.equity_daily:
    description: Daily equity prices (cleaned)
    date_col: PricingDate
    date_partition: month

    source_location: code/equity_source.py
    source_class_name: EquitySource

    # Rename columns from source
    columns_to_rename:
      price_open: Open
      price_close: Close

    # Remove unwanted columns
    columns_to_drop:
      - internal_id
      - load_timestamp

    # Drop rows with null values in these columns
    dropna_columns:
      - Close
      - Volume

    # Remove duplicates
    dedup_columns:
      - PricingDate
      - Symbol
    dedup_keep: last  # keep last occurrence
```

### Configuration reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | (from YAML key) | Dataset name (used as the YAML key under `datasets:`) |
| `description` | `str` | `""` | Human-readable description of the dataset |
| `date_col` | `str` | `"Date"` | Name of the date column in the data |
| `date_partition` | `str` | `"day"` | Partition granularity: `day`, `week`, `month`, `quarter`, `year` |
| `partition_columns` | `list[str]` | `[]` | Columns to partition by (in directory order) |
| `sort_columns` | `list[str]` | `[date_col]` | Sort order within partition files; defaults to `[date_col]` if not provided |
| `repull_n_days` | `int` | `0` | Re-fetch this many recent business days on each update |
| `row_group_size` | `int` | `None` (PyArrow default: 1,048,576 rows) | Maximum rows per Parquet row group. Smaller values enable row-group-level predicate pushdown at the cost of more file metadata. |
| `instrument_column` | `str` | `None` | Column name holding instrument identifiers (e.g., `"ticker"`). Required when `num_instrument_buckets` is set. |
| `num_instrument_buckets` | `int` | `None` | Enable hash bucketing: group tickers into this many bucket directories instead of one directory per ticker. Must be set together with `instrument_column`. |
| `instruments` | `list[str]` | `None` | List of instruments to filter on (uses `instrument_column`) |
| `start_date_str` | `str` | `None` | Override start date (debugging only, format: `"YYYY-MM-DD"`) |
| `end_date_str` | `str` | `None` | Override end date (debugging only, format: `"YYYY-MM-DD"`) |
| `source_location` | `str` | `""` | Path to Python file with DataSource class (empty for built-ins) |
| `source_class_name` | `str` | required | Name of the DataSource class |
| `source_init_args` | `dict` | `{}` | Arguments passed to DataSource constructor |
| `columns_to_drop` | `list[str]` | `[]` | Columns to remove from the data |
| `columns_to_rename` | `dict[str, str]` | `{}` | Mapping of old column names to new names |
| `dropna_columns` | `list[str]` | `[]` | Drop rows where any of these columns are null |
| `dedup_columns` | `list[str]` | `[]` | Columns to deduplicate on |
| `dedup_keep` | `str` | `"last"` | Which duplicate to keep: `"first"` or `"last"` |
| `cleaning_class_location` | `str` | `None` | Path to Python file with DataCleaner class |
| `cleaning_class_name` | `str` | `None` | Name of the DataCleaner class |
| `cleaning_init_args` | `dict` | `{}` | Arguments passed to DataCleaner constructor |

## Data Cleaning

For complex transformations beyond what YAML provides, create a `DataCleaner` class:

```python
# code/my_cleaner.py
from ionbus_parquet_cache import DataCleaner
import duckdb

class PriceDataCleaner(DataCleaner):
    def __init__(self, dataset, min_price: float = 1.0):
        super().__init__(dataset)
        self.min_price = min_price

    def __call__(self, rel: duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
        # Filter out penny stocks
        return rel.filter(f"Close >= {self.min_price}")
```

Reference it in your YAML:

```yaml
datasets:
  md.equity_daily:
    # ... other config ...
    cleaning_class_location: code/my_cleaner.py
    cleaning_class_name: PriceDataCleaner
    cleaning_init_args:
      min_price: 1.0
```

## Instrument Hash Bucketing

### What is it and when to use it?

By default, when a DataSource returns one PartitionSpec per ticker per year, the
cache creates one directory per ticker:

```
eod_prices/
    ticker=AAPL/year=Y2024/...
    ticker=MSFT/year=Y2024/...
    ...  (32,000 directories)
```

With 32,000 tickers this creates a filesystem metadata problem — listing or
opening the dataset directory is slow and inode-hungry.

**Instrument hash bucketing** groups tickers into a fixed number of bucket
directories.  Each ticker is assigned to a bucket by hashing:

```python
bucket = zlib.crc32(ticker.encode()) % num_instrument_buckets
```

The resulting layout is:

```
eod_prices_bucketed/
    __instrument_bucket__=02K/year=Y2024/...  # ~125 tickers per bucket
    __instrument_bucket__=073/year=Y2024/...
    ...  (256 directories, not 32,000)
```

Use bucketing when:
- You have hundreds or thousands of instruments
- Per-ticker directories are causing filesystem performance problems

### Configuration

Set both fields together in YAML:

```yaml
datasets:
  eod_prices_bucketed:
    description: "EOD historical prices, hash-bucketed"
    date_col: date
    date_partition: year
    partition_columns: []      # Do NOT include ticker here
    sort_columns:
      - ticker
      - date
    instrument_column: ticker  # Column holding the ticker identifier
    num_instrument_buckets: 256

    source_location: "code/my_source.py"
    source_class_name: MySource
```

Or in Python:

```python
from ionbus_parquet_cache.dated_dataset import DatedParquetDataset

dpd = DatedParquetDataset(
    cache_dir=Path("/path/to/cache"),
    name="eod_prices_bucketed",
    date_col="date",
    date_partition="year",
    partition_columns=[],      # Do NOT include ticker here
    instrument_column="ticker",
    num_instrument_buckets=256,
)
```

**Constraints:**
- `instrument_column` must NOT appear in `partition_columns` — the library
  manages it internally via `__instrument_bucket__`
- `__instrument_bucket__` is a reserved column name; you cannot use it in
  `partition_columns` or `sort_columns`
- `num_instrument_buckets` is a breaking change: once a dataset is built with
  N buckets, re-opening it with a different count raises `ValidationError`

**Your DataSource subclasses `BucketedDataSource`.** You implement two methods
and the base class handles all bucketing logic transparently:

```python
from ionbus_parquet_cache.data_source import BucketedDataSource

class MySource(BucketedDataSource):
    def get_instruments_for_time_period(
        self, start_date: dt.date, end_date: dt.date
    ) -> list:
        # Return all instruments available for this period.
        # Called once per date partition (e.g. once per year) and cached.
        return self._all_instruments

    def get_data_for_bucket(
        self,
        instruments: list,
        start_date: dt.date,
        end_date: dt.date,
    ) -> pa.Table | None:
        # Fetch and return data for this subset of instruments.
        # Return None if there is no data for this bucket/period.
        ...
```

The base class:
1. Generates `num_buckets × num_date_partitions` specs with zero I/O
2. On first `get_data()` call for a period, calls `get_instruments_for_time_period()`,
   hashes the results into buckets, and caches the mapping for that period
3. Short-circuits empty buckets (returns `None` immediately without calling
   `get_data_for_bucket()`)

### Reading with an instrument filter

When bucketing is active, use the `instruments` parameter on `read_data()` or
`read_data_pl()` to push down a filter to the correct bucket partitions:

```python
# Single ticker
df = dpd.read_data(instruments="AAPL")

# Multiple tickers (list or set)
df = dpd.read_data(instruments=["AAPL", "MSFT", "GOOGL"])

# Combined with date range
df = dpd.read_data(
    start_date="2023-01-01",
    end_date="2023-12-31",
    instruments=["AAPL", "MSFT"],
)

# Also works with read_data_pl
df_pl = dpd.read_data_pl(instruments="TSLA")
```

The `instruments` filter:
1. Computes which bucket(s) contain the requested tickers (partition pruning)
2. Applies an exact row-level filter on `instrument_column` within those buckets

Without the `instruments` filter, `read_data()` returns all tickers as normal.

### Incremental updates and bucketed datasets

> **Current limitation:** `update(..., instruments=[...])` raises `ValidationError`
> on bucketed datasets. Writing only a subset of instruments to a bucket file
> would silently overwrite the other instruments already stored in that file.
> Full-universe updates (no `instruments` argument) work correctly. Support for
> partial-instrument updates requires a read-merge path that is not yet
> implemented — see PLAN.md for the design.

### DataSource.get_data() returning None

A DataSource's `get_data()` may return `None` to signal that a partition has no
data.  The pipeline skips that partition — no file is written and no error is
raised.  This is useful when a DataSource does not have data for every
combination of (ticker, year) and wants to avoid writing empty files.

This behavior applies both in non-bucketed and bucketed datasets. In bucketed
mode, the base class short-circuits immediately when a bucket has no instruments
for a period — `get_data_for_bucket()` is never called.

## Update Operations

### Temp directory

During an update, partition files are written to a temporary directory inside
the dataset directory (`<dataset>/_tmp_<suffix>/`) and moved into place at
publish time. Keeping the temp directory on the same filesystem as the
destination means the final "move" is a rename — a directory-entry update with
no data copied. This makes publish fast regardless of dataset size.

The `_tmp_<suffix>/` directory is visible while the update runs, which lets you
monitor write progress. It is removed automatically when the update completes
(success or failure). If you see a stale `_tmp_*/` directory it means a
previous update was killed before it could clean up — safe to delete manually.

### Update lock

`update()` writes a `<name>_update.lock` file at the start of every run and
removes it on completion. If a second process tries to update the same dataset
while a lock exists, it raises `UpdateLockedError` immediately with the locking
host, PID, age, and instructions for clearing it:

```
UpdateLockedError: Dataset 'eod_prices_bucketed' is locked for update.
  Lock file : /path/to/cache/eod_prices_bucketed/eod_prices_bucketed_update.lock
  Locked by : my-host.local (PID 12345)
  Started   : 2026-04-19T10:30:00 (2.3 minutes ago)

  To remove the lock and allow new updates:
    Shell : rm /path/to/.../eod_prices_bucketed_update.lock
    Python: dpd.clear_update_lock(force=True)
```

To clear a stale lock (e.g. after a process was killed):

```python
dpd.clear_update_lock()          # checks PID is dead first (same host only)
dpd.clear_update_lock(force=True)  # unconditional
```

**Configuration options:**

| Parameter | Default | Description |
|-----------|---------|-------------|
| `lock_dir` | `None` (dataset dir) | Override lock file location. Useful when the dataset directory is read-only or GCS-backed — point to a local or writable path instead. Lock filename is `<name>_update.lock` within this directory. |
| `use_update_lock` | `True` | Set to `False` to disable locking entirely. Appropriate when single-writer is guaranteed by convention (e.g. a scheduled Cloud job with no concurrency). |

## CLI Tools

### yaml-create-datasets

Create new datasets or update existing ones using YAML configuration.

The first argument is the path to the YAML file. The second optional argument
is the dataset name. The cache directory is determined from the YAML file
(via `cache_dir` key) or defaults to one directory up from the YAML file.

```bash
# Create/update all datasets defined in YAML
python -m ionbus_parquet_cache.yaml_create_datasets /path/to/config.yaml

# Create/update a specific dataset
python -m ionbus_parquet_cache.yaml_create_datasets /path/to/config.yaml md.futures_daily

# Specify date range for initial load
python -m ionbus_parquet_cache.yaml_create_datasets /path/to/config.yaml \
    --start-date 2024-01-01 --end-date 2024-03-31

# Preview without making changes
python -m ionbus_parquet_cache.yaml_create_datasets /path/to/config.yaml --dry-run --verbose

# Update using a different DataSource but keep existing cache configuration
python -m ionbus_parquet_cache.yaml_create_datasets /path/to/alt_source.yaml md.futures_daily \
    --preserve-config
```

The `--preserve-config` option keeps the existing cache structure (date_partition, partition_columns, etc.) and only uses the YAML file for the DataSource definition. This is useful when you want to update a cache using a different data provider without changing how the data is partitioned or organized.

### update-cache

Update existing datasets using stored metadata (no YAML files needed).

The first argument is the cache directory. The second optional argument
is the dataset name.

```bash
# Update all datasets with new data
python -m ionbus_parquet_cache.update_datasets /path/to/cache

# Update a specific dataset
python -m ionbus_parquet_cache.update_datasets /path/to/cache md.futures_daily

# Backfill historical data
python -m ionbus_parquet_cache.update_datasets /path/to/cache \
    --backfill --start-date 2020-01-01

# Restate (fix bad data)
python -m ionbus_parquet_cache.update_datasets /path/to/cache \
    --restate --start-date 2024-01-15 --end-date 2024-01-20

# Update specific instruments only
python -m ionbus_parquet_cache.update_datasets /path/to/cache \
    --instruments ES,NQ

# Preview without making changes
python -m ionbus_parquet_cache.update_datasets /path/to/cache --dry-run --verbose
```

### cleanup-cache

Analyze disk usage and generate cleanup scripts. Snapshot cleanup generates scripts
without modifying the cache. Trim mode (`--keep-days`, `--before-date`) renames files
immediately (marking them for deletion) but requires running the generated script to
actually delete them.

Generated scripts use the naming pattern `_cleanup_{suffix}.bat/.sh` where
`{suffix}` is a 7-character base-36 timestamp (e.g., `_cleanup_1H4DW00.bat`).

```bash
# List all snapshots and reclaimable space (no action taken)
python -m ionbus_parquet_cache.cleanup_cache /path/to/cache

# Keep only the 3 most recent snapshots, generate cleanup script
python -m ionbus_parquet_cache.cleanup_cache /path/to/cache --keep-last 3

# Target snapshots older than 30 days
python -m ionbus_parquet_cache.cleanup_cache /path/to/cache --older-than 30

# Target a specific snapshot suffix
python -m ionbus_parquet_cache.cleanup_cache /path/to/cache --snapshot 1H4DW00

# Find orphaned files (not in any snapshot) - generates cleanup script
python -m ionbus_parquet_cache.cleanup_cache /path/to/cache --find-orphans

# Analyze only DPDs or only NPDs
python -m ionbus_parquet_cache.cleanup_cache /path/to/cache --dpd-only
python -m ionbus_parquet_cache.cleanup_cache /path/to/cache --npd-only

# Analyze specific datasets
python -m ionbus_parquet_cache.cleanup_cache /path/to/cache --dataset md.futures_daily

# TRIM: Remove old data (dangerous - modifies files immediately)
# Generates _cleanup_{suffix}_delete.bat/.sh and _cleanup_{suffix}_undo.bat/.sh
python -m ionbus_parquet_cache.cleanup_cache /path/to/cache --keep-days 252
python -m ionbus_parquet_cache.cleanup_cache /path/to/cache --before-date 2023-01-01
```

Output includes a generated script path. Review the script, then run it to delete files.

### sync-cache

Copy data between cache locations. Supports local paths and GCS (`gs://`).
S3 support will be implemented in a future release.

```bash
# Sync all datasets
python -m ionbus_parquet_cache.sync_cache push /local/cache /remote/cache
python -m ionbus_parquet_cache.sync_cache pull gs://my-bucket/cache /local/cache

# Sync specific datasets (whitelist)
python -m ionbus_parquet_cache.sync_cache push /local/cache gs://my-bucket/cache \
    --datasets md.futures_daily
python -m ionbus_parquet_cache.sync_cache push /local/cache gs://my-bucket/cache \
    --datasets md.futures_daily md.equity_daily

# Exclude specific datasets (blacklist)
python -m ionbus_parquet_cache.sync_cache pull gs://my-bucket/cache /local/cache \
    --ignore-datasets eod_prices_bucketed

# Sync specific snapshots
python -m ionbus_parquet_cache.sync_cache pull gs://my-bucket/cache /local/cache \
    --datasets md.futures_daily --snapshot 1H4DW01 1H4DW02

# Include all historical snapshots (not just current)
python -m ionbus_parquet_cache.sync_cache push /local/cache gs://my-bucket/cache \
    --datasets md.futures_daily --all-snapshots

# Copy dataset with a new name (single dataset only)
python -m ionbus_parquet_cache.sync_cache push /local /remote \
    --rename "md.futures:md.futures_backup"

# Delete files at destination not in source (local only)
python -m ionbus_parquet_cache.sync_cache push /local /remote --delete

# Continuous sync from GCS (daemon mode)
python -m ionbus_parquet_cache.sync_cache pull gs://my-bucket/cache /local \
    --daemon --update-interval 60

# Parallel upload/download (8 workers by default)
python -m ionbus_parquet_cache.sync_cache push /local/cache gs://my-bucket/cache \
    --workers 16
```

GCS sync uses size-based change detection. Requires `pip install gcsfs`.

### rename-cache

Rename a DatedParquetDataset directory in place. This updates the metadata
files (which store the dataset name internally) and renames the directory — the
parquet data files themselves are untouched.

```bash
# Preview what would happen (no changes made)
python -m ionbus_parquet_cache.rename_cache /path/to/cache old_name new_name --dry-run

# Rename for real
python -m ionbus_parquet_cache.rename_cache /path/to/cache old_name new_name
```

**Recovery ordering** (safe at any interruption point):
1. Write new `<new_name>_<suffix>.pkl.gz` metadata files alongside the old ones
2. Rename directory `old_name/` → `new_name/`
3. Delete old `<old_name>_*.pkl.gz` from `new_name/_meta_data/` (cleanup only)

If interrupted at step 1, delete the new metadata files — the old cache is
untouched. If interrupted at step 2, rename the directory back.

## Common Patterns

### Detecting new data

For long-running processes or notebooks, you may want to check if new snapshots are available and reload the data.

#### Refreshing a single dataset

Use `refresh()` to check for and load a newer snapshot. It returns `True` if a newer snapshot was found and loaded, `False` otherwise.

```python
from ionbus_parquet_cache import CacheRegistry

registry = CacheRegistry.instance(cache="/path/to/cache")
dpd = registry.get_dataset("md.futures_daily")

# Check if new data is available and refresh if so
if dpd.refresh():
    print("Dataset was refreshed with new data!")
else:
    print("Already on latest snapshot")
```

This works the same for non-dated datasets (NPDs):

```python
npd = registry.get_dataset("ref.instrument_master")

if npd.refresh():
    print("Reference data updated!")
```

#### Refreshing all datasets

For long-running notebooks where you've accessed multiple datasets, use `refresh_all()` to refresh everything at once. It refreshes all DPDs and NPDs that have been accessed via the registry.

```python
# Refresh all cached datasets (useful in long-running notebooks)
if registry.refresh_all():
    print("Some datasets were refreshed!")
else:
    print("All datasets already current")
```

#### Polling for updates in a loop

For continuous processes, combine `refresh()` with a polling loop:

```python
from ionbus_parquet_cache import CacheRegistry
import time

registry = CacheRegistry.instance(cache="/path/to/cache")
dpd = registry.get_dataset("md.futures_daily")

while True:
    # Check for new snapshots periodically
    if dpd.refresh():
        print("New data available!")

    # Use the data
    df = dpd.read_data(start_date="2024-01-01")
    process(df)

    time.sleep(60)
```

### Setting up a daily update job

Create a shell script or cron job:

```bash
#!/bin/bash
# daily_update.sh

CACHE_DIR="/path/to/cache"

# Update all existing datasets (uses stored metadata)
python -m ionbus_parquet_cache.update_datasets "$CACHE_DIR" --verbose

# Generate cleanup script (keep last 5 snapshots)
# Creates _cleanup_{suffix}.sh - review and run to delete old snapshots
python -m ionbus_parquet_cache.cleanup_cache "$CACHE_DIR" --keep-last 5

# Sync to team share
python -m ionbus_parquet_cache.sync_cache push "$CACHE_DIR" /team/cache
```

### Reading across multiple caches

```python
from ionbus_parquet_cache import CacheRegistry

# Register multiple caches in priority order
registry = CacheRegistry.instance(
    local="/fast/local/cache",
    firm="/slow/firm/cache",
)

# CacheRegistry picks the FIRST cache containing the dataset (in registration order)
# It does NOT merge data across caches by date coverage
df = registry.read_data("md.futures_daily", start_date="2020-01-01")

# If your local cache only has recent data and you need historical data from
# the firm cache, you must explicitly specify the cache:
df_historical = registry.read_data(
    "md.futures_daily",
    start_date="2020-01-01",
    end_date="2022-12-31",
    cache_name="firm",  # Explicitly read from firm cache
)
```

### Discovering available datasets

```python
from ionbus_parquet_cache import CacheRegistry

registry = CacheRegistry.instance(cache="/path/to/cache")

# List all DPDs across all registered caches
dpds = registry.discover_all_dpds()
for name, (cache_name, path) in dpds.items():
    print(f"DPD: {name} (in {cache_name})")

# List all NPDs across all registered caches
npds = registry.discover_all_npds()
for name, (cache_name, path) in npds.items():
    print(f"NPD: {name} (in {cache_name})")

# Get summary of all datasets
summary_df = registry.data_summary()
print(summary_df)
```
