# Integration Tests Plan

## Data Sources

### 1. Futures Daily Data (Real Data)
- **Location**: `C:\Users\cplag\OneDrive\Documents\Data\Futures_daily_data\parquet_files`
- **Source zip files**: `C:\Users\cplag\OneDrive\Documents\Data\Futures_daily_data\*.zip`
- **Conversion script**: `C:\Users\cplag\OneDrive\Documents\Python\ionbus\data\convert_futures_to_parquet.py`
- **Schema**: `date, open, high, low, close, volume, open_int`
- **Hive partitions**: `future_root/year/contract_name`
- **Date range**: `1959-07-01` to `2002-10-01` (719,052 rows)
- **DPD Configuration**:
  - `date_col`: `date`
  - `date_partition`: `year`
  - `partition_columns`: `[future_root]`
  - `sort_columns`: `[contract_name, date]`

#### How to determine date range
```python
import polars as pl
import pyarrow.dataset as pds

ds = pds.dataset('C:/Users/cplag/OneDrive/Documents/Data/Futures_daily_data/parquet_files/')
df = pl.from_arrow(ds.to_table())
print(f"Min: {df['date'].min()}, Max: {df['date'].max()}, Rows: {len(df):,}")
# Output: Min: 1959-07-01, Max: 2002-10-01, Rows: 719,052
```

### 2. Fake Equity Data (Generated)
- **Generator**: `create_bars.py` (in parquet_claude directory)
- **Output location**: `../parquet_test_data/fake_bars/`
- **Configuration**:
  - **Letters**: A, B, C, D, E (5 letters)
  - **Tickers per letter**: 1,000 (5,000 total tickers)
  - **Date range**: 2020-01-01 to 2025-12-31
  - **Seed**: 42 (for reproducibility)

#### Generate commands

Generate data one letter at a time for memory efficiency:

```bash
cd c:/Users/cplag/OneDrive/Documents/Python/ionbus/parquet_claude

# Letter A (creates tickers.parquet with all 5000 tickers)
~/bin/python_env_management/run_env.sh py312_pd22_x64 python create_bars.py \
    --start-date 2020-01-01 \
    --end-date 2025-12-31 \
    --letters A \
    --tickers-per-letter 1000 \
    --output-dir ../parquet_test_data/fake_bars \
    --seed 42

# Letters B-E (tickers already exist, no --tickers-per-letter needed)
for letter in B C D E; do
    ~/bin/python_env_management/run_env.sh py312_pd22_x64 python create_bars.py \
        --start-date 2020-01-01 \
        --end-date 2025-12-31 \
        --letters $letter \
        --output-dir ../parquet_test_data/fake_bars \
        --seed 42
done
```

Or run all letters at once (if you have enough memory):
```bash
~/bin/python_env_management/run_env.sh py312_pd22_x64 python create_bars.py \
    --start-date 2020-01-01 \
    --end-date 2025-12-31 \
    --letters A-E \
    --tickers-per-letter 1000 \
    --output-dir ../parquet_test_data/fake_bars \
    --seed 42
```

#### Minute Bars
- **Schema**: `timestamp, ticker, open, high, low, close, date`
- **Location**: `../parquet_test_data/fake_bars/minute/`
- **Source partitioning**: `first_letter={A}/week=W{YYYY}-{WW}/` (Hive, weekly)
- **Estimated size**: ~5,000 tickers × ~1,500 trading days × 390 bars = ~3 billion rows
- **DPD Configuration** (for cache):
  - `date_col`: `date` (ISO string like "2024-01-15")
  - `date_partition`: `month`
  - `partition_columns`: `[first_letter]`
  - `sort_columns`: `[ticker, date]`

#### Daily Bars
- **Schema**: `date, ticker, open, high, low, close`
- **Location**: `../parquet_test_data/fake_bars/daily/`
- **Source partitioning**: `first_letter={A}/week=W{YYYY}-{WW}/` (Hive, weekly)
- **Estimated size**: ~5,000 tickers × ~1,500 trading days = ~7.5 million rows
- **DPD Configuration** (for cache):
  - `date_col`: `date`
  - `date_partition`: `year`
  - `partition_columns`: `[first_letter]`
  - `sort_columns`: `[ticker, date]`

---

## New Functionality Required

### 1. Extend sync-cache to Support Rename
Extend `sync-cache` to allow copying a cache to a new location with a different dataset name.

**CLI syntax** (proposed):
```bash
# Copy with rename
sync-cache push source_cache target_cache --rename "old_name:new_name"

# Copy specific snapshot
sync-cache push source_cache target_cache --snapshot 1H4DW00 --rename "old_name:new_name"
```

### 2. Update Without Overwriting Config (`--preserve-config`)
Add a flag to `update-cache` that uses the YAML's DataSource definition but keeps the target cache's existing configuration (`date_partition`, `partition_columns`, etc.).

**Use case**: Update a cache using a different YAML file with a different DataSource, without changing the cache's structure.

**CLI syntax** (proposed):
```bash
update-cache --yaml config.yaml --dataset md.futures --preserve-config
```

---

## Test Scenarios

### Test 1: Initial Load (Partial Data)
Create caches with only a subset of data (up to a cutoff date, not the full date range).

| Dataset | Date Range (Initial) | Full Range | Cutoff Reason |
|---------|---------------------|------------|---------------|
| Futures Daily | 1959-07-01 to 1999-12-31 | 1959-07-01 to 2002-10-01 | Stop before 2000 |
| Equity Daily | 2020-01-01 to 2023-06-30 | 2020-01-01 to 2025-12-31 | Stop mid-2023 |
| Equity Minute | 2020-01-01 to 2023-06-30 | 2020-01-01 to 2025-12-31 | Stop mid-2023 |

### Test 2: Incremental Update
Update the caches to include the remaining data, verifying:
- New snapshots are created
- Data is correctly appended
- Old data is preserved

### Test 3: Copy with Rename
Copy each cache to a new location with a different name:
- Verify data integrity after copy
- Verify can read from copied cache
- Verify specific snapshot copy works

### Test 4: Restate Mode (Fix Corrupted Data)
1. Create a copy of the minute bar cache
2. Corrupt the copy: multiply OHLC prices by 2 for tickers starting with a specific letter (e.g., "A")
3. Run restate to fix the corrupted data
4. Verify prices are corrected

### Test 5: Preserve Config Update
1. Create a cache with specific partition settings
2. Create a different YAML with different settings but same DataSource
3. Run update with `--preserve-config`
4. Verify cache structure unchanged, data updated

---

## Test Data Location

Test data lives in a sibling directory to `parquet_claude`:
```
ionbus/
    parquet_claude/           # This repo
    parquet_test_data/        # Integration test data
        fake_bars/            # Generated equity data
            minute/
            daily/
            tickers.parquet
        test_caches/          # Test cache outputs
```

---

## Test Execution

Integration tests are **shell scripts** that run existing CLI tools and verify results - not pytest.

### Prerequisites
1. Generate fake equity data (see "Generate commands" section above, or run from parquet_claude directory):
   ```bash
   # Letter A first (creates ticker metadata)
   ~/bin/python_env_management/run_env.sh py312_pd22_x64 python create_bars.py \
       --start-date 2020-01-01 \
       --end-date 2025-12-31 \
       --letters A \
       --tickers-per-letter 1000 \
       --output-dir ../parquet_test_data/fake_bars \
       --seed 42

   # Then B-E (one at a time for memory efficiency)
   for letter in B C D E; do
       ~/bin/python_env_management/run_env.sh py312_pd22_x64 python create_bars.py \
           --start-date 2020-01-01 --end-date 2025-12-31 --letters $letter \
           --output-dir ../parquet_test_data/fake_bars --seed 42
   done
   ```
   **Note**: This generates ~3 billion minute bars and ~7.5 million daily bars. Process one letter at a time to avoid memory issues.

2. Futures data already exists at `C:\Users\cplag\OneDrive\Documents\Data\Futures_daily_data\parquet_files`

### Test Runner Structure
```
integration_tests/
    run_all.sh               # Master script to run all tests
    run_all.bat              # Windows version

    01_setup_data.sh         # Generate fake data if needed
    02_initial_load.sh       # Create caches with partial data
    03_incremental_update.sh # Update caches with remaining data
    04_copy_rename.sh        # Test copy with rename
    05_restate.sh            # Test restate mode (fix corrupted data)
    06_preserve_config.sh    # Test preserve-config flag

    verify.py                # Python script to verify test results
                             # Checks row counts, date ranges, schemas, etc.
```

### How Tests Work

1. **Run CLI commands**: Each test script runs `yaml-create-datasets`, `update-cache`, `sync-cache`, etc. with specific options
2. **Verify results**: `verify.py` reads the resulting caches and checks:
   - Row counts match expected
   - Date ranges are correct
   - Schemas are correct
   - Data values are reasonable (e.g., prices > 0)
   - Snapshots exist with expected suffixes

### Example Test Flow (02_initial_load.sh)
```bash
#!/bin/bash
set -e

# Create YAML configs for partial date range
# Run yaml-create-datasets to create initial caches
# Run verify.py to check results

python -m ionbus_parquet_cache.yaml_create_datasets \
    --yaml integration_tests/configs/futures_partial.yaml

python integration_tests/verify.py check-initial-load
```
