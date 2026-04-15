#!/usr/bin/env python
"""Verify integration test results.

Usage:
    python verify.py check-initial-load
    python verify.py check-incremental-update
    python verify.py corrupt-data <cache_path> <letter>
    python verify.py check-restate <corrupt_cache> <original_cache>
"""

import sys
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from ionbus_parquet_cache.cache_registry import CacheRegistry


# Expected values for validation
EXPECTED = {
    "futures_daily": {
        "partial_end": date(1999, 12, 31),
        "full_end": date(2002, 10, 1),
        "full_start": date(1959, 7, 1),
        "partition_col": "future_root",
    },
    "equity_daily": {
        "partial_end": date(2023, 6, 30),
        "full_end": date(2025, 12, 31),
        "full_start": date(2020, 1, 1),
        "partition_col": "first_letter",
    },
    "equity_minute": {
        "partial_end": date(2023, 6, 30),
        "full_end": date(2025, 12, 31),
        "full_start": date(2020, 1, 1),
        "partition_col": "first_letter",
    },
}


def get_cache_dir() -> Path:
    """Get the test cache directory."""
    return Path(__file__).parent.parent.parent.parent / "parquet_test_data" / "test_caches"


def check_dataset_exists(registry: CacheRegistry, name: str) -> bool:
    """Check if a dataset exists in the registry."""
    dataset = registry.get_dataset(name)
    return dataset is not None


def get_date_range(registry: CacheRegistry, name: str) -> tuple[date, date]:
    """Get the date range of a dataset."""
    dataset = registry.get_dataset(name)
    if dataset is None:
        raise ValueError(f"Dataset {name} not found")

    # Read all data and get date range
    df = dataset.read_data_pl()
    date_col = dataset.date_col

    # Handle string dates
    if df[date_col].dtype == pl.Utf8:
        min_date = date.fromisoformat(df[date_col].min())
        max_date = date.fromisoformat(df[date_col].max())
    else:
        min_date = df[date_col].min()
        max_date = df[date_col].max()

    return min_date, max_date


def get_row_count(registry: CacheRegistry, name: str) -> int:
    """Get the row count of a dataset."""
    dataset = registry.get_dataset(name)
    if dataset is None:
        raise ValueError(f"Dataset {name} not found")

    df = dataset.read_data_pl()
    return len(df)


def check_initial_load():
    """Verify initial load created partial datasets correctly."""
    print("Checking initial load...")
    cache_dir = get_cache_dir()
    registry = CacheRegistry()
    registry.register_cache("test", cache_dir)

    errors = []

    for dataset_key, expected in EXPECTED.items():
        name = f"md.{dataset_key}"
        print(f"  Checking {name}...")

        # Check dataset exists
        if not check_dataset_exists(registry, name):
            errors.append(f"{name}: Dataset not found")
            continue

        # Check date range
        min_date, max_date = get_date_range(registry, name)
        print(f"    Date range: {min_date} to {max_date}")

        if max_date > expected["partial_end"]:
            errors.append(
                f"{name}: Max date {max_date} exceeds partial end {expected['partial_end']}"
            )

        if min_date > expected["full_start"]:
            errors.append(
                f"{name}: Min date {min_date} is after expected start {expected['full_start']}"
            )

        # Check row count is reasonable
        row_count = get_row_count(registry, name)
        print(f"    Row count: {row_count:,}")

        if row_count == 0:
            errors.append(f"{name}: No rows found")

    if errors:
        print("\n[FAIL] Initial load verification failed:")
        for error in errors:
            print(f"  - {error}")
        sys.exit(1)
    else:
        print("\n[OK] Initial load verification passed")


def check_incremental_update():
    """Verify incremental update extended datasets to full range."""
    print("Checking incremental update...")
    cache_dir = get_cache_dir()
    registry = CacheRegistry()
    registry.register_cache("test", cache_dir)

    errors = []

    for dataset_key, expected in EXPECTED.items():
        name = f"md.{dataset_key}"
        print(f"  Checking {name}...")

        # Check dataset exists
        if not check_dataset_exists(registry, name):
            errors.append(f"{name}: Dataset not found")
            continue

        # Check date range extends to full end
        min_date, max_date = get_date_range(registry, name)
        print(f"    Date range: {min_date} to {max_date}")

        # For futures, max_date should match full_end exactly (it's real data)
        # For equity, it depends on what was generated
        if dataset_key == "futures_daily":
            if max_date != expected["full_end"]:
                errors.append(
                    f"{name}: Max date {max_date} doesn't match expected {expected['full_end']}"
                )
        else:
            # For equity, just check it extended past partial end
            if max_date <= expected["partial_end"]:
                errors.append(
                    f"{name}: Max date {max_date} did not extend past partial end {expected['partial_end']}"
                )

        # Check row count increased (we can't check exact numbers without storing partial counts)
        row_count = get_row_count(registry, name)
        print(f"    Row count: {row_count:,}")

        if row_count == 0:
            errors.append(f"{name}: No rows found")

    if errors:
        print("\n[FAIL] Incremental update verification failed:")
        for error in errors:
            print(f"  - {error}")
        sys.exit(1)
    else:
        print("\n[OK] Incremental update verification passed")


def corrupt_data(cache_path: str, letter: str):
    """Corrupt data by multiplying prices by 2 for a specific letter.

    Args:
        cache_path: Path to the cache directory
        letter: First letter to corrupt (e.g., 'A')
    """
    print(f"Corrupting data in {cache_path} for letter {letter}...")
    cache_dir = Path(cache_path)

    if not cache_dir.exists():
        print(f"[ERROR] Cache directory not found: {cache_dir}")
        sys.exit(1)

    # Find all parquet files for the specified letter
    data_dir = cache_dir / "data"
    if not data_dir.exists():
        print(f"[ERROR] Data directory not found: {data_dir}")
        sys.exit(1)

    # Look for partition directories matching the letter
    letter_pattern = f"first_letter={letter}"
    corrupted_files = 0

    for parquet_file in data_dir.rglob("*.parquet"):
        # Check if this file is for the target letter
        if letter_pattern not in str(parquet_file):
            continue

        print(f"  Corrupting: {parquet_file.relative_to(cache_dir)}")

        # Read the file
        table = pq.read_table(parquet_file)
        df = pl.from_arrow(table)

        # Multiply price columns by 2
        price_cols = ["open", "high", "low", "close"]
        for col in price_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col) * 2)

        # Write back
        df.write_parquet(parquet_file)
        corrupted_files += 1

    if corrupted_files == 0:
        print(f"[WARNING] No files found for letter {letter}")
    else:
        print(f"[OK] Corrupted {corrupted_files} files")


def check_restate(corrupt_cache_path: str, original_cache_path: str):
    """Verify restate fixed the corrupted data.

    Args:
        corrupt_cache_path: Path to the restated (formerly corrupt) cache
        original_cache_path: Path to the original (uncorrupted) cache
    """
    print(f"Checking restate results...")
    print(f"  Restated cache: {corrupt_cache_path}")
    print(f"  Original cache: {original_cache_path}")

    corrupt_dir = Path(corrupt_cache_path)
    original_dir = Path(original_cache_path)

    if not corrupt_dir.exists():
        print(f"[ERROR] Restated cache not found: {corrupt_dir}")
        sys.exit(1)

    if not original_dir.exists():
        print(f"[ERROR] Original cache not found: {original_dir}")
        sys.exit(1)

    # Compare data directories
    corrupt_data = corrupt_dir / "data"
    original_data = original_dir / "data"

    errors = []
    files_checked = 0

    for original_file in original_data.rglob("*.parquet"):
        relative_path = original_file.relative_to(original_data)
        corrupt_file = corrupt_data / relative_path

        if not corrupt_file.exists():
            errors.append(f"Missing file in restated cache: {relative_path}")
            continue

        # Compare price values
        original_df = pl.read_parquet(original_file)
        restated_df = pl.read_parquet(corrupt_file)

        price_cols = ["open", "high", "low", "close"]
        for col in price_cols:
            if col in original_df.columns and col in restated_df.columns:
                # Check if values match (within floating point tolerance)
                orig_sum = original_df[col].sum()
                restated_sum = restated_df[col].sum()

                if orig_sum is None or restated_sum is None:
                    continue

                if abs(orig_sum - restated_sum) > 0.01:
                    errors.append(
                        f"{relative_path}: {col} sum mismatch "
                        f"(original={orig_sum:.2f}, restated={restated_sum:.2f})"
                    )

        files_checked += 1

    print(f"  Files checked: {files_checked}")

    if errors:
        print("\n[FAIL] Restate verification failed:")
        for error in errors[:10]:  # Limit output
            print(f"  - {error}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more errors")
        sys.exit(1)
    else:
        print("\n[OK] Restate verification passed - data restored correctly")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == "check-initial-load":
        check_initial_load()
    elif command == "check-incremental-update":
        check_incremental_update()
    elif command == "corrupt-data":
        if len(sys.argv) < 4:
            print("Usage: verify.py corrupt-data <cache_path> <letter>")
            sys.exit(1)
        corrupt_data(sys.argv[2], sys.argv[3])
    elif command == "check-restate":
        if len(sys.argv) < 4:
            print("Usage: verify.py check-restate <corrupt_cache> <original_cache>")
            sys.exit(1)
        check_restate(sys.argv[2], sys.argv[3])
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
