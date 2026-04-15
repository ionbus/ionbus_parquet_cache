#!/bin/bash
# Integration Test: Restate Mode
# Tests fixing corrupted data by restating from source
#
# This test:
# 1. Creates a copy of the equity minute cache
# 2. Corrupts the copy (multiplies prices by 2 for letter A)
# 3. Runs restate to fix the corrupted data
# 4. Verifies prices are restored

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_DATA_DIR="$PROJECT_DIR/../parquet_test_data"
RUN_ENV="$HOME/bin/python_env_management/run_env.sh"
ENV_NAME="py312_pd22_x64"

echo "=== Integration Test: Restate Mode ==="

cd "$PROJECT_DIR"

# Step 1: Create a copy of equity daily cache for corruption test
echo ""
echo "--- Creating test copy of equity daily cache ---"
ORIGINAL_CACHE="$TEST_DATA_DIR/test_caches/md.equity_daily"
CORRUPT_CACHE="$TEST_DATA_DIR/test_caches/md.equity_daily_corrupt"

if [ -d "$CORRUPT_CACHE" ]; then
    rm -rf "$CORRUPT_CACHE"
fi
cp -r "$ORIGINAL_CACHE" "$CORRUPT_CACHE"
echo "[OK] Created copy at: $CORRUPT_CACHE"

# Step 2: Corrupt the data
echo ""
echo "--- Corrupting data (multiply prices by 2 for letter A) ---"
$RUN_ENV $ENV_NAME python "$SCRIPT_DIR/verify.py" corrupt-data "$CORRUPT_CACHE" A

# Step 3: Run restate
echo ""
echo "--- Running restate to fix corrupted data ---"
# Create a YAML config pointing to the corrupt cache
cat > "$SCRIPT_DIR/configs/equity_daily_restate.yaml" << 'EOF'
cache_dir: ../../../parquet_test_data/test_caches

datasets:
  md.equity_daily_corrupt:
    description: Daily equity OHLC data (for restate test)
    date_col: date
    date_partition: year
    partition_columns:
      - first_letter
    sort_columns:
      - ticker
      - date

    source_class_name: HiveParquetSource
    source_init_args:
      path: "../../../parquet_test_data/fake_bars/daily"
EOF

$RUN_ENV $ENV_NAME python -m ionbus_parquet_cache.yaml_create_datasets \
    --yaml "$SCRIPT_DIR/configs/equity_daily_restate.yaml" \
    --restate

# Step 4: Verify fix
echo ""
echo "--- Verifying restate fixed the data ---"
$RUN_ENV $ENV_NAME python "$SCRIPT_DIR/verify.py" check-restate "$CORRUPT_CACHE" "$ORIGINAL_CACHE"

# Cleanup
rm -f "$SCRIPT_DIR/configs/equity_daily_restate.yaml"
rm -rf "$CORRUPT_CACHE"

echo ""
echo "=== Restate Mode Complete ==="
