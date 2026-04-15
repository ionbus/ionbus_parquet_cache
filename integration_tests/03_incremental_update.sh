#!/bin/bash
# Integration Test: Incremental Update
# Updates caches with remaining data (full date range)

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONFIG_DIR="$SCRIPT_DIR/configs"
RUN_ENV="$HOME/bin/python_env_management/run_env.sh"
ENV_NAME="py312_pd22_x64"

echo "=== Integration Test: Incremental Update ==="

cd "$PROJECT_DIR"

# Update futures daily (full range)
echo ""
echo "--- Updating futures daily (full range) ---"
$RUN_ENV $ENV_NAME python -m ionbus_parquet_cache.yaml_create_datasets \
    --yaml "$CONFIG_DIR/futures_daily.yaml"

# Update equity daily (full range)
echo ""
echo "--- Updating equity daily (full range) ---"
$RUN_ENV $ENV_NAME python -m ionbus_parquet_cache.yaml_create_datasets \
    --yaml "$CONFIG_DIR/equity_daily.yaml"

# Update equity minute (full range)
echo ""
echo "--- Updating equity minute (full range) ---"
$RUN_ENV $ENV_NAME python -m ionbus_parquet_cache.yaml_create_datasets \
    --yaml "$CONFIG_DIR/equity_minute.yaml"

# Verify results
echo ""
echo "--- Verifying incremental update ---"
$RUN_ENV $ENV_NAME python "$SCRIPT_DIR/verify.py" check-incremental-update

echo ""
echo "=== Incremental Update Complete ==="
