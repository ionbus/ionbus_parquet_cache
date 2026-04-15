#!/bin/bash
# Integration Test: Initial Load (Partial Data)
# Creates caches with partial date ranges

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONFIG_DIR="$SCRIPT_DIR/configs"
RUN_ENV="$HOME/bin/python_env_management/run_env.sh"
ENV_NAME="py312_pd22_x64"

echo "=== Integration Test: Initial Load (Partial Data) ==="

cd "$PROJECT_DIR"

# Load futures daily (partial - up to 1999-12-31)
echo ""
echo "--- Loading futures daily (partial) ---"
$RUN_ENV $ENV_NAME python -m ionbus_parquet_cache.yaml_create_datasets \
    --yaml "$CONFIG_DIR/futures_daily_partial.yaml"

# Load equity daily (partial - up to 2023-06-30)
echo ""
echo "--- Loading equity daily (partial) ---"
$RUN_ENV $ENV_NAME python -m ionbus_parquet_cache.yaml_create_datasets \
    --yaml "$CONFIG_DIR/equity_daily_partial.yaml"

# Load equity minute (partial - up to 2023-06-30)
echo ""
echo "--- Loading equity minute (partial) ---"
$RUN_ENV $ENV_NAME python -m ionbus_parquet_cache.yaml_create_datasets \
    --yaml "$CONFIG_DIR/equity_minute_partial.yaml"

# Verify results
echo ""
echo "--- Verifying initial load ---"
$RUN_ENV $ENV_NAME python "$SCRIPT_DIR/verify.py" check-initial-load

echo ""
echo "=== Initial Load Complete ==="
