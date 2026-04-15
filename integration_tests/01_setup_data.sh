#!/bin/bash
# Integration Test: Setup Data
# Verifies that required test data exists

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEST_DATA_DIR="$PROJECT_DIR/../parquet_test_data"

echo "=== Integration Test: Setup Data ==="
echo "Project dir: $PROJECT_DIR"
echo "Test data dir: $TEST_DATA_DIR"

# Check futures data (should already exist)
FUTURES_DATA="C:/Users/cplag/OneDrive/Documents/Data/Futures_daily_data/parquet_files"
if [ -d "$FUTURES_DATA" ]; then
    echo "[OK] Futures data exists: $FUTURES_DATA"
else
    echo "[ERROR] Futures data not found: $FUTURES_DATA"
    exit 1
fi

# Check fake equity data
FAKE_BARS="$TEST_DATA_DIR/fake_bars"
if [ -d "$FAKE_BARS/minute" ] && [ -d "$FAKE_BARS/daily" ]; then
    echo "[OK] Fake equity bars exist: $FAKE_BARS"
else
    echo "[WARNING] Fake equity bars not found: $FAKE_BARS"
    echo ""
    echo "Generate with:"
    echo "  cd $PROJECT_DIR"
    echo "  # Letter A (creates ticker metadata)"
    echo "  ~/bin/python_env_management/run_env.sh py312_pd22_x64 python create_bars.py \\"
    echo "      --start-date 2020-01-01 --end-date 2025-12-31 \\"
    echo "      --letters A --tickers-per-letter 1000 \\"
    echo "      --output-dir ../parquet_test_data/fake_bars --seed 42"
    echo ""
    echo "  # Letters B-E"
    echo "  for letter in B C D E; do"
    echo "      ~/bin/python_env_management/run_env.sh py312_pd22_x64 python create_bars.py \\"
    echo "          --start-date 2020-01-01 --end-date 2025-12-31 --letters \$letter \\"
    echo "          --output-dir ../parquet_test_data/fake_bars --seed 42"
    echo "  done"
    echo ""
    exit 1
fi

# Clean up any previous test caches
TEST_CACHES="$TEST_DATA_DIR/test_caches"
if [ -d "$TEST_CACHES" ]; then
    echo "Cleaning previous test caches..."
    rm -rf "$TEST_CACHES"
fi
mkdir -p "$TEST_CACHES"
echo "[OK] Test cache directory ready: $TEST_CACHES"

echo ""
echo "=== Setup Complete ==="
