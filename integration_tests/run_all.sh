#!/bin/bash
# Run all integration tests
#
# Usage:
#   ./run_all.sh           # Run all tests
#   ./run_all.sh --skip-setup  # Skip data setup (useful for re-runs)

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SKIP_SETUP=false
for arg in "$@"; do
    case $arg in
        --skip-setup)
            SKIP_SETUP=true
            shift
            ;;
    esac
done

echo "========================================"
echo "  ionbus_parquet_cache Integration Tests"
echo "========================================"
echo ""

START_TIME=$(date +%s)

# Test 1: Setup
if [ "$SKIP_SETUP" = false ]; then
    echo ">>> Running 01_setup_data.sh"
    bash "$SCRIPT_DIR/01_setup_data.sh"
    echo ""
fi

# Test 2: Initial Load
echo ">>> Running 02_initial_load.sh"
bash "$SCRIPT_DIR/02_initial_load.sh"
echo ""

# Test 3: Incremental Update
echo ">>> Running 03_incremental_update.sh"
bash "$SCRIPT_DIR/03_incremental_update.sh"
echo ""

# Test 4: Copy with Rename (placeholder)
echo ">>> Running 04_copy_rename.sh"
bash "$SCRIPT_DIR/04_copy_rename.sh"
echo ""

# Test 5: Restate Mode
echo ">>> Running 05_restate.sh"
bash "$SCRIPT_DIR/05_restate.sh"
echo ""

# Test 6: Preserve Config (placeholder)
echo ">>> Running 06_preserve_config.sh"
bash "$SCRIPT_DIR/06_preserve_config.sh"
echo ""

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo "========================================"
echo "  All Integration Tests Complete!"
echo "  Elapsed time: ${ELAPSED}s"
echo "========================================"
