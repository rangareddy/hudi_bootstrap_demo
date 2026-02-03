#!/usr/bin/env bash
#
# End-to-end Hudi bootstrap demo:
#   1. Generate source Parquet data (generate_source_data.py)
#   2. Run Hudi bootstrap for all tables (run_hudi_bootstrap.sh)
#   3. Validate reads via Spark, Trino, and Presto (validate_hudi_bootstrap_data.py)
#
# Run from this directory so config.yaml and paths resolve correctly.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

echo "=============================================="
echo "Hudi Bootstrap E2E – working directory: ${SCRIPT_DIR}"
echo "=============================================="

# ---------------------------------------------------------------------------
# 1. Generate source data
# ---------------------------------------------------------------------------
echo ""
echo "########## Step 1: Generate source Parquet data ##########"
spark-submit generate_source_data.py
echo "Step 1 done."

# ---------------------------------------------------------------------------
# 2. Run Hudi bootstrap (COW + MOR)
# ---------------------------------------------------------------------------
echo ""
echo "########## Step 2: Run Hudi bootstrap ##########"
bash run_hudi_bootstrap.sh all
echo "Step 2 done."

# ---------------------------------------------------------------------------
# 3. Validate reads (Spark, Trino, Presto)
# ---------------------------------------------------------------------------
echo ""
echo "########## Step 3: Validate Hudi reads ##########"
spark-submit validate_hudi_bootstrap_data.py
echo "Step 3 done."

echo ""
echo "=============================================="
echo "E2E completed successfully."
echo "=============================================="
