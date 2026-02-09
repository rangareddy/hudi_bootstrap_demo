#!/usr/bin/env bash
#
# End-to-end Hudi bootstrap demo:
#   1. Generate source Parquet data (generate_source_parquet.py)
#   2. Run Hudi bootstrap for all tables (bootstrap_hudi_tables.sh)
#   3. Validate reads via Spark, Trino, and Presto (validate_hudi_tables.py)
#
# Run from this directory so config.yaml and paths resolve correctly.
#

set -e
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

validate_spark_submit() {
    if ! command -v spark-submit &> /dev/null; then
        echo "spark-submit could not be found. Please install Spark and add it to your PATH."
        exit 1
    fi
}

validate_packages() {
    if ! python -c "import trino, prestodb" &>/dev/null; then
        echo "Required packages not found. Installing..."
        pip install -r requirements.txt
    fi
}

validate_spark_submit
validate_packages

export CONFIG_FILE="${SCRIPT_DIR}/config.yaml"

VARS_EVAL=$(python -c "
import yaml
try:
    with open('$CONFIG_FILE', 'r') as f:
        cfg = yaml.safe_load(f)
        common = cfg.get('common', {})
        spark = cfg.get('spark', {})
        
        print(f\"HIVE_METASTORE_URIS='{common.get('hive_metastore_uris', '')}'\")
        print(f\"HIVE_SYNC_DB='{common.get('schema', '')}'\")
        print(f\"SPARK_MASTER='{spark.get('master', '')}'\")
        print(f\"WAREHOUSE_BASE_PATH='{common.get('base_path', 's3a://warehouse')}'\")
        print(f\"BASE_TABLE_NAME='{common.get('base_table_name', 'trips_hudi')}'\")
        print(f\"HUDI_VERSION='{common.get('hudi_version', '1.0.2')}'\")
        print(f\"HUDI_JARS_PATH='{common.get('hudi_jars_path', '/opt/hudi')}'\")  
except Exception as e:
    pass
")

eval "$VARS_EVAL"

export SPARK_VERSION=${SPARK_VERSION:-$(spark-submit --version 2>&1 | awk '/version/ {print $NF; exit}')}   
export SPARK_MAJOR_VERSION=${SPARK_MAJOR_VERSION:-$(echo "${SPARK_VERSION}" | cut -d. -f1,2)}
export SCALA_VERSION=${SCALA_VERSION:-$(spark-submit --version 2>&1 | grep 'Scala version' | awk '{print $4}' | cut -d. -f1,2)}

export HUDI_VERSION=${HUDI_VERSION:-1.0.2}
export HUDI_JARS_PATH=${HUDI_JARS_PATH:-/opt/hudi}
export HUDI_UTILITIES_JAR=${HUDI_JARS_PATH}/hudi-utilities-slim-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar
export HUDI_SPARK_JAR=${HUDI_JARS_PATH}/hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar

download_hudi_jars() {
    MVN_HUDI_URL=https://repo1.maven.org/maven2/org/apache/hudi
    mkdir -p $HUDI_JARS_PATH
    if [ ! -f "${HUDI_UTILITIES_JAR}" ]; then
      echo "Downloading HUDI_UTILITIES_JAR: ${HUDI_UTILITIES_JAR}"
      curl -L -o "${HUDI_UTILITIES_JAR}" \
        "$MVN_HUDI_URL/hudi-utilities-slim-bundle_${SCALA_VERSION}/${HUDI_VERSION}/hudi-utilities-slim-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"
    fi

    if [ ! -f "${HUDI_SPARK_JAR}" ]; then
      echo "Downloading HUDI_SPARK_JAR: ${HUDI_SPARK_JAR}"
      curl -L -o "${HUDI_SPARK_JAR}" \
        "$MVN_HUDI_URL/hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}/${HUDI_VERSION}/hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"
    fi
}

download_hudi_jars

LOG_DIR=${SCRIPT_DIR}/logs/
LOG_DIR_PATH=${LOG_DIR}/hudi_bootstrap_e2e_$(date +%Y%m%d_%H%M%S).log
mkdir -p ${LOG_DIR}

echo "===================================================================="
echo "Hudi bootstrap E2E started."
echo "SPARK_VERSION: ${SPARK_VERSION} and SCALA_VERSION: ${SCALA_VERSION} and HUDI_VERSION: ${HUDI_VERSION}"
echo "===================================================================="

# ---------------------------------------------------------------------------
# 1. Generate source data
# ---------------------------------------------------------------------------
echo ""
echo "########## Step 1: Generate source Parquet data ##########"
spark-submit generate_source_parquet.py 2>&1 | tee "${LOG_DIR_PATH}"
echo "Step 1 done with status: $?"

# ---------------------------------------------------------------------------
# 2. Run Hudi bootstrap (COW + MOR)
# ---------------------------------------------------------------------------

echo ""
echo "########## Step 2: Run Hudi bootstrap ##########"
bash bootstrap_hudi_tables.sh 2>&1 | tee "${LOG_DIR_PATH}"
echo "Step 2 done with status: $?"

# ---------------------------------------------------------------------------
# 3. Validate reads (Spark, Trino, Presto)
# ---------------------------------------------------------------------------
echo ""
echo "########## Step 3: Validate Hudi reads ##########"
spark-submit --jars ${HUDI_SPARK_JAR} validate_hudi_tables.py 2>&1 | tee "${LOG_DIR_PATH}"
echo "Step 3 done with status: $?"

echo ""
echo "===================================================================="
echo "Hudi bootstrap E2E completed successfully."
echo "Step logs: ${LOG_DIR_PATH}"
echo "===================================================================="