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

if ! command -v spark-submit &> /dev/null; then
    echo "spark-submit could not be found. Please install Spark and add it to your PATH."
    exit 1
fi

if ! pip list 2>/dev/null | grep -q -e trino -e presto-python-client; then
    echo "Required packages not found. Installing..."
    pip install -r requirements.txt
fi

export SPARK_VERSION=$(spark-submit --version 2>&1 | awk '/version/ {print $NF; exit}')
export SPARK_MAJOR_VERSION=$(echo "${SPARK_VERSION}" | cut -d. -f1,2)
export HUDI_VERSION="${HUDI_VERSION:-1.0.2}"
export SCALA_VERSION="${SCALA_VERSION:-2.12}"

export IS_SPARK_VALIDATION_ENABLED=${IS_SPARK_VALIDATION_ENABLED:-true}
export IS_TRINO_VALIDATION_ENABLED=${IS_TRINO_VALIDATION_ENABLED:-true}
export IS_PRESTO_VALIDATION_ENABLED=${IS_PRESTO_VALIDATION_ENABLED:-true}

LOG_DIR="${SCRIPT_DIR}/logs/${HUDI_VERSION}"
mkdir -p ${LOG_DIR}
LOG_GENERATE="${LOG_DIR}/generate_source_parquet.log"
LOG_BOOTSTRAP="${LOG_DIR}/bootstrap_hudi_tables.log"
LOG_VALIDATE="${LOG_DIR}/validate_hudi_tables.log"

echo "=============================================="
echo "Hudi Bootstrap E2E Started"
echo "SPARK_VERSION: ${SPARK_VERSION} and SPARK_MAJOR_VERSION: ${SPARK_MAJOR_VERSION}"
echo "HUDI_VERSION: ${HUDI_VERSION} and SCALA_VERSION: ${SCALA_VERSION}"
echo "IS_SPARK_VALIDATION_ENABLED: ${IS_SPARK_VALIDATION_ENABLED} and IS_TRINO_VALIDATION_ENABLED: ${IS_TRINO_VALIDATION_ENABLED} and IS_PRESTO_VALIDATION_ENABLED: ${IS_PRESTO_VALIDATION_ENABLED}"
echo "=============================================="

export HUDI_JARS_PATH="${HUDI_JARS_PATH:-/opt/hudi}"
export MVN_HUDI_URL=https://repo1.maven.org/maven2/org/apache/hudi
export HUDI_UTILITIES_JAR="${HUDI_UTILITIES_JAR:-${HUDI_JARS_PATH}/hudi-utilities-slim-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar}"
export HUDI_SPARK_JAR="${HUDI_SPARK_JAR:-${HUDI_JARS_PATH}/hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar}"

mkdir -p $HUDI_JARS_PATH

if [ ! -f "${HUDI_UTILITIES_JAR}" ]; then
  echo "Downloading HUDI_UTILITIES_JAR: ${HUDI_UTILITIES_JAR}"
  curl -L -o -s "${HUDI_UTILITIES_JAR}" \
    "$MVN_HUDI_URL/hudi-utilities-slim-bundle_${SCALA_VERSION}/${HUDI_VERSION}/hudi-utilities-slim-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"
fi

if [ ! -f "${HUDI_SPARK_JAR}" ]; then
  echo "Downloading HUDI_SPARK_JAR: ${HUDI_SPARK_JAR}"
  curl -L -o -s "${HUDI_SPARK_JAR}" \
    "$MVN_HUDI_URL/hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}/${HUDI_VERSION}/hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"
fi

# ---------------------------------------------------------------------------
# 1. Generate source data
# ---------------------------------------------------------------------------
echo ""
echo "########## Step 1: Generate source Parquet data ##########"
echo "Step 1 log: ${LOG_GENERATE}"
spark-submit generate_source_parquet.py 2>&1 | tee "${LOG_GENERATE}"
echo "Step 1 done with status: $?"

# ---------------------------------------------------------------------------
# 2. Run Hudi bootstrap (COW + MOR)
# ---------------------------------------------------------------------------

echo ""
echo "########## Step 2: Run Hudi bootstrap ##########"
echo "Step 2 log: ${LOG_BOOTSTRAP}"
bash bootstrap_hudi_tables.sh all 2>&1 | tee "${LOG_BOOTSTRAP}"
echo "Step 2 done with status: $?"

# ---------------------------------------------------------------------------
# 3. Validate reads (Spark, Trino, Presto)
# ---------------------------------------------------------------------------
echo ""
echo "########## Step 3: Validate Hudi reads ##########"
echo "Step 3 log: ${LOG_VALIDATE}"
VALIDATION_ENGINES=""
if [ "${IS_SPARK_VALIDATION_ENABLED}" == "true" ]; then
    VALIDATION_ENGINES+="spark,"
fi
if [ "${IS_TRINO_VALIDATION_ENABLED}" == "true" ]; then
    VALIDATION_ENGINES+="trino,"
fi
if [ "${IS_PRESTO_VALIDATION_ENABLED}" == "true" ]; then
    VALIDATION_ENGINES+="presto,"
fi

# Remove the trailing comma if the string is not empty
VALIDATION_ENGINES=${VALIDATION_ENGINES%,}

if [ -z "$VALIDATION_ENGINES" ]; then
    echo "Error: At least one engine (spark, trino, or presto) must be enabled for validation."
    exit 1
fi 

echo "Enabled engines: $VALIDATION_ENGINES"
spark-submit --jars ${HUDI_SPARK_JAR} validate_hudi_tables.py --engines ${VALIDATION_ENGINES} 2>&1 | tee "${LOG_VALIDATE}"
echo "Step 3 done with status: $?"

echo ""
echo "=============================================="
echo "E2E completed successfully."
echo "Step logs: ${LOG_GENERATE} | ${LOG_BOOTSTRAP} | ${LOG_VALIDATE}"
echo "=============================================="