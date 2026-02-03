#!/usr/bin/env bash
#
# Automate Hudi bootstrap for COW and MOR, non-partitioned and partitioned,
# FULL_RECORD and METADATA_ONLY. Requires source Parquet data to exist at
# SOURCE_BASE_PATH (see generate_source_parquet.py).
#

set -e

# ---------------------------------------------------------------------------
# Config (override via env)
# ---------------------------------------------------------------------------
WAREHOUSE_BASE="${WAREHOUSE_BASE:-s3a://warehouse}"
HUDI_DATA_BASE="${WAREHOUSE_BASE}/hudi_data"
SOURCE_DATA_BASE="${WAREHOUSE_BASE}/source_data"
SOURCE_PARQUET="${SOURCE_DATA_BASE}/source_parquet"
SOURCE_PARTITION_PARQUET="${SOURCE_DATA_BASE}/source_partition_parquet"

SPARK_VERSION="${SPARK_VERSION:-3.5}"
HUDI_VERSION="${HUDI_VERSION:-1.0.2}"
SCALA_VERSION="${SCALA_VERSION:-2.12}"
HUDI_JARS_PATH="${HUDI_JARS_PATH:-/opt/hudi}"
mkdir -p $HUDI_JARS_PATH

HUDI_UTILITIES_JAR="${HUDI_UTILITIES_JAR:-${HUDI_JARS_PATH}/hudi-utilities-slim-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar}"
HUDI_SPARK_JAR="${HUDI_SPARK_JAR:-${HUDI_JARS_PATH}/hudi-spark${SPARK_VERSION}-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar}"

if [ ! -f "${HUDI_UTILITIES_JAR}" ]; then
  echo "Downloading HUDI_UTILITIES_JAR: ${HUDI_UTILITIES_JAR}"
  curl -L -o "${HUDI_UTILITIES_JAR}" "https://repo1.maven.org/maven2/org/apache/hudi/hudi-utilities-slim-bundle_${SCALA_VERSION}/${HUDI_VERSION}/hudi-utilities-slim-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"
fi

if [ ! -f "${HUDI_SPARK_JAR}" ]; then
  echo "Downloading HUDI_SPARK_JAR: ${HUDI_SPARK_JAR}"
  curl -L -o "${HUDI_SPARK_JAR}" "https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark${SPARK_VERSION}-bundle_${SCALA_VERSION}/${HUDI_VERSION}/hudi-spark${SPARK_VERSION}-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar"
fi

if ! command -v spark-submit &> /dev/null; then
  echo "spark-submit could not be found"
  exit 1
fi

HUDI_JARS="${HUDI_JARS:-${HUDI_UTILITIES_JAR},${HUDI_SPARK_JAR}}"

HIVE_METASTORE_URIS="${HIVE_METASTORE_URIS:-thrift://hive-metastore:9083}"
HIVE_SYNC_DB="${HIVE_SYNC_DB:-bootstrap_db}"

SPARK_MASTER="${SPARK_MASTER:-local}"
STREAMER_CLASS="org.apache.hudi.utilities.streamer.HoodieStreamer"

# ---------------------------------------------------------------------------
# Common spark-submit base args
# ---------------------------------------------------------------------------
spark_submit_base() {
  spark-submit \
    --master "${SPARK_MASTER}" \
    --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
    --jars "${HUDI_JARS}" \
    --class "${STREAMER_CLASS}" "${HUDI_UTILITIES_JAR}" \
    "$@"
}

# ---------------------------------------------------------------------------
# Run one bootstrap job
# ---------------------------------------------------------------------------
run_bootstrap() {
  local target_base_path="$1"
  local target_table="$2"
  local table_type="$3"
  local bootstrap_base_path="$4"
  local bootstrap_mode="$5"
  local partitioned="$6"  # "true" or "false"

  echo "=============================================="
  echo "Bootstrap: ${target_table}"
  echo "  Table Type: ${table_type} | Partitioned: ${partitioned} | Mode: ${bootstrap_mode}"
  echo "=============================================="

  local -a args=(
    --run-bootstrap
    --bootstrap-overwrite
    --target-base-path "${target_base_path}"
    --target-table "${target_table}"
    --table-type "${table_type}"
    --hoodie-conf "hoodie.bootstrap.base.path=${bootstrap_base_path}"
    --hoodie-conf hoodie.datasource.write.recordkey.field=uuid
    --hoodie-conf hoodie.datasource.write.precombine.field=ts
    --hoodie-conf hoodie.metadata.index.column.stats.enable=false
    --hoodie-conf "hoodie.bootstrap.mode.selector=org.apache.hudi.client.bootstrap.selector.BootstrapRegexModeSelector"
    --hoodie-conf "hoodie.bootstrap.mode.selector.regex.mode=${bootstrap_mode}"
    --hoodie-conf hoodie.datasource.write.hive_style_partitioning="${partitioned}"
    --enable-hive-sync
    --hoodie-conf hoodie.datasource.hive_sync.mode=hms
    --hoodie-conf "hoodie.datasource.hive_sync.metastore.uris=${HIVE_METASTORE_URIS}"
    --hoodie-conf "hoodie.datasource.hive_sync.database=${HIVE_SYNC_DB}"
    --hoodie-conf "hoodie.datasource.hive_sync.table=${target_table}"
  )

  if [[ "${partitioned}" == "true" ]]; then
    args+=(
      --hoodie-conf hoodie.datasource.write.partitionpath.field=city
      --hoodie-conf "hoodie.bootstrap.keygen.class=org.apache.hudi.keygen.SimpleKeyGenerator"
      --hoodie-conf "hoodie.bootstrap.mode.selector.regex=.*"
    )
  else
    args+=(
      --hoodie-conf "hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.NonpartitionedKeyGenerator"
      --hoodie-conf "hoodie.bootstrap.keygen.class=org.apache.hudi.keygen.NonpartitionedKeyGenerator"
    )
  fi

  spark_submit_base "${args[@]}"
}

# ---------------------------------------------------------------------------
# COPY_ON_WRITE (COW) Bootstrap
# ---------------------------------------------------------------------------
run_cow_bootstrap() {
  echo ""
  echo "########## COPY_ON_WRITE (COW) Bootstrap ##########"

  # Non-Partitioned – FULL_RECORD
  run_bootstrap \
    "${HUDI_DATA_BASE}/trips_hudi_cow_bootstrap_fl/" \
    "trips_hudi_cow_bootstrap_fl" \
    "COPY_ON_WRITE" \
    "${SOURCE_PARQUET}/" \
    "FULL_RECORD" \
    "false"

  # Non-Partitioned – METADATA_ONLY
  run_bootstrap \
    "${HUDI_DATA_BASE}/trips_hudi_cow_bootstrap_mo/" \
    "trips_hudi_cow_bootstrap_mo" \
    "COPY_ON_WRITE" \
    "${SOURCE_PARQUET}/" \
    "METADATA_ONLY" \
    "false"

  # Partitioned – FULL_RECORD
  run_bootstrap \
    "${HUDI_DATA_BASE}/trips_hudi_cow_bootstrap_partitioned_fl/" \
    "trips_hudi_cow_bootstrap_partitioned_fl" \
    "COPY_ON_WRITE" \
    "${SOURCE_PARTITION_PARQUET}/" \
    "FULL_RECORD" \
    "true"

  # Partitioned – METADATA_ONLY
  run_bootstrap \
    "${HUDI_DATA_BASE}/trips_hudi_cow_bootstrap_partitioned_mo/" \
    "trips_hudi_cow_bootstrap_partitioned_mo" \
    "COPY_ON_WRITE" \
    "${SOURCE_PARTITION_PARQUET}/" \
    "METADATA_ONLY" \
    "true"
}

# ---------------------------------------------------------------------------
# MERGE_ON_READ (MOR) Bootstrap
# ---------------------------------------------------------------------------
run_mor_bootstrap() {
  echo ""
  echo "########## MERGE_ON_READ (MOR) Bootstrap ##########"

  # Non-Partitioned – FULL_RECORD
  run_bootstrap \
    "${HUDI_DATA_BASE}/trips_hudi_mor_bootstrap_fl/" \
    "trips_hudi_mor_bootstrap_fl" \
    "MERGE_ON_READ" \
    "${SOURCE_PARQUET}/" \
    "FULL_RECORD" \
    "false"

  # Non-Partitioned – METADATA_ONLY
  run_bootstrap \
    "${HUDI_DATA_BASE}/trips_hudi_mor_bootstrap_mo/" \
    "trips_hudi_mor_bootstrap_mo" \
    "MERGE_ON_READ" \
    "${SOURCE_PARQUET}/" \
    "METADATA_ONLY" \
    "false"

  # Partitioned – FULL_RECORD
  run_bootstrap \
    "${HUDI_DATA_BASE}/trips_hudi_mor_bootstrap_partitioned_fl/" \
    "trips_hudi_mor_bootstrap_partitioned_fl" \
    "MERGE_ON_READ" \
    "${SOURCE_PARTITION_PARQUET}/" \
    "FULL_RECORD" \
    "true"

  # Partitioned – METADATA_ONLY
  run_bootstrap \
    "${HUDI_DATA_BASE}/trips_hudi_mor_bootstrap_partitioned_mo/" \
    "trips_hudi_mor_bootstrap_partitioned_mo" \
    "MERGE_ON_READ" \
    "${SOURCE_PARTITION_PARQUET}/" \
    "METADATA_ONLY" \
    "true"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
  local mode="${1:-all}"

  echo "Hudi Bootstrap Automation"
  echo "  WAREHOUSE_BASE=${WAREHOUSE_BASE}"
  echo "  HIVE_METASTORE_URIS=${HIVE_METASTORE_URIS}"
  echo "  HIVE_SYNC_DB=${HIVE_SYNC_DB}"
  echo "  Mode: ${mode}"

  case "${mode}" in
    cow)
      run_cow_bootstrap
      ;;
    mor)
      run_mor_bootstrap
      ;;
    all)
      run_cow_bootstrap
      run_mor_bootstrap
      ;;
    *)
      echo "Usage: $0 { all | cow | mor }" >&2
      echo "  all  - run COW and MOR bootstrap (default)" >&2
      echo "  cow  - run only COPY_ON_WRITE bootstrap" >&2
      echo "  mor  - run only MERGE_ON_READ bootstrap" >&2
      exit 1
      ;;
  esac

  echo ""
  echo "########## All bootstrap jobs completed successfully ##########"
}

main "$@"
