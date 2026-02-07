#!/usr/bin/env bash
#
# Automate Hudi bootstrap for COW and MOR, non-partitioned and partitioned,
# FULL_RECORD and METADATA_ONLY. Requires source Parquet data to exist at
# SOURCE_BASE_PATH (see generate_source_parquet.py).
#

set -e
set -o pipefail

# ---------------------------------------------------------------------------
# Config (override via env). SPARK_VERSION, HUDI_VERSION, SCALA_VERSION are
# set by run_demo_e2e.sh when running the full demo; defaults below for standalone runs.
# ---------------------------------------------------------------------------

if ! command -v spark-submit &> /dev/null; then
    echo "spark-submit could not be found. Please install Spark and add it to your PATH."
    exit 1
fi

export SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export CONFIG_FILE="${SCRIPT_DIR}/config.yaml"

VARS_EVAL=$(python3 -c "
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

export WAREHOUSE_BASE_PATH="${WAREHOUSE_BASE_PATH:-'s3a://warehouse'}"
export HUDI_DATA_BASE_PATH="${WAREHOUSE_BASE_PATH}/hudi_data"
export SOURCE_DATA_BASE_PATH="${WAREHOUSE_BASE_PATH}/source_data"
export SOURCE_PARQUET_PATH="${SOURCE_DATA_BASE_PATH}/source_parquet"
export SOURCE_PARTITION_PARQUET_PATH="${SOURCE_DATA_BASE_PATH}/source_partition_parquet"
export HIVE_METASTORE_URIS="${HIVE_METASTORE_URIS:-'thrift://hive-metastore:9083'}"
export HIVE_SYNC_DB="${HIVE_SYNC_DB:-'bootstrap_db'}"
export SPARK_MASTER="${SPARK_MASTER:-'local[2]'}"

export SPARK_VERSION=${SPARK_VERSION:-$(spark-submit --version 2>&1 | awk '/version/ {print $NF; exit}')}
export SPARK_MAJOR_VERSION=${SPARK_MAJOR_VERSION:-$(echo "${SPARK_VERSION}" | cut -d. -f1,2)}
export SCALA_VERSION=${SCALA_VERSION:-$(spark-submit --version 2>&1 | grep 'Scala version' | awk '{print $4}' | cut -d. -f1,2)}

export HUDI_VERSION=${HUDI_VERSION:-1.0.2}  
export HUDI_JARS_PATH="${HUDI_JARS_PATH:-/opt/hudi}"
export HUDI_UTILITIES_JAR="${HUDI_UTILITIES_JAR:-${HUDI_JARS_PATH}/hudi-utilities-slim-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar}"
export HUDI_SPARK_JAR="${HUDI_SPARK_JAR:-${HUDI_JARS_PATH}/hudi-spark${SPARK_MAJOR_VERSION}-bundle_${SCALA_VERSION}-${HUDI_VERSION}.jar}"
export HUDI_JARS="${HUDI_UTILITIES_JAR},${HUDI_SPARK_JAR}"
export STREAMER_CLASS="org.apache.hudi.utilities.streamer.HoodieStreamer"

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
# Run bootstrap job
# ---------------------------------------------------------------------------
run_bootstrap() {
  local target_base_path="$1"
  local target_table="$2"
  local table_type="$3"
  local bootstrap_base_path="$4"
  local bootstrap_mode="$5"
  local partitioned="$6"  # "true" or "false"

  echo "=============================================="
  echo "Bootstrap: ${target_table} | Table Type: ${table_type} | Partitioned: ${partitioned} | Mode: ${bootstrap_mode}"
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


bootstrap_hudi_tables() {
    SCENARIOS=()
    export base_table_name="${BASE_TABLE_NAME:-$YAML_BASE_TABLE_NAME}"

    for table_type in "COW" "MOR"; do
        for partitioned in "false" "true"; do
            for bootstrap_mode in "FULL_RECORD" "METADATA_ONLY"; do
                part_suffix=""
                if [ "$partitioned" = "true" ]; then
                    part_suffix="_partitioned"
                fi
                table_type_lower=$(echo "$table_type" | tr '[:upper:]' '[:lower:]')
                bootstrap_mode_suffix="mo"
                if [ "$bootstrap_mode" = "FULL_RECORD" ]; then
                    bootstrap_mode_suffix="fr"
                fi
                table_name="${base_table_name}_${table_type_lower}_bootstrap${part_suffix}_${bootstrap_mode_suffix}"
                table_type_val="COPY_ON_WRITE"
                if [ "$table_type" = "MOR" ]; then
                    table_type_val="MERGE_ON_READ"
                fi
                SCENARIOS+=("${table_name}|${table_type_val}|${partitioned}|${bootstrap_mode}")
            done
        done
    done

    for scenario in "${SCENARIOS[@]}"; do
        IFS="|" read -r table_name table_type partitioned bootstrap_mode <<< "$scenario"
        source_parquet_path="${SOURCE_PARQUET_PATH}/"
        if [ "$partitioned" = "true" ]; then
            source_parquet_path="${SOURCE_PARTITION_PARQUET_PATH}/"
        fi
        target_base_path="${HUDI_DATA_BASE_PATH}/${table_name}/"
        echo "=============================================="
        echo "Running bootstrap for ${table_name}" 
        echo "Table Type: ${table_type}"
        echo "Partitioned: ${partitioned}"
        echo "Bootstrap Mode: ${bootstrap_mode}"
        echo "Target base path: ${target_base_path}"
        echo "Source parquet path: ${source_parquet_path}"
        echo "==============================================" 
        run_bootstrap "${target_base_path}" "${table_name}" "${table_type}" "${source_parquet_path}" "${bootstrap_mode}" "${partitioned}"
        echo "Bootstrap completed with status: $?"
        echo "=============================================="
    done
}


main() {
  echo "Running Hudi bootstrap automation."
  bootstrap_hudi_tables
  echo "Hudi bootstrap automation completed successfully."
}

main "$@"
