# Apache Hudi Bootstrap Demo

This demo shows how to **bootstrap** existing Parquet data into Apache Hudi tables (COPY_ON_WRITE and MERGE_ON_READ), then validate reads from **Spark**, **Trino**, and **Presto**.

## What this demo does

1. **Generate source data** – Writes sample trip data as non-partitioned and city-partitioned Parquet under a warehouse path (e.g. S3). Skips generation if source data already exists.
2. **Bootstrap** – Runs Hudi’s bootstrap to create 8 tables (4 COW + 4 MOR):
   - **COW** and **MOR**, each with:
     - Non-partitioned: FULL_RECORD and METADATA_ONLY
     - Partitioned by `city`: FULL_RECORD and METADATA_ONLY
   - Table names follow `{base_table_name}_{cow|mor}_bootstrap[_partitioned]_{fr|mo}` (e.g. `trips_hudi_cow_bootstrap_fr`, `trips_hudi_mor_bootstrap_partitioned_mo`).
3. **Validate** – Queries each table from Spark, Trino, and/or Presto (as enabled in config); checks that `_hoodie_commit_time` (metadata) and `ts` (data) are visible; prints a summary table with a **Notes** column for any exceptions.

## Prerequisites

- **Python 3** with `pip`
- **Spark 3.x** (e.g. 3.5) with `spark-submit` on `PATH`
- **Hudi JARs** – `run_demo_e2e.sh` builds JAR paths from `HUDI_JARS_PATH`, `HUDI_VERSION`, `SCALA_VERSION`, and Spark major version, and downloads the utilities and Spark bundle from Maven if they are not already present.
- **Object storage** (e.g. S3) and a **Hive Metastore** for the bootstrap step
- **Trino** and **Presto** (optional) for validation; disable in `config.yaml` if not available

## Configuration

All main settings are in **`config.yaml`**:

| Section   | Key                   | Description |
| --------- | --------------------- | ----------- |
| `common`  | `base_path`           | Warehouse base path (e.g. `s3a://warehouse/`) |
| `common`  | `schema`              | Hive schema/database name (e.g. `bootstrap_db`) |
| `common`  | `base_table_name`     | Base name for Hudi tables (e.g. `trips_hudi`) |
| `common`  | `hive_metastore_uris` | Hive metastore URI for sync |
| `common`  | `hudi_version`        | Hudi version (e.g. `1.0.2`) |
| `common`  | `hudi_jars_path`      | Directory for Hudi JARs (e.g. `/opt/hudi`) |
| `spark`   | `app_name`, `master`  | Spark application name and master URL |
| `trino`   | `enabled`, `host`, `port`, `user`, `catalog`, `schema` | Trino connection and enable/disable |
| `presto`  | `enabled`, `host`, `port`, `user`, `catalog`, `schema` | Presto connection and enable/disable |

**`run_demo_e2e.sh`** reads `config.yaml` and exports these as environment variables for the bootstrap script; you can still override them with `export VAR=value` before running.

## Quick start (end-to-end)

From the `hudi_bootstrap_demo` directory:

```bash
bash run_demo_e2e.sh
```

The script:

1. **Validates** – Checks that `spark-submit` is on `PATH` and that Python can `import trino, prestodb` (runs `pip install -r requirements.txt` if not).
2. **Loads config** – Reads `config.yaml` and exports `HIVE_METASTORE_URIS`, `HIVE_SYNC_DB`, `SPARK_MASTER`, `WAREHOUSE_BASE_PATH`, `BASE_TABLE_NAME`, `HUDI_VERSION`, `HUDI_JARS_PATH` for child scripts. Spark/Scala versions are taken from `spark-submit --version` unless overridden by env.
3. **Downloads Hudi JARs** – If the utilities and Spark bundle JARs are not present under `HUDI_JARS_PATH`, downloads them from Maven.
4. **Step 1** – `spark-submit generate_source_parquet.py` → writes Parquet under `base_path/source_data/` (skips if already present). Output is teed to the run log.
5. **Step 2** – `bash bootstrap_hudi_tables.sh` → creates all 8 Hudi tables and syncs to Hive. Output teed to the run log.
6. **Step 3** – `spark-submit --jars ${HUDI_SPARK_JAR} validate_hudi_tables.py` → validates using engines enabled in config. Output teed to the run log.

**Log file** – All step output is teed to a single timestamped file: **`logs/hudi_bootstrap_e2e_YYYYMMDD_HHMMSS.log`** (e.g. `logs/hudi_bootstrap_e2e_20250203_143022.log`). The script prints this path at the end.

## Log files

When you run **`run_demo_e2e.sh`**, all step output is teed to one timestamped log file:

| Log path | Description |
| -------- | ----------- |
| `logs/hudi_bootstrap_e2e_YYYYMMDD_HHMMSS.log` | Single log file for the run (e.g. `logs/hudi_bootstrap_e2e_20250203_143022.log`). Each step’s stdout/stderr is teed to this file. |

## Files in this directory

| File | Purpose |
| ---- | ------- |
| `config.yaml` | Single source for common, Spark, Trino, and Presto settings (paths, Hive, table names, engine enable flags) |
| `yaml_util.py` | Loads and returns `config.yaml` as a dict; used by Python scripts |
| `generate_source_parquet.py` | Generate source Parquet (skips if paths exist); uses `config.yaml` |
| `bootstrap_hudi_tables.sh` | Run Hudi bootstrap for all 8 COW/MOR tables; reads config via Python and env |
| `run_demo_e2e.sh` | Run full demo: validate deps, load config, download Hudi JARs if needed, then generate → bootstrap → validate; tees all output to `logs/hudi_bootstrap_e2e_YYYYMMDD_HHMMSS.log` |
| `validate_hudi_tables.py` | Validate Hudi tables with Spark/Trino/Presto per config; print summary with Table and Notes |
| `requirements.txt` | Python dependencies (trino, presto-python-client, pyyaml) |

## Troubleshooting

- **Spark / Hudi not found** – Ensure `spark-submit` is on `PATH`. If JARs are not present, the e2e script will try to download them to `hudi_jars_path`; otherwise set `HUDI_UTILITIES_JAR` and `HUDI_SPARK_JAR`.
- **S3 / path errors** – Set `base_path` in `config.yaml` (or override `WAREHOUSE_BASE_PATH`) to a path your Spark and cluster can read/write.
- **Hive sync failures** – Ensure Hive Metastore is reachable at `hive_metastore_uris` in config and the database in `schema` exists (or can be created).
- **Trino / Presto validation** – Set `trino.enabled` / `presto.enabled` to `false` in `config.yaml` if those engines are not available; validation will run only for enabled engines. Connection or query errors appear in the **Notes** column of the summary table.

## References

1. [Hudi Bootstrap Procedures](https://hudi.apache.org/docs/procedures#bootstrap)
2. [Hudi Migration Guide](https://hudi.apache.org/docs/migration_guide)
