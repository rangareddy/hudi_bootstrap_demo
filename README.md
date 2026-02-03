# Apache Hudi Bootstrap Demo

This demo shows how to **bootstrap** existing Parquet data into Apache Hudi tables (COPY_ON_WRITE and MERGE_ON_READ), then validate reads from **Spark**, **Trino**, and **Presto**.

## What this demo does

1. **Generate source data** – Writes sample trip data as non-partitioned and city-partitioned Parquet under a warehouse path (e.g. S3).
2. **Bootstrap** – Runs Hudi’s bootstrap to create 8 tables:
   - **COW** and **MOR**, each with:
     - Non-partitioned: FULL_RECORD and METADATA_ONLY
     - Partitioned by `city`: FULL_RECORD and METADATA_ONLY
3. **Validate** – Queries each table from Spark, Trino, and Presto; checks that `_hoodie_commit_time` (metadata) and `ts` (data) are visible, and prints a summary table.

## Prerequisites

- **Python 3** with `pip`
- **Spark 3.x** (e.g. 3.5) with `spark-submit` on `PATH`
- **Hudi JARs** at the paths used by `bootstrap_hudi_tables.sh` (default: `/opt/hudi/`), or set `HUDI_UTILITIES_JAR` and `HUDI_SPARK_JAR`
- **Object storage** (e.g. S3) and a **Hive Metastore** for the bootstrap step
- **Trino** and **Presto** (optional) for the validation step; if unavailable, validation will report connection failures for those engines

## Configuration

- **`config.yaml`** – Spark app name/master, Trino and Presto connection (host, port, catalog, schema, user). Adjust for your environment.
- **Warehouse path** – Source and Hudi data paths are derived from `WAREHOUSE_BASE`. Default: `s3a://warehouse/`. Override with:
  - `generate_source_parquet.py`: set env `SOURCE_BASE_PATH` (or rely on default in script).
  - `bootstrap_hudi_tables.sh`: set env `WAREHOUSE_BASE` (e.g. `export WAREHOUSE_BASE=s3a://my-bucket/warehouse`).

## Quick start (end-to-end)

From the `hudi_bootstrap_demo` directory:

```bash
# Install Python dependencies (PySpark, Trino/Presto clients, PyYAML)
pip install -r requirements.txt

# Run all steps: generate source data → bootstrap all 8 tables → validate
bash run_demo_e2e.sh
```

This will:

1. Run `spark-submit generate_source_parquet.py` (writes Parquet under `SOURCE_BASE_PATH`).
2. Run `bash bootstrap_hudi_tables.sh all` (creates Hudi tables and syncs to Hive).
3. Run `spark-submit validate_hudi_tables.py` (Spark + Trino + Presto validation and summary table).

## Running steps individually

### 1. Generate source Parquet data

```bash
spark-submit generate_source_parquet.py
```

- Reads `config.yaml` for Spark settings.
- Writes non-partitioned Parquet to `.../source_data/source_parquet/` and partitioned (by `city`) to `.../source_data/source_partition_parquet/`.
- Validates row count before and after write. Override base path with `SOURCE_BASE_PATH` if needed.

### 2. Run Hudi bootstrap

```bash
./bootstrap_hudi_tables.sh [all|cow|mor]
```

- **`all`** (default) – Bootstrap all 8 tables (4 COW + 4 MOR).
- **`cow`** – Only the 4 COPY_ON_WRITE tables.
- **`mor`** – Only the 4 MERGE_ON_READ tables.

Optional environment variables (see `bootstrap_hudi_tables.sh` header):

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `WAREHOUSE_BASE` | `s3a://warehouse` | Base path for source and Hudi data |
| `HIVE_METASTORE_URIS` | `thrift://hive-metastore:9083` | Hive metastore for sync |
| `HIVE_SYNC_DB` | `bootstrap_db` | Hive database for synced tables |
| `HUDI_UTILITIES_JAR` | `/opt/hudi/hudi-utilities-slim-bundle_2.12-1.0.2.jar` | Hudi utilities JAR |
| `HUDI_SPARK_JAR` | `/opt/hudi/hudi-spark3.5-bundle_2.12-1.0.2.jar` | Hudi Spark bundle JAR |
| `SPARK_MASTER` | `local` | Spark master URL |

### 3. Validate Hudi reads

```bash
spark-submit validate_hudi_tables.py
```

- Reads `config.yaml` for Spark, Trino, and Presto.
- For each of the 8 tables, runs queries from Spark, Trino, and Presto.
- **Metadata visible** = at least one row with `_hoodie_commit_time IS NOT NULL`.
- **Data visible** = at least one row with `ts IS NOT NULL`.
- Prints a markdown summary table: Engine × Table Type × Partitioned × Bootstrap Mode × Hoodie Metadata Visible × Hoodie Data Visible.

## Files in this directory

| File | Purpose |
| ---- | ------- |
| `config.yaml` | Spark, Trino, and Presto settings |
| `generate_source_parquet.py` | Generate source Parquet data and validate by row count |
| `bootstrap_hudi_tables.sh` | Run Hudi bootstrap for COW/MOR tables (all or subset) |
| `run_demo_e2e.sh` | Run full demo: generate → bootstrap → validate |
| `validate_hudi_tables.py` | Validate Hudi table reads (Spark, Trino, Presto) and print summary table |
| `requirements.txt` | Python dependencies |

## Troubleshooting

- **Spark / Hudi not found** – Ensure `spark-submit` is on `PATH` and Hudi JAR paths in `bootstrap_hudi_tables.sh` (or `HUDI_UTILITIES_JAR` / `HUDI_SPARK_JAR`) are correct.
- **S3 / path errors** – Set `WAREHOUSE_BASE` (and `SOURCE_BASE_PATH` for generation) to a path your Spark and cluster can read/write (e.g. `s3a://bucket/prefix/`).
- **Hive sync failures** – Ensure Hive Metastore is reachable at `HIVE_METASTORE_URIS` and the database `HIVE_SYNC_DB` exists (or can be created).
- **Trino / Presto validation fails** – Ensure Trino and Presto are running and `config.yaml` host/port/catalog/schema match your setup. Validation will still run for Spark and report connection errors for the other engines.
