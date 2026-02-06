import logging
import argparse
import os
import sys
import trino
import prestodb
from yaml_util import load_config
from pyspark.sql import SparkSession

VALID_ENGINES = {"spark", "trino", "presto"}

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

current_file_path = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(current_file_path, "config.yaml")

# Load config
config = load_config(config_path)

SCENARIOS = []

base_table_name = config['common']['base_table_name']
for table_type in ["COW", "MOR"]:
    for partitioned in [False, True]:
        for bootstrap_mode in ["FULL_RECORD", "METADATA_ONLY"]:
            table_name = f"{base_table_name}_{table_type}_bootstrap_{'partitioned' if partitioned else 'non_partitioned'}_{bootstrap_mode}"
            SCENARIOS.append((table_name, table_type, partitioned, bootstrap_mode))


def yes_no(value):
    return "✅ Yes" if value else "❌ No"


def _notes_for_display(notes):
    """Shorten and escape notes for markdown table (no newlines, no pipe)."""
    if not notes:
        return "-"
    one_line = str(notes).replace("\n", " ").replace("|", " ").strip()
    return (one_line[:120] + "…") if len(one_line) > 120 else one_line


def print_results_table(results):
    """Print validation results as a markdown table."""
    headers = [
        "Engine",
        "Table",
        "Table Type",
        "Table Partitioned",
        "Bootstrap Mode",
        "Hoodie Metadata Visible",
        "Hoodie Data Visible",
        "Notes",
    ]
    sep = "| " + " | ".join(["------"] * len(headers)) + " |"
    header_row = "| " + " | ".join(headers) + " |"
    lines = [header_row, sep]
    for r in results:
        row = [
            r["engine"],
            r["table"],
            r["table_type"],
            yes_no(r["partitioned"]),
            r["bootstrap_mode"],
            yes_no(r["metadata_visible"]),
            yes_no(r["data_visible"]),
            _notes_for_display(r.get("notes")),
        ]
        lines.append("| " + " | ".join(row) + " |")
    table = "\n".join(lines)
    print("\n" + table + "\n")
    return table


# -------------------------------------------------------------------
# Spark Validation
# -------------------------------------------------------------------
def validate_with_spark():
    spark_config = config["spark"]
    print(f"Spark connection config: {spark_config}")
    spark = (
        SparkSession.builder
        .appName(spark_config["app_name"])
        .master(spark_config["master"])
        .config("spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )

    results = []
    print("\n===== Spark Validation =====\n")
    database = spark_config["db_name"]
    for table, table_type, partitioned, mode in SCENARIOS:
        metadata_visible = False
        data_visible = False
        notes = ""
        try:

            full_table = f"{database}.{table}"
            print(f"[Spark] Validating {full_table}")
            # Metadata visible: _hoodie_commit_time is not null
            try:
                meta_df = spark.sql(
                    f"SELECT COUNT(*) AS c FROM {full_table} WHERE _hoodie_commit_time IS NOT NULL"
                )
                metadata_visible = meta_df.collect()[0]["c"] > 0
            except Exception:
                metadata_visible = False

            # Data visible: ts column is not null
            try:
                data_df = spark.sql(
                    f"SELECT COUNT(*) AS c FROM {full_table} WHERE ts IS NOT NULL"
                )
                data_visible = data_df.collect()[0]["c"] > 0
            except Exception:
                data_visible = False

            count = spark.sql(f"SELECT COUNT(*) FROM {full_table}").collect()[0][0]
            print(f"  Rows                : {count}")
            print(f"  Metadata Visible    : {'YES' if metadata_visible else 'NO'} (_hoodie_commit_time IS NOT NULL)")
            print(f"  Data Visible        : {'YES' if data_visible else 'NO'} (ts IS NOT NULL)")
            agg_df = spark.sql(
                f"SELECT city, SUM(fare) AS total_fare "
                f"FROM {full_table} GROUP BY city"
            )
            agg_df.show(truncate=False)
        except Exception as e:
            notes = str(e)
            print(f"  ❌ FAILED: {e}")

        results.append({
            "engine": "Spark",
            "table": table,
            "table_type": table_type,
            "partitioned": partitioned,
            "bootstrap_mode": mode,
            "metadata_visible": metadata_visible,
            "data_visible": data_visible,
            "notes": notes,
        })

    spark.stop()
    return results


# -------------------------------------------------------------------
# Trino Validation
# -------------------------------------------------------------------
def validate_with_trino():
    print("\n===== Trino Validation =====\n")
    trino_config = config["trino"]
    print(f"Trino connection config: {trino_config}")
    results = []
    db_name = trino_config["schema"]
    catalog = trino_config["catalog"]
    try:
        conn = trino.dbapi.connect(
            host=trino_config["host"],
            port=trino_config["port"],
            user=trino_config["user"],
            catalog=catalog,
            schema=db_name,
        )
        cur = conn.cursor()
    except Exception as e:
        conn_err = str(e)
        print(f"Trino connection failed: {e}")
        for table, table_type, partitioned, mode in SCENARIOS:
            full_table = f"{db_name}.{table}"
            results.append({
                "engine": "Trino",
                "table": full_table,
                "table_type": table_type,
                "partitioned": partitioned,
                "bootstrap_mode": mode,
                "metadata_visible": False,
                "data_visible": False,
                "notes": f"Connection failed: {conn_err}",
            })
        return results

    for table, table_type, partitioned, mode in SCENARIOS:
        metadata_visible = False
        data_visible = False
        notes = ""
        try:
            full_table = f"{db_name}.{table}"
            print(f"[Trino] Validating {full_table}")
            # Metadata visible: _hoodie_commit_time is not null
            try:
                cur.execute(
                    f"SELECT COUNT(*) FROM {full_table} WHERE _hoodie_commit_time IS NOT NULL"
                )
                metadata_visible = (cur.fetchone()[0] or 0) > 0
            except Exception:
                metadata_visible = False
            # Data visible: ts column is not null
            try:
                cur.execute(f"SELECT COUNT(*) FROM {full_table} WHERE ts IS NOT NULL")
                data_visible = (cur.fetchone()[0] or 0) > 0
            except Exception:
                data_visible = False
            cur.execute(f"SELECT COUNT(*) FROM {full_table}")
            count = cur.fetchone()[0]
            cur.execute(
                f"SELECT city, SUM(fare) FROM {full_table} GROUP BY city"
            )
            rows = cur.fetchall()
            print(f"  Rows                : {count}")
            print(f"  Metadata Visible    : {'YES' if metadata_visible else 'NO'} (_hoodie_commit_time IS NOT NULL)")
            print(f"  Data Visible        : {'YES' if data_visible else 'NO'} (ts IS NOT NULL)")
            print(f"  Aggregates          : {rows}")
        except Exception as e:
            notes = str(e)
            print(f"  ❌ FAILED: {e}")

        results.append({
            "engine": "Trino",
            "table": full_table,
            "table_type": table_type,
            "partitioned": partitioned,
            "bootstrap_mode": mode,
            "metadata_visible": metadata_visible,
            "data_visible": data_visible,
            "notes": notes,
        })

    cur.close()
    conn.close()
    return results


# -------------------------------------------------------------------
# Presto Validation
# -------------------------------------------------------------------
def validate_with_presto():
    print("\n===== Presto Validation =====\n")
    presto_config = config["presto"]
    print(f"Presto connection config: {presto_config}")
    results = []
    db_name = presto_config["schema"]
    catalog = presto_config["catalog"]
    try:
        conn = prestodb.dbapi.connect(
            host=presto_config["host"],
            port=presto_config["port"],
            user=presto_config["user"],
            catalog=catalog,
            schema=db_name,
        )
        cur = conn.cursor()
    except Exception as e:
        conn_err = str(e)
        print(f"Presto connection failed: {e}")
        for table, table_type, partitioned, mode in SCENARIOS:
            full_table = f"{db_name}.{table}"
            results.append({
                "engine": "Presto",
                "table": full_table,
                "table_type": table_type,
                "partitioned": partitioned,
                "bootstrap_mode": mode,
                "metadata_visible": False,
                "data_visible": False,
                "notes": f"Connection failed: {conn_err}",
            })
        return results

    for table, table_type, partitioned, mode in SCENARIOS:
        metadata_visible = False
        data_visible = False
        notes = ""
        try:
            full_table = f"{db_name}.{table}"
            print(f"[Presto] Validating {full_table}")
            # Metadata visible: _hoodie_commit_time is not null
            try:
                cur.execute(
                    f"SELECT COUNT(*) FROM {full_table} WHERE _hoodie_commit_time IS NOT NULL"
                )
                metadata_visible = (cur.fetchone()[0] or 0) > 0
            except Exception:
                metadata_visible = False
            # Data visible: ts column is not null
            try:
                cur.execute(f"SELECT COUNT(*) FROM {full_table} WHERE ts IS NOT NULL")
                data_visible = (cur.fetchone()[0] or 0) > 0
            except Exception:
                data_visible = False
            cur.execute(f"SELECT COUNT(*) FROM {full_table}")
            count = cur.fetchone()[0]
            cur.execute(
                f"SELECT city, SUM(fare) FROM {full_table} GROUP BY city"
            )
            rows = cur.fetchall()
            print(f"  Rows                : {count}")
            print(f"  Metadata Visible    : {'YES' if metadata_visible else 'NO'} (_hoodie_commit_time IS NOT NULL)")
            print(f"  Data Visible        : {'YES' if data_visible else 'NO'} (ts IS NOT NULL)")
            print(f"  Aggregates          : {rows}")
        except Exception as e:
            notes = str(e)
            print(f"  ❌ FAILED: {e}")

        results.append({
            "engine": "Presto",
            "table": full_table,
            "table_type": table_type,
            "partitioned": partitioned,
            "bootstrap_mode": mode,
            "metadata_visible": metadata_visible,
            "data_visible": data_visible,
            "notes": notes,
        })

    cur.close()
    conn.close()
    return results


if __name__ == "__main__":
    engines = [engine for engine in VALID_ENGINES if config[engine]["enabled"] == True]
    if not engines:
        print("No engines selected. Please enable at least one engine in config.yaml.")
        sys.exit(1)
    hudi_version = os.environ.get("HUDI_VERSION", "")
    all_results = []
    if "spark" in engines:
        all_results.extend(validate_with_spark())
    if "trino" in engines:
        all_results.extend(validate_with_trino())
    if "presto" in engines:
        all_results.extend(validate_with_presto())

    print(f"Validating with engine(s): {', '.join(engines)} and HUDI_VERSION: {hudi_version}")
    print("\n" + "=" * 80)
    print(f"VALIDATION SUMMARY: (HUDI_VERSION: {hudi_version})")
    print("=" * 80)
    print_results_table(all_results)
