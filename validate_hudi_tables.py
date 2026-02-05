import argparse
import os
import sys
import yaml
from pyspark.sql import SparkSession
import trino
import prestodb

VALID_ENGINES = {"spark", "trino", "presto"}


# -------------------------------------------------------------------
# Load Config
# -------------------------------------------------------------------
with open("config.yaml") as f:
    config = yaml.safe_load(f)


# -------------------------------------------------------------------
# Scenario Matrix: (table_name, table_type, partitioned, bootstrap_mode)
# -------------------------------------------------------------------
SCENARIOS = [
    # COW
    ("trips_hudi_cow_bootstrap_fl", "COW", False, "FULL_RECORD"),
    ("trips_hudi_cow_bootstrap_mo", "COW", False, "METADATA_ONLY"),
    ("trips_hudi_cow_bootstrap_partitioned_fl", "COW", True, "FULL_RECORD"),
    ("trips_hudi_cow_bootstrap_partitioned_mo", "COW", True, "METADATA_ONLY"),
    # MOR
    ("trips_hudi_mor_bootstrap_fl", "MOR", False, "FULL_RECORD"),
    ("trips_hudi_mor_bootstrap_mo", "MOR", False, "METADATA_ONLY"),
    ("trips_hudi_mor_bootstrap_partitioned_fl", "MOR", True, "FULL_RECORD"),
    ("trips_hudi_mor_bootstrap_partitioned_mo", "MOR", True, "METADATA_ONLY"),
]


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
    print(f"Spark connection config: {config['spark']}")
    spark = (
        SparkSession.builder
        .appName(config["spark"]["app_name"])
        .master(config["spark"]["master"])
        .config("spark.sql.extensions",
                "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .getOrCreate()
    )

    results = []
    print("\n===== Spark Validation =====\n")

    for table, table_type, partitioned, mode in SCENARIOS:
        metadata_visible = False
        data_visible = False
        notes = ""
        try:
            print(f"[Spark] Validating {table}")
            full_table = f"bootstrap_db.{table}"

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
    print(f"Trino connection config: {config['trino']}")
    results = []

    try:
        conn = trino.dbapi.connect(
            host=config["trino"]["host"],
            port=config["trino"]["port"],
            user=config["trino"]["user"],
            catalog=config["trino"]["catalog"],
            schema=config["trino"]["schema"],
        )
        cur = conn.cursor()
    except Exception as e:
        conn_err = str(e)
        print(f"Trino connection failed: {e}")
        for table, table_type, partitioned, mode in SCENARIOS:
            results.append({
                "engine": "Trino",
                "table": table,
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
            print(f"[Trino] Validating {table}")
            # Metadata visible: _hoodie_commit_time is not null
            try:
                cur.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE _hoodie_commit_time IS NOT NULL"
                )
                metadata_visible = (cur.fetchone()[0] or 0) > 0
            except Exception:
                metadata_visible = False
            # Data visible: ts column is not null
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE ts IS NOT NULL")
                data_visible = (cur.fetchone()[0] or 0) > 0
            except Exception:
                data_visible = False
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            cur.execute(
                f"SELECT city, SUM(fare) FROM {table} GROUP BY city"
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
            "table": table,
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
    print(f"Presto connection config: {config['presto']}")
    results = []

    try:
        conn = prestodb.dbapi.connect(
            host=config["presto"]["host"],
            port=config["presto"]["port"],
            user=config["presto"]["user"],
            catalog=config["presto"]["catalog"],
            schema=config["presto"]["schema"],
        )
        cur = conn.cursor()
    except Exception as e:
        conn_err = str(e)
        print(f"Presto connection failed: {e}")
        for table, table_type, partitioned, mode in SCENARIOS:
            results.append({
                "engine": "Presto",
                "table": table,
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
            print(f"[Presto] Validating {table}")
            # Metadata visible: _hoodie_commit_time is not null
            try:
                cur.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE _hoodie_commit_time IS NOT NULL"
                )
                metadata_visible = (cur.fetchone()[0] or 0) > 0
            except Exception:
                metadata_visible = False
            # Data visible: ts column is not null
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE ts IS NOT NULL")
                data_visible = (cur.fetchone()[0] or 0) > 0
            except Exception:
                data_visible = False
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            cur.execute(
                f"SELECT city, SUM(fare) FROM {table} GROUP BY city"
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
            "table": table,
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
# CLI: parse --engines
# -------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate Hudi bootstrap tables with Spark, Trino, and/or Presto.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  spark-submit validate_hudi_tables.py                    # all engines (default)
  spark-submit validate_hudi_tables.py --engines spark
  spark-submit validate_hudi_tables.py --engines trino,presto
  spark-submit validate_hudi_tables.py -e spark -e presto
""",
    )
    parser.add_argument(
        "--engines", "-e",
        action="append",
        default=None,
        metavar="ENGINE",
        help="Engine(s) to use: spark, trino, presto. Can be repeated or comma-separated. Default: all.",
    )
    args = parser.parse_args()

    if args.engines is None:
        return sorted(VALID_ENGINES)  # default: all

    # Flatten: -e spark,trino -e presto -> ['spark','trino','presto']
    chosen = set()
    for part in args.engines:
        for name in (s.strip().lower() for s in part.split(",") if s.strip()):
            if name not in VALID_ENGINES:
                parser.error(f"Invalid engine: {name}. Choose from: {', '.join(sorted(VALID_ENGINES))}")
            chosen.add(name)
    if not chosen:
        parser.error("At least one engine must be selected.")
    return sorted(chosen)


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
if __name__ == "__main__":
    engines = parse_args()
    hudi_version = os.environ.get("HUDI_VERSION", "")
    all_results = []
    if "spark" in engines:
        all_results.extend(validate_with_spark())
    if "trino" in engines:
        all_results.extend(validate_with_trino())
    if "presto" in engines:
        all_results.extend(validate_with_presto())

    if all_results:
        print(f"Validating with engine(s): {', '.join(engines)} and HUDI_VERSION: {hudi_version}")
        print("\n" + "=" * 80)
        print(f"VALIDATION SUMMARY: (HUDI_VERSION: {hudi_version})")
        print("=" * 80)
        print_results_table(all_results)
    else:
        print("No validation results (no engines selected).")
        sys.exit(1)
