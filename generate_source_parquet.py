"""
Generate source Parquet data for Hudi bootstrap demo.
Creates non-partitioned and city-partitioned datasets under the configured base path.
"""

import logging
import os
import sys
import yaml
from pyspark.sql import SparkSession

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


# -------------------------------------------------------------------
# Load Config
# -------------------------------------------------------------------
def load_config(config_path: str = "config.yaml"):
    with open(config_path) as f:
        return yaml.safe_load(f)


# -------------------------------------------------------------------
# SparkSession
# -------------------------------------------------------------------
def create_spark_session(config: dict) -> SparkSession:
    """Create and return a SparkSession using config."""
    spark = (
        SparkSession.builder
        .appName(config["spark"]["app_name"] + "-generate-source")
        .master(config["spark"]["master"])
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created: appName=%s", spark.sparkContext.appName)
    return spark


# -------------------------------------------------------------------
# Schema and source data
# -------------------------------------------------------------------
EXPECTED_COLUMNS = ["ts", "uuid", "rider", "driver", "fare", "city"]

SOURCE_DATA = [
    ("2025-08-10 08:15:30", "uuid-001", "rider-A", "driver-X", 18.50, "new_york"),
    ("2025-08-10 09:22:10", "uuid-002", "rider-B", "driver-Y", 22.75, "san_francisco"),
    ("2025-08-10 10:05:45", "uuid-003", "rider-C", "driver-Z", 14.60, "chicago"),
    ("2025-08-10 11:40:00", "uuid-004", "rider-D", "driver-W", 31.90, "new_york"),
    ("2025-08-10 12:55:15", "uuid-005", "rider-E", "driver-V", 25.10, "san_francisco"),
    ("2025-08-10 13:20:35", "uuid-006", "rider-F", "driver-U", 19.80, "chicago"),
    ("2025-08-10 14:10:05", "uuid-007", "rider-G", "driver-T", 28.45, "san_francisco"),
    ("2025-08-10 15:00:20", "uuid-008", "rider-H", "driver-S", 16.25, "new_york"),
    ("2025-08-10 15:45:50", "uuid-009", "rider-I", "driver-R", 24.35, "chicago"),
    ("2025-08-10 16:30:00", "uuid-010", "rider-J", "driver-Q", 20.00, "new_york"),
]


def validate_by_count(df, expected_count: int, tag: str = "source") -> bool:
    """
    Validate DataFrame by row count only.
    Returns True if df.count() == expected_count, False otherwise.
    """
    actual_count = df.count()
    if actual_count != expected_count:
        logger.error(
            "[%s] Count validation failed: expected %d, got %d",
            tag, expected_count, actual_count,
        )
        return False
    logger.info("[%s] Count validation passed: %d rows", tag, actual_count)
    return True


def generate_source_data(spark: SparkSession, base_path: str):
    """Build source DataFrame, validate it, and write non-partitioned and partitioned Parquet."""
    data_path = os.path.join(base_path, "source_data")

    expected_count = len(SOURCE_DATA)
    # Create DataFrame
    df = spark.createDataFrame(SOURCE_DATA).toDF(*EXPECTED_COLUMNS)
    logger.info("Created source DataFrame with %d rows", df.count())

    # Validation by count
    if not validate_by_count(df, expected_count, "source"):
        logger.error("Data validation failed; aborting write.")
        sys.exit(1)
    df.show()

    # Non-partitioned
    source_table = "source_parquet"
    source_table_path = os.path.join(data_path, source_table)
    logger.info("Writing non-partitioned Parquet to %s", source_table_path)
    df.repartition(1).write.mode("overwrite").save(source_table_path)

    # Validate written non-partitioned data by count (read back)
    read_back = spark.read.parquet(source_table_path)
    if not validate_by_count(read_back, expected_count, "source_parquet (read-back)"):
        logger.error("Read-back validation failed for %s", source_table_path)
        sys.exit(1)
    logger.info("Read-back validation passed for %s", source_table_path)

    # Partitioned by city
    source_partition_table = "source_partition_parquet"
    source_partition_table_path = os.path.join(data_path, source_partition_table)
    logger.info("Writing partitioned Parquet to %s (partitionBy city)", source_partition_table_path)
    df.repartition(1).write.partitionBy("city").mode("overwrite").save(source_partition_table_path)

    read_back_p = spark.read.parquet(source_partition_table_path)
    if not validate_by_count(read_back_p, expected_count, "source_partition_parquet (read-back)"):
        logger.error("Read-back validation failed for %s", source_partition_table_path)
        sys.exit(1)
    logger.info("Read-back validation passed for %s", source_partition_table_path)

    logger.info("Source data generation completed successfully.")


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
if __name__ == "__main__":
    hudi_version = os.environ.get("HUDI_VERSION", "")
    if hudi_version:
        logger.info("HUDI_VERSION (from env): %s", hudi_version)
    config = load_config()
    spark = create_spark_session(config)
    try:
        base_path = config.get("common", {}).get("base_path", "s3a://warehouse/")
        generate_source_data(spark, base_path)
    finally:
        spark.stop()
        logger.info("SparkSession stopped.")
