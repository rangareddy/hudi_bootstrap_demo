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

def path_exists(spark, file_path):
    """Check if a file path exists."""
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        p = spark._jvm.org.apache.hadoop.fs.Path(file_path)
        return fs.exists(p)
    except Exception as e:
        logger.error("Error checking if path %s exists: %s", file_path, e)
        return False

def generate_source_data(spark: SparkSession, base_path: str):
    """Build source DataFrame, validate it, and write non-partitioned and partitioned Parquet."""
    data_path = os.path.join(base_path, "source_data")

    source_table = "source_parquet"
    source_table_path = os.path.join(data_path, source_table)

    source_partition_table = "source_partition_parquet"
    source_partition_table_path = os.path.join(data_path, source_partition_table)

    is_source_table_path_exists = path_exists(spark, source_table_path)
    is_source_partition_table_path_exists = path_exists(spark, source_partition_table_path)

    if not is_source_table_path_exists or not is_source_partition_table_path_exists:
        df = spark.createDataFrame(SOURCE_DATA).toDF(*EXPECTED_COLUMNS)
        df.show()
        if not is_source_table_path_exists:
            logger.info("Source table path %s does not exist, creating it", source_table_path)
            df.repartition(1).write.mode("overwrite").save(source_table_path)
        if not is_source_partition_table_path_exists:
            logger.info("Source partition table path %s does not exist, creating it", source_partition_table_path)
            df.repartition(1).write.partitionBy("city").mode("overwrite").save(source_partition_table_path)
        logger.info("Source data generation completed successfully.")
    else:
        logger.info("Source data already generated, skipping generation.")


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
