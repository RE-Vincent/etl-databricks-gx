import os
from pyspark.sql import SparkSession


def is_databricks():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str = "etl-project") -> SparkSession:

    builder = SparkSession.builder.appName(app_name)

    if not is_databricks():
        # Configs necesarias solo en local
        builder = (
            builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )

    spark = builder.getOrCreate()

    return spark