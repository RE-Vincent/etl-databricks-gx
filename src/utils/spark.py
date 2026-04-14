import os
from pyspark.sql import SparkSession

def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

def get_spark(app_name: str = "etl-project") -> SparkSession:
    if is_databricks():
        # En Serverless, 'getOrCreate' de la sesión estándar 
        # suele ser suficiente, pero el secreto es NO pasar configuraciones
        # que puedan chocar con el túnel ya establecido.
        try:
            # Intentamos capturar la sesión de Databricks Connect
            from databricks.connect import DatabricksSession
            return DatabricksSession.builder.getOrCreate()
        except ImportError:
            return SparkSession.builder.getOrCreate()
    else:
        # Configuración para desarrollo local (fuera de Databricks)
        return (SparkSession.builder
                .appName(app_name)
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate())