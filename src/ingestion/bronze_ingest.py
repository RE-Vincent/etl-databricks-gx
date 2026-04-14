from src.utils.spark import get_spark

spark = get_spark("bronze-ingest")

landing_path = "/dbfs/FileStore/landing_zone/"

# cargar CSV
customers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{landing_path}customers.csv")
products = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{landing_path}products.csv")
orders = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{landing_path}orders.csv")

# guardar en bronze
# Crear la base de datos si no existe (Para el esquema Bronze)
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

customers.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.customers")
products.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.products")
orders.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.orders")