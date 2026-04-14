from src.utils.spark import get_spark

spark = get_spark("bronze-ingest")

# 1. Corregir la ruta al Volumen (la que creamos en el paso anterior)
landing_path = "/Volumes/workspace/default/landing_zone/"
print(f"Versión de Spark: {spark.version}")

# 2. Cargar CSV usando la ruta del Volumen
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

# 3. Guardar en Bronze (Delta)
print("Guardando tablas en esquema Bronze...")

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

print("¡Ingesta a Bronze completada!")