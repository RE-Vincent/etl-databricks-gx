from src.utils.spark import get_spark
import great_expectations as gx
from src.quality.validate import validate_all_layers
import os

spark = get_spark("silver-ingest")

# 1. Configurar GX (Apunta a la carpeta de tu repo)
repo_root = os.path.abspath(os.path.join(os.getcwd()))
context = gx.get_context(project_root_dir=f"{repo_root}/gx")

# 2. Leer bronze
customers = spark.table("bronze.customers")
products = spark.table("bronze.products")
orders = spark.table("bronze.orders")

# 3. VALIDAR (In-memory, antes de limpiar)
# Pasamos el contexto y los 3 dataframes
validate_all_layers(context, customers, products, orders)

# 4. Limpieza (Solo llegamos aquí si la validación pasó)
customers_clean = customers.dropna()
products_clean = products.filter("price > 0")
orders_clean = orders.filter("quantity > 0")

# 5. Guardar silver
# Crear la base de datos si no existe (Para el esquema Silver)
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
customers_clean.write.format("delta").mode("overwrite").saveAsTable("silver.customers")
products_clean.write.format("delta").mode("overwrite").saveAsTable("silver.products")
orders_clean.write.format("delta").mode("overwrite").saveAsTable("silver.orders")
