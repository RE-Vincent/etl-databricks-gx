from pyspark.sql import SparkSession
from src.quality.validate import validate_dataframe

spark = SparkSession.builder.getOrCreate()

# leer bronze
customers = spark.table("bronze.customers")
products = spark.table("bronze.products")
orders = spark.table("bronze.orders")

# validar
validate_dataframe(customers, "customers_suite.json")
validate_dataframe(products, "products_suite.json")
validate_dataframe(orders, "orders_suite.json")

# limpieza simple
customers_clean = customers.dropna()
products_clean = products.filter("price > 0")
orders_clean = orders.filter("quantity > 0")

# guardar silver
customers_clean.write.format("delta").mode("overwrite").saveAsTable("silver.customers")
products_clean.write.format("delta").mode("overwrite").saveAsTable("silver.products")
orders_clean.write.format("delta").mode("overwrite").saveAsTable("silver.orders")