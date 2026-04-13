from src.utils.spark import get_spark

spark = get_spark("bronze-ingest")

# cargar CSV
customers = spark.read.csv("customers.csv", header=True, inferSchema=True)
products = spark.read.csv("products.csv", header=True, inferSchema=True)
orders = spark.read.csv("orders.csv", header=True, inferSchema=True)

# guardar en bronze
customers.write.format("delta").mode("overwrite").saveAsTable("bronze.customers")
products.write.format("delta").mode("overwrite").saveAsTable("bronze.products")
orders.write.format("delta").mode("overwrite").saveAsTable("bronze.orders")