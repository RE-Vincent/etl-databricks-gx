from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.getOrCreate()

orders = spark.table("silver.orders")
products = spark.table("silver.products")

# join + agregación
df = orders.join(products, "product_id")

gold = df.groupBy("product_name").agg(
    sum("quantity").alias("total_quantity")
)

gold.write.format("delta").mode("overwrite").saveAsTable("gold.sales")