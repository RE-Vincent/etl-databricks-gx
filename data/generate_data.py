import pandas as pd
import numpy as np

np.random.seed(42)

# -------------------
# CUSTOMERS
# -------------------
customers = pd.DataFrame({
    "customer_id": range(1, 101),
    "name": [f"cliente_{i}" for i in range(1, 101)],
    "age": np.random.randint(18, 70, 100),
    "country": np.random.choice(["PE", "CL", "MX"], 100)
})

# introducir errores
customers.loc[5, "age"] = None

# -------------------
# PRODUCTS
# -------------------
products = pd.DataFrame({
    "product_id": range(1, 51),
    "product_name": [f"producto_{i}" for i in range(1, 51)],
    "price": np.random.uniform(10, 500, 50)
})

# error
products.loc[10, "price"] = -100

# -------------------
# ORDERS
# -------------------
orders = pd.DataFrame({
    "order_id": range(1, 201),
    "customer_id": np.random.choice(customers["customer_id"], 200),
    "product_id": np.random.choice(products["product_id"], 200),
    "quantity": np.random.randint(1, 5, 200)
})

# error
orders.loc[3, "quantity"] = -1

# guardar CSV
customers.to_csv("customers.csv", index=False)
products.to_csv("products.csv", index=False)
orders.to_csv("orders.csv", index=False)

print("Datos generados!")