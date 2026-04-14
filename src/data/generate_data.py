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
    "country": np.random.choice(["PE", "CL", "MX"], 100),
    "phone_number": [f"+51{np.random.randint(900000000, 999999999)}" for _ in range(100)],
    "signup_date": pd.to_datetime("2023-01-01") + pd.to_timedelta(np.random.randint(0, 365, 100), unit='D')
})

# introducir errores
customers.loc[5, "age"] = 100
customers.loc[35, "age"] = 10
customers.loc[10, "phone_number"] = "12345"
customers.loc[100, "phone_number"] = None
customers.loc[15, "signup_date"] = "2027-01-01"
customers = pd.concat([customers, customers.iloc[0:5]], ignore_index=True)  # duplicar primeros 5 clientes

# -------------------
# PRODUCTS
# -------------------
products = pd.DataFrame({
    "product_id": range(1, 51),
    "product_name": [f"producto_{i}" for i in range(1, 51)],
    "price": np.random.uniform(10, 500, 50),
    "discount": np.random.uniform(0, 0.5, 50),
    "product_rating": np.random.uniform(1, 5, 50)
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
    "quantity": np.random.randint(1, 5, 200),
    "order_date": pd.to_datetime("2024-01-01") + pd.to_timedelta(np.random.randint(0, 30, 200), unit='D')
})

# error
orders.loc[3, "quantity"] = -1
orders.loc[3, "product_id"] = None

# guardar CSV
landing_path = "/dbfs/FileStore/landing_zone/"


customers.to_csv(f"{landing_path}customers.csv", index=False)
products.to_csv(f"{landing_path}products.csv", index=False)
orders.to_csv(f"{landing_path}orders.csv", index=False)

print("Datos generados!")