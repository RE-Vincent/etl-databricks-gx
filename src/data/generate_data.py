import pandas as pd
import numpy as np
import os
from src.utils.spark import get_spark

# Inicializar Spark
spark = get_spark("generate-data")

# 1. Definir la ruta del Volumen (Ruta FUSE)
# La estructura en Databricks para volúmenes es: /Volumes/<catalog>/<schema>/<volume_name>/
landing_path = "/Volumes/workspace/default/landing_zone/"

# Asegurarnos de que el directorio existe dentro del volumen
if not os.path.exists(landing_path):
    os.makedirs(landing_path, exist_ok=True)

np.random.seed(42)

# -------------------
# GENERACIÓN DE DATOS (Tu lógica original)
# -------------------
customers = pd.DataFrame({
    "customer_id": range(1, 101),
    "name": [f"cliente_{i}" for i in range(1, 101)],
    "age": np.random.randint(18, 70, 100),
    "country": np.random.choice(["PE", "CL", "MX"], 100),
    "phone_number": [f"+51{np.random.randint(900000000, 999999999)}" for _ in range(100)],
    "signup_date": pd.to_datetime("2023-01-01") + pd.to_timedelta(np.random.randint(0, 365, 100), unit='D')
})

# Introducir errores
customers.loc[5, "age"] = 100
customers.loc[35, "age"] = 10
customers.loc[10, "phone_number"] = "12345"
customers.loc[100, "phone_number"] = None
customers.loc[15, "signup_date"] = "2027-01-01"
customers = pd.concat([customers, customers.iloc[0:5]], ignore_index=True)

products = pd.DataFrame({
    "product_id": range(1, 51),
    "product_name": [f"producto_{i}" for i in range(1, 51)],
    "price": np.random.uniform(10, 500, 50),
    "discount": np.random.uniform(0, 0.5, 50),
    "product_rating": np.random.uniform(1, 5, 50)
})
products.loc[10, "price"] = -100

orders = pd.DataFrame({
    "order_id": range(1, 201),
    "customer_id": np.random.choice(customers["customer_id"], 200),
    "product_id": np.random.choice(products["product_id"], 200),
    "quantity": np.random.randint(1, 5, 200),
    "order_date": pd.to_datetime("2024-01-01") + pd.to_timedelta(np.random.randint(0, 30, 200), unit='D')
})
orders.loc[3, "quantity"] = -1
orders.loc[3, "product_id"] = None

# -------------------
# GUARDAR EN EL VOLUMEN
# -------------------
files = {
    "customers.csv": customers,
    "products.csv": products,
    "orders.csv": orders
}

print(f"Escribiendo archivos en el volumen: {landing_path}")

for name, df in files.items():
    full_path = os.path.join(landing_path, name)
    # Pandas escribe directamente al volumen gracias al soporte FUSE de Databricks
    df.to_csv(full_path, index=False)
    print(f"Creado: {name}")

print("\n¡Datos aterrizados exitosamente!")