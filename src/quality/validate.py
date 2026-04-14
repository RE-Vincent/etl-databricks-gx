import great_expectations as gx
import pandas as pd

def validate_all_layer(context: gx.DataContext, customers_df: pd.DataFrame, products_df: pd.DataFrame, orders_df: pd.DataFrame) -> bool:
    # Definimos qué DataFrame va con qué validación del Checkpoint
    batch_requests = [
        {
            "datasource_name": "my_spark_datasource",
            "data_asset_name": "customers_asset",
            "batch_data": customers_df,
        },
        {
            "datasource_name": "my_spark_datasource",
            "data_asset_name": "products_asset",
            "batch_data": products_df,
        },
        {
            "datasource_name": "my_spark_datasource",
            "data_asset_name": "orders_asset",
            "batch_data": orders_df,
        }
    ]

    # Ejecutamos el checkpoint pasando los datos "vivos"
    # Esto sobreescribe los assets del YAML con tus variables de Spark
    result = context.run_checkpoint(
        checkpoint_name="bronze_to_silver_check",
        validations=batch_requests
    )

    if not result.success:
        # GX ya generó los JSON y el HTML automáticamente por las "Actions"
        raise Exception("Data quality failed. Check Data Docs for details.")
    
    return True