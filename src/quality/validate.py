import great_expectations as gx
from great_expectations.dataset import SparkDFDataset

def validate_dataframe(df, suite_path):
    import json

    with open(suite_path) as f:
        suite = json.load(f)

    gx_df = SparkDFDataset(df)

    results = []

    for exp in suite["expectations"]:
        func = getattr(gx_df, exp["expectation_type"])
        result = func(**exp["kwargs"])
        results.append(result["success"])

    success = all(results)

    if not success:
        raise Exception(f"Data quality failed: {suite_path}")

    return True