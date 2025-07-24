
from airflow.decorators import task
import pandas as pd

@task()
def polygon_response_to_df(response_json: dict) -> pd.DataFrame:
    """
    Transforms the Polygon API response JSON into a pandas DataFrame.
    Ensures all expected columns are present, filling missing ones with None.
    Args:
        response_json (dict): The JSON response from the Polygon API.
    Returns:
        pd.DataFrame: DataFrame containing the market data.
    """
    expected_columns = [
        'symbol', 'from', 'open', 'high', 'low', 'close', 'afterHours', 'preMarket', 'volume', 'status'
    ]
    if not response_json or not isinstance(response_json, dict):
        return pd.DataFrame(columns=expected_columns)

    # Build row with all expected columns, fill missing with None
    row = {col: response_json.get(col, None) for col in expected_columns}
    return pd.DataFrame([row], columns=expected_columns)
