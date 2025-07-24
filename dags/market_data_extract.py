from airflow.decorators import task
import requests

from airflow.hooks.base import BaseHook

@task()
def market_data_extract(symbol: str, date: str, connection_id: str = "polygon_api"):
    """
    Extracts market data for a given symbol and date from the Polygon API.
    Args:
        symbol (str): The stock symbol to fetch data for (e.g., 'AAPL').
        date (str): The date for which to fetch data (YYYY-MM-DD).
        connection_id (str): Airflow connection ID where the API key is stored in the 'extra' field.
    Returns:
        dict: The JSON response from the API if successful, else None.
    """
    # Fetch API key from Airflow connection
    conn = BaseHook.get_connection(connection_id)
    api_key = None
    if conn.extra:
        import json
        try:
            extra = json.loads(conn.extra)
            api_key = extra.get("api_key")
        except Exception as e:
            print(f"Error parsing connection extra: {e}")
    if not api_key:
        print("API key not found in Airflow connection.")
        return None

    url = f"https://api.polygon.io/v1/open-close/{symbol}/{date}?adjusted=true&apiKey={api_key}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from Polygon API: {e}")
        return None
