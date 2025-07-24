from airflow.sdk.definitions.asset import Asset
from airflow.decorators import dag, task
from datetime  import datetime,timedelta
import requests
from market_data_extract import market_data_extract

from market_data_transform import polygon_response_to_df
from market_data_load import load_market_data

# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    'market_etl',
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=True,
     max_active_runs=1,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["Data_engineering", "ETL", "Market_Data"],
)
def market_etl():
    # Example usage: symbol and date; API key is fetched from Airflow connection
    symbol = "AAPL"
    date = "2025-01-02"

    extract = market_data_extract(symbol=symbol, date=date)
    transform = polygon_response_to_df(extract)
    load = load_market_data(transform)
    extract >> transform >> load
# Instantiate the DAG
market_etl()
