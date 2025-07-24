from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
from market_data_extract import market_data_extract
from market_data_transform import polygon_response_to_df
from market_data_load import load_market_data

# Define the basic parameters of the DAG for multiple symbols
@dag(
    dag_id='market_etl_multi_symbol',
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3, 
        "retry_delay": timedelta(minutes=2),
        "depends_on_past": False,
    },
    tags=["Data_engineering", "ETL", "Market_Data", "Multi_Symbol"],
    description="Daily market data ETL pipeline for multiple stock symbols",
)
def market_etl_multi_symbol():
    """
    Daily ETL pipeline to extract, transform, and load market data for multiple symbols.
    Each DAG run processes one day of data for all specified symbols.
    """
    
    # Define multiple symbols to process
    symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]
    
    @task()
    def process_multiple_symbols(symbols_list: list, date: str):
        """
        Process multiple symbols for a given date and combine into single DataFrame
        """
        all_data = []
        
        for symbol in symbols_list:
            print(f"Processing {symbol} for date {date}")
            
            # Extract data for this symbol
            response = market_data_extract.function(symbol=symbol, date=date)
            
            if response:
                # Transform to DataFrame
                df = polygon_response_to_df.function(response)
                if not df.empty:
                    all_data.append(df)
                    print(f"Successfully processed {symbol}")
                else:
                    print(f"No data returned for {symbol}")
            else:
                print(f"Failed to extract data for {symbol}")
        
        if all_data:
            # Combine all DataFrames
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"Combined data for {len(all_data)} symbols, total rows: {len(combined_df)}")
            return combined_df
        else:
            print("No data to process for any symbol")
            return pd.DataFrame()
    
    # Process all symbols for the execution date
    combined_data = process_multiple_symbols(symbols, "{{ ds }}")
    
    # Load combined data to PostgreSQL
    load = load_market_data(combined_data)
    
    # Define task dependencies
    combined_data >> load

# Instantiate the DAG
market_etl_multi_symbol()
