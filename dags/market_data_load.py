import pandas as pd
from airflow.decorators import task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

@task()
def load_market_data(df: pd.DataFrame, table_name: str = "market_data1", connection_id: str = "polygon_api_database_conn"):
    """
    Loads a pandas DataFrame into a SQLite database table using Airflow's SqliteHook.
    Creates the table if it doesn't exist, and appends data if it does.
    Args:
        df (pd.DataFrame): DataFrame to load.
        table_name (str): Name of the table to load data into.
        connection_id (str): Airflow connection ID for the SQLite database.
    """
    if df is None or df.empty:
        print("No data to load.")
        return
    # Rename 'from' column to 'from_date' if present
    if 'from' in df.columns:
        df = df.rename(columns={'from': 'from_date'})
    hook = SqliteHook(sqlite_conn_id=connection_id)
    columns_with_types = ', '.join([f'"{col}" TEXT' for col in df.columns])
    create_table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({columns_with_types});'
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    conn.commit()
    hook.insert_rows(table=table_name, rows=df.values.tolist(), target_fields=list(df.columns), replace=False)
    print(f"Loaded {len(df)} rows into {table_name}.")
